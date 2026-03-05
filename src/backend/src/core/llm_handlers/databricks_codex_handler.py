"""
Databricks Codex Handler

Dedicated LLM handler for gpt-5.3-codex on Databricks, which ONLY supports
the Responses API (/serving-endpoints/responses).

Key differences from the base OpenAICompletion:

1. **Phase preservation** — gpt-5.3-codex emits a ``phase`` field on assistant
   output items (``null``, ``"commentary"``, ``"final_answer"``).  Dropping
   this metadata causes significant performance degradation including early
   stopping where the model returns text instead of calling tools.  This
   handler captures raw output items from each response and injects them
   back into subsequent requests.

2. **Stop-word suppression** — GPT-5 reasoning models reject the ``stop``
   parameter; we override ``supports_stop_words`` to return ``False``.

3. **Debug logging** — every request/response round-trip is logged with
   tool counts, output item types, and phase values so tool-calling issues
   can be diagnosed from crew.log.

Reference: https://developers.openai.com/cookbook/examples/gpt-5/codex_prompting_guide/
"""

from __future__ import annotations

import json
import logging
from typing import Any

from crewai.llms.providers.openai.completion import OpenAICompletion
from crewai.events.types.llm_events import LLMCallType

# Use the "crew" logger so messages appear in crew.log alongside other
# subprocess output (the root logger is set to WARNING in subprocesses).
logger = logging.getLogger("crew")


class DatabricksCodexCompletion(OpenAICompletion):
    """OpenAICompletion subclass tailored for Databricks-hosted gpt-5.3-codex.

    Preserves the ``phase`` field on assistant output items across
    multi-turn conversations so the model does not degrade into early
    text-only responses that skip tool calls.
    """

    def __init__(self, **kwargs: Any) -> None:
        # Force Responses API — codex only works with this endpoint
        kwargs.setdefault("api", "responses")
        super().__init__(**kwargs)

        # Store raw output items (with phase) from the last response.
        # These are injected into the ``input`` array on the next call
        # so the model sees its own prior output with phase metadata intact.
        self._last_output_items: list[dict[str, Any]] = []

        # Track how many tool calls the model has made.  We keep
        # tool_choice='required' until the model has made enough
        # invocations, then switch to 'auto' so it can produce a
        # final text answer.
        #
        # The minimum is computed dynamically from the number of
        # available tools (see _prepare_responses_params) so the
        # model explores a meaningful fraction before it is allowed
        # to stop.  This prevents gpt-5.3-codex from making a
        # single tool call and immediately jumping to final_answer.
        self._tool_call_count: int = 0
        self._min_required_tool_calls: int | None = None  # computed on first call

    # ------------------------------------------------------------------
    # Capability overrides
    # ------------------------------------------------------------------

    def supports_function_calling(self) -> bool:
        """gpt-5.3-codex supports native function calling via Responses API."""
        return True

    def supports_stop_words(self) -> bool:
        """GPT-5 reasoning models reject the 'stop' parameter."""
        return False

    # ------------------------------------------------------------------
    # Phase-aware param preparation
    # ------------------------------------------------------------------

    def _prepare_responses_params(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        response_model: Any | None = None,
    ) -> dict[str, Any]:
        """Build Responses API params, injecting prior output items with phase."""
        params = super()._prepare_responses_params(
            messages=messages, tools=tools, response_model=response_model
        )

        # Sanitise input items for Responses API compatibility.
        # CrewAI's executor builds messages in Chat Completions format
        # (role: assistant/tool with tool_calls), but the Responses API
        # uses a different schema:
        #   - assistant tool calls → function_call items
        #   - tool results (role: "tool") → function_call_output items
        #   - content: null is rejected → use empty string
        #   - id fields max 64 chars
        sanitised_input: list[Any] = []
        for item in params.get("input", []):
            if isinstance(item, dict):
                item = dict(item)  # shallow copy

                # Truncate oversized IDs
                if "id" in item and isinstance(item["id"], str) and len(item["id"]) > 64:
                    item["id"] = item["id"][:64]

                # Convert role:"tool" → function_call_output
                if item.get("role") == "tool":
                    call_id = item.get("tool_call_id", "")
                    output = item.get("content", "")
                    if output is None:
                        output = ""
                    sanitised_input.append({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": str(output),
                    })
                    continue

                # Convert assistant tool_calls → function_call items
                if item.get("role") == "assistant" and "tool_calls" in item:
                    tool_calls = item.get("tool_calls", [])
                    for tc in tool_calls:
                        func = tc.get("function", {})
                        sanitised_input.append({
                            "type": "function_call",
                            "call_id": tc.get("id", ""),
                            "name": func.get("name", ""),
                            "arguments": func.get("arguments", "{}"),
                        })
                    continue

                # Replace null content with empty string
                if "content" in item and item["content"] is None:
                    item["content"] = ""

            sanitised_input.append(item)
        params["input"] = sanitised_input

        # Force tool_choice when tools are present.
        # gpt-5.3-codex tends to skip tool calls and go straight to
        # final_answer phase.  We keep "required" until the model has
        # made enough tool calls to explore the available tools, then
        # switch to "auto" so it can generate a final text answer.
        #
        # The minimum is derived from the tool count:
        #   - At least 1 call per 4 tools (ensures broad exploration)
        #   - Minimum floor of 2 (always try more than one tool)
        #   - Capped at 10 to avoid excessive forced iterations
        # Examples:  4 tools → min 2,  8 tools → min 2,
        #           12 tools → min 3, 19 tools → min 5, 40 → min 10
        if params.get("tools"):
            tool_count = len(params["tools"])
            if self._min_required_tool_calls is None:
                self._min_required_tool_calls = max(2, min(10, tool_count // 4 + 1))
                logger.info(
                    "[DatabricksCodex] Computed min_required_tool_calls=%d "
                    "from %d available tools",
                    self._min_required_tool_calls,
                    tool_count,
                )

            if self._tool_call_count >= self._min_required_tool_calls:
                params["tool_choice"] = "auto"
                logger.info(
                    "[DatabricksCodex] Set tool_choice='auto' "
                    "(tool_call_count=%d >= min=%d)",
                    self._tool_call_count,
                    self._min_required_tool_calls,
                )
            else:
                params["tool_choice"] = "required"
                logger.info(
                    "[DatabricksCodex] Set tool_choice='required' "
                    "(tool_call_count=%d < min=%d)",
                    self._tool_call_count,
                    self._min_required_tool_calls,
                )

        self._log_request_params(params)
        return params

    # ------------------------------------------------------------------
    # Phase-aware response handling
    # ------------------------------------------------------------------

    def _handle_responses(
        self,
        params: dict[str, Any],
        available_functions: dict[str, Any] | None = None,
        from_task: Any | None = None,
        from_agent: Any | None = None,
        response_model: Any | None = None,
    ) -> Any:
        """Handle Responses API call, capturing output items with phase."""
        from openai.types.responses import Response

        try:
            response: Response = self.client.responses.create(**params)

            # Capture raw output items WITH phase for next turn
            self._capture_output_items(response)

            # Track response ID for auto-chaining
            if self.auto_chain and response.id:
                self._last_response_id = response.id

            # Track reasoning items for ZDR auto-chaining
            if self.auto_chain_reasoning:
                reasoning_items = self._extract_reasoning_items(response)
                if reasoning_items:
                    self._last_reasoning_items = reasoning_items

            usage = self._extract_responses_token_usage(response)
            self._track_token_usage_internal(usage)

            self._log_response(response)

            # If parse_tool_outputs is enabled, return structured result
            if self.parse_tool_outputs:
                parsed_result = self._extract_builtin_tool_outputs(response)
                parsed_result.text = self._apply_stop_words(parsed_result.text)
                self._emit_call_completed_event(
                    response=parsed_result.text,
                    call_type=LLMCallType.LLM_CALL,
                    from_task=from_task,
                    from_agent=from_agent,
                    messages=params.get("input", []),
                )
                return parsed_result

            function_calls = self._extract_function_calls_from_response(response)
            if function_calls and not available_functions:
                # Increment tool-call counter; once it reaches the
                # minimum threshold, subsequent requests switch to
                # tool_choice="auto".
                self._tool_call_count += len(function_calls)
                logger.info(
                    "[DatabricksCodex] Tool call count now %d "
                    "(+%d this turn)",
                    self._tool_call_count,
                    len(function_calls),
                )

                # Wrap in OpenAI Chat Completions format so CrewAI's
                # _is_tool_call_list() recognises them (it checks for
                # "function" key).  The Responses API returns {id, name,
                # arguments} but the executor expects {id, function: {name, arguments}}.
                wrapped_calls = [
                    {
                        "id": fc.get("id", ""),
                        "function": {
                            "name": fc.get("name", ""),
                            "arguments": fc.get("arguments", "{}"),
                        },
                    }
                    for fc in function_calls
                ]
                self._emit_call_completed_event(
                    response=wrapped_calls,
                    call_type=LLMCallType.TOOL_CALL,
                    from_task=from_task,
                    from_agent=from_agent,
                    messages=params.get("input", []),
                )
                return wrapped_calls

            if function_calls and available_functions:
                for call in function_calls:
                    function_name = call.get("name", "")
                    function_args = call.get("arguments", {})
                    if isinstance(function_args, str):
                        try:
                            function_args = json.loads(function_args)
                        except json.JSONDecodeError:
                            function_args = {}

                    result = self._handle_tool_execution(
                        function_name=function_name,
                        function_args=function_args,
                        available_functions=available_functions,
                        from_task=from_task,
                        from_agent=from_agent,
                    )
                    if result is not None:
                        return result

            content = response.output_text or ""

            if response_model:
                try:
                    structured_result = self._validate_structured_output(
                        content, response_model
                    )
                    self._emit_call_completed_event(
                        response=structured_result,
                        call_type=LLMCallType.LLM_CALL,
                        from_task=from_task,
                        from_agent=from_agent,
                        messages=params.get("input", []),
                    )
                    return structured_result
                except ValueError as e:
                    logging.warning(f"Structured output validation failed: {e}")

            content = self._apply_stop_words(content)

            self._emit_call_completed_event(
                response=content,
                call_type=LLMCallType.LLM_CALL,
                from_task=from_task,
                from_agent=from_agent,
                messages=params.get("input", []),
            )

            return content

        except Exception as e:
            logger.error("[DatabricksCodex] API error: %s", str(e)[:300])
            # LLMCallFailedEvent.error expects a string, not an Exception
            self._emit_call_failed_event(
                error=str(e),
                from_task=from_task,
                from_agent=from_agent,
            )
            raise

    # ------------------------------------------------------------------
    # Output-item capture (phase preservation)
    # ------------------------------------------------------------------

    def _capture_output_items(self, response: Any) -> None:
        """Extract output items from the response, preserving phase metadata.

        The Responses API returns output items like::

            {
                "type": "message",
                "role": "assistant",
                "content": [...],
                "phase": "commentary"   # <-- must be preserved
            }

        We serialise each item to a dict so it can be injected back into
        the ``input`` array on the next request.
        """
        items: list[dict[str, Any]] = []
        if not hasattr(response, "output") or not response.output:
            self._last_output_items = items
            return

        for item in response.output:
            try:
                if hasattr(item, "model_dump"):
                    item_dict = item.model_dump(exclude_none=False)
                elif hasattr(item, "to_dict"):
                    item_dict = item.to_dict()
                elif isinstance(item, dict):
                    item_dict = item
                else:
                    # Fallback: try to convert to dict
                    item_dict = dict(item) if hasattr(item, "__iter__") else {"type": str(type(item).__name__)}

                # The Responses API enforces a 64-char max on input[].id.
                # Output items may carry longer IDs (e.g. response IDs);
                # truncate them to avoid BAD_REQUEST errors on re-injection.
                if "id" in item_dict and isinstance(item_dict["id"], str) and len(item_dict["id"]) > 64:
                    item_dict["id"] = item_dict["id"][:64]

                items.append(item_dict)
            except Exception:
                logger.debug("[DatabricksCodex] Could not serialise output item: %s", type(item).__name__)

        self._last_output_items = items

        phases = [it.get("phase") for it in items if it.get("phase")]
        if phases:
            logger.debug("[DatabricksCodex] Captured %d output items, phases: %s", len(items), phases)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _log_request_params(self, params: dict[str, Any]) -> None:
        """Log outgoing request details for debugging tool-calling issues."""
        tool_count = len(params.get("tools", []))
        input_count = len(params.get("input", []))
        has_instructions = bool(params.get("instructions"))
        tool_choice = params.get("tool_choice", "not set")
        tool_names = [t.get("name", "?") for t in params.get("tools", []) if isinstance(t, dict)]

        logger.info(
            "[DatabricksCodex] Responses API request: model=%s, input_items=%d, "
            "tools=%d%s, tool_choice=%s, has_instructions=%s",
            params.get("model", "?"),
            input_count,
            tool_count,
            f" ({', '.join(tool_names)})" if tool_names else "",
            tool_choice,
            has_instructions,
        )

        # Log full tool schemas on first call for debugging format issues
        if params.get("tools"):
            for i, tool in enumerate(params["tools"]):
                logger.info(
                    "[DatabricksCodex] Tool[%d] schema: %s",
                    i,
                    json.dumps(tool, default=str)[:500],
                )

    def _log_response(self, response: Any) -> None:
        """Log response details for debugging."""
        output_types = []
        phases = []
        if hasattr(response, "output") and response.output:
            for item in response.output:
                item_type = getattr(item, "type", "unknown")
                output_types.append(item_type)
                phase = getattr(item, "phase", None)
                if phase:
                    phases.append(phase)

        function_calls = self._extract_function_calls_from_response(response)
        text_len = len(response.output_text or "") if hasattr(response, "output_text") else 0

        logger.info(
            "[DatabricksCodex] Responses API response: output_items=%s, "
            "function_calls=%d, text_len=%d, phases=%s, status=%s",
            output_types,
            len(function_calls),
            text_len,
            phases or "none",
            getattr(response, "status", "unknown"),
        )

        # Log first 200 chars of text output for quick debugging
        if text_len > 0 and text_len < 500:
            logger.info(
                "[DatabricksCodex] Response text: %s",
                (response.output_text or "")[:200],
            )

        # Log function call details if any
        for i, fc in enumerate(function_calls):
            logger.info(
                "[DatabricksCodex] Function call[%d]: name=%s, args=%s",
                i,
                fc.get("name", "?"),
                str(fc.get("arguments", ""))[:200],
            )
