"""
Databricks Responses API

The LLM for Databricks-served models that speak the Responses API rather than
chat completions. gpt-5.3-codex is the only such model today and every quirk
below was found on it, but the class is named for the API because that is what
decides which models land here.

The Responses API base path differs from chat/embeddings:
- AI Gateway on:  /ai-gateway/openai/v1  (→ /ai-gateway/openai/v1/responses)
- AI Gateway off: /serving-endpoints     (→ /serving-endpoints/responses)
The correct base_url is computed by DatabricksURLUtils.construct_responses_base_url()
in llm_manager.configure_kasal_llm — do NOT pass the chat base (/ai-gateway/mlflow/v1),
which has no /responses route and returns 404 "Supervisor API is not enabled".

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

import hashlib
import json
import logging
from typing import Any

from src.core.events import LLMCallType
from src.core.llm.transport import OpenAICompletion

# Use the "crew" logger so messages appear in crew.log alongside other
# subprocess output (the root logger is set to WARNING in subprocesses).
logger = logging.getLogger("crew")


class DatabricksResponsesLLM(OpenAICompletion):
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

    # ------------------------------------------------------------------
    # Capability overrides
    # ------------------------------------------------------------------

    def supports_function_calling(self) -> bool:
        """gpt-5.3-codex supports native function calling via Responses API."""
        return True

    def supports_native_structured_output(self) -> bool:
        """The Responses API validates output_pydantic directly (see
        _handle_responses → _validate_structured_output), so a task's
        output_pydantic is passed to the model as ``text.format`` and enforced,
        returning a typed object. Signals the converter selection NOT to
        downgrade this model to the soft output_json prompt, which would set
        ``output_json = True`` (a bool CrewAI can't validate) and silently drop
        fields the model omits — breaking routers that branch on those fields."""
        return True

    def supports_stop_words(self) -> bool:
        """GPT-5 reasoning models reject the 'stop' parameter."""
        return False

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def call(
        self,
        messages: str | list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        callbacks: list[Any] | None = None,
        available_functions: dict[str, Any] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
        response_model: Any | None = None,
    ) -> Any:
        """Drive the Responses API through this handler's own ``_handle_responses``.

        The base ``OpenAICompletion.call`` builds the response first and passes a
        ``Response`` into ``_handle_responses``; ours instead takes the request
        PARAMS — it owns response creation (with caching), phase capture, tool
        execution and event emission. Bridge the two here so the codex path uses
        its own handler. Without this override the base passes a ``Response``
        where ``_handle_responses`` expects params, and ``create(**response)``
        raises "argument after ** must be a mapping, not Response".
        """
        conversation = self._normalize_messages(messages)
        self._emit_call_started_event(conversation, tools, from_task, from_agent)
        params = self._prepare_responses_params(
            conversation, tools, response_model=response_model
        )
        return self._handle_responses(
            params,
            available_functions=available_functions,
            from_task=from_task,
            from_agent=from_agent,
            response_model=response_model,
        )

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

        # Cap the output budget. model_configs carries the model's CAPABILITY
        # (max_output_tokens=128000), which used to flow into every request —
        # ~30x the largest response ever observed (p99 well under 4k tokens)
        # and an open invitation for a runaway generation to bill 128k output
        # tokens. Override via KASAL_CODEX_MAX_OUTPUT_TOKENS when a workload
        # genuinely needs more.
        import os as _os

        cap = int(_os.environ.get("KASAL_CODEX_MAX_OUTPUT_TOKENS", "16000"))
        current = params.get("max_output_tokens")
        if current is None:
            explicit = getattr(self, "max_completion_tokens", None) or getattr(
                self, "max_tokens", None
            )
            params["max_output_tokens"] = min(int(explicit), cap) if explicit else cap
        elif int(current) > cap:
            params["max_output_tokens"] = cap

        # Sanitise input items for Responses API compatibility.
        # CrewAI's executor builds messages in Chat Completions format
        # (role: assistant/tool with tool_calls), but the Responses API
        # uses a different schema:
        #   - assistant tool calls → function_call items
        #   - tool results (role: "tool") → function_call_output items
        #   - content: null is rejected → use empty string
        #   - id fields max 64 chars
        sanitised_input: list[Any] = []
        # A tool call with a blank function name cannot be sent: the Responses
        # API requires input[].name (min length 1) on a function_call. Track any
        # we drop so their matching function_call_output is dropped too — an
        # orphaned output ("No tool call found for call_id …") is just a
        # different 400. With the delegated-call shape now matching the transport
        # (see the return in _handle_responses), a blank name should not arise;
        # this is the belt-and-suspenders that stops one from failing the run.
        dropped_call_ids: set[str] = set()
        for item in params.get("input", []):
            if isinstance(item, dict):
                item = dict(item)  # shallow copy

                # CrewAI's executor stamps a top-level ``cache_breakpoint`` flag on
                # messages for prompt caching (crewai.llms.cache.mark_cache_breakpoint).
                # Only Claude's native caching understands it; the Responses API
                # rejects it with 400 "Unknown parameter: 'input[N].cache_breakpoint'".
                # Strip it here — the sibling handlers (databricks_retry_llm and the
                # exported-app databricks_llm) do the same for chat completions.
                item.pop("cache_breakpoint", None)

                # Drop an empty ``name``. CrewAI can emit a message with
                # ``name=""``; the Responses API rejects it with 400
                # "Invalid 'input[N].name': empty string. Expected a string with
                # minimum length 1". The field is optional, so omit it when blank.
                if "name" in item and not item["name"]:
                    item.pop("name", None)

                # Truncate oversized IDs
                if (
                    "id" in item
                    and isinstance(item["id"], str)
                    and len(item["id"]) > 64
                ):
                    item["id"] = item["id"][:64]

                # Convert role:"tool" → function_call_output
                if item.get("role") == "tool":
                    call_id = item.get("tool_call_id", "")
                    if call_id in dropped_call_ids:
                        # Its function_call was dropped for a blank name; a
                        # function_call_output with no matching call is a 400.
                        continue
                    output = item.get("content", "")
                    if output is None:
                        output = ""
                    sanitised_input.append(
                        {
                            "type": "function_call_output",
                            "call_id": call_id,
                            "output": str(output),
                        }
                    )
                    continue

                # Convert assistant tool_calls → function_call items
                if item.get("role") == "assistant" and "tool_calls" in item:
                    tool_calls = item.get("tool_calls", [])
                    for tc in tool_calls:
                        func = tc.get("function", {})
                        name = func.get("name", "")
                        if not name:
                            # Cannot send a function_call with a blank name;
                            # drop it and remember to drop its output too.
                            dropped_call_ids.add(tc.get("id", ""))
                            continue
                        sanitised_input.append(
                            {
                                "type": "function_call",
                                "call_id": tc.get("id", ""),
                                "name": name,
                                "arguments": func.get("arguments", "{}"),
                            }
                        )
                    continue

                # Replace null content with empty string
                if "content" in item and item["content"] is None:
                    item["content"] = ""

            sanitised_input.append(item)
        params["input"] = sanitised_input

        # tool_choice is deliberately NOT set here. This handler used to keep
        # "required" until a tool-call counter passed
        # max(2, min(10, tool_count // 4 + 1)) — a floor of two forced calls,
        # scaling UP with the number of attached tools. The phase preservation
        # above addresses the symptom that rule was written for (assistant output
        # losing `phase` degrades into early text-only turns), and forcing on top
        # of it made a greeting call tools twice before it could answer. See
        # handlers/__init__.py for why no handler decides this.
        self._log_request_params(params)
        return params

    # ------------------------------------------------------------------
    # Phase-aware response handling
    # ------------------------------------------------------------------

    @staticmethod
    def _responses_cache_key(params: dict[str, Any]) -> str:
        """Stable cache key for a Responses API request.

        litellm's own ``get_cache_key`` only hashes its known chat params (model,
        messages, …) and would drop the Responses-API fields (``input``,
        ``instructions``, ``tools``), collapsing every codex request onto the same
        key. So hash the full request payload ourselves. ``default=str`` tolerates
        any non-JSON-serializable values in the params.
        """
        payload = json.dumps(params, sort_keys=True, default=str)
        return "codex-responses:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _cached_responses_create(self, params: dict[str, Any]) -> Any:
        """Call the Responses API, served from litellm's cache when possible.

        gpt-5.3-codex bypasses ``litellm.completion`` (it uses the OpenAI Responses
        API directly), so litellm's response cache never sees these calls. Reuse
        the configured ``litellm.cache`` object as a plain key/value store —
        honoring its enabled / TTL / disk settings — so identical codex requests
        are served instantly on repeat. Best-effort: any cache error falls through
        to a live request. Codex requests are stateless (the full ``input`` is sent
        each turn; no ``previous_response_id``), so a cached response is safe to
        replay.
        """
        import litellm
        from openai.types.responses import Response

        # Reset per call: _handle_responses reads this to decide whether the
        # response's token usage represents real API spend (cache replays cost
        # zero tokens and must not inflate the crew's total_tokens aggregate).
        self._last_response_from_cache = False

        cache = getattr(litellm, "cache", None)
        cache_key = None
        if cache is not None:
            try:
                cache_key = self._responses_cache_key(params)
                cached = cache.get_cache(cache_key=cache_key)
                if cached is not None:
                    logger.info("[DatabricksCodex] Responses cache HIT")
                    self._last_response_from_cache = True
                    # Reconstruct with the OpenAI SDK's lenient ``construct`` — the
                    # SAME path the live client uses to parse responses. Strict
                    # ``model_validate`` rejects Databricks-specific values the live
                    # parser accepts (e.g. ``prompt_cache_retention='in_memory'``,
                    # which is not one of the SDK's ``'in-memory'``/``'24h'``
                    # literals), which would make every cache hit raise and fall
                    # through to a live call — defeating the cache entirely.
                    return Response.construct(**cached)
            except Exception as exc:  # noqa: BLE001 — cache is best-effort
                logger.debug("[DatabricksCodex] cache read skipped: %s", exc)
                cache_key = None

        # CrewAI 1.14+ exposes the lazy _get_sync_client(); 1.9.x builds
        # self.client eagerly in __init__ — support both.
        getter = getattr(self, "_get_sync_client", None)
        client = getter() if callable(getter) else self.client
        response = client.responses.create(**params)

        if cache is not None and cache_key is not None:
            try:
                cache.add_cache(response.model_dump(), cache_key=cache_key)
            except Exception as exc:  # noqa: BLE001 — cache is best-effort
                logger.debug("[DatabricksCodex] cache write skipped: %s", exc)
        return response

    def _handle_responses(
        self,
        params: dict[str, Any],
        available_functions: dict[str, Any] | None = None,
        from_task: Any | None = None,
        from_agent: Any | None = None,
        response_model: Any | None = None,
    ) -> Any:
        """Drive the Responses API, running the whole tool-call loop in one call.

        The Kasal runtime's contract (``runtime/executor.py``: "the transport
        runs the whole tool-call loop inside one ``call``") means that when
        ``available_functions`` is given, THIS method must execute the tools,
        feed the results back, and keep going until the model answers — exactly
        what the base ``OpenAICompletion._call_responses_api`` does.

        It used to be single-shot: it ran the FIRST tool call and returned that
        tool's raw output as the answer. So an agent that opened with a
        ``todo``/plan call (or any parallel calls) never got a turn to act on the
        plan — the MCP search it queued next never ran, and the task "completed"
        with a 0/N plan. Now it loops, round-bounded by the execution budget.

        The endpoint is stateless (no ``previous_response_id``), so each
        follow-up round resends the model's own ``function_call`` items next to
        their ``function_call_output``s — see ``_run_tool_round``.

        When ``available_functions`` is absent the model's decision is handed
        back to a caller that owns its own loop (the CrewAI executor); that path
        returns after the first response, unchanged.
        """
        from openai.types.responses import Response  # noqa: F401 (parity import)

        # Round cap + wall-clock deadline for the tool loop. getattr-guarded so
        # the isolated unit test's minimal fake base (no _execution_budget) still
        # runs; the real base always provides both.
        budget = getattr(self, "_execution_budget", None)
        rounds, deadline = budget(from_agent) if callable(budget) else (10, None)

        usage_for_event: dict[str, Any] | None = None
        try:
            for _round in range(max(1, rounds)):
                check_deadline = getattr(self, "_check_deadline", None)
                if callable(check_deadline):
                    check_deadline(deadline, _round, params.get("input"))

                # CrewAI 1.14+ moved the OpenAI client to a private attr; the
                # cached create uses the lazy getter. Served from litellm's cache
                # when an identical request is seen again (codex bypasses
                # litellm.completion, so we reuse the cache object directly).
                response: Response = self._cached_responses_create(params)

                # Capture raw output items WITH phase for next turn
                self._capture_output_items(response)

                if self.auto_chain and response.id:
                    self._last_response_id = response.id
                if self.auto_chain_reasoning:
                    reasoning_items = self._extract_reasoning_items(response)
                    if reasoning_items:
                        self._last_reasoning_items = reasoning_items

                usage = self._extract_responses_token_usage(response)
                if getattr(self, "_last_response_from_cache", False):
                    # Cache replay: the original usage is embedded in the cached
                    # payload but no API tokens were spent — counting it would
                    # overstate crew total_tokens by roughly the cache hit rate.
                    logger.debug(
                        "[DatabricksCodex] cache hit — token usage not counted"
                    )
                    usage_for_event = None
                else:
                    self._track_token_usage_internal(usage)
                    # Surface per-call usage on the event bus
                    # (LLMCallCompletedEvent carries it to the OTel bridge →
                    # execution_trace) and in the logs — this path bypasses
                    # litellm, so without this the codex path records zero token
                    # usage anywhere.
                    usage_for_event = usage
                    if usage:
                        logger.info(
                            "[DatabricksCodex] usage: prompt=%s completion=%s "
                            "total=%s",
                            usage.get("prompt_tokens"),
                            usage.get("completion_tokens"),
                            usage.get("total_tokens"),
                        )

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
                        usage=usage_for_event,
                    )
                    return parsed_result

                function_calls = self._extract_function_calls_from_response(response)

                if function_calls and not available_functions:
                    # The caller owns the tool loop and wants only the DECISION.
                    # Hand back the transport's normalized shape — flat
                    # {id, name, arguments}, exactly what base OpenAICompletion
                    # returns from its delegated path. The sole consumer,
                    # _as_crewai_tool_calls (harnesses/crewai/llm.py), reads these
                    # keys at the TOP LEVEL. Nesting name/arguments under
                    # "function" (the Chat Completions shape) made it read both as
                    # absent, so every codex tool call became a nameless
                    # _ToolCall and the next request 400'd on the empty
                    # function_call name.
                    delegated_calls = [
                        {
                            "id": fc.get("id", ""),
                            "name": fc.get("name", ""),
                            "arguments": fc.get("arguments", "{}"),
                        }
                        for fc in function_calls
                    ]
                    self._emit_call_completed_event(
                        response=delegated_calls,
                        call_type=LLMCallType.TOOL_CALL,
                        from_task=from_task,
                        from_agent=from_agent,
                        messages=params.get("input", []),
                        usage=usage_for_event,
                    )
                    return delegated_calls

                if function_calls and available_functions:
                    answer = self._run_tool_round(
                        params, function_calls, available_functions
                    )
                    if answer is not None:
                        # A result_as_answer tool: its output IS the answer.
                        answer = self._apply_stop_words(answer)
                        self._emit_call_completed_event(
                            response=answer,
                            call_type=LLMCallType.TOOL_CALL,
                            from_task=from_task,
                            from_agent=from_agent,
                            messages=params.get("input", []),
                            usage=usage_for_event,
                        )
                        return answer
                    # Tools ran; their outputs are now in params["input"]. Loop
                    # so the model can read them and decide the next step.
                    continue

                # No tool calls: this response is the answer.
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
                            usage=usage_for_event,
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
                    usage=usage_for_event,
                )

                return content

            # Round budget exhausted while still calling tools. One final,
            # tool-less request so the model answers with what it gathered rather
            # than the run ending on a bare tool result.
            params.pop("tools", None)
            response = self._cached_responses_create(params)
            self._capture_output_items(response)
            if not getattr(self, "_last_response_from_cache", False):
                usage_for_event = self._extract_responses_token_usage(response)
                self._track_token_usage_internal(usage_for_event)
            content = self._apply_stop_words(response.output_text or "")
            self._emit_call_completed_event(
                response=content,
                call_type=LLMCallType.LLM_CALL,
                from_task=from_task,
                from_agent=from_agent,
                messages=params.get("input", []),
                usage=usage_for_event,
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

    def _run_tool_round(
        self,
        params: dict[str, Any],
        function_calls: list[dict[str, Any]],
        available_functions: dict[str, Any],
    ) -> str | None:
        """Execute EVERY tool call in one round; append the calls + outputs.

        Returns a string only when a tool is marked ``result_as_answer`` — its
        output is the final answer and the loop should stop. Otherwise returns
        None, meaning the outputs were appended to ``params["input"]`` and the
        caller should re-query the model with them.

        Both the model's ``function_call`` item and its ``function_call_output``
        are appended for every call, because the endpoint is stateless: an output
        whose call is absent from ``input`` is rejected with a 400 ("No tool call
        found for call_id ..."). This is also where the old single-shot bug lived
        — it ran only ``function_calls[0]`` and returned; here every call runs.
        """
        conversation = params.setdefault("input", [])
        for call in function_calls:
            call_id = call.get("id", "")
            name = call.get("name", "")
            raw_args = call.get("arguments", "{}")

            # Resend the call so its output has a matching function_call.
            conversation.append(
                {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": (
                        raw_args if isinstance(raw_args, str) else json.dumps(raw_args)
                    ),
                }
            )

            args: Any = raw_args
            if isinstance(args, str):
                try:
                    args = json.loads(args) if args.strip() else {}
                except json.JSONDecodeError:
                    args = {}

            result = self._handle_tool_execution(name, args, available_functions)
            if result is None:
                # Unknown tool. Every sent call needs an output (a missing one is
                # a 400), so report the error back rather than dropping it.
                result = f"Error: tool {name!r} is not available."

            conversation.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": str(result),
                }
            )

            fn = available_functions.get(name)
            if fn is not None and getattr(fn, "result_as_answer", False):
                return str(result)
        return None

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
                    item_dict = (
                        dict(item)
                        if hasattr(item, "__iter__")
                        else {"type": str(type(item).__name__)}
                    )

                # The Responses API enforces a 64-char max on input[].id.
                # Output items may carry longer IDs (e.g. response IDs);
                # truncate them to avoid BAD_REQUEST errors on re-injection.
                if (
                    "id" in item_dict
                    and isinstance(item_dict["id"], str)
                    and len(item_dict["id"]) > 64
                ):
                    item_dict["id"] = item_dict["id"][:64]

                items.append(item_dict)
            except Exception:
                logger.debug(
                    "[DatabricksCodex] Could not serialise output item: %s",
                    type(item).__name__,
                )

        self._last_output_items = items

        phases = [it.get("phase") for it in items if it.get("phase")]
        if phases:
            logger.debug(
                "[DatabricksCodex] Captured %d output items, phases: %s",
                len(items),
                phases,
            )

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _log_request_params(self, params: dict[str, Any]) -> None:
        """Log outgoing request details for debugging tool-calling issues."""
        tool_count = len(params.get("tools", []))
        input_count = len(params.get("input", []))
        has_instructions = bool(params.get("instructions"))
        tool_choice = params.get("tool_choice", "not set")
        tool_names = [
            t.get("name", "?") for t in params.get("tools", []) if isinstance(t, dict)
        ]

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

        # Log full tool schemas ONCE per handler instance for debugging format
        # issues. Previously this fired on EVERY request (the request line
        # above already carries the tool names) — up to 500 chars per tool per
        # iteration, re-logged even on cache hits.
        if params.get("tools") and not getattr(self, "_tool_schemas_logged", False):
            self._tool_schemas_logged = True
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
        text_len = (
            len(response.output_text or "") if hasattr(response, "output_text") else 0
        )

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
