"""OpenAICompletion — OpenAI-compatible provider (Chat Completions + Responses).

Authored module; surface validated against the kasal_engine datamodel
(the 33 kasal-required members of crewAI 1.15.5's 84; kasal's
DatabricksCodexCompletion subclasses this and overrides the Responses
plumbing). Uses the ``openai`` SDK — an optional dependency; a clear
ImportError is raised on first use if it is missing.
"""

import logging
import os
import time
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, PrivateAttr

from ..events.bus import crewai_event_bus
from ..events.types import LLMCallType, LLMStreamChunkEvent
from .base import BaseLLM
from .exceptions import (
    ExecutionBudgetExceededError,
    LLMContextLengthExceededError,
    is_context_length_exceeded,
)

logger = logging.getLogger(__name__)

_MAX_TOOL_ROUNDS = 15


class OpenAICompletion(BaseLLM):
    llm_type: Literal["openai"] = "openai"

    model: str = "gpt-4o"
    timeout: float | None = None
    max_retries: int = 2
    top_p: float | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None
    max_tokens: int | None = None
    max_completion_tokens: int | None = None
    stream: bool = False
    response_format: Any | None = None
    reasoning_effort: str | None = None
    api: Literal["completions", "responses"] = "completions"
    instructions: str | None = None
    store: bool | None = None
    parse_tool_outputs: bool = False
    auto_chain: bool = False
    auto_chain_reasoning: bool = False
    api_base: str | None = None

    _client: Any = PrivateAttr(default=None)
    _last_response_id: str | None = PrivateAttr(default=None)
    _last_reasoning_items: list[Any] | None = PrivateAttr(default=None)

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError(
                    "kasal_engine.llm.OpenAICompletion requires the 'openai' "
                    "package: pip install openai"
                ) from e
            self._client = OpenAI(
                api_key=self.api_key or os.environ.get("OPENAI_API_KEY"),
                base_url=self.base_url or self.api_base,
                timeout=self.timeout,
                max_retries=self.max_retries,
            )
        return self._client

    def supports_native_structured_output(self) -> bool:
        return True

    def supports_function_calling(self) -> bool:
        return True

    def supports_stop_words(self) -> bool:
        model = self.model.lower()
        if "gpt-5" in model:
            return False
        return not model.startswith(("o1", "o3", "o4"))

    async def acall(
        self,
        messages: str | list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        callbacks: list[Any] | None = None,
        available_functions: dict[str, Callable[..., Any]] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
        response_model: type[BaseModel] | None = None,
    ) -> str:
        import asyncio

        return await asyncio.to_thread(
            self.call, messages, tools, callbacks, available_functions,
            from_task, from_agent, response_model,
        )

    # ------------------------------- call -------------------------------

    def call(
        self,
        messages: str | list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        callbacks: list[Any] | None = None,
        available_functions: dict[str, Callable[..., Any]] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
        response_model: type[BaseModel] | None = None,
    ) -> str:
        conversation = self._normalize_messages(messages)
        self._emit_call_started_event(conversation, tools, from_task, from_agent)
        try:
            if self.api == "responses":
                text, usage, call_type = self._call_responses_api(
                    conversation, tools, available_functions, from_agent=from_agent
                )
            else:
                text, usage, call_type = self._call_completions_api(
                    conversation, tools, available_functions, from_agent=from_agent
                )
        except LLMContextLengthExceededError:
            raise
        except Exception as e:
            self._emit_call_failed_event(str(e), from_task, from_agent)
            if is_context_length_exceeded(e):
                raise LLMContextLengthExceededError(str(e)) from e
            raise

        if self.supports_stop_words():
            text = self._apply_stop_words(text)
        self._emit_call_completed_event(
            text, call_type, usage, conversation, from_task, from_agent
        )
        return text

    # --------------------------- chat completions ---------------------------

    def _prepare_completion_params(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        skip_file_processing: bool = False,
    ) -> dict[str, Any]:
        # skip_file_processing: crewAI 1.14.5 signature compatibility — kasal's
        # LLM subclasses pass it through; the engine has no file-input
        # processing path, so it is accepted and inert.
        params: dict[str, Any] = {"model": self.model, "messages": messages}
        for key, value in (
            ("temperature", self.temperature),
            ("top_p", self.top_p),
            ("frequency_penalty", self.frequency_penalty),
            ("presence_penalty", self.presence_penalty),
            ("reasoning_effort", self.reasoning_effort),
            ("stop", self.stop if self.supports_stop_words() else None),
            ("tools", tools),
        ):
            if value is not None:
                params[key] = value
        if self.max_completion_tokens is not None:
            params["max_completion_tokens"] = self.max_completion_tokens
        elif self.max_tokens is not None:
            params["max_tokens"] = self.max_tokens
        if isinstance(self.response_format, type) and issubclass(
            self.response_format, BaseModel
        ):
            params["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": self.response_format.__name__,
                    "schema": self.response_format.model_json_schema(),
                },
            }
        elif self.response_format is not None:
            params["response_format"] = self.response_format
        params.update(self.additional_params)
        return params

    def _execution_budget(self, from_agent: Any) -> tuple[int, float | None]:
        """Resolve (max tool rounds, wall-clock deadline) for one call().

        Agent.max_iter and Agent.max_execution_time were accepted-but-inert
        fields (crewAI never enforced them either); here they become real.
        Direct LLM calls with no agent keep the engine default round cap.
        """
        rounds = _MAX_TOOL_ROUNDS
        deadline: float | None = None
        if from_agent is not None:
            max_iter = getattr(from_agent, "max_iter", None)
            if isinstance(max_iter, int) and max_iter > 0:
                rounds = max_iter
            max_seconds = getattr(from_agent, "max_execution_time", None)
            if isinstance(max_seconds, (int, float)) and max_seconds > 0:
                deadline = time.monotonic() + float(max_seconds)
        return rounds, deadline

    def _check_deadline(self, deadline: float | None, rounds_done: int) -> None:
        if deadline is not None and time.monotonic() >= deadline:
            raise ExecutionBudgetExceededError(
                f"max_execution_time exceeded after {rounds_done} tool round(s) "
                f"for model {self.model}."
            )

    def _trim_conversation_to_window(
        self, conversation: list[dict[str, Any]], from_agent: Any = None
    ) -> None:
        """Best-effort in-place trim so a tool-heavy turn cannot overflow the
        context window (which previously failed the whole run): once the
        estimated size (chars/4 ≈ tokens) approaches the 0.85-derated window,
        the OLDEST tool results are replaced with a stub — never the system
        prompt, user messages, or tool_call structure (pairing must survive).
        Honors Agent.respect_context_window (default on; previously inert).
        """
        if from_agent is not None and getattr(from_agent, "respect_context_window", True) is False:
            return
        window = self.get_context_window_size()
        if not window:
            return

        def estimated_tokens() -> int:
            total = 0
            for message in conversation:
                for key in ("content", "output"):
                    value = message.get(key)
                    if isinstance(value, str):
                        total += len(value)
                if message.get("tool_calls"):
                    total += len(str(message["tool_calls"]))
            return total // 4

        if estimated_tokens() <= window:
            return
        stub = "[earlier tool result trimmed to fit the context window]"
        for message in conversation:
            is_tool_result = (
                message.get("role") == "tool"
                or message.get("type") == "function_call_output"
            )
            if not is_tool_result:
                continue
            key = "content" if message.get("role") == "tool" else "output"
            if message.get(key) == stub:
                continue
            message[key] = stub
            if estimated_tokens() <= window:
                return

    def _call_completions_api(
        self,
        conversation: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        available_functions: dict[str, Callable[..., Any]] | None,
        from_agent: Any = None,
    ) -> tuple[str, dict[str, Any] | None, LLMCallType]:
        call_type = LLMCallType.LLM_CALL
        usage: dict[str, Any] | None = None
        rounds, deadline = self._execution_budget(from_agent)
        for _round in range(rounds):
            self._check_deadline(deadline, _round)
            self._trim_conversation_to_window(conversation, from_agent)
            params = self._prepare_completion_params(conversation, tools)
            if self.stream:
                content, usage, function_calls = self._stream_chat_completion(params)
            else:
                response = self.client.chat.completions.create(**params)
                usage = self._extract_chat_token_usage(response)
                self._track_token_usage_internal(usage)
                function_calls = self._extract_function_calls_from_response(response)
                content = response.choices[0].message.content
            if function_calls and available_functions:
                call_type = LLMCallType.TOOL_CALL
                conversation.append(
                    {
                        "role": "assistant",
                        "content": content,
                        "tool_calls": [
                            {
                                "id": fc["id"],
                                "type": "function",
                                "function": {
                                    "name": fc["name"],
                                    "arguments": fc["arguments"],
                                },
                            }
                            for fc in function_calls
                        ],
                    }
                )
                for fc in function_calls:
                    result = self._handle_tool_execution(
                        fc["name"], fc["arguments"], available_functions
                    )
                    conversation.append(
                        {
                            "role": "tool",
                            "tool_call_id": fc["id"],
                            "content": result if result is not None else "Tool not found.",
                        }
                    )
                continue
            return content or "", usage, call_type
        raise ExecutionBudgetExceededError(
            f"Tool-calling did not converge within {rounds} rounds "
            f"for model {self.model}."
        )

    def _stream_chat_completion(
        self, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
        """One streamed chat completion: emits LLMStreamChunkEvent per text
        delta, accumulates tool-call deltas, returns (text, usage, calls)."""
        params = {**params, "stream": True, "stream_options": {"include_usage": True}}
        try:
            response_stream = self.client.chat.completions.create(**params)
        except Exception as e:
            # Some OpenAI-compatible servers reject stream_options; retry
            # without it (usage is then unavailable for this call).
            if "stream_options" not in str(e):
                raise
            params.pop("stream_options", None)
            response_stream = self.client.chat.completions.create(**params)

        chunks: list[str] = []
        calls_by_index: dict[int, dict[str, Any]] = {}
        usage: dict[str, Any] | None = None
        chunk_index = 0
        for part in response_stream:
            if getattr(part, "usage", None) is not None:
                usage = self._extract_chat_token_usage(part)
            choices = getattr(part, "choices", None)
            if not choices:
                continue
            delta = choices[0].delta
            text = getattr(delta, "content", None)
            if text:
                chunks.append(text)
                crewai_event_bus.emit(
                    self,
                    LLMStreamChunkEvent(
                        model=self.model, chunk=text, chunk_index=chunk_index
                    ),
                )
                chunk_index += 1
            for tc in getattr(delta, "tool_calls", None) or []:
                slot = calls_by_index.setdefault(
                    tc.index, {"id": None, "name": "", "arguments": ""}
                )
                if getattr(tc, "id", None):
                    slot["id"] = tc.id
                function = getattr(tc, "function", None)
                if function is not None:
                    if getattr(function, "name", None):
                        slot["name"] = function.name
                    if getattr(function, "arguments", None):
                        slot["arguments"] += function.arguments

        if usage:
            self._track_token_usage_internal(usage)
        function_calls = [
            {
                "id": slot["id"] or f"call_{index}",
                "name": slot["name"],
                "arguments": slot["arguments"],
            }
            for index, slot in sorted(calls_by_index.items())
            if slot["name"]
        ]
        return "".join(chunks), usage, function_calls

    def _extract_chat_token_usage(self, response: Any) -> dict[str, Any] | None:
        usage = getattr(response, "usage", None)
        if usage is None:
            return None
        details = getattr(usage, "prompt_tokens_details", None)
        return {
            "prompt_tokens": getattr(usage, "prompt_tokens", 0),
            "completion_tokens": getattr(usage, "completion_tokens", 0),
            "total_tokens": getattr(usage, "total_tokens", 0),
            "cached_prompt_tokens": getattr(details, "cached_tokens", 0) if details else 0,
        }

    def _extract_function_calls_from_response(self, response: Any) -> list[dict[str, Any]]:
        """Normalized tool calls from a chat completion or a Responses object."""
        calls: list[dict[str, Any]] = []
        choices = getattr(response, "choices", None)
        if choices:
            tool_calls = getattr(choices[0].message, "tool_calls", None) or []
            for tc in tool_calls:
                function = getattr(tc, "function", None)
                if function is not None:
                    calls.append(
                        {
                            "id": tc.id,
                            "name": function.name,
                            "arguments": function.arguments,
                        }
                    )
            return calls
        for item in getattr(response, "output", None) or []:
            if getattr(item, "type", None) == "function_call":
                calls.append(
                    {
                        "id": getattr(item, "call_id", None) or getattr(item, "id", None),
                        "name": item.name,
                        "arguments": item.arguments,
                    }
                )
        return calls

    # ----------------------------- responses api -----------------------------

    def _prepare_responses_params(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        response_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"model": self.model, "input": messages}
        if self.instructions is not None:
            params["instructions"] = self.instructions
        if self.store is not None:
            params["store"] = self.store
        if self.auto_chain and self._last_response_id:
            params["previous_response_id"] = self._last_response_id
        if self.max_completion_tokens or self.max_tokens:
            params["max_output_tokens"] = self.max_completion_tokens or self.max_tokens
        if self.temperature is not None:
            params["temperature"] = self.temperature
        if self.reasoning_effort is not None:
            params["reasoning"] = {"effort": self.reasoning_effort}
        if tools:
            params["tools"] = [
                {
                    "type": "function",
                    "name": t["function"]["name"],
                    "description": t["function"].get("description", ""),
                    "parameters": t["function"].get("parameters", {}),
                }
                if t.get("type") == "function" and "function" in t
                else t
                for t in tools
            ]
        params.update(self.additional_params)
        return params

    def _call_responses_api(
        self,
        conversation: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        available_functions: dict[str, Callable[..., Any]] | None,
        from_agent: Any = None,
    ) -> tuple[str, dict[str, Any] | None, LLMCallType]:
        call_type = LLMCallType.LLM_CALL
        usage: dict[str, Any] | None = None
        rounds, deadline = self._execution_budget(from_agent)
        for _round in range(rounds):
            self._check_deadline(deadline, _round)
            self._trim_conversation_to_window(conversation, from_agent)
            response = self.client.responses.create(
                **self._prepare_responses_params(conversation, tools)
            )
            usage = self._extract_responses_token_usage(response)
            self._track_token_usage_internal(usage)
            text, function_calls = self._handle_responses(response)
            if function_calls and available_functions:
                call_type = LLMCallType.TOOL_CALL
                for fc in function_calls:
                    result = self._handle_tool_execution(
                        fc["name"], fc["arguments"], available_functions
                    )
                    conversation.append(
                        {
                            "type": "function_call_output",
                            "call_id": fc["id"],
                            "output": result if result is not None else "Tool not found.",
                        }
                    )
                continue
            return text, usage, call_type
        raise ExecutionBudgetExceededError(
            f"Tool-calling did not converge within {rounds} rounds "
            f"for model {self.model}."
        )

    def _handle_responses(
        self,
        response: Any,
        params: dict[str, Any] | None = None,
        available_functions: dict[str, Any] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
        response_model: type[BaseModel] | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        """Extract output text + function calls; track chaining state."""
        self._last_response_id = getattr(response, "id", None)
        if self.auto_chain_reasoning:
            self._last_reasoning_items = self._extract_reasoning_items(response)
        text = getattr(response, "output_text", None)
        if text is None:
            chunks = []
            for item in getattr(response, "output", None) or []:
                for part in getattr(item, "content", None) or []:
                    part_text = getattr(part, "text", None)
                    if part_text:
                        chunks.append(part_text)
            text = "".join(chunks)
        return text, self._extract_function_calls_from_response(response)

    def _extract_responses_token_usage(self, response: Any) -> dict[str, Any] | None:
        usage = getattr(response, "usage", None)
        if usage is None:
            return None
        return {
            "prompt_tokens": getattr(usage, "input_tokens", 0),
            "completion_tokens": getattr(usage, "output_tokens", 0),
            "total_tokens": getattr(usage, "total_tokens", 0),
        }

    def _extract_reasoning_items(self, response: Any) -> list[Any]:
        return [
            item
            for item in (getattr(response, "output", None) or [])
            if getattr(item, "type", None) == "reasoning"
        ]

    def _extract_builtin_tool_outputs(self, response: Any) -> list[dict[str, Any]]:
        outputs = []
        for item in getattr(response, "output", None) or []:
            item_type = getattr(item, "type", "")
            if item_type in (
                "web_search_call",
                "file_search_call",
                "code_interpreter_call",
                "computer_call",
            ):
                outputs.append(
                    {
                        "id": getattr(item, "id", None),
                        "status": getattr(item, "status", None),
                        "type": item_type,
                    }
                )
        return outputs
