"""BaseLLM — the engine's LLM contract.

Authored module; surface validated against the kasal_engine datamodel.
Satisfies the orchestration core's duck-typed contract:
``call(messages, tools=, available_functions=, from_task=, from_agent=) -> str``
plus ``get_usage_metrics()`` for Crew.token_usage.

Engine-native fixes for things kasal fought in crewAI:
- ``__copy__``/``__deepcopy__`` preserve the subclass (crewAI's hardcoded
  ``return LLM(...)`` dropped subclass type and instance attrs, forcing
  kasal's _VLLMFunctionCallingLLM to re-stamp ``__class__``).
- LLM events are emitted here with the engine bus, so ambient
  ``event_context`` lands on every LLMCall* event.
"""

import asyncio
import json
import re
import logging
import threading
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from ..events.bus import crewai_event_bus
from ..events.types import (
    LLMCallCompletedEvent,
    LLMCallFailedEvent,
    LLMCallStartedEvent,
    LLMCallType,
)
from .constants import (
    CONTEXT_WINDOW_USAGE_RATIO,
    DEFAULT_CONTEXT_WINDOW_SIZE,
    LLM_CONTEXT_WINDOW_SIZES,
)

logger = logging.getLogger(__name__)


class BaseLLM(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    model: str
    temperature: float | None = None
    stop: list[str] | None = None
    api_key: str | None = None
    base_url: str | None = None
    provider: str | None = None
    is_litellm: bool = False
    additional_params: dict[str, Any] = Field(default_factory=dict)

    _usage: dict[str, int] = PrivateAttr(
        default_factory=lambda: {
            "total_tokens": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "cached_prompt_tokens": 0,
            "successful_requests": 0,
        }
    )
    _usage_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def call(
        self,
        messages: str | list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        callbacks: list[Any] | None = None,
        available_functions: dict[str, Callable[..., Any]] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
    ) -> str:
        raise NotImplementedError("Subclasses must implement call().")

    async def acall(
        self,
        messages: str | list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        callbacks: list[Any] | None = None,
        available_functions: dict[str, Callable[..., Any]] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
    ) -> str:
        return await asyncio.to_thread(
            self.call, messages, tools, callbacks, available_functions,
            from_task, from_agent,
        )

    # ----------------------------- capabilities -----------------------------

    def supports_function_calling(self) -> bool:
        return True

    def supports_stop_words(self) -> bool:
        return True

    def supports_native_structured_output(self) -> bool:
        return False

    def get_context_window_size(self) -> int:
        window = LLM_CONTEXT_WINDOW_SIZES.get(self.model)
        if window is None:
            matches = [
                key for key in LLM_CONTEXT_WINDOW_SIZES
                if self.model.startswith(key) or key in self.model
            ]
            if matches:
                window = LLM_CONTEXT_WINDOW_SIZES[max(matches, key=len)]
        if window is None:
            window = DEFAULT_CONTEXT_WINDOW_SIZE
        return int(window * CONTEXT_WINDOW_USAGE_RATIO)

    # ------------------------------- helpers -------------------------------

    @staticmethod
    def _normalize_messages(
        messages: str | list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if isinstance(messages, str):
            return [{"role": "user", "content": messages}]
        return list(messages)

    def _apply_stop_words(self, text: str) -> str:
        if not self.stop:
            return text
        cut = len(text)
        for stop_word in self.stop:
            index = text.find(stop_word)
            if index != -1:
                cut = min(cut, index)
        return text[:cut]

    def _handle_tool_execution(
        self,
        name: str,
        arguments: str | dict[str, Any],
        available_functions: dict[str, Callable[..., Any]],
    ) -> str | None:
        function = available_functions.get(name)
        if function is None:
            logger.warning("LLM requested unknown function %r", name)
            return None
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments) if arguments.strip() else {}
            except json.JSONDecodeError:
                logger.warning("unparseable tool arguments for %r: %.200s", name, arguments)
                return None
        try:
            result = function(**arguments)
        except Exception as e:
            # EVERY tool failure is an answer, not a crash.
            #
            # A blocked call (denied approval / policy hook) was already handled
            # this way; everything else propagated — out of call(), through
            # run_agent's retry loop, and finally out of the crew, so one dead
            # link ended the run. Observed: a news site answering 302-redirect-loop
            # then 404 killed a 51s crew after three full agent turns, each one
            # re-running every tool call that had already succeeded.
            #
            # The model is the right place to decide what a failed tool call
            # means: handed the error as the result, it can try another source,
            # use what it already gathered, or say why it cannot continue.
            # Runaway retrying is bounded by the tool-round cap and max_iter.
            #
            # NOT swallowed: BaseException (KeyboardInterrupt, SystemExit,
            # asyncio.CancelledError), so cancelling an execution still stops it.
            from ..core.executor import ToolExecutionBlockedError

            if isinstance(e, ToolExecutionBlockedError):
                return f"Tool call blocked: {e}"
            logger.warning("tool %r failed: %s", name, e, exc_info=True)
            # Truncated: a verbose error (or one echoing a whole page) would
            # otherwise eat the context window it is reported into.
            detail = f"{type(e).__name__}: {e}"
            if len(detail) > 500:
                detail = detail[:500] + "…"
            return (
                f"Tool {name!r} failed — {detail}. "
                "This is the tool's result, not a crash: do not repeat the same "
                "call. Try a different input or source, or continue with what you "
                "already have."
            )
        return result if isinstance(result, str) else str(result)

    def _track_token_usage_internal(self, usage: dict[str, Any] | None) -> None:
        if not usage:
            return
        with self._usage_lock:
            self._usage["prompt_tokens"] += usage.get("prompt_tokens", 0) or 0
            self._usage["completion_tokens"] += usage.get("completion_tokens", 0) or 0
            self._usage["total_tokens"] += usage.get("total_tokens", 0) or 0
            self._usage["cached_prompt_tokens"] += usage.get("cached_prompt_tokens", 0) or 0
            self._usage["successful_requests"] += 1

    def get_usage_metrics(self) -> dict[str, Any]:
        with self._usage_lock:
            return dict(self._usage)

    def _validate_structured_output(
        self, text: str, response_format: Any
    ) -> Any:
        """Parse ``text`` into ``response_format``, or return it unchanged.

        Callers do ``isinstance(response, Model) or Model.model_validate(response)``;
        ``model_validate(<str>)`` raises, so handing back a JSON *string* makes
        them fall back silently (long-term-memory consolidation reporting
        "analysis failed, defaulting to insert" is the usual symptom). Returning
        the original text on failure keeps that fallback available.
        """
        if not (isinstance(response_format, type) and issubclass(response_format, BaseModel)):
            return text
        if not isinstance(text, str):
            return text
        candidate = text.strip()
        # Some models wrap structured output in a ```json … ``` fence.
        if candidate.startswith("```"):
            candidate = re.sub(r"^```[a-zA-Z0-9]*\s*", "", candidate)
            candidate = re.sub(r"\s*```$", "", candidate).strip()
        try:
            return response_format.model_validate_json(candidate)
        except Exception:
            logger.warning(
                "structured output did not validate against %s", response_format.__name__
            )
            return text

    # ------------------------------- events -------------------------------

    def _emit_call_started_event(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        from_task: Any = None,
        from_agent: Any = None,
    ) -> None:
        crewai_event_bus.emit(
            self,
            LLMCallStartedEvent(
                model=self.model,
                messages=messages,
                tools=tools,
                temperature=self.temperature,
                from_task=from_task,
                from_agent=from_agent,
            ),
        )

    def _emit_call_completed_event(
        self,
        response: Any,
        call_type: LLMCallType = LLMCallType.LLM_CALL,
        usage: dict[str, Any] | None = None,
        messages: list[dict[str, Any]] | None = None,
        from_task: Any = None,
        from_agent: Any = None,
    ) -> None:
        crewai_event_bus.emit(
            self,
            LLMCallCompletedEvent(
                model=self.model,
                response=response,
                call_type=call_type,
                usage=usage,
                messages=messages,
                from_task=from_task,
                from_agent=from_agent,
            ),
        )

    def _emit_call_failed_event(
        self, error: str, from_task: Any = None, from_agent: Any = None
    ) -> None:
        crewai_event_bus.emit(
            self,
            LLMCallFailedEvent(
                model=self.model, error=error, from_task=from_task, from_agent=from_agent
            ),
        )

    # copy.copy preserves the subclass through pydantic's own __copy__ — the
    # crewAI behavior kasal had to fight (its 1.14 LLM hardcoded
    # `return LLM(...)`) cannot recur here. deepcopy needs help because the
    # usage lock is not picklable: fields are deep-copied, private state
    # (client, lock) starts fresh, usage counters carry over by value.
    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> "BaseLLM":
        import copy as _copy

        fields = _copy.deepcopy(dict(self.__dict__), memo or {})
        cloned = self.model_copy(update=fields)
        cloned._usage = dict(self._usage)
        cloned._usage_lock = threading.Lock()
        for attr in ("_client", "_async_client"):
            if hasattr(cloned, attr):
                object.__setattr__(cloned, attr, None)
        return cloned
