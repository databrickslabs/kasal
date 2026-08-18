"""A CrewAI LLM whose calls go through Kasal's own transport.

This is the highest-leverage decision in the whole dual-harness design, so it is
worth stating plainly: **the CrewAI harness does not use CrewAI's LLM.** It uses
a ``crewai.llms.base_llm.BaseLLM`` subclass that forwards every call to the
``src.core.llm.transport`` object ``LLMManager`` already builds.

## What that buys

Everything hanging off an LLM call stays IDENTICAL across harnesses, for free:

* Databricks OBO → PAT → SPN auth and the partner User-Agent telemetry
* ``DatabricksRetryLLM`` / ``DatabricksResponsesLLM`` / ``VLLMFunctionCallingLLM``
* model fallback, RPM control, the output clamp and the context-window budget
* ``LLMCallStartedEvent`` / ``Completed`` / ``Failed`` / ``LLMStreamChunkEvent``
  on the KASAL bus — so traces, token accounting, cost and the live chat stream
  need no bridging at all
* the run deadline, so ``Capability.RUN_DEADLINE`` holds on CrewAI too

And it sidesteps the dependency conflict: CrewAI 1.15 moved litellm to an extra
wanting ``>=1.84``, while this project pins ``1.74.9``. Not using CrewAI's LLM
means never enabling that extra.

The point that matters most is the last one, though. If the two harnesses called
models through different stacks, "did switching the harness change the answer?"
would have no answer — every difference could be the runtime or could be the
provider layer. Sharing the transport makes the comparison mean something.

## Why it is nearly free to write

Kasal's transport was authored as a drop-in for CrewAI's LLM, and the signature
never diverged. Both are::

    call(messages, tools=None, callbacks=None, available_functions=None,
         from_task=None, from_agent=None, response_model=None) -> str

CrewAI's executor passes ``tools`` as OpenAI tool schemas (``_openai_tools``),
which is exactly what the transport expects. So this class forwards positionally
and adds nothing to the request path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from src.core.logger import LoggerManager
from src.services.execution.harnesses.crewai.availability import crewai_symbols

logger = LoggerManager.get_instance().crew


@dataclass(frozen=True)
class _Function:
    name: str
    arguments: str


@dataclass(frozen=True)
class _ToolCall:
    """One tool call, in the shape CrewAI reads it.

    OBJECTS, not dicts, and the distinction is a silent-failure trap rather
    than a style choice. CrewAI's ``is_tool_call_list`` accepts a dict with a
    ``"function"`` key — so a list of dicts passes the type check and is stored
    as pending work — but ``extract_tool_call_info`` then reads
    ``tool_call.function.name`` by ATTRIBUTE, gets nothing from a dict, returns
    None, and the call is skipped. The agent would sit there having decided to
    call a tool that never runs, with nothing logged.
    """

    id: str
    function: _Function


def _as_crewai_tool_calls(calls: List[Dict[str, Any]]) -> List[_ToolCall]:
    """The transport's normalized calls, in CrewAI's shape."""
    return [
        _ToolCall(
            id=str(call.get("id") or f"call_{index}"),
            function=_Function(
                name=str(call.get("name") or ""),
                arguments=call.get("arguments") or "{}",
            ),
        )
        for index, call in enumerate(calls)
    ]


def build_kasal_backed_llm(inner: Any) -> Any:
    """Wrap a Kasal transport LLM as something ``crewai.Agent`` accepts.

    Built through a factory rather than declared at module scope because the
    base class only exists once ``crewai`` is imported, and importing it at
    module scope would cost every Kasal-harness process ~2.6s for nothing.
    """
    base = crewai_symbols()["BaseLLM"]

    class KasalBackedLLM(base):  # type: ignore[misc, valid-type]
        """A CrewAI LLM that is a thin forwarder onto Kasal's transport.

        ``inner`` is the object ``LLMManager.configure_kasal_llm`` returned —
        already carrying this tenant's credentials, endpoint, retry policy and
        parameter rules. Nothing is re-derived here; re-deriving is how the two
        harnesses would drift.
        """

        model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

        def __init__(self, **data: Any) -> None:
            # Popped BEFORE super(): pydantic rejects underscore-prefixed keys,
            # and the transport object is not a modellable value anyway. It is
            # set past the model so it can never be serialized into a config
            # dict — which is what a declared field would invite, and which is
            # how a credential-bearing object ends up somewhere it is read back.
            inner_llm = data.pop("_inner", None)
            super().__init__(**data)
            object.__setattr__(self, "_inner", inner_llm)

        @property
        def inner(self) -> Any:
            return getattr(self, "_inner", None)

        # ---------------------------------------------------------------
        # The one abstract method, and its async twin
        # ---------------------------------------------------------------

        def call(
            self,
            messages: Any,
            tools: Optional[list] = None,
            callbacks: Optional[list] = None,
            available_functions: Optional[dict[str, Callable[..., Any]]] = None,
            from_task: Any = None,
            from_agent: Any = None,
            response_model: Any = None,
        ) -> Any:
            answer = self.inner.call(
                messages,
                tools,
                callbacks,
                available_functions,
                from_task,
                from_agent,
                response_model,
            )
            # A list means the model chose tools and the transport handed the
            # decision back for CrewAI's executor to act on — see
            # `delegate_tool_calls`. Text passes straight through.
            if isinstance(answer, list):
                return _as_crewai_tool_calls(answer)
            return answer

        async def acall(
            self,
            messages: Any,
            tools: Optional[list] = None,
            callbacks: Optional[list] = None,
            available_functions: Optional[dict[str, Callable[..., Any]]] = None,
            from_task: Any = None,
            from_agent: Any = None,
            response_model: Any = None,
        ) -> Any:
            answer = await self.inner.acall(
                messages,
                tools,
                callbacks,
                available_functions,
                from_task,
                from_agent,
                response_model,
            )
            if isinstance(answer, list):
                return _as_crewai_tool_calls(answer)
            return answer

        # ---------------------------------------------------------------
        # Capability questions CrewAI asks before it builds a request
        # ---------------------------------------------------------------

        def get_context_window_size(self) -> int:
            """The transport's answer, not CrewAI's model table.

            CrewAI trims against this. Its table does not know a Databricks
            serving endpoint or a model a user added to the catalogue, and a
            wrong window here is not a small thing: too large and the endpoint
            refuses the request, too small and the conversation is silently
            amputated. The transport already registers the real figure per
            model (``LLMManager._register_context_window``).
            """
            inner = self.inner
            for attr in ("get_context_window_size", "context_window_size"):
                value = getattr(inner, attr, None)
                if callable(value):
                    try:
                        return int(value())
                    except Exception:  # noqa: BLE001
                        break
                if isinstance(value, int) and value > 0:
                    return value
            return super().get_context_window_size()

        def supports_function_calling(self) -> bool:
            """Whether the model takes native tool calls. THE important one.

            CrewAI decides between native function calling and a ReAct PROSE
            loop with this, and it probes for it with ``hasattr`` — so a
            wrapper that merely omits the method is read as "cannot", with no
            error anywhere.

            The cost of getting that wrong is not subtle, and it was observed:
            the agent falls back to emitting ``Action Input:`` as text, CrewAI
            parses it with ``_validate_tool_input``, and any tool the model
            calls without arguments fails with "the Action Input is not a valid
            key, value dictionary" — then retries with a made-up
            ``{"dummy": ""}`` to get past the parser. Same tool, same model,
            working fine on the Kasal harness, because the transport has always
            made native tool calls.
            """
            return self._ask("supports_function_calling", default=True)

        def supports_stop_words(self) -> bool:
            return self._ask("supports_stop_words", default=True)

        def supports_multimodal(self) -> bool:
            return self._ask("supports_multimodal", default=False)

        def supports_native_structured_output(self) -> bool:
            return self._ask("supports_native_structured_output", default=False)

        def _ask(self, name: str, default: bool) -> bool:
            """Put a capability question to the transport, which owns the answer.

            The defaults are only for a transport that cannot answer at all.
            They are the CONSERVATIVE reading in every case except function
            calling, where "unknown" has to mean yes: the transport drives an
            OpenAI-protocol tool loop for every endpoint it supports, and
            answering no silently degrades every tool call to prose parsing.
            """
            probe = getattr(self.inner, name, None)
            if callable(probe):
                try:
                    return bool(probe())
                except Exception as e:  # noqa: BLE001 — never fail a run on this
                    logger.debug("transport could not answer %s (%s)", name, e)
            return default

        def __repr__(self) -> str:
            """Never print the credential: execution logs are downloadable."""
            return f"KasalBackedLLM(model={getattr(self.inner, 'model', '?')!r})"

    fields = {
        "model": str(getattr(inner, "model", "") or "unknown"),
        "temperature": getattr(inner, "temperature", None),
        "stop": list(getattr(inner, "stop", None) or []),
        "provider": str(getattr(inner, "provider", None) or "openai"),
        # NOT api_key/base_url. The wrapped transport holds the credential and
        # makes the request; copying it onto a second object would put it in a
        # second repr, a second serialization and a second place to leak from.
        "_inner": inner,
    }
    # CrewAI's executor owns the tool loop: it applies reflection prompts,
    # iteration limits and its tool-failure policy between rounds. So the
    # transport must hand back the model's decision rather than executing it —
    # without this a tool-call response reaches CrewAI as an empty string and
    # surfaces as "Invalid response from LLM call - None or empty."
    #
    # Safe to set on this instance: `configure_kasal_llm` builds a fresh
    # transport per agent, and the Kasal harness never goes through this wrapper.
    try:
        inner.delegate_tool_calls = True
    except Exception as e:  # noqa: BLE001 — a handler that does not model it
        logger.warning(
            "Could not enable tool-call delegation on %s (%s); CrewAI tool "
            "calls on this endpoint may come back empty",
            type(inner).__name__,
            e,
        )

    built = KasalBackedLLM(**fields)
    logger.info(
        "CrewAI agent LLM bound to Kasal transport for model %s", fields["model"]
    )
    return built
