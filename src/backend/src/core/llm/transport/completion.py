"""OpenAICompletion — OpenAI-compatible provider (Chat Completions + Responses).

Authored module; surface validated against the kasal_engine datamodel
(the 33 kasal-required members of crewAI 1.15.5's 84; kasal's
DatabricksCodexCompletion subclasses this and overrides the Responses
plumbing). Uses the ``openai`` SDK — an optional dependency; a clear
ImportError is raised on first use if it is missing.
"""

import logging
import os
import re
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, PrivateAttr

from src.core.events.bus import event_bus
from src.core.events.types import (
    LLMCallType,
    LLMReasoningChunkEvent,
    LLMStreamChunkEvent,
)
from src.core.llm.model_capabilities import (
    ReasoningStyle,
    allowed_efforts,
    reasoning_style,
)

from .base import BaseLLM
from .budget import (
    check_deadline,
    exhausted_mid_round,
    resolve_execution_budget,
    rounds_exhausted,
    wrapup_conversation,
)
from .context_recovery import MAX_REJECTIONS_PER_ROUND
from .context_window import ContextWindowBudget
from .exceptions import (
    LLMContextLengthExceededError,
    is_context_length_exceeded,
)

# Aliased: `function_calls` is the local variable name throughout the round
# loops, and a module-level import of the same name would shadow confusingly.
from .response_parsing import (
    REDACTED_REASONING,
    answer_from_reasoning,
    builtin_tool_outputs,
    chat_token_usage,
)
from .response_parsing import function_calls as parse_function_calls
from .response_parsing import (
    reasoning_items,
    responses_reasoning_text,
    responses_token_usage,
    split_message_content,
)
from .rpm import throttle
from .tool_rounds import (
    NO_ANSWER_MARKUP_ONLY,
    RepeatGuard,
    answer_without_markup,
    run_chat_round,
    run_responses_round,
    stub_repeated_chat_round,
    stub_repeated_responses_round,
)

logger = logging.getLogger(__name__)


# OpenAI's GPT-5.6 line refuses `reasoning_effort` on /v1/chat/completions when
# the request also carries function tools:
#
#   Function tools with reasoning_effort are not supported for gpt-5.6-terra in
#   /v1/chat/completions. To use function tools, use /v1/responses or set
#   reasoning_effort to 'none'.
#
# Every crew agent that has tools hits this as a hard 400, killing the run. The
# remedy is the API's own: send effort "none" on tool-carrying calls. The model's
# tool-free calls keep the configured effort.
#
# Matched by name rather than applied to all reasoning models on purpose — the
# Databricks-served gpt-5* endpoints accept `reasoning_effort` alongside tools,
# and blanket-dropping it there would silently disable reasoning that works.
# An optional provider prefix ("openai/gpt-5.6-terra") is tolerated.
_TOOLS_REJECT_REASONING_EFFORT_RE = re.compile(r"(?:^|/)gpt-5\.6")

# Anthropic models that accept `thinking: {"type": "enabled", "budget_tokens": N}`
# AND return real thinking text. Enumerated per-model rather than by family,
# because the split does NOT follow the version boundary you would expect —
# every entry was probed live 2026-08-05 with max_tokens=16000:
#
#     haiku-4-5    1,867 chars      opus-4-1       562
#     sonnet-4-5   1,739            opus-4-5       375
#     sonnet-4-6      76            opus-4-6       167
#
# EXCLUDED because they reject "enabled" and demand `{"type": "adaptive"}`
# ('"thinking.type.enabled" is not supported for this model. Use
# "thinking.type.adaptive"') — see _THINKING_ADAPTIVE_MODELS.
#
# Note opus-4-7/4-8 sit on the ADAPTIVE side despite being "4.x". A regex like
# `claude-(opus|sonnet|haiku)-4-\d` looks right and is wrong: it would send
# "enabled" to those two and 400 the call. Hence the explicit list.
_THINKING_BUDGET_MODELS = (
    "claude-opus-4-1",
    "claude-opus-4-5",
    "claude-opus-4-6",
    "claude-sonnet-4-5",
    "claude-sonnet-4-6",
    "claude-haiku-4-5",
)

# Models on ADAPTIVE thinking: `{"type": "adaptive", "display": "summarized"}`.
# They reject `type: "enabled"` and take no budget_tokens.
#
# `display` is the whole story, and missing it cost real time here. Per
# platform.claude.com/docs/en/build-with-claude/thinking#controlling-thinking-display
# it defaults to "omitted" on exactly these models, which returns "thinking blocks
# with an empty `thinking` field" — the signature only. Sending `adaptive` WITHOUT
# `display` therefore looks identical to a provider that redacts its reasoning,
# and that is what an earlier version of this code concluded. Opting in returns
# the summary: fable-5 255 chars, opus-5 1,629 (measured 2026-08-05).
#
# opus-4-7 and opus-4-8 are adaptive too but returned 0 even with
# display="summarized", so they are listed for correct REQUEST shape while the
# UI still reports nothing to show for them.
_THINKING_ADAPTIVE_MODELS = (
    "claude-opus-5",
    "claude-sonnet-5",
    "claude-fable-5",
    "claude-mythos-5",
    "claude-opus-4-7",
    "claude-opus-4-8",
)


def thinking_mode(model_name: str | None) -> str | None:
    """Which thinking surface ``model_name`` has: "manual", "adaptive" or None.

    Answered from ``core.llm.model_capabilities`` — the measured per-model
    registry — rather than from lists kept here, so the transport, the API that
    tells the UI which control to render, and the request builder can never
    disagree. The cost of disagreement is a 400 on a real run: sending a budget
    to an adaptive model, or `enabled` to one that demands `adaptive`, is
    rejected outright.

    * "manual"   — takes `{"type": "enabled", "budget_tokens": N}` (Claude 4.1–4.6)
    * "adaptive" — takes `{"type": "adaptive"}` plus an effort (Claude 4.7+/5/Fable)
    * None       — no thinking surface; anything sent would be an error
    """
    style = reasoning_style(model_name)
    if style is ReasoningStyle.ADAPTIVE_EFFORT:
        return "adaptive"
    if style is ReasoningStyle.TOKEN_BUDGET:
        return "manual"
    return None


def valid_thinking_efforts(model_name: str | None) -> tuple[str, ...]:
    """Effort values ``model_name`` accepts — per model, never a global list.

    There are five distinct scales across the seeded catalogue (Anthropic
    adaptive takes low..max; gpt-5 takes minimal..high but rejects "none";
    gpt-5-1 takes "none" but rejects "minimal"; the 5-2/5-4/5-6 line adds
    "xhigh"; Gemini takes only low/medium/high). A single constant would be wrong
    for most of them, which is why this delegates to the registry.
    """
    return allowed_efforts(model_name)


class OpenAICompletion(ContextWindowBudget, BaseLLM):
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
    #: Anthropic extended thinking. Claude does NOT accept `reasoning_effort`;
    #: its budget is `thinking: {"type": "enabled", "budget_tokens": N}`, and the
    #: endpoint enforces `max_tokens > budget_tokens`. Set to a token count to
    #: enable; None leaves the request untouched. See `_thinking_for`.
    #:
    #: Only MANUAL-mode models (Claude 4.1–4.6) use the number. On ADAPTIVE
    #: models a non-None value means "thinking on" and the depth comes from
    #: `thinking_effort` instead — they reject a budget outright.
    thinking_budget_tokens: int | None = None
    #: Depth for ADAPTIVE thinking, sent as `output_config: {"effort": ...}`.
    #: Ignored by manual-mode models, whose depth is the budget. Must be a value
    #: THIS model accepts — the scales differ per model, so validate against
    #: `model_capabilities.allowed_efforts(model)` rather than a global list.
    #: None lets the endpoint apply its own default (currently "high").
    thinking_effort: str | None = None
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
    #: Reasoning/thinking text the model exposed on the CURRENT call, collected
    #: across streamed deltas (or read once when not streaming). Reported on
    #: LLMCallCompletedEvent so the trace records it once per call instead of
    #: once per delta. Reset per call, like _finish_reason.
    _reasoning_text: str = PrivateAttr(default="")
    #: How far the server's token count has been observed to exceed the
    #: chars-per-token estimate on THIS model's traffic — 1.0 until a rejection
    #: teaches otherwise. Divides the trim budget so the proactive trim fires
    #: where the server actually draws the line instead of every few rounds
    #: later. Only ever grows; see ``_compact_after_rejection``.
    _estimate_correction: float = PrivateAttr(default=1.0)

    def _add_reasoning(self, reasoning: str) -> None:
        """Accumulate a reasoning delta.

        ``REDACTED_REASONING`` is a FLAG, not prose: it means "the model thought
        but the provider encrypted the trace". Every delta of an encrypted stream
        reports it, so appending it like text rendered
        ``__kasal_reasoning_redacted____kasal_reasoning_redacted__…`` in the UI —
        the frontend tests the value for EQUALITY, so a repeated sentinel matched
        nothing and leaked to the user verbatim. Keep it idempotent, and never let
        it mix with real text: if any delta carries actual reasoning, that wins.
        """
        if reasoning == REDACTED_REASONING:
            if not self._reasoning_text:
                self._reasoning_text = REDACTED_REASONING
            return
        if self._reasoning_text == REDACTED_REASONING:
            self._reasoning_text = ""  # real text supersedes the placeholder
        self._reasoning_text += reasoning

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError(
                    "src.core.llm.transport.OpenAICompletion requires the 'openai' "
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
            self.call,
            messages,
            tools,
            callbacks,
            available_functions,
            from_task,
            from_agent,
            response_model,
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

        if isinstance(text, list):
            # Tool calls handed back for the caller to execute
            # (``delegate_tool_calls``). Not an answer: stop words, structured
            # output and the empty-answer recovery all describe TEXT. The
            # completed event still fires, carrying what the model DECIDED, so
            # the trace shows the round rather than a gap where one happened.
            self._emit_call_completed_event(
                f"[tool_calls] {', '.join(c.get('name', '?') for c in text)}",
                call_type,
                usage,
                conversation,
                from_task,
                from_agent,
                finish_reason=self._finish_reason,
                reasoning=self._reasoning_text or None,
            )
            return text

        if self.supports_stop_words():
            text = self._apply_stop_words(text)
        self._emit_call_completed_event(
            text,
            call_type,
            usage,
            conversation,
            from_task,
            from_agent,
            finish_reason=self._finish_reason,
            reasoning=self._reasoning_text or None,
        )
        # `response_model` was accepted and ignored here, so structured-output
        # callers got a JSON *string* and their
        # `isinstance(r, Model) or Model.model_validate(r)` fell back silently.
        # DatabricksRetryLLM compensated with a private coercion, which meant the
        # feature worked on exactly one provider. Honour it for all of them; the
        # helper returns the text unchanged when it does not parse, so the
        # caller's own fallback still applies.
        if response_model is not None:
            return self._validate_structured_output(text, response_model)
        return text

    # --------------------------- chat completions ---------------------------

    def _reasoning_effort_for(self, tools: list[dict[str, Any]] | None) -> str | None:
        """The `reasoning_effort` to send for a call carrying ``tools``.

        Returns the configured effort unchanged for every model except the ones
        whose chat-completions endpoint rejects the combination — see
        ``_TOOLS_REJECT_REASONING_EFFORT_RE``, where the API requires "none".

        Sending nothing is NOT the same as sending "none" on those models. They
        apply a reasoning budget by default server-side, so a tool-carrying call
        that simply omits the parameter is still refused:

            400 Function tools with reasoning_effort are not supported for
            gpt-5.6-sol in /v1/chat/completions. To use function tools, use
            /v1/responses or set reasoning_effort to 'none'.

        That killed every chat-mode run on gpt-5.6 with tools attached, even
        though kasal had set no effort at all (the LLM logged
        reasoning_effort=None). Hence the model+tools check runs BEFORE the
        "nothing configured" shortcut: "none" has to be stated explicitly.
        """
        if tools and _TOOLS_REJECT_REASONING_EFFORT_RE.search(str(self.model).lower()):
            logging.getLogger(__name__).debug(
                "%s rejects reasoning_effort alongside function tools on chat "
                "completions; sending 'none' for this call",
                self.model,
            )
            return "none"
        return self.reasoning_effort

    def _thinking_for(self, params: dict[str, Any]) -> dict[str, Any] | None:
        """The Anthropic `thinking` block for this call, or None.

        Claude does not take ``reasoning_effort`` ("Extra inputs are not
        permitted"); it takes a ``thinking`` block, and the two generations want
        DIFFERENT shapes. Verified live 2026-08-05 and documented at
        platform.claude.com/docs/en/build-with-claude/thinking plus
        docs.databricks.com/aws/en/machine-learning/model-serving/query-reason-models:

        * MANUAL (Claude 4.1–4.6) — ``{"type": "enabled", "budget_tokens": N}``,
          subject to ``max_tokens > budget_tokens``. Measured: haiku-4-5 1,867
          chars, sonnet-4-5 1,739, opus-4-1 562, opus-4-5 375, opus-4-6 167,
          sonnet-4-6 76.
        * ADAPTIVE (Claude 4.7+, 5, Fable) — ``{"type": "adaptive"}``; "enabled"
          is rejected ('"thinking.type.enabled" is not supported for this model')
          and no budget is accepted. Depth is the model's own decision.

        BOTH need ``display: "summarized"`` to return any text. Per the docs it
        defaults to ``"omitted"`` on Claude Fable 5, Mythos 5, Opus 5, Sonnet 5,
        Opus 4.8 and Opus 4.7, which returns "thinking blocks with an empty
        `thinking` field" — the encrypted ``signature`` alone. Omitting `display`
        is therefore indistinguishable from a provider that redacts its
        reasoning, and that is exactly the wrong conclusion this code reached
        before: opting in yields fable-5 255 chars and opus-5 1,629.

        ``max_tokens`` is raised to clear the budget rather than letting the
        endpoint 400 on an otherwise reasonable configuration.
        """
        model = str(self.model).lower()
        has_budget = (
            bool(self.thinking_budget_tokens) and self.thinking_budget_tokens > 0
        )
        is_adaptive = any(name in model for name in _THINKING_ADAPTIVE_MODELS)

        # An ADAPTIVE model's opt-in knob is `thinking_effort`, because it REJECTS
        # a budget. Gating the whole function on a budget therefore made
        # effort-only configuration a silent no-op: no `thinking` block was sent,
        # so `display` defaulted to "omitted", the reply carried an encrypted
        # signature with an empty `thinking` field, and this code reported it as
        # redacted — a summary the user was entitled to, shown as a placeholder.
        #
        # On an adaptive model we ask for the summary even with NOTHING configured.
        # It reasons regardless — probed live 2026-08-05, an unconfigured request
        # still comes back with a signed reasoning block — so `display` only
        # decides whether we can SEE work already being paid for. Requesting it is
        # not the same as turning thinking on, and no model config seeds a
        # thinking default, so without this every adaptive Claude shows the
        # redaction placeholder out of the box. Manual-mode models still require
        # an explicit budget: there, `thinking` genuinely enables the feature and
        # consumes part of `max_tokens`.
        if not (has_budget or is_adaptive):
            return None

        # Adaptive models: no budget, but `display` is what makes it visible.
        # Depth is `output_config: {"effort": ...}` — a sibling of `thinking`,
        # not a key inside it, so it is applied to `params` here.
        if is_adaptive:
            # Validated against THIS model's own accepted set, not a global list:
            # the scales differ per model, so a value that is valid for one
            # Anthropic model can 400 on another.
            accepted = allowed_efforts(self.model)
            effort = (self.thinking_effort or "").strip().lower()
            if effort and effort in accepted:
                body = dict(params.get("extra_body") or {})
                output_config = dict(body.get("output_config") or {})
                output_config["effort"] = effort
                body["output_config"] = output_config
                params["extra_body"] = body
            elif effort:
                logger.debug(
                    "Ignoring effort %r for %s; it accepts %s",
                    self.thinking_effort,
                    self.model,
                    accepted,
                )
            return {"type": "adaptive", "display": "summarized"}

        if not any(name in model for name in _THINKING_BUDGET_MODELS):
            # Not an Anthropic model — `thinking` is not part of its surface.
            return None
        if not has_budget:
            # Manual mode with only an effort set: the number IS the depth here,
            # and effort is not part of this model's surface. Nothing to send.
            return None

        budget = int(self.thinking_budget_tokens or 0)
        cap = params.get("max_tokens") or params.get("max_completion_tokens")
        if cap is not None and cap <= budget:
            params["max_tokens"] = budget + 4096
            params.pop("max_completion_tokens", None)
        elif cap is None:
            params["max_tokens"] = budget + 4096
        return {
            "type": "enabled",
            "budget_tokens": budget,
            "display": "summarized",
        }

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
        # The escape hatch goes in FIRST so the declared fields below override
        # it. A typed, validated field must not be silently displaced by a loose
        # dict — and it is the order CrewAI's native provider uses
        # (providers/openai/completion.py: update(additional_params), then
        # per-field `if is not None`). Merging it last, as this did, meant the
        # only way to set a parameter also became the only way to break one.
        params.update(self.additional_params)
        for key, value in (
            ("temperature", self.temperature),
            ("top_p", self.top_p),
            ("frequency_penalty", self.frequency_penalty),
            ("presence_penalty", self.presence_penalty),
            ("reasoning_effort", self._reasoning_effort_for(tools)),
            ("stop", self.stop if self.supports_stop_words() else None),
            ("tools", tools),
        ):
            if value is not None:
                params[key] = value
        if self.max_completion_tokens is not None:
            params["max_completion_tokens"] = self.max_completion_tokens
        elif self.max_tokens is not None:
            params["max_tokens"] = self.max_tokens
        # Anthropic extended thinking. Emitted AFTER max_tokens because the
        # endpoint requires `max_tokens > budget_tokens` and will 400 otherwise —
        # this raises max_tokens to fit rather than letting a valid budget fail.
        #
        # Goes in `extra_body`, NOT as a top-level kwarg: the OpenAI SDK validates
        # its signature and raises "Completions.create() got an unexpected keyword
        # argument 'thinking'" before any request is made. `extra_body` is the
        # SDK's documented passthrough and is what the Databricks docs use.
        thinking = self._thinking_for(params)
        if thinking is not None:
            extra_body = dict(params.get("extra_body") or {})
            extra_body["thinking"] = thinking
            params["extra_body"] = extra_body
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
        # Last, so it sees the final message list and any max_tokens supplied
        # through additional_params.
        self._clamp_output_budget(params)
        return params

    def _execution_budget(self, from_agent: Any) -> tuple[int, float | None]:
        """(max tool rounds, wall-clock deadline) for one call — see budget.py."""
        return resolve_execution_budget(from_agent)

    def _check_deadline(
        self,
        deadline: float | None,
        rounds_done: int,
        conversation: list[dict[str, Any]] | None = None,
    ) -> None:
        check_deadline(deadline, rounds_done, self.model, conversation)

    def _executor(
        self, available_functions: dict[str, Callable[..., Any]] | None
    ) -> Callable[[str, Any], Any]:
        """A (name, arguments) -> result callable over this call's tool table."""
        return lambda name, arguments: self._handle_tool_execution(
            name, arguments, available_functions
        )

    def _answer_within_budget(
        self,
        error: Exception,
        conversation: list[dict[str, Any]],
        usage: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any] | None, LLMCallType]:
        """One last tool-less call, so a spent budget still yields an answer.

        Raising discarded everything the agent had gathered — for a run eleven
        searches deep, the worst available outcome. crewAI
        (``handle_max_iterations_exceeded``) and LangChain
        (``early_stopping_method="generate"``) both spend one call here instead.

        No tools are passed, so the round loop returns immediately and cannot
        recurse. If this call fails or says nothing, the original budget error
        is raised as before and the degrade path still keeps the partial.
        """
        try:
            text = self.call(wrapup_conversation(conversation))
        except Exception as wrapup_failed:
            logger.warning("wrap-up call after %s failed: %s", error, wrapup_failed)
            raise error from wrapup_failed
        # The wrap-up must not hand back un-executed tool-call markup either —
        # that is exactly the state a degenerate model is stuck in when the
        # budget goes. The nested call() has already replaced markup-only text
        # with NO_ANSWER_MARKUP_ONLY, which counts as "said nothing" here: with
        # nothing real to fall back on, the budget error is raised as before,
        # and its partial is what the degrade path keeps.
        text = answer_without_markup(text, conversation, when_nothing_real="")
        if not text.strip() or text == NO_ANSWER_MARKUP_ONLY:
            raise error
        logger.warning("budget spent (%s); answered from what was gathered", error)
        return text, usage, LLMCallType.LLM_CALL

    def _call_completions_api(
        self,
        conversation: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        available_functions: dict[str, Callable[..., Any]] | None,
        from_agent: Any = None,
    ) -> tuple[str, dict[str, Any] | None, LLMCallType]:
        call_type = LLMCallType.LLM_CALL
        usage: dict[str, Any] | None = None
        # Per-call, like _finish_reason: a previous call's thinking must not be
        # reported against this one. Both API paths share the emit site.
        self._reasoning_text = ""
        rounds, deadline = self._execution_budget(from_agent)
        repeat_guard = RepeatGuard()
        for _round in range(rounds):
            self._check_deadline(deadline, _round, conversation)
            throttle(from_agent)
            self._trim_conversation_to_window(conversation, from_agent, tools)
            # Where THIS round's thinking starts. _reasoning_text is reset per
            # CALL (above) but appended per ROUND, so salvaging from the whole
            # buffer let an empty final round reach back into an earlier round's
            # deliberation and return the model's own tool ARGUMENTS as the
            # answer. Verified against a two-round tool call.
            reasoning_mark = len(self._reasoning_text)
            content, usage, function_calls = self._request_chat_round(
                conversation, tools, from_agent
            )
            if function_calls and not available_functions and self.delegate_tool_calls:
                # The caller runs its own tool loop; give it the decision.
                # Falling through here returns "" — a tool-call response has no
                # content — which the caller can only report as "empty response
                # from the LLM", nowhere near the real cause.
                return function_calls, usage, LLMCallType.TOOL_CALL
            if function_calls and available_functions:
                call_type = LLMCallType.TOOL_CALL
                # A batch identical to the last two is a degenerate loop (the
                # 21-times browser_close run): stop executing it, tell the
                # model to answer, and one repeat later drop the tools so the
                # next round CAN only answer.
                repeats = repeat_guard.observe(function_calls)
                if repeats >= 2:
                    stub_repeated_chat_round(conversation, content, function_calls)
                    if repeats >= 3:
                        tools = None
                    continue
                outcome = run_chat_round(
                    conversation,
                    content,
                    function_calls,
                    self._executor(available_functions),
                    deadline,
                    available_functions,
                )
                if outcome.final_answer is not None:
                    return outcome.final_answer, usage, call_type
                if outcome.exhausted:
                    return self._answer_within_budget(
                        exhausted_mid_round(self.model, conversation),
                        conversation,
                        usage,
                    )
                continue
            answer = self._answer_or_recover(
                content, usage, self._reasoning_text[reasoning_mark:]
            )
            # A "final answer" that is un-executed tool-call markup is not an
            # answer; see answer_without_markup for what stands in for it.
            answer = answer_without_markup(answer, conversation)
            return answer, usage, call_type
        return self._answer_within_budget(
            rounds_exhausted(rounds, self.model, conversation), conversation, usage
        )

    def _request_chat_round(
        self,
        conversation: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        from_agent: Any = None,
    ) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
        """One chat request → ``(content, usage, function_calls)``.

        Retried behind forced compaction when the server rejects the prompt as
        too long (``_compact_after_rejection``); any other error propagates on
        the first attempt. The params are rebuilt per attempt because the output
        clamp is sized from the messages that were just stubbed.
        """
        rejections = 0
        while True:
            params = self._prepare_completion_params(conversation, tools)
            try:
                if self.stream:
                    return self._stream_chat_completion(params)
                response = self.client.chat.completions.create(**params)
            except Exception as e:
                rejections += 1
                if (
                    rejections > MAX_REJECTIONS_PER_ROUND
                    or not self._compact_after_rejection(
                        conversation, from_agent, tools, e
                    )
                ):
                    raise
                continue
            usage = self._extract_chat_token_usage(response)
            self._track_token_usage_internal(usage)
            function_calls = self._extract_function_calls_from_response(response)
            # Same block-list shape as the streaming path: without this the
            # reasoning blocks became part of the returned "answer".
            content, reasoning = split_message_content(response.choices[0].message)
            if reasoning:
                self._add_reasoning(reasoning)
                event_bus.emit(
                    self,
                    LLMReasoningChunkEvent(model=self.model, reasoning=reasoning),
                )
            self._finish_reason = getattr(response.choices[0], "finish_reason", None)
            return content, usage, function_calls

    def _answer_or_recover(self, content: str, usage: Any, reasoning: str) -> str:
        """The answer, salvaged from the reasoning channel if it went there.

        An endpoint that returns ``content=None`` while writing the deliverable
        into ``reasoning_content`` used to yield an empty string that travelled
        all the way to a task result and reported SUCCESS. Downstream that is
        indistinguishable from "the model had nothing to say": the structured
        output failed to parse, no field reached flow state, and a router
        condition over it evaluated False with nothing anywhere saying why.

        Observed on a self-hosted vLLM endpoint — 1,255 completion tokens, empty
        content, and a complete valid JSON answer sitting in the reasoning.

        Recovery is conservative (see ``answer_from_reasoning``): only a clearly
        delimited payload counts. When there is nothing to salvage the answer
        stays empty, but says so loudly rather than passing silently.
        """
        if content:
            return content

        recovered = answer_from_reasoning(reasoning)
        if recovered:
            logger.warning(
                "[llm] %s returned empty content; recovered %d chars of answer "
                "from the reasoning channel. The model did not close its "
                "thinking block.",
                self.model,
                len(recovered),
            )
            return recovered

        produced = (usage or {}).get("completion_tokens") or 0
        if self._finish_reason == "length":
            # A DIFFERENT failure, and the only one the caller can act on:
            # generation stopped because the output allowance ran out, not
            # because the model had nothing to say. Reasoning tokens count
            # against max_output_tokens, so a thinking model can spend the whole
            # budget deliberating and be cut off mid-sentence before writing a
            # word of answer. Observed at 8,192 completion tokens against an
            # 8,192 cap with 35,606 chars of reasoning and no content.
            logger.error(
                "[llm] %s ran out of output budget: stopped at %s completion "
                "tokens (finish_reason=length) with %d chars of reasoning and "
                "no answer. Raise this model's max_output_tokens, or give it "
                "less to do. Note that reasoning counts against that budget.",
                self.model,
                produced,
                len(reasoning),
            )
        elif produced or reasoning:
            logger.error(
                "[llm] %s produced NO answer: content empty, %s completion "
                "tokens, %d chars of reasoning, and nothing salvageable in it. "
                "Callers will see an empty result.",
                self.model,
                produced,
                len(reasoning),
            )
        return ""

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
        finish_reason: str | None = None
        for part in response_stream:
            if getattr(part, "usage", None) is not None:
                usage = self._extract_chat_token_usage(part)
            choices = getattr(part, "choices", None)
            if not choices:
                continue
            # The last chunk carries why generation stopped. "length" means the
            # allowance ran out mid-sentence — the model never finished, and the
            # accumulated text is not an answer.
            if getattr(choices[0], "finish_reason", None):
                finish_reason = choices[0].finish_reason
            delta = choices[0].delta
            # `content` is a plain string on most endpoints, but Anthropic-style
            # reasoning models (Claude Fable 5) send a LIST of typed blocks and
            # MIX the two within one stream. Passing that list straight into
            # LLMStreamChunkEvent(chunk=...) — declared `chunk: str` — killed
            # every run with a pydantic string_type error, and appending it to
            # `chunks` would have put the reasoning block into the answer.
            text, reasoning = split_message_content(delta)
            if reasoning:
                self._add_reasoning(reasoning)
                # Don't stream the placeholder: it is a per-call fact, not a
                # chunk, and one event per delta is what produced the repeated
                # sentinel the user saw. The final LLMCallCompletedEvent carries
                # it once.
                if reasoning != REDACTED_REASONING:
                    event_bus.emit(
                        self,
                        LLMReasoningChunkEvent(
                            model=self.model,
                            reasoning=reasoning,
                            chunk_index=chunk_index,
                        ),
                    )
            if text:
                chunks.append(text)
                event_bus.emit(
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
        self._finish_reason = finish_reason
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

    # Delegates: the bodies live in response_parsing.py, but these names are a
    # subclass contract (DatabricksResponsesLLM calls them; its tests patch
    # them), so they stay methods.
    def _extract_chat_token_usage(self, response: Any) -> dict[str, Any] | None:
        return chat_token_usage(response)

    def _extract_function_calls_from_response(
        self, response: Any
    ) -> list[dict[str, Any]]:
        return parse_function_calls(response)

    # ----------------------------- responses api -----------------------------

    def _responses_text_format(self) -> dict[str, Any] | None:
        """``self.response_format`` as the Responses API's ``text.format``.

        Same intent as the ``response_format`` param on chat completions, but the
        two APIs disagree on the envelope: completions nests the schema under
        ``json_schema``, Responses puts ``name``/``schema`` at the top level of
        the format object. ``strict`` is what makes a required field actually
        required rather than a suggestion.

        Without this, ``response_format`` was accepted on the LLM and then never
        sent for anything on the Responses API — the whole GPT-5/Codex family.
        A schema that does not reach the endpoint is indistinguishable from no
        schema, so a caller ends up trusting fields the model was never obliged
        to return.

        Accepts what callers actually set: a pydantic model, a dict already in
        Responses shape, or one in completions shape (unwrapped here rather than
        ignored, since copying the chat-completions form is the easy mistake).
        Returns None only when nothing was requested or the dict is in neither
        shape — and then no ``text`` param is sent at all.
        """
        fmt = self.response_format
        if fmt is None:
            return None
        if isinstance(fmt, type) and issubclass(fmt, BaseModel):
            return {
                "type": "json_schema",
                "name": fmt.__name__,
                "schema": fmt.model_json_schema(),
                "strict": True,
            }
        if isinstance(fmt, dict):
            # Already a Responses-shaped format object.
            if "schema" in fmt or fmt.get("type") == "json_object":
                return fmt
            # A completions-shaped one — unwrap the nested envelope.
            nested = fmt.get("json_schema")
            if isinstance(nested, dict) and "schema" in nested:
                return {
                    "type": "json_schema",
                    "name": nested.get("name", "response"),
                    "schema": nested["schema"],
                    "strict": nested.get("strict", True),
                }
        return None

    def _prepare_responses_params(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        response_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"model": self.model, "input": messages}
        # First, for the same reason as the chat path: declared fields win.
        params.update(self.additional_params)
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
        # Structured output, spelled the Responses way. The chat-completions path
        # sends `response_format`; this API takes `text.format` with the schema
        # inline. Without this branch `response_format` was accepted on the LLM
        # and then silently dropped for the whole GPT-5/Codex family — which is
        # indistinguishable from having no schema at all, and is how a caller
        # ends up trusting fields the model never had to return.
        text_format = self._responses_text_format()
        if text_format is not None:
            params["text"] = {"format": text_format}
        if tools:
            params["tools"] = [
                (
                    {
                        "type": "function",
                        "name": t["function"]["name"],
                        "description": t["function"].get("description", ""),
                        "parameters": t["function"].get("parameters", {}),
                    }
                    if t.get("type") == "function" and "function" in t
                    else t
                )
                for t in tools
            ]
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
        # Per-call, like _finish_reason: a previous call's thinking must not be
        # reported against this one. Both API paths share the emit site.
        self._reasoning_text = ""
        rounds, deadline = self._execution_budget(from_agent)
        repeat_guard = RepeatGuard()
        for _round in range(rounds):
            self._check_deadline(deadline, _round, conversation)
            throttle(from_agent)
            self._trim_conversation_to_window(conversation, from_agent, tools)
            # Where THIS round's thinking starts — same reason as the chat
            # path: _reasoning_text is reset per CALL but appended per ROUND.
            reasoning_mark = len(self._reasoning_text)
            response = self._request_responses_round(conversation, tools, from_agent)
            usage = self._extract_responses_token_usage(response)
            self._track_token_usage_internal(usage)
            text, function_calls = self._handle_responses(response)
            if function_calls and not available_functions and self.delegate_tool_calls:
                # Same contract as the chat path above.
                return function_calls, usage, LLMCallType.TOOL_CALL
            if function_calls and available_functions:
                call_type = LLMCallType.TOOL_CALL
                # Same degenerate-loop breaker as the chat path.
                repeats = repeat_guard.observe(function_calls)
                if repeats >= 2:
                    stub_repeated_responses_round(conversation, function_calls)
                    if repeats >= 3:
                        tools = None
                    continue
                outcome = run_responses_round(
                    conversation,
                    function_calls,
                    self._executor(available_functions),
                    deadline,
                    available_functions,
                )
                if outcome.final_answer is not None:
                    return outcome.final_answer, usage, call_type
                if outcome.exhausted:
                    return self._answer_within_budget(
                        exhausted_mid_round(self.model, conversation),
                        conversation,
                        usage,
                    )
                continue
            answer = self._answer_or_recover(
                text, usage, self._reasoning_text[reasoning_mark:]
            )
            answer = answer_without_markup(answer, conversation)
            return answer, usage, call_type
        return self._answer_within_budget(
            rounds_exhausted(rounds, self.model, conversation), conversation, usage
        )

    def _request_responses_round(
        self,
        conversation: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        from_agent: Any = None,
    ) -> Any:
        """One Responses request — the chat path's twin, same recovery."""
        rejections = 0
        while True:
            try:
                return self.client.responses.create(
                    **self._prepare_responses_params(conversation, tools)
                )
            except Exception as e:
                rejections += 1
                if (
                    rejections > MAX_REJECTIONS_PER_ROUND
                    or not self._compact_after_rejection(
                        conversation, from_agent, tools, e
                    )
                ):
                    raise

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
        reasoning = responses_reasoning_text(response)
        if reasoning:
            self._add_reasoning(reasoning)
            event_bus.emit(
                self,
                LLMReasoningChunkEvent(model=self.model, reasoning=reasoning),
            )
        # `output_text` is an SDK PROPERTY that joins the `output_text` blocks
        # and returns "" when there are none (openai 2.32.0) — it is never None.
        # Testing `is None` therefore made this fallback dead code for every real
        # SDK object, so a response whose text sat somewhere other than a
        # `message`/`output_text` block read as empty.
        text = getattr(response, "output_text", None)
        if not text:
            chunks = []
            for item in getattr(response, "output", None) or []:
                for part in getattr(item, "content", None) or []:
                    part_text = getattr(part, "text", None)
                    if part_text:
                        chunks.append(part_text)
            text = "".join(chunks)
        return text, self._extract_function_calls_from_response(response)

    def _extract_responses_token_usage(self, response: Any) -> dict[str, Any] | None:
        return responses_token_usage(response)

    def _extract_reasoning_items(self, response: Any) -> list[Any]:
        return reasoning_items(response)

    def _extract_builtin_tool_outputs(self, response: Any) -> list[dict[str, Any]]:
        return builtin_tool_outputs(response)
