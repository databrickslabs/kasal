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
import time
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, PrivateAttr

from src.core.events.bus import event_bus
from src.core.events.types import (
    ContextCompactionEvent,
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
from .constants import CONTEXT_WINDOW_USAGE_RATIO, LLM_CONTEXT_WINDOW_SIZES
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
from .tool_rounds import run_chat_round, run_responses_round

logger = logging.getLogger(__name__)


#: Characters per token assumed when estimating a prompt's size.
#:
#: chars/4 is the prose rule of thumb; tool-calling conversations are JSON and
#: tokenize denser. Measured against vLLM's own count on two rejected Qwen
#: requests, chars/4 was ~15% low. 3.4 covers that without being so pessimistic
#: that a normal turn gets compacted for nothing.
_CHARS_PER_TOKEN = 3.4

#: Tokens held back from every budget calculation.
#:
#: Servers count a few tokens nobody models client-side: the chat template's
#: role scaffolding and the generation prompt appended after the last message.
#: Both observed failures overflowed by EXACTLY one token — the budget maths was
#: right up to that scaffolding — so the reserve exists to make equality safe
#: rather than to cover a large error.
_WINDOW_SAFETY_TOKENS = 128

#: How much of the input budget the trim will actually fill.
#:
#: Not a second derate of the window — a discount on our own ESTIMATE, which
#: undercounts whenever the content is denser than ``_CHARS_PER_TOKEN`` assumes
#: (German compounds, CJK, base64, minified JSON). The output clamp already
#: reserves 15% for the same drift; this is that reservation applied to the
#: other half of the same budget.
#:
#: 0.8 rather than the clamp's 0.85, and the difference is load-bearing. Work
#: the failing run's numbers with a REGISTERED window: 131,072 - 8,192 output
#: - 128 scaffolding = 122,752, and 122,752 estimated tokens is ~131,389 real
#: ones at the 2.7 chars/token that content actually measured — still over the
#: server's 131,072, so 0.85 would have kept the bug for any model whose window
#: is known. 0.8 gives ~123,700, which fits.
_TRIM_ESTIMATE_MARGIN = 0.8

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
        if not (text and text.strip()):
            raise error
        logger.warning("budget spent (%s); answered from what was gathered", error)
        return text, usage, LLMCallType.LLM_CALL

    def _estimate_tokens(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None
    ) -> int:
        """Rough token count for a message list (chars/4), tools included.

        Deliberately ONE estimator for both directions — the input trim below and
        the output clamp in ``_prepare_completion_params`` are two halves of the
        same budget (``prompt + max_tokens <= window``), and two different
        estimates of "how big is this prompt" can disagree enough to trim what
        did not need trimming while still overflowing the request.

        Tool/function schemas count toward the server-side prompt, so they are
        included when given.

        Divides by ``_CHARS_PER_TOKEN`` rather than a flat 4: the classic chars/4
        rule is calibrated on prose, and these conversations are mostly tool JSON
        — braces, quotes, ids and numbers all tokenize denser than English. Two
        observed vLLM rejections had the server counting 15% more tokens than
        chars/4 predicted, both overflowing by exactly one token. The estimate is
        a BUDGETING input, so it must err high; an over-estimate compacts a
        little early, an under-estimate fails the request outright.
        """
        total = 0
        for message in messages:
            if not isinstance(message, dict):
                continue
            for key in ("content", "output"):
                value = message.get(key)
                if isinstance(value, str):
                    total += len(value)
                elif isinstance(value, list):
                    # Structured content blocks (e.g. cache_control parts).
                    total += len(str(value))
            if message.get("tool_calls"):
                total += len(str(message["tool_calls"]))
        if tools:
            total += len(str(tools))
        return int(total / _CHARS_PER_TOKEN)

    def _raw_context_window(self) -> int:
        """The model's FULL advertised window, 0 when unknown.

        ``get_context_window_size()`` returns the 0.85-derated figure used for
        trimming decisions. The output clamp needs the real number: the limit it
        protects against is the server's own ``prompt + max_tokens <=
        max-model-len``, and derating twice would shrink outputs for no reason.
        """
        if not self._model_window_is_known():
            return 0
        return int(self.get_context_window_size() / CONTEXT_WINDOW_USAGE_RATIO)

    def _clamp_output_budget(self, params: dict[str, Any]) -> None:
        """Shrink the requested output so ``prompt + max_tokens`` fits the window.

        Servers that enforce the sum (vLLM and other self-hosted OpenAI-compatible
        backends, notably) return a 400 — "passed N input and requested M output"
        — rather than truncating. ``max_tokens`` comes from the model config's
        ``max_output_tokens`` and knows nothing about the ACTUAL prompt size, so a
        large prompt plus a full output request overflows.

        Only ever reduces, never grows, and no-ops when the window is unknown or
        the request already fits — so a model with room to spare is untouched.

        The margin is generous on purpose: a chars/4 estimate systematically
        UNDERCOUNTS a real tokenizer (observed 1-5% on Qwen, varying with
        content), and vLLM 400s on a one-token overrun. Tight margins (256, then
        1024/5%) each came up short in practice; 15% with a 2048 floor covers the
        drift. A small model cannot do both a huge prompt and a huge output — this
        trades output headroom for never 400ing.
        """
        key = (
            "max_completion_tokens"
            if "max_completion_tokens" in params
            else "max_tokens"
        )
        want = params.get(key)
        window = self._raw_context_window()
        if not want or not window:
            return
        input_tokens = self._estimate_tokens(
            params.get("messages") or [], params.get("tools")
        )
        margin = max(2048, int(input_tokens * 0.15))
        allowed = window - input_tokens - margin - _WINDOW_SAFETY_TOKENS
        if allowed < want:
            params[key] = max(256, allowed)
            logger.warning(
                "output clamp: model=%s input~=%d + %s=%d > window=%d; clamped to %d",
                self.model,
                input_tokens,
                key,
                want,
                window,
                params[key],
            )

    def _model_window_is_known(self) -> bool:
        """Does LLM_CONTEXT_WINDOW_SIZES actually know this model?

        Mirrors BaseLLM.get_context_window_size's exact-then-substring lookup.
        Needed because that method returns DEFAULT_CONTEXT_WINDOW_SIZE for an
        unknown model, indistinguishable from a model genuinely sized at 8192.
        """
        if self.model in LLM_CONTEXT_WINDOW_SIZES:
            return True
        return any(
            self.model.startswith(key) or key in self.model
            for key in LLM_CONTEXT_WINDOW_SIZES
        )

    def _effective_context_window(self, from_agent: Any = None) -> int:
        """Window to trim against.

        The model table stays authoritative when it KNOWS the model — an agent
        may only claim a window the provider cannot honour, and trimming too
        late is a hard request failure rather than a degraded one.

        It is the UNKNOWN case that needs help: an unregistered model silently
        gets DEFAULT_CONTEXT_WINDOW_SIZE (8192 → 6963 after the 0.85 derate).
        For a self-hosted model that can be off by 4x, so the trim shreds tool
        results the agent still needs. ``src.services.llm.manager`` registers every
        configured model at import and covers the common path, but nothing
        guarantees it ran — a direct engine embedding, or a model added to an
        agent but not to MODEL_CONFIGS, both land here. When the table has no
        opinion, the agent's explicitly configured size is the better estimate
        than a hardcoded 8192.
        """
        if not self._model_window_is_known():
            configured = (
                getattr(from_agent, "max_context_window_size", None)
                if from_agent
                else None
            )
            if isinstance(configured, int) and configured > 0:
                return int(configured * CONTEXT_WINDOW_USAGE_RATIO)
        return self.get_context_window_size()

    def _input_budget(self, from_agent: Any = None) -> int:
        """How many prompt tokens may actually be sent. 0 when unknown.

        The server enforces ``prompt + max_tokens <= window``, so the room for
        the PROMPT is the window minus the output we are about to ask for — not
        the 0.85-derated window the trim used to compare against.

        The difference is what made a run unrecoverable. With a 28,672 window and
        an 8,192 output request, compaction triggered at 24,371 (0.85 x window)
        while the server would only serve 20,480. A conversation between those
        two numbers was too big to serve and too small to compact: every attempt
        was rejected, the agent retried at the same size, and the run looped
        until it failed.

        Falls back to the derated window when no output size is configured —
        there is no reservation to subtract, and the derate remains a sane
        default.
        """
        reserved_output = self.max_completion_tokens or self.max_tokens or 0
        if not reserved_output:
            return self._effective_context_window(from_agent)

        raw = self._raw_context_window()
        if not raw:
            # Window unknown to the table: _effective_context_window may still
            # have the agent's own figure, which is already derated.
            return self._effective_context_window(from_agent)

        budget = raw - reserved_output - _WINDOW_SAFETY_TOKENS
        # A configured output larger than the window itself would leave nothing.
        # Keep a floor so the trim degrades to "compact hard" instead of "delete
        # every tool result and still be over".
        return max(budget, int(raw * 0.25))

    def _trim_budget(self, from_agent: Any = None) -> int:
        """``_input_budget`` with room for the estimator being wrong.

        The estimate is chars/``_CHARS_PER_TOKEN``, and it is a GUESS. The output
        clamp has always allowed for that with a 15% margin; the trim compared
        against the raw budget, so the two halves of one budget disagreed by
        exactly the amount the estimator drifts.

        What that cost, on a run whose whole point was tool output: a German /
        Swiss job search tokenized nearer 2.7 chars per token than 3.4, so an
        111,411-token ceiling meant roughly 140,000 real ones. The server's limit
        was 131,072. Every round the trim decided the conversation fit; the
        request was rejected; nothing was ever compacted, and the run died with
        52 tool results (~900,000 characters) it was allowed to stub and never
        did.

        Erring low costs a little context the agent could have kept. Erring high
        costs the entire run.
        """
        budget = self._input_budget(from_agent)
        if not budget:
            return 0
        return int(budget * _TRIM_ESTIMATE_MARGIN)

    def _trim_conversation_to_window(
        self,
        conversation: list[dict[str, Any]],
        from_agent: Any = None,
        tools: list[dict[str, Any]] | None = None,
    ) -> None:
        """Best-effort in-place trim so a tool-heavy turn cannot overflow the
        context window (which previously failed the whole run): once the
        estimated size exceeds what the server will actually accept as a prompt
        (see ``_input_budget``), the OLDEST tool results are replaced with a stub
        — never the system prompt, user messages, or tool_call structure (pairing
        must survive). Honors Agent.respect_context_window (default on).

        Compaction is lossy, so every trim that actually drops something emits a
        ContextCompactionEvent — it used to happen with no trace at all, which
        made the resulting re-query loop impossible to diagnose from the UI.

        ``tools`` is not optional in spirit: the schemas count toward the
        server-side prompt, and leaving them out is what let a 139,516-token
        request reach a 131,072-token server while this method concluded, every
        round, that the conversation fit. The estimator has always accepted them
        — the output clamp passes them, this call site did not — which is the
        exact disagreement ``_estimate_tokens`` documents as the thing to avoid.
        """
        if (
            from_agent is not None
            and getattr(from_agent, "respect_context_window", True) is False
        ):
            return
        window = self._trim_budget(from_agent)
        if not window:
            return

        def estimated_tokens() -> int:
            return self._estimate_tokens(conversation, tools)

        tokens_before = estimated_tokens()
        if tokens_before <= window:
            return
        stub = "[earlier tool result trimmed to fit the context window]"
        compacted = 0
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
            compacted += 1
            if estimated_tokens() <= window:
                break
        if compacted:
            self._emit_compaction(
                tokens_before=tokens_before,
                tokens_after=estimated_tokens(),
                window=window,
                messages_compacted=compacted,
                from_agent=from_agent,
            )

    def _emit_compaction(
        self,
        *,
        tokens_before: int,
        tokens_after: int,
        window: int,
        messages_compacted: int,
        from_agent: Any = None,
    ) -> None:
        """Announce a compaction on the event bus. Never raises — observability
        must not be able to fail a run."""
        try:
            event_bus.emit(
                self,
                ContextCompactionEvent(
                    model=self.model,
                    from_agent=from_agent,
                    strategy="tool_result_stub",
                    tokens_before=tokens_before,
                    tokens_after=tokens_after,
                    window=window,
                    messages_compacted=messages_compacted,
                    reason=(
                        f"conversation reached ~{tokens_before} tokens against a "
                        f"{window}-token budget; {messages_compacted} of the oldest "
                        f"tool result(s) replaced with a stub"
                    ),
                ),
            )
        except Exception:  # noqa: BLE001
            pass

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
            params = self._prepare_completion_params(conversation, tools)
            if self.stream:
                content, usage, function_calls = self._stream_chat_completion(params)
            else:
                response = self.client.chat.completions.create(**params)
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
                self._finish_reason = getattr(
                    response.choices[0], "finish_reason", None
                )
            if function_calls and available_functions:
                call_type = LLMCallType.TOOL_CALL
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
            return (
                self._answer_or_recover(
                    content, usage, self._reasoning_text[reasoning_mark:]
                ),
                usage,
                call_type,
            )
        return self._answer_within_budget(
            rounds_exhausted(rounds, self.model, conversation), conversation, usage
        )

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
        for _round in range(rounds):
            self._check_deadline(deadline, _round, conversation)
            throttle(from_agent)
            self._trim_conversation_to_window(conversation, from_agent, tools)
            # Where THIS round's thinking starts — same reason as the chat
            # path: _reasoning_text is reset per CALL but appended per ROUND.
            reasoning_mark = len(self._reasoning_text)
            response = self.client.responses.create(
                **self._prepare_responses_params(conversation, tools)
            )
            usage = self._extract_responses_token_usage(response)
            self._track_token_usage_internal(usage)
            text, function_calls = self._handle_responses(response)
            if function_calls and available_functions:
                call_type = LLMCallType.TOOL_CALL
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
            return (
                self._answer_or_recover(
                    text, usage, self._reasoning_text[reasoning_mark:]
                ),
                usage,
                call_type,
            )
        return self._answer_within_budget(
            rounds_exhausted(rounds, self.model, conversation), conversation, usage
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
