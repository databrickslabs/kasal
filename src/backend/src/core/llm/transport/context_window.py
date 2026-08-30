"""Context-window budgeting for ``OpenAICompletion``: estimate, clamp, trim, recover.

One budget, four consumers. ``prompt + max_tokens <= window`` is what the server
enforces; the pieces here are the client-side halves of that inequality —
estimating the prompt (``_estimate_tokens``), shrinking the output request to
fit (``_clamp_output_budget``), stubbing old tool results BEFORE the request so
the prompt fits (``_trim_conversation_to_window``), and stubbing AFTER the
server has rejected it anyway (``_compact_after_rejection``, over
``context_recovery``).

A mixin rather than a module of functions because every step reads the same
per-model facts — the registered window, the configured output size, the
per-instance estimate correction — and ``OpenAICompletion`` is where those
live. It carries no pydantic fields of its own: the attributes it reads are
declared on the model class, and the ``TYPE_CHECKING`` block only tells mypy so.

Split out of ``completion.py`` when the reactive path pushed that module past
the 1,500-line ceiling; the code moved unchanged, commentary and all.
"""

import logging
from typing import TYPE_CHECKING, Any

from src.core.events.bus import event_bus
from src.core.events.types import ContextCompactionEvent

from .constants import CONTEXT_WINDOW_USAGE_RATIO, LLM_CONTEXT_WINDOW_SIZES
from .context_recovery import (
    MAX_ESTIMATE_CORRECTION,
    reported_prompt_tokens,
    stub_oldest_tool_results,
)
from .exceptions import is_context_length_exceeded

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


class ContextWindowBudget:
    """See the module docstring. Mixed into ``OpenAICompletion``."""

    if TYPE_CHECKING:
        model: str
        max_tokens: int | None
        max_completion_tokens: int | None
        _estimate_correction: float

        def get_context_window_size(self) -> int: ...

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

        The margin is a constant guess at the estimator's drift; once the server
        has REJECTED a prompt it has stated the drift exactly, and that ratio
        (``_estimate_correction``) divides the budget from then on. The margin
        was calibrated on 2.7 chars/token; the failing run's escaped Cyrillic
        measured 1.4, which no fixed margin should be asked to cover.
        """
        budget = self._input_budget(from_agent)
        if not budget:
            return 0
        return int(budget * _TRIM_ESTIMATE_MARGIN / self._estimate_correction)

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
        compacted = stub_oldest_tool_results(conversation, estimated_tokens, window)
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
        strategy: str = "tool_result_stub",
        reason: str | None = None,
    ) -> None:
        """Announce a compaction on the event bus. Never raises — observability
        must not be able to fail a run."""
        try:
            event_bus.emit(
                self,
                ContextCompactionEvent(
                    model=self.model,
                    from_agent=from_agent,
                    strategy=strategy,
                    tokens_before=tokens_before,
                    tokens_after=tokens_after,
                    window=window,
                    messages_compacted=messages_compacted,
                    reason=reason
                    or (
                        f"conversation reached ~{tokens_before} tokens against a "
                        f"{window}-token budget; {messages_compacted} of the oldest "
                        f"tool result(s) replaced with a stub"
                    ),
                ),
            )
        except Exception:  # noqa: BLE001
            pass

    def _compact_after_rejection(
        self,
        conversation: list[dict[str, Any]],
        from_agent: Any,
        tools: list[dict[str, Any]] | None,
        error: Exception,
    ) -> bool:
        """The server refused the prompt as too long after the trim said it fit.

        Stub the oldest tool results — sized by the server's own count when its
        message carries one, halved otherwise — and say whether the round is
        worth retrying. False means "not a context overflow", "the agent opted
        out of trimming", or "nothing left to stub"; the caller re-raises.

        The server's count also calibrates the estimator for the rest of this
        LLM's life (``_estimate_correction``), so the proactive trim starts
        firing where the server draws the line rather than a rejection every
        few rounds. A ratio above ``MAX_ESTIMATE_CORRECTION`` — or one that does
        not even exceed the estimate — is not believed; those halve instead.
        """
        if not is_context_length_exceeded(error):
            return False
        if (
            from_agent is not None
            and getattr(from_agent, "respect_context_window", True) is False
        ):
            return False

        def estimated_tokens() -> int:
            return self._estimate_tokens(conversation, tools)

        tokens_before = estimated_tokens()
        reported = reported_prompt_tokens(str(error))
        ratio = reported / tokens_before if reported and tokens_before else 0.0
        calibrated = 1.0 < ratio <= MAX_ESTIMATE_CORRECTION
        if calibrated:
            self._estimate_correction = max(self._estimate_correction, ratio)
        budget = self._trim_budget(from_agent) if calibrated else 0
        target = budget if 0 < budget < tokens_before else tokens_before // 2
        compacted = stub_oldest_tool_results(conversation, estimated_tokens, target)
        if not compacted:
            return False
        tokens_after = estimated_tokens()
        logger.warning(
            "context rejection: model=%s server counted %s prompt tokens where the "
            "estimate said %d (x%.2f); stubbed %d tool result(s) down to ~%d and "
            "retrying the round",
            self.model,
            reported if reported else "?",
            tokens_before,
            ratio,
            compacted,
            tokens_after,
        )
        self._emit_compaction(
            tokens_before=tokens_before,
            tokens_after=tokens_after,
            window=target,
            messages_compacted=compacted,
            from_agent=from_agent,
            strategy="tool_result_stub_after_rejection",
            reason=(
                f"server rejected a prompt the estimate put at ~{tokens_before} "
                f"tokens (it counted {reported or 'more'}); {compacted} of the "
                f"oldest tool result(s) replaced with a stub, round retried"
            ),
        )
        return True
