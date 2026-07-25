"""
Databricks GPT-OSS Handler

This module provides specialized handling for Databricks GPT-OSS models which have
unique response formats that differ from standard OpenAI-compatible models.

GPT-OSS models return content as a list with reasoning blocks and text blocks,
rather than a simple string, which requires special handling for CrewAI integration.
"""

import asyncio
import concurrent.futures
import re
import time as _time_mod
from typing import Any, ClassVar, Dict, List, Optional, Union
from kasal_engine.llm import LLM
from kasal_engine.llm import LLMContextLengthExceededError
import litellm

# Use centralized logger
from src.core.logger import get_logger

# Configure logger using centralized configuration
logger = get_logger(__name__)


def _get_retry_tracer():
    """Lazily obtain an OTel tracer for LLM retry instrumentation.

    Returns ``None`` when OpenTelemetry is not installed or no global
    TracerProvider has been configured (e.g. outside subprocess execution).
    The caller must handle ``None`` gracefully (no-op).
    """
    try:
        from opentelemetry import trace as _otel_trace

        tracer = _otel_trace.get_tracer("kasal.llm.retry")
        # If no real provider is set the tracer will be a no-op proxy;
        # that is fine – spans simply won't be exported.
        return tracer
    except Exception:
        return None


# DatabricksGPTOSSHandler lived here: a Harmony-format text extractor plus
# apply_monkey_patch(), which rewrote
# litellm.llms.databricks.chat.transformation.DatabricksConfig.extract_content_str
# and .extract_reasoning_content so gpt-oss reasoning blocks (which arrive with
# no "signature" field) would parse. It patched litellm's Databricks provider,
# which the engine never invokes, so it had no effect on any live call — and the
# models it existed for (databricks-gpt-oss-20b/120b) are on the REMOVED_MODELS
# prune list in seeds/model_configs.py. Recover it from git if a gpt-oss endpoint
# is ever re-seeded; it would need reimplementing against the engine's response
# handling rather than litellm's.


# Placeholder injected into tool-call-only assistant HISTORY messages — the
# Databricks API rejects assistant messages whose content is empty when they
# carry tool_calls (see _sanitize_messages_for_databricks). After many tool
# turns the model — Haiku especially — can MIMIC the pattern and emit this
# placeholder as its actual final text answer (observed: crews answering
# literally "Calling tools."). Any response that is just this placeholder is
# therefore treated as EMPTY and retried with a corrective nudge.
TOOL_CALL_PLACEHOLDER = "Calling tools."

_PLACEHOLDER_NUDGE = (
    'Your previous reply was just "Calling tools." — that is a placeholder, '
    "not an answer. Using the tool results already gathered above, write your "
    "complete final answer now. Do not repeat that phrase."
)


def _is_placeholder_response(response) -> bool:
    """True when the model's text answer is only the tool-call placeholder."""
    if not isinstance(response, str):
        return False
    return response.strip().rstrip(".").strip().lower() == "calling tools"


def _append_placeholder_nudge(messages) -> None:
    """Append the corrective nudge (once) so the retry breaks the mimicry."""
    if not isinstance(messages, list):
        return
    last = messages[-1] if messages else None
    if isinstance(last, dict) and last.get("content") == _PLACEHOLDER_NUDGE:
        return
    messages.append({"role": "user", "content": _PLACEHOLDER_NUDGE})


# Sentinel distinguishing "no fallback was taken" from a real (possibly falsy)
# fallback result, used by DatabricksRetryLLM's model-fallback helpers.
_NO_FALLBACK = object()


def _run_coro_sync(coro):
    """Run an async coroutine to completion from a synchronous context.

    DatabricksRetryLLM.call() runs in a CrewAI worker thread (no running event
    loop), so asyncio.run works directly; the ThreadPoolExecutor branch is a
    safety net for the rare case a loop is already running on this thread.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(coro)).result()


class DatabricksRetryLLM(LLM):
    """
    Custom LLM wrapper for Databricks models that adds retry logic for empty responses.

    Databricks models (including Llama 4 Maverick) can intermittently return empty responses,
    especially after many tool iterations. This wrapper retries the call with exponential
    backoff when an empty response is detected.

    Rate limit errors use longer backoffs (30s base) since Databricks rate limits
    typically reset after 60 seconds.
    """

    # CrewAI 1.13+ made ``LLM`` a Pydantic BaseModel, so class-level constants
    # must be annotated as ClassVar to avoid being mistaken for model fields.

    # Standard retry settings (for timeouts, connection errors, empty responses)
    MAX_RETRIES: ClassVar[int] = 3
    INITIAL_BACKOFF: ClassVar[float] = 1.0  # seconds

    # Rate limit specific settings - longer backoffs to allow quota reset
    RATE_LIMIT_MAX_RETRIES: ClassVar[int] = 5
    RATE_LIMIT_INITIAL_BACKOFF: ClassVar[float] = (
        30.0  # Databricks rate limits reset ~60s
    )
    RATE_LIMIT_MAX_BACKOFF: ClassVar[float] = 120.0  # cap at 2 minutes

    # Request timeout - prevents hanging on unresponsive endpoints.
    # litellm default is 6000s (100 min) which is way too long.
    # Databricks server-side limit is 297s, so we match it.
    REQUEST_TIMEOUT: ClassVar[float] = 297.0

    # NOTE: DatabricksRetryLLM intentionally does NOT report
    # supports_native_structured_output. Enforcing output_pydantic on the litellm
    # path routes structured output through instructor/response_model, which (a)
    # conflicts with tool-calling agents and (b) biases the model to fill the
    # schema directly instead of running the ReAct tool loop — observed as Claude
    # crews no longer calling their MCP/search tools. Databricks chat models
    # therefore keep the soft output_json downgrade (which leaves the tool loop
    # intact). Only the codex/Responses-API handler claims native support, because
    # it enforces the schema on a separate channel AND forces tool_choice, so
    # tools + structured output coexist. For deterministic structured output WITH
    # tools on Databricks, use a Responses-API (codex) model.

    def __init__(self, **kwargs):
        """Initialize the Databricks Retry LLM wrapper."""
        # Set default timeout if not provided to prevent hanging requests
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.REQUEST_TIMEOUT

        # ONE retry policy, and it is this class's. The engine hands `max_retries`
        # to the OpenAI SDK client, which retries internally with its own backoff
        # — underneath the loop below, which cannot see it. At the default of 2
        # that turned a rate-limited call into (2+1) x 5 = 15 HTTP attempts, each
        # outer attempt paying the SDK's backoff before ours. The wrapper's
        # retryable set already covers what the SDK would retry (timeout,
        # connection, 429, 5xx) and its backoff is tuned to how Databricks quota
        # actually resets, so the inner layer is switched off. An explicit
        # max_retries still wins, for callers that want the SDK behaviour.
        kwargs.setdefault("max_retries", 0)

        # A global `litellm.request_timeout = REQUEST_TIMEOUT` used to be set here,
        # because litellm's Databricks provider ignored the per-call timeout. The
        # engine does not call litellm at all — `timeout` above is handed straight
        # to the OpenAI client that OpenAICompletion builds — so the global did
        # nothing but mutate process-wide litellm state.

        super().__init__(**kwargs)
        self._original_model_name = kwargs.get("model", "")
        timeout_val = kwargs.get("timeout", self.REQUEST_TIMEOUT)

        # Capture group_id at init time (main thread / request context) so
        # _try_refresh_token() can pass it explicitly to get_auth_context().
        # Background threads don't propagate contextvars, so without this the
        # PAT lookup from DB fails and the fallback silently gives up.
        self._group_id: str | None = None
        try:
            from src.utils.user_context import UserContext

            ctx = UserContext.get_group_context()
            if ctx and hasattr(ctx, "primary_group_id"):
                self._group_id = ctx.primary_group_id
        except Exception:
            pass

        # Model-fallback state. When a call fails with a model-swappable error
        # (context-window exceeded, fatal model 4xx, sustained rate limit) we
        # rebuild another enabled model and delegate to it instead of failing.
        # Candidates are loaded lazily on first need (keeps the happy path free
        # of an extra DB query). _active_fallback, once set, short-circuits all
        # later calls so we don't re-fail on the original model every turn.
        self._fallback_candidates = None  # lazy: None=unloaded, []=none available
        self._tried_models = {self._current_model_key()}
        self._fallback_llm_cache: Dict[str, Any] = {}
        self._active_fallback = None

        logger.info(
            f"Initialized DatabricksRetryLLM wrapper for model: {self._original_model_name} (timeout: {timeout_val}s, litellm.request_timeout: {litellm.request_timeout}s)"
        )

    def _current_model_key(self) -> str:
        """The bare model key (no provider prefix) for the model in use."""
        return str(getattr(self, "model", "") or self._original_model_name).split("/")[-1]

    # ---- model fallback -------------------------------------------------

    def _ensure_fallback_candidates(self):
        """Lazily load the enabled-model candidate list (once)."""
        if self._fallback_candidates is None:
            try:
                from src.core.llm_manager import LLMManager

                self._fallback_candidates = _run_coro_sync(
                    LLMManager.load_fallback_candidates(
                        self._current_model_key(), self._group_id
                    )
                )
            except Exception as e:
                self._get_crew_logger().warning(
                    f"[DatabricksRetryLLM] could not load fallback candidates: {e}"
                )
                self._fallback_candidates = []
        return self._fallback_candidates

    def _select_fallback(self, candidates, reason):
        """Choose the next model for ``reason`` given what's already been tried."""
        from src.core.llm.handlers.model_fallback import select_fallback

        current_window = 0
        try:
            from kasal_engine.llm import LLM_CONTEXT_WINDOW_SIZES

            current_window = LLM_CONTEXT_WINDOW_SIZES.get(getattr(self, "model", ""), 0) or 0
        except Exception:
            pass
        return select_fallback(
            candidates,
            current_window,
            reason,
            self._tried_models,
            current_model=self._current_model_key(),
        )

    def _emit_fallback_span(self, reason, candidate, method):
        """Surface the model switch in the trace (mirrors _emit_retry_span)."""
        try:
            self._emit_retry_span(
                attempt=len(self._tried_models),
                max_retries=len(self._tried_models),
                backoff=0.0,
                error_type=f"model_fallback:{reason}",
                error_message=f"Falling back to model '{candidate.name}' ({reason})",
                is_rate_limit=(reason == "rate_limit"),
                method=method,
            )
        except Exception:
            pass

    def _build_fallback_llm(self, candidate):
        """Build (and cache) a fully-configured LLM for ``candidate`` via the
        normal config path, so it reuses the correct per-model params/auth.
        Returns None if it can't be built (e.g. no group_id)."""
        if candidate.name in self._fallback_llm_cache:
            return self._fallback_llm_cache[candidate.name]
        if not self._group_id:
            self._get_crew_logger().warning(
                "[DatabricksRetryLLM] cannot build fallback LLM without group_id"
            )
            return None
        try:
            from src.core.llm_manager import LLMManager

            llm = _run_coro_sync(
                LLMManager.configure_kasal_llm(candidate.name, self._group_id)
            )
            self._disable_nested_fallback(llm)
            self._fallback_llm_cache[candidate.name] = llm
            return llm
        except Exception as e:
            self._get_crew_logger().error(
                f"[DatabricksRetryLLM] failed to build fallback LLM '{candidate.name}': {e}"
            )
            return None

    async def _abuild_fallback_llm(self, candidate):
        """Async variant of _build_fallback_llm (no event-loop juggling)."""
        if candidate.name in self._fallback_llm_cache:
            return self._fallback_llm_cache[candidate.name]
        if not self._group_id:
            return None
        try:
            from src.core.llm_manager import LLMManager

            llm = await LLMManager.configure_kasal_llm(candidate.name, self._group_id)
            self._disable_nested_fallback(llm)
            self._fallback_llm_cache[candidate.name] = llm
            return llm
        except Exception as e:
            self._get_crew_logger().error(
                f"[DatabricksRetryLLM] failed to build fallback LLM '{candidate.name}': {e}"
            )
            return None

    @staticmethod
    def _disable_nested_fallback(llm):
        """Stop a fallback LLM from spawning its own fallbacks — the original
        wrapper owns the chain and tracks what's been tried."""
        try:
            llm._fallback_candidates = []
        except Exception:
            pass

    # supports_function_calling() used to be overridden here to return True,
    # because litellm's registry reported None for Databricks-hosted models and
    # crewAI would then fall back to the ReAct text pattern (which GPT-5 does not
    # follow — it hallucinates tool results instead of emitting Action:). The
    # engine's BaseLLM returns True for every model and consults no registry, so
    # the override said nothing the base class did not already say.
    # Infinite tool-call loops are still bounded by call()'s MAX_TOOL_CALLS.

    def supports_stop_words(self) -> bool:
        """Check if this model supports the 'stop' parameter.

        Mostly redundant with ``OpenAICompletion.supports_stop_words`` (which
        already returns False when "gpt-5" appears in the model id). Kept for the
        one case it covers and the base does not: an endpoint named "gpt5" with no
        hyphen. Delegates otherwise.
        """
        model_lower = self._original_model_name.lower()
        if "gpt-5" in model_lower or "gpt5" in model_lower:
            return False
        return super().supports_stop_words()

    def _get_crew_logger(self):
        """Get the crew logger for subprocess-compatible logging."""
        try:
            from src.core.logger import LoggerManager

            return LoggerManager.get_instance().crew
        except Exception:
            return logger  # Fallback to module logger

    def _is_rate_limit_error(self, error_str: str) -> bool:
        """Check if an error string indicates a rate limit error.

        Args:
            error_str: Lowercase error string to check

        Returns:
            True if this is a rate limit error
        """
        return any(
            term in error_str
            for term in [
                "rate limit",
                "ratelimit",
                "too many requests",
                "429",
                "request_limit_exceeded",
                "rate_limit_exceeded",
            ]
        )

    def _is_retryable_error(self, error_str: str) -> bool:
        """Check if an error is retryable (including rate limits).

        Args:
            error_str: Lowercase error string to check

        Returns:
            True if this error should be retried
        """
        return any(
            term in error_str
            for term in [
                "timeout",
                "connection",
                "rate limit",
                "ratelimit",
                "too many requests",
                "service unavailable",
                # litellm.ServiceUnavailableError lowercases WITHOUT a space, and
                # Databricks capacity shedding reports error_code
                # TEMPORARILY_UNAVAILABLE with no numeric status in the message
                # (seen on brand-new FMAPI models like claude-fable-5).
                "serviceunavailable",
                "temporarily_unavailable",
                "capacity constraints",
                "503",
                "429",
                "502",
                "504",
                "gateway",
                "request_limit_exceeded",
                # Databricks model serving 5xx: litellm maps an upstream 502/500
                # into an InternalServerError whose string is
                # 'databricksException - {"error_code":"INTERNAL_ERROR", ...}' and
                # contains none of the numeric codes above. These are transient
                # (endpoint overload / cold start / upstream hiccup) and safe to
                # retry — a completion is idempotent.
                "internalservererror",
                "internal_error",
                "invalid response from an upstream server",
                "bad gateway",
            ]
        )

    def _context_length_hint(self, error_str: str) -> Optional[str]:
        """Actionable message when the prompt exceeds the model's context window
        (commonly a tool such as ScrapeWebsiteTool returning a whole web page).
        Returns None when the error is not a context-length error. Retrying the
        same oversized prompt can't help, so we surface guidance instead.

        The returned text deliberately starts with "context length exceeded" —
        recovery (summarize-and-continue under respect_context_window) is
        triggered by PHRASE-matching str(exception) against CONTEXT_LIMIT_ERRORS,
        not by exception type alone.

        Matching is delegated to src/core/llm/context_limits, which extends the
        engine's shared phrase list. This method used to keep a private tuple of
        markers, so a phrase learned here never reached the engine's own recovery
        path (and vice versa)."""
        from src.core.llm.context_limits import is_context_limit_error

        if is_context_limit_error(error_str):
            return (
                "Context length exceeded: the crew's input exceeded the model's "
                "context window. This usually means a tool returned too much "
                "content (e.g. ScrapeWebsiteTool scraped an entire web page). "
                "Reduce tool output — scrape fewer / smaller pages, or split the "
                "work into more focused tasks — then retry."
            )
        return None

    def _is_auth_error(self, error_str: str) -> bool:
        """Check if an error indicates an expired or invalid token.

        Args:
            error_str: Lowercase error string to check

        Returns:
            True if this is an authentication/token error
        """
        return any(
            term in error_str
            for term in [
                "invalid token",
                "invalid access token",
                "token expired",
                "token is expired",
                "authenticationerror",
                "401",
                "unauthorized",
            ]
        )

    def _try_refresh_token(self) -> bool:
        """Attempt to refresh the api_key by falling back to PAT or SPN auth.

        When the OBO token expires mid-execution, this method tries to obtain
        a fresh token via the auth chain (PAT → SPN) and updates self.api_key.

        Returns:
            True if a new token was obtained and set, False otherwise.
        """
        crew_log = self._get_crew_logger()
        try:
            import asyncio
            from src.utils.databricks_auth import get_auth_context

            # get_auth_context is async; run it in a new event loop if needed
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            # Pass group_id explicitly so get_auth_context() can find the PAT
            # from the DB even in a background thread where UserContext context
            # vars are not propagated (Python contextvars copy-on-task-create
            # but NOT copy-on-thread-create).
            gid = self._group_id

            if loop and loop.is_running():
                # We're inside an async context — use a thread to avoid nesting
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    auth_ctx = pool.submit(
                        lambda: asyncio.run(
                            get_auth_context(user_token=None, group_id=gid)
                        )
                    ).result(timeout=15)
            else:
                auth_ctx = asyncio.run(get_auth_context(user_token=None, group_id=gid))

            if auth_ctx and auth_ctx.token and auth_ctx.token != self.api_key:
                old_method = "obo"  # the one that just failed
                crew_log.info(
                    f"[DatabricksRetryLLM] Token refresh: {old_method} → {auth_ctx.auth_method} "
                    f"(token length: {len(auth_ctx.token)})"
                )
                self.api_key = auth_ctx.token
                return True
            else:
                crew_log.warning(
                    "[DatabricksRetryLLM] Token refresh: no alternative token available"
                )
                return False
        except Exception as refresh_err:
            crew_log.warning(
                f"[DatabricksRetryLLM] Token refresh failed: {refresh_err}"
            )
            return False

    def _get_backoff_time(self, attempt: int, is_rate_limit: bool) -> float:
        """Calculate backoff time based on error type and attempt number.

        Rate limit errors use longer backoffs (30s, 60s, 120s) to allow
        Databricks quota to reset (~60 seconds).

        Other errors use standard short backoffs (1s, 2s, 4s).

        Args:
            attempt: Current attempt number (0-indexed)
            is_rate_limit: Whether this is a rate limit error

        Returns:
            Backoff time in seconds
        """
        if is_rate_limit:
            backoff = self.RATE_LIMIT_INITIAL_BACKOFF * (2**attempt)
            return min(backoff, self.RATE_LIMIT_MAX_BACKOFF)
        else:
            return self.INITIAL_BACKOFF * (2**attempt)

    def _get_max_retries(self, is_rate_limit: bool) -> int:
        """Get max retries based on error type.

        Rate limit errors get more retries since they just need time to reset.

        Args:
            is_rate_limit: Whether this is a rate limit error

        Returns:
            Maximum number of retry attempts
        """
        return self.RATE_LIMIT_MAX_RETRIES if is_rate_limit else self.MAX_RETRIES

    def _emit_retry_span(
        self,
        attempt: int,
        max_retries: int,
        backoff: float,
        error_type: str,
        error_message: str,
        is_rate_limit: bool,
        method: str,
    ) -> None:
        """Emit an OTel span covering the retry backoff wait.

        The span duration matches the ``time.sleep(backoff)`` so the trace
        timeline visually shows each wait period.  Attributes carry enough
        detail to diagnose retry storms from the trace UI alone.

        Args:
            attempt: Current 0-indexed attempt number.
            max_retries: Maximum retries configured for this error category.
            backoff: Backoff duration in seconds about to be slept.
            error_type: "rate_limit", "empty_response", or "retryable_error".
            error_message: Truncated error description.
            is_rate_limit: Whether this was classified as a rate-limit error.
            method: Originating method ("call" or "_handle_non_streaming_response").
        """
        tracer = _get_retry_tracer()
        if tracer is None:
            # OTel not available – just sleep without tracing
            _time_mod.sleep(backoff)
            return

        try:
            from opentelemetry.trace import StatusCode

            with tracer.start_as_current_span("kasal.llm.retry") as span:
                span.set_attribute("kasal.event_type", "llm_retry")
                span.set_attribute("kasal.retry.attempt", attempt + 1)
                span.set_attribute("kasal.retry.max_retries", max_retries)
                span.set_attribute("kasal.retry.backoff_seconds", backoff)
                span.set_attribute("kasal.retry.error_type", error_type)
                span.set_attribute("kasal.retry.is_rate_limit", is_rate_limit)
                span.set_attribute("kasal.retry.method", method)
                span.set_attribute("kasal.retry.model", self._original_model_name)
                if error_message:
                    span.set_attribute("kasal.retry.error_message", error_message[:500])
                span.set_status(StatusCode.OK, f"Retry backoff {backoff}s")

                # The sleep happens *inside* the span so the span duration
                # matches the actual wait – making it visible in the timeline.
                _time_mod.sleep(backoff)
        except Exception:
            # Never let tracing failures interrupt the retry logic
            _time_mod.sleep(backoff)

    def _record_retry_summary(
        self, total_attempts: int, total_backoff: float, method: str
    ) -> None:
        """Add a span event summarising the retry sequence on the current span.

        This is a lightweight annotation: if an outer span is active (e.g.
        from the CrewAI event bridge) the summary will appear as an event on
        that span.  If there is no active span this is a no-op.
        """
        try:
            from opentelemetry import trace as _otel_trace

            current = _otel_trace.get_current_span()
            if current and current.is_recording():
                current.add_event(
                    "llm_retry_summary",
                    attributes={
                        "kasal.retry.total_attempts": total_attempts,
                        "kasal.retry.total_backoff_seconds": total_backoff,
                        "kasal.retry.model": self._original_model_name,
                        "kasal.retry.method": method,
                    },
                )
        except Exception:
            pass  # tracing must never break the hot path

    @staticmethod
    def _sanitize_messages_for_databricks(messages):
        """Fix messages that would be rejected by Databricks API.

        Databricks (Claude-based endpoints) rejects:
        1. Assistant messages where ``content`` is None/empty when the message also carries ``tool_calls``
        2. Conversations that end with an assistant message (Claude requires ending with user message)

        CrewAI's retry logic can produce such messages when a tool-call-only
        response fails validation and is re-sent as conversation history.

        This method modifies the list **in-place** so that callers holding a
        reference to the original list (e.g. after a shallow dict copy) still
        see the fixes.  Returns the same list for convenience.
        """
        if not messages or not isinstance(messages, list):
            return messages

        # CrewAI stamps a top-level ``cache_breakpoint`` flag on messages for
        # prompt caching, but Databricks serving endpoints reject it as an
        # unknown field: only Claude's native caching understands it and litellm
        # does NOT translate it for the ``databricks/`` provider, so non-Claude
        # endpoints (llama, qwen, gemma, gpt-oss, gemini) 400 with
        # 'Bad request: json: unknown field "cache_breakpoint"'. Strip it from a
        # copy of each message (don't mutate CrewAI's originals) before sending.
        for idx, m in enumerate(messages):
            if isinstance(m, dict) and "cache_breakpoint" in m:
                messages[idx] = {k: v for k, v in m.items() if k != "cache_breakpoint"}

        i = 0
        while i < len(messages):
            msg = messages[i]
            if not isinstance(msg, dict):
                i += 1
                continue

            if msg.get("role") == "assistant":
                content = msg.get("content")
                has_tool_calls = bool(msg.get("tool_calls"))
                content_is_empty = content is None or (
                    isinstance(content, str) and not content.strip()
                )

                if content_is_empty and has_tool_calls:
                    messages[i] = {**msg, "content": TOOL_CALL_PLACEHOLDER}
                    i += 1
                elif content_is_empty and not has_tool_calls:
                    messages.pop(i)
                else:
                    i += 1
            else:
                i += 1

        # CRITICAL FIX: Claude models through Databricks do not support "assistant message prefill"
        # This means conversations MUST end with a user message, not an assistant message.
        # If the last message is from the assistant, add a continuation prompt.
        if messages and messages[-1].get("role") == "assistant":
            logger.info(
                "[DatabricksRetryLLM] Claude model detected: conversation ends with assistant message. "
                "Adding user continuation prompt to satisfy Claude API requirements."
            )
            messages.append(
                {"role": "user", "content": "Please continue with your response."}
            )

        return messages

    def _fix_message_format_for_llama(self, messages, crew_log):
        """
        Fix message format for Llama models only.

        Llama 4 (like Mistral) requires:
        1. Messages to alternate between user and assistant
        2. Last message should be 'user' role (not 'assistant')

        This fix is NOT applied to other models (Claude, Qwen, DBRX, etc.)
        which have their own message format requirements.

        Returns fixed messages list.
        """
        if not messages or not isinstance(messages, list):
            return messages

        # Only apply fix for Llama models - other models (Claude, Qwen, etc.) don't need it
        model_lower = self._original_model_name.lower()
        if "llama" not in model_lower:
            return messages

        # Check if last message is 'assistant' - Llama doesn't like this
        if messages[-1].get("role") == "assistant":
            crew_log.info(
                "[DatabricksRetryLLM] Fixing message format for Llama: adding user continuation prompt"
            )
            return [
                *messages,
                {"role": "user", "content": "Please continue with your response."},
            ]

        return messages

    def _maybe_model_fallback(self, exc, method, call_kwargs):
        """On a model-swappable failure, switch to another enabled model and
        return its result; otherwise return the _NO_FALLBACK sentinel so the
        caller preserves its existing error handling. Synchronous path.

        Cascades: if the chosen fallback ITSELF fails with a swappable reason
        (notably ENDPOINT_NOT_FOUND — the model isn't deployed in this
        workspace), it moves on to the next candidate instead of dying. This is
        what keeps a run alive when the context-window fallback target (e.g.
        databricks-gemini-2-5-flash) is enabled in config but not served here.
        """
        from src.core.llm.handlers.model_fallback import classify_llm_error

        reason = classify_llm_error(exc)
        if not reason:
            return _NO_FALLBACK
        # Bounded cascade: try successive candidates until one answers or none remain.
        max_hops = 4
        for _ in range(max_hops):
            candidate = self._select_fallback(self._ensure_fallback_candidates(), reason)
            if candidate is None:
                return _NO_FALLBACK
            self._tried_models.add(candidate.name)  # mark tried BEFORE call, so a failing one is skipped next hop
            fallback_llm = self._build_fallback_llm(candidate)
            if fallback_llm is None:
                continue
            self._active_fallback = fallback_llm
            self._emit_fallback_span(reason, candidate, method)
            self._get_crew_logger().warning(
                f"[DatabricksRetryLLM] model fallback ({reason}): "
                f"{self._current_model_key()} -> {candidate.name}"
            )
            try:
                return fallback_llm.call(**call_kwargs)
            except Exception as fb_exc:  # noqa: BLE001
                fb_reason = classify_llm_error(fb_exc)
                if not fb_reason:
                    raise  # a non-swappable error from the fallback → surface it
                from src.core.llm.handlers.model_fallback import (
                    ENDPOINT_MISSING,
                    mark_endpoint_missing,
                )
                if fb_reason == ENDPOINT_MISSING:
                    # This model has no serving endpoint here — remember it so it's
                    # never offered again (this run or later), preventing the
                    # rate-limit→undeployed-endpoint cascade from recurring.
                    mark_endpoint_missing(candidate.name)
                self._get_crew_logger().warning(
                    f"[DatabricksRetryLLM] fallback candidate {candidate.name} failed "
                    f"({fb_reason}); trying next candidate"
                )
                reason = fb_reason
                continue
        return _NO_FALLBACK

    async def _amaybe_model_fallback(self, exc, method, call_kwargs):
        """Async variant of _maybe_model_fallback (cascades on swappable failures)."""
        from src.core.llm.handlers.model_fallback import classify_llm_error

        reason = classify_llm_error(exc)
        if not reason:
            return _NO_FALLBACK
        max_hops = 4
        for _ in range(max_hops):
            candidate = self._select_fallback(self._ensure_fallback_candidates(), reason)
            if candidate is None:
                return _NO_FALLBACK
            self._tried_models.add(candidate.name)  # mark tried BEFORE call
            fallback_llm = await self._abuild_fallback_llm(candidate)
            if fallback_llm is None:
                continue
            self._active_fallback = fallback_llm
            self._emit_fallback_span(reason, candidate, method)
            self._get_crew_logger().warning(
                f"[DatabricksRetryLLM] model fallback ({reason}): "
                f"{self._current_model_key()} -> {candidate.name}"
            )
            try:
                return await fallback_llm.acall(**call_kwargs)
            except Exception as fb_exc:  # noqa: BLE001
                fb_reason = classify_llm_error(fb_exc)
                if not fb_reason:
                    raise
                from src.core.llm.handlers.model_fallback import (
                    ENDPOINT_MISSING,
                    mark_endpoint_missing,
                )
                if fb_reason == ENDPOINT_MISSING:
                    mark_endpoint_missing(candidate.name)
                self._get_crew_logger().warning(
                    f"[DatabricksRetryLLM] fallback candidate {candidate.name} failed "
                    f"({fb_reason}); trying next candidate"
                )
                reason = fb_reason
                continue
        return _NO_FALLBACK

    def _coerce_to_response_model(self, result, kwargs):
        """Parse a JSON-string result into ``response_model``.

        The parsing itself lives in ``BaseLLM._validate_structured_output`` — the
        engine honours ``response_model`` for every provider now, so this is only
        the adapter for the ``**kwargs`` shape this wrapper's callers use, and no
        longer a second implementation. Returns ``result`` untouched when there is
        no model to parse into.
        """
        rm = kwargs.get("response_model")
        if rm is None or not isinstance(result, str):
            return result
        return self._validate_structured_output(result, rm)

    def call(
        self,
        messages,
        tools=None,
        callbacks=None,
        available_functions=None,
        from_task=None,
        from_agent=None,
        **kwargs,  # Accept additional kwargs for CrewAI 1.9.x compatibility (e.g., response_model)
    ):
        """
        Override the call method to add retry logic for empty responses.

        When Databricks returns an empty response (which happens intermittently,
        especially after many tool iterations), we retry with exponential backoff.
        Also fixes message format for Llama 4 compatibility.

        Rate limit errors get special treatment with longer backoffs (30s, 60s, 120s)
        and more retries (5 vs 3) since they just need time for quota to reset.

        Retry attempts are emitted as OTel spans so they appear in the trace
        timeline.  Each span covers the backoff wait period.

        Note: kwargs accepts additional parameters like response_model (CrewAI 1.9.x structured outputs)
        """
        crew_log = self._get_crew_logger()

        # Already fell back to a working model on a previous turn — keep using it.
        if self._active_fallback is not None:
            return self._active_fallback.call(
                messages,
                tools=tools,
                callbacks=callbacks,
                available_functions=available_functions,
                from_task=from_task,
                from_agent=from_agent,
                **kwargs,
            )

        last_error = None
        is_rate_limit = False
        attempt = 0
        total_backoff = 0.0

        # Fix message format for Llama 4 before making calls
        fixed_messages = self._fix_message_format_for_llama(messages, crew_log)
        # Sanitize empty content blocks that Databricks API rejects
        fixed_messages = self._sanitize_messages_for_databricks(fixed_messages)
        # Gemini-on-Databricks quirks. These ran inside a litellm.completion monkey
        # patch, which the engine bypasses entirely — so a Databricks-served Gemini
        # model was reaching the endpoint unsanitized: multiple system prompts (only
        # one is supported) and $defs/$ref in tool schemas (rejected). Applying them
        # on the path the request actually takes is both the fix and the reason the
        # patch could be deleted.
        _merge_system_messages_for_gemini(fixed_messages, str(getattr(self, "model", "")))
        if tools and isinstance(tools, list):
            _sanitize_tools_for_gemini(tools, str(getattr(self, "model", "")))
        msg_count = len(fixed_messages) if isinstance(fixed_messages, list) else 1

        # --- Tool-call limiter: prevent infinite tool-calling loops ---
        # Count tool-result messages in the conversation history.
        # After MAX_TOOL_CALLS tool results, strip tools from the request
        # to force the model to produce a text answer instead of more tool_calls.
        # This is critical for GPT-5 which keeps requesting tool_calls indefinitely.
        MAX_TOOL_CALLS = 8
        if tools and isinstance(fixed_messages, list):
            tool_result_count = sum(
                1
                for m in fixed_messages
                if (isinstance(m, dict) and m.get("role") == "tool")
                or (hasattr(m, "role") and getattr(m, "role", None) == "tool")
            )
            if tool_result_count >= MAX_TOOL_CALLS:
                crew_log.warning(
                    f"[DatabricksRetryLLM] Reached {tool_result_count} tool results "
                    f"(limit: {MAX_TOOL_CALLS}). Stripping tools to force final text answer."
                )
                tools = None

        while True:
            max_retries = self._get_max_retries(is_rate_limit)

            if attempt >= max_retries:
                break

            try:
                crew_log.info(
                    f"[DatabricksRetryLLM] call() attempt {attempt + 1}/{max_retries} with {msg_count} messages"
                )

                # Call the parent class method with all arguments (including new kwargs like response_model)
                result = super().call(
                    fixed_messages,
                    tools=tools,
                    callbacks=callbacks,
                    available_functions=available_functions,
                    from_task=from_task,
                    from_agent=from_agent,
                    **kwargs,
                )

                # Check if response is empty — a bare "Calling tools."
                # placeholder echo is treated the same (it is never an answer).
                is_placeholder = _is_placeholder_response(result)
                if result is None or result == "" or is_placeholder:
                    if attempt < max_retries - 1:
                        if is_placeholder:
                            _append_placeholder_nudge(fixed_messages)
                        backoff = self._get_backoff_time(attempt, is_rate_limit=False)
                        crew_log.warning(
                            f"[DatabricksRetryLLM] "
                            f"{'Placeholder (Calling tools.)' if is_placeholder else 'Empty'} "
                            f"response (attempt {attempt + 1}/{max_retries}). "
                            f"Retrying in {backoff}s..."
                        )
                        self._emit_retry_span(
                            attempt=attempt,
                            max_retries=max_retries,
                            backoff=backoff,
                            error_type="empty_response",
                            error_message=(
                                "LLM echoed the tool-call placeholder"
                                if is_placeholder
                                else "LLM returned empty response"
                            ),
                            is_rate_limit=False,
                            method="call",
                        )
                        total_backoff += backoff
                        attempt += 1
                        continue
                    else:
                        crew_log.error(
                            f"[DatabricksRetryLLM] Empty response after {max_retries} attempts - failing"
                        )
                        if attempt > 0:
                            self._record_retry_summary(
                                attempt + 1, total_backoff, "call"
                            )
                        return ""

                # Success
                if attempt > 0:
                    self._record_retry_summary(attempt + 1, total_backoff, "call")
                crew_log.info(
                    f"[DatabricksRetryLLM] Success, response length: {len(str(result))}"
                )
                # Structured-output callers (e.g. memory consolidation) expect a
                # response_model instance, not a JSON string — coerce it here.
                return self._coerce_to_response_model(result, kwargs)

            except Exception as e:
                last_error = e
                error_str = str(e).lower()

                # Check error type
                is_retryable = self._is_retryable_error(error_str)
                is_rate_limit = self._is_rate_limit_error(error_str)

                # Update max_retries based on error type (rate limits get more retries)
                max_retries = self._get_max_retries(is_rate_limit)

                if is_retryable and attempt < max_retries - 1:
                    backoff = self._get_backoff_time(attempt, is_rate_limit)
                    error_type_label = (
                        "rate_limit" if is_rate_limit else "retryable_error"
                    )
                    crew_log.warning(
                        f"[DatabricksRetryLLM] {error_type_label} (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {backoff}s..."
                    )
                    self._emit_retry_span(
                        attempt=attempt,
                        max_retries=max_retries,
                        backoff=backoff,
                        error_type=error_type_label,
                        error_message=str(e),
                        is_rate_limit=is_rate_limit,
                        method="call",
                    )
                    total_backoff += backoff
                    attempt += 1
                    continue
                else:
                    # Auth errors: try refreshing token (OBO → PAT/SPN fallback)
                    if self._is_auth_error(error_str) and self._try_refresh_token():
                        crew_log.info(
                            f"[DatabricksRetryLLM] Retrying after token refresh (attempt {attempt + 1})"
                        )
                        attempt += 1
                        continue
                    # Model fallback: switch to another enabled model before
                    # giving up (context-window exceeded, fatal model 4xx, or
                    # rate-limit after same-model backoff). For a context-window
                    # error with no larger model available, this returns the
                    # sentinel and we fall through to CrewAI's summarization.
                    fb = self._maybe_model_fallback(
                        e,
                        "call",
                        {
                            "messages": fixed_messages,
                            "tools": tools,
                            "callbacks": callbacks,
                            "available_functions": available_functions,
                            "from_task": from_task,
                            "from_agent": from_agent,
                            **kwargs,
                        },
                    )
                    if fb is not _NO_FALLBACK:
                        return fb
                    hint = self._context_length_hint(error_str)
                    if hint:
                        crew_log.error(
                            f"[DatabricksRetryLLM] Context window exceeded: {e}"
                        )
                        # Must be CrewAI's context-length exception (with a
                        # CONTEXT_LIMIT_ERRORS-matching message) so that
                        # respect_context_window summarization fires instead of
                        # the agent replaying the whole task max_retry times.
                        raise LLMContextLengthExceededError(hint) from e
                    crew_log.error(f"[DatabricksRetryLLM] Non-retryable error: {e}")
                    if attempt > 0:
                        self._record_retry_summary(attempt + 1, total_backoff, "call")
                    raise

        # If we get here, we've exhausted retries
        if attempt > 0:
            self._record_retry_summary(attempt, total_backoff, "call")
        if last_error:
            raise last_error
        return ""

    async def acall(
        self,
        messages,
        tools=None,
        callbacks=None,
        available_functions=None,
        from_task=None,
        from_agent=None,
        **kwargs,  # e.g. response_model (CrewAI structured outputs)
    ):
        """Async counterpart of call() with model fallback.

        The base LLM.acall (used by CrewAI's context-window summarization,
        among others) bypassed this wrapper entirely, so async failures — most
        importantly a summarization prompt that itself blows the context window
        — fell straight through with no fallback. This override closes that gap:
        it applies the same message sanitization and, on a model-swappable
        error, delegates to another enabled model.
        """
        crew_log = self._get_crew_logger()

        if self._active_fallback is not None:
            return await self._active_fallback.acall(
                messages,
                tools=tools,
                callbacks=callbacks,
                available_functions=available_functions,
                from_task=from_task,
                from_agent=from_agent,
                **kwargs,
            )

        fixed_messages = self._fix_message_format_for_llama(messages, crew_log)
        fixed_messages = self._sanitize_messages_for_databricks(fixed_messages)

        try:
            result = await super().acall(
                fixed_messages,
                tools=tools,
                callbacks=callbacks,
                available_functions=available_functions,
                from_task=from_task,
                from_agent=from_agent,
                **kwargs,
            )
            # Coerce a JSON-string structured-output result into its response_model
            # (parity with call()), so structured-output callers get an instance.
            return self._coerce_to_response_model(result, kwargs)
        except Exception as e:
            fb = await self._amaybe_model_fallback(
                e,
                "acall",
                {
                    "messages": fixed_messages,
                    "tools": tools,
                    "callbacks": callbacks,
                    "available_functions": available_functions,
                    "from_task": from_task,
                    "from_agent": from_agent,
                    **kwargs,
                },
            )
            if fb is not _NO_FALLBACK:
                return fb
            raise

    # NOTE: the crewAI-era _handle_non_streaming_response override is gone:
    # kasal_engine's LLM funnels every completion through call()/acall(),
    # which this class already wraps with retry + fallback logic.


# NOTE: the crewAI-era apply_tool_calls_fix() patch is obsolete under
# kasal_engine: its completions loop executes tool_calls whenever they are
# present, even when the response also carries content text
# (see kasal_engine.llm.completion.OpenAICompletion._call_completions_api).



def _resolve_schema_refs(schema):
    """Recursively resolve ``$ref`` references in a JSON Schema and remove ``$defs``.

    Gemini models served via Databricks reject tool parameter schemas that
    contain ``$defs`` and ``$ref`` (standard JSON Schema features).  This
    helper inlines every ``$ref`` and strips ``$defs`` so the schema is
    self-contained.
    """
    if not isinstance(schema, dict):
        return schema

    defs = schema.get("$defs") or schema.get("definitions") or {}

    def _resolve(node):
        if isinstance(node, dict):
            if "$ref" in node:
                ref_path = node["$ref"]  # e.g. "#/$defs/Foo"
                ref_name = ref_path.rsplit("/", 1)[-1]
                resolved = defs.get(ref_name, {})
                # Merge any sibling keys (e.g. description) with the resolved def
                merged = {
                    **_resolve(resolved),
                    **{k: v for k, v in node.items() if k != "$ref"},
                }
                return merged
            return {
                k: _resolve(v)
                for k, v in node.items()
                if k not in ("$defs", "definitions")
            }
        if isinstance(node, list):
            return [_resolve(item) for item in node]
        return node

    return _resolve(schema)


def _is_gemini_model(model: str) -> bool:
    """Check whether a model name refers to a Gemini model on Databricks."""
    if not model:
        return False
    return "gemini" in model.lower()


def _merge_system_messages_for_gemini(messages, model):
    """Merge multiple system messages into one for Gemini models.

    Gemini models on Databricks reject conversations with more than one
    system prompt.  CrewAI builds messages with multiple system entries
    (agent backstory, task instructions, etc.).  This helper collapses
    them into a single system message placed at the start of the list.

    Modifies ``messages`` **in-place** and returns it for convenience.
    """
    if not messages or not isinstance(messages, list) or not _is_gemini_model(model):
        return messages

    system_contents = []
    non_system = []
    for msg in messages:
        if isinstance(msg, dict) and msg.get("role") == "system":
            content = msg.get("content", "")
            if content:
                system_contents.append(
                    content if isinstance(content, str) else str(content)
                )
        else:
            non_system.append(msg)

    if len(system_contents) <= 1:
        # Zero or one system message – nothing to merge
        return messages

    logger.info(
        f"[Gemini] Merging {len(system_contents)} system messages into one "
        f"({sum(len(c) for c in system_contents)} chars total)"
    )

    merged = {"role": "system", "content": "\n\n".join(system_contents)}
    messages.clear()
    messages.append(merged)
    messages.extend(non_system)
    return messages


def _sanitize_tools_for_gemini(tools, model):
    """Remove ``$defs``/``$ref`` from tool function schemas for Gemini models.

    Modifies the tools list **in-place** when the model is Gemini.
    """
    if not tools or not _is_gemini_model(model):
        return

    for tool in tools:
        if not isinstance(tool, dict):
            continue
        func = tool.get("function") or {}
        params = func.get("parameters")
        if (
            params
            and isinstance(params, dict)
            and ("$defs" in params or "$ref" in params or "definitions" in params)
        ):
            func["parameters"] = _resolve_schema_refs(params)


# `apply_empty_content_fix()` used to live here and ran at import: it wrapped
# litellm.completion to sanitize empty assistant content plus Gemini tool
# schemas / system messages "at the litellm level [so] every code path is
# covered". No code path goes through litellm any more — the engine talks to
# the endpoint with the OpenAI SDK — so it covered nothing. Its three fixes now
# run inside DatabricksRetryLLM.call(), on the path requests actually take.
# LLMManager.completion_with_usage, the one remaining direct litellm caller,
# applies _sanitize_messages_for_databricks itself.
