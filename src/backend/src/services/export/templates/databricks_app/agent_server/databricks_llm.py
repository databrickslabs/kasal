"""Databricks endpoint policy for the exported app.

Kasal drives Databricks serving endpoints through ``DatabricksRetryLLM``
(``src/services/llm/handlers/databricks_retry_llm.py``). That class cannot be
vendored: it reaches into ``LLMManager``, ``UserContext``, ``databricks_auth``
and the model catalogue at seven call sites, none of which exist standalone.

So this is the standalone equivalent, following the shape
``services/llm/handlers/vllm.py`` already uses — a thin subclass of the vendored
transport's ``LLM`` that adds only what a Databricks endpoint needs. The
transport itself (tool-call loop, streaming, context trimming, usage accounting,
events) is shared code, not reimplemented here.

**Ported from ``DatabricksRetryLLM``, and why each one is not optional:**

- *Message sanitization.* Claude-backed Databricks endpoints reject an assistant
  message whose ``content`` is empty when it carries ``tool_calls``, and reject a
  conversation that ends on an assistant turn. Both arise naturally from a
  tool-calling loop. Without this, a Claude crew 400s mid-run.
- *``cache_breakpoint`` stripping.* Non-Claude endpoints (llama, qwen, gemma,
  gpt-oss) 400 with ``unknown field "cache_breakpoint"``.
- *Retry with backoff.* Databricks sheds capacity under load — 429, 503, and a
  ``TEMPORARILY_UNAVAILABLE`` error code carrying no numeric status. A completion
  is idempotent, so retrying is safe, and rate limits get longer backoffs
  because the quota window is ~60s.
- *Llama message alternation.* Llama 4 wants the last message to be a user turn.

**Deliberately NOT ported:** model fallback (needs the model catalogue) and
OTel retry spans (Phase 4 covers tracing). Token refresh IS here, via the
``token_provider`` hook, because Databricks Apps rotate credentials and a run
long enough to outlive one is ordinary.

Divergence risk is real and is why every rule above names its canonical source.
When ``databricks_retry_llm.py`` changes, this file is what needs the matching
change.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, ClassVar, Optional

from agent_server.kasal_runtime.core.llm.transport import (
    LLM,
    LLMContextLengthExceededError,
)

logger = logging.getLogger(__name__)

# Stand-in content for a tool-call-only assistant message (Databricks rejects an
# empty one). Canonical: databricks_retry_llm.TOOL_CALL_PLACEHOLDER.
TOOL_CALL_PLACEHOLDER = "Calling tools."

_CONTINUE = "Please continue with your response."

# Canonical: DatabricksRetryLLM._is_rate_limit_error.
_RATE_LIMIT_TERMS = (
    "rate limit",
    "ratelimit",
    "too many requests",
    "429",
    "request_limit_exceeded",
    "rate_limit_exceeded",
)

# Canonical: DatabricksRetryLLM._is_retryable_error. Note the non-numeric ones —
# Databricks capacity shedding reports TEMPORARILY_UNAVAILABLE with no status
# code, and an upstream 5xx surfaces as a bare INTERNAL_ERROR payload.
_RETRYABLE_TERMS = _RATE_LIMIT_TERMS + (
    "timeout",
    "connection",
    "service unavailable",
    "serviceunavailable",
    "temporarily_unavailable",
    "capacity constraints",
    "503",
    "502",
    "504",
    "gateway",
    "internalservererror",
    "internal_error",
    "invalid response from an upstream server",
    "bad gateway",
)

_AUTH_TERMS = ("401", "403", "invalid access token", "unauthorized", "expired")


def sanitize_messages_for_databricks(messages: Any) -> Any:
    """Make a conversation acceptable to Databricks-served Claude endpoints.

    Mutates ``messages`` in place (callers may hold the same list) and returns
    it. Canonical: ``DatabricksRetryLLM._sanitize_messages_for_databricks``.
    """
    if not messages or not isinstance(messages, list):
        return messages

    # Only Claude's native caching understands `cache_breakpoint`; every other
    # Databricks endpoint 400s on the unknown field. Replace the dict rather
    # than mutating it — the caller's copy may be shared.
    for idx, m in enumerate(messages):
        if isinstance(m, dict) and "cache_breakpoint" in m:
            messages[idx] = {k: v for k, v in m.items() if k != "cache_breakpoint"}

    i = 0
    while i < len(messages):
        msg = messages[i]
        if not isinstance(msg, dict) or msg.get("role") != "assistant":
            i += 1
            continue
        content = msg.get("content")
        empty = content is None or (isinstance(content, str) and not content.strip())
        if empty and msg.get("tool_calls"):
            messages[i] = {**msg, "content": TOOL_CALL_PLACEHOLDER}
            i += 1
        elif empty:
            messages.pop(i)  # an empty, tool-less assistant turn says nothing
        else:
            i += 1

    # Claude on Databricks does not support assistant prefill: the conversation
    # must end on a user turn.
    if messages and isinstance(messages[-1], dict):
        if messages[-1].get("role") == "assistant":
            messages.append({"role": "user", "content": _CONTINUE})
    return messages


class DatabricksLLM(LLM):
    """A Databricks serving endpoint, with the retries and message fixes it needs.

    ``token_provider`` is an optional zero-arg callable returning a fresh bearer
    token; it is consulted once per run of retries when a call fails on auth.
    """

    MAX_RETRIES: ClassVar[int] = 3
    INITIAL_BACKOFF: ClassVar[float] = 1.0
    RATE_LIMIT_MAX_RETRIES: ClassVar[int] = 5
    RATE_LIMIT_INITIAL_BACKOFF: ClassVar[float] = 30.0
    RATE_LIMIT_MAX_BACKOFF: ClassVar[float] = 120.0

    token_provider: Optional[Callable[[], str]] = None

    # ---------------------------------------------------------------- helpers

    @property
    def _endpoint(self) -> str:
        """The endpoint name, without the ``databricks/`` provider prefix."""
        return str(self.model or "").split("/")[-1]

    def supports_stop_words(self) -> bool:
        """The base class already returns False for ``gpt-5``; this also covers
        an endpoint named ``gpt5`` with no hyphen. Canonical:
        ``DatabricksRetryLLM.supports_stop_words``."""
        lowered = self._endpoint.lower()
        if "gpt-5" in lowered or "gpt5" in lowered:
            return False
        return super().supports_stop_words()

    @staticmethod
    def _is_rate_limit(error: str) -> bool:
        return any(term in error for term in _RATE_LIMIT_TERMS)

    @staticmethod
    def _is_retryable(error: str) -> bool:
        return any(term in error for term in _RETRYABLE_TERMS)

    @staticmethod
    def _is_auth_error(error: str) -> bool:
        return any(term in error for term in _AUTH_TERMS)

    def _backoff(self, attempt: int, rate_limited: bool) -> float:
        if rate_limited:
            return min(
                self.RATE_LIMIT_INITIAL_BACKOFF * (2**attempt),
                self.RATE_LIMIT_MAX_BACKOFF,
            )
        return self.INITIAL_BACKOFF * (2**attempt)

    def _max_retries_for(self, rate_limited: bool) -> int:
        return self.RATE_LIMIT_MAX_RETRIES if rate_limited else self.MAX_RETRIES

    def _fix_llama_alternation(self, messages: Any) -> Any:
        """Llama 4 wants the conversation to end on a user turn. Applied ONLY to
        llama endpoints — other families have their own rules and adding a
        trailing user turn to them changes the prompt for nothing. Canonical:
        ``DatabricksRetryLLM._fix_message_format_for_llama``."""
        if not messages or not isinstance(messages, list):
            return messages
        if "llama" not in self._endpoint.lower():
            return messages
        if isinstance(messages[-1], dict) and messages[-1].get("role") == "assistant":
            return [*messages, {"role": "user", "content": _CONTINUE}]
        return messages

    def _prepare_conversation(self, messages: Any) -> Any:
        if isinstance(messages, str):
            return messages
        return self._fix_llama_alternation(sanitize_messages_for_databricks(messages))

    def _refresh_token(self) -> bool:
        """Swap in a fresh bearer token and drop the cached client that holds the
        stale one. Returns True when the token actually changed."""
        if self.token_provider is None:
            return False
        try:
            fresh = self.token_provider()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[DatabricksLLM] token refresh failed: {exc}")
            return False
        if not fresh or fresh == self.api_key:
            return False
        self.api_key = fresh
        self._client = None  # force the openai client to be rebuilt
        logger.info(
            "[DatabricksLLM] refreshed the Databricks token after an auth error"
        )
        return True

    # ------------------------------------------------------------------ call

    def call(
        self,
        messages: Any,
        tools: Any = None,
        callbacks: Any = None,
        available_functions: Any = None,
        from_task: Any = None,
        from_agent: Any = None,
        response_model: Any = None,
    ) -> str:
        """Sanitize, then call with retry/backoff on transient Databricks errors.

        Context-length errors are re-raised immediately: they are deterministic,
        so retrying only burns the wall clock the run has left.

        The full parameter list is spelled out rather than forwarded as
        ``*args, **kwargs`` ON PURPOSE. ``runtime/executor.call_llm`` builds its
        kwargs from ``inspect.signature(llm.call).parameters`` — under a
        ``**kwargs`` signature it sees only ``messages``, and silently drops
        ``tools`` and ``available_functions``. The agent would still run, still
        answer, and never call a tool.
        """
        conversation = self._prepare_conversation(messages)
        attempt = 0
        refreshed = False
        while True:
            try:
                return super().call(
                    conversation,
                    tools=tools,
                    callbacks=callbacks,
                    available_functions=available_functions,
                    from_task=from_task,
                    from_agent=from_agent,
                    response_model=response_model,
                )
            except LLMContextLengthExceededError:
                raise
            except Exception as exc:  # noqa: BLE001
                error = str(exc).lower()
                if self._is_auth_error(error) and not refreshed:
                    refreshed = True
                    if self._refresh_token():
                        continue
                if not self._is_retryable(error):
                    raise
                rate_limited = self._is_rate_limit(error)
                if attempt >= self._max_retries_for(rate_limited) - 1:
                    raise
                delay = self._backoff(attempt, rate_limited)
                logger.warning(
                    f"[DatabricksLLM] {self._endpoint} failed "
                    f"({'rate limit' if rate_limited else 'transient'}), "
                    f"retrying in {delay:.0f}s: {exc}"
                )
                time.sleep(delay)
                attempt += 1

    async def acall(
        self,
        messages: Any,
        tools: Any = None,
        callbacks: Any = None,
        available_functions: Any = None,
        from_task: Any = None,
        from_agent: Any = None,
        response_model: Any = None,
    ) -> str:
        """Async entry point — inherited behaviour, spelled out.

        ``OpenAICompletion.acall`` is ``asyncio.to_thread(self.call, ...)``, so
        overriding ``call`` above already covers the retry path. Kept explicit so
        the signature matches (see the note on ``call``).
        """
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
