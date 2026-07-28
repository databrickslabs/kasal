"""Token-usage telemetry, sourced from engine LLM events.

Databricks usage attribution (Partner Well-Architected Framework) used to be
collected by a litellm ``CustomLogger`` registered on ``litellm.callbacks`` /
``litellm.success_callback``. The engine does not call litellm — it drives
endpoints with the OpenAI SDK — so those callbacks stopped firing for every crew,
flow and chat call the moment the engine landed. Only ``completion_with_usage``,
the one remaining direct litellm caller, still reported.

The engine already counts exactly what telemetry needs: ``LLMCallCompletedEvent``
carries the per-call ``usage`` dict, and ``BaseLLM`` accumulates the same numbers
for ``Crew.token_usage``. So this listens on the bus instead of re-deriving
anything — one source of truth for token counts, one place that forwards them.

Registration is idempotent and happens at ``llm_manager`` import, which every
process that runs an LLM performs (API process and execution subprocesses alike).
"""

import asyncio
import logging
import os
from typing import Any, Optional

from src.core.events import LLMCallCompletedEvent
from src.core.events.bus import event_bus

logger = logging.getLogger(__name__)

_registered = False


def _resolve_user_token() -> Optional[str]:
    """User token for OBO telemetry, or None.

    Contextvars do not propagate into the worker threads LLM calls run on in
    subprocess execution, hence the module-level fallback that
    ``llm_manager.set_subprocess_user_token`` populates.
    """
    from src.core.llm_manager import _subprocess_user_token
    from src.utils.user_context import UserContext

    return UserContext.get_user_token() or _subprocess_user_token


def _should_send(usage: Optional[dict]) -> bool:
    """Cheap, first-thing guard.

    Telemetry targets Databricks "logfood"; a purely local deployment has no
    workspace to send to. Bail out BEFORE the auth chain, any coroutine
    scheduling or any logging: this fires once per LLM call (and a tool-using
    agent makes many), and the previous version's unguarded no-op path scheduled
    tens of thousands of coroutines, flooding the logs and starving the loop.
    Databricks Apps always set DATABRICKS_HOST; OBO flows carry a user token —
    so this only short-circuits genuinely local setups.
    """
    if not usage:
        return False
    if os.getenv("DATABRICKS_HOST"):
        return True
    return bool(_resolve_user_token())


def _product_context(source: Any) -> str:
    """Which kasal product made the call, from the User-Agent we stamped on it.

    ``kasal_agent/0.1.0`` -> ``agent``. Falls back to "llm" when the LLM carries
    no telemetry header (non-Databricks endpoints do not get one).
    """
    headers = getattr(source, "extra_headers", None) or {}
    user_agent = headers.get("User-Agent", "") if isinstance(headers, dict) else ""
    if "_" in user_agent and "/" in user_agent:
        return user_agent.split("_")[1].split("/")[0]
    return "llm"


def _on_llm_call_completed(source: Any, event: LLMCallCompletedEvent) -> None:
    """Forward one call's usage. Never raises — the bus logs, but telemetry must
    not be able to disturb a run either way."""
    usage = getattr(event, "usage", None)
    if not _should_send(usage):
        return

    model = getattr(event, "model", None) or getattr(source, "model", "unknown")
    product_context = _product_context(source)
    logger.info(
        "[TokenTelemetry] model=%s context=%s tokens=%s",
        model,
        product_context,
        usage.get("total_tokens", 0),
    )

    try:
        from src.utils.telemetry import send_logfood_telemetry

        # skip_db_auth=True: this runs inside an LLM worker thread, and opening a
        # database session here conflicts with the transaction the request holds.
        coro = send_logfood_telemetry(
            usage=usage,
            model=model,
            product_context=product_context,
            user_token=_resolve_user_token(),
            skip_db_auth=True,
        )
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(coro)
        else:
            loop.create_task(coro)
    except Exception as e:  # noqa: BLE001 — telemetry is best-effort
        logger.debug("Token telemetry failed: %s", e)


def register_usage_telemetry() -> None:
    """Subscribe once per process. Safe to call repeatedly."""
    global _registered
    if _registered:
        return
    event_bus.register_handler(LLMCallCompletedEvent, _on_llm_call_completed)
    _registered = True
    logger.info("Token telemetry registered on LLMCallCompletedEvent")
