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

Off the call path. The bus runs handlers inline, on the thread that made the
LLM call, so this handler only queues the send. One daemon thread per process
owns a long-lived event loop and makes the requests: an LLM call never waits on
the workspace API, and a telemetry failure never reaches it. The queue is
bounded and drops when full. Before a process exits, ``flush`` gives queued
sends a bounded chance to go out: it is registered with ``atexit`` (the API
process, and the crew child, which leaves through ``sys.exit``) and called by
``flush_queues_before_exit`` (the flow child, which leaves through
``os._exit``, where ``atexit`` never runs). A send still pending after that is
dropped, which is acceptable for best-effort telemetry.
"""

import asyncio
import atexit
import functools
import logging
import os
import queue
import threading
import time
from typing import Any, Callable, Optional, TypeGuard, cast

from src.core.events import BaseEvent, LLMCallCompletedEvent
from src.core.events.bus import event_bus

logger = logging.getLogger(__name__)

_registered = False

#: Sends waiting for the worker. A slow or unreachable workspace API costs
#: dropped telemetry, never memory and never the caller's time.
MAX_PENDING_SENDS = 256

_pending: "queue.Queue[Callable[[], object]]" = queue.Queue(maxsize=MAX_PENDING_SENDS)
_worker: Optional[threading.Thread] = None
_worker_lock = threading.Lock()


def _drain_forever() -> None:
    """Worker body: run each queued send, on this thread's own loop.

    One loop for the life of the process instead of a throwaway
    ``asyncio.run`` loop per call. The auth the send resolves is cached in
    ``utils/databricks_auth`` in plain module-level dicts, not in anything
    bound to a loop, and this thread now writes those dicts too, alongside the
    request and LLM threads that already did.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        send = _pending.get()
        try:
            result = send()
            if asyncio.iscoroutine(result):
                loop.run_until_complete(result)
        except Exception as e:  # noqa: BLE001 — telemetry is best-effort
            logger.debug("Token telemetry send failed: %s", e)
        finally:
            _pending.task_done()


def _ensure_worker() -> None:
    global _worker
    if _worker is not None and _worker.is_alive():
        return
    with _worker_lock:
        if _worker is None or not _worker.is_alive():
            _worker = threading.Thread(
                target=_drain_forever, name="kasal-usage-telemetry", daemon=True
            )
            _worker.start()
            # Idempotent: atexit.unregister drops any earlier registration.
            atexit.unregister(flush)
            atexit.register(flush)


def flush(timeout: float = 2.0) -> bool:
    """Wait up to ``timeout`` seconds for queued sends to finish.

    True when the queue drained, False when the time ran out. Never raises and
    never waits longer than ``timeout``: a stuck send leaves the rest unsent,
    it does not hold the process open.
    """
    try:
        deadline = time.monotonic() + timeout
        with _pending.all_tasks_done:
            while _pending.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.debug(
                        "[TokenTelemetry] %d sends unsent at exit",
                        _pending.unfinished_tasks,
                    )
                    return False
                _pending.all_tasks_done.wait(remaining)
        return True
    except Exception as e:  # noqa: BLE001 — never disturb a process exit
        logger.debug("Token telemetry flush failed: %s", e)
        return False


def submit_telemetry(send: Callable[[], object]) -> bool:
    """Queue ``send`` for the telemetry worker. Never blocks, never raises.

    ``send`` runs on the worker thread; if it returns a coroutine, the worker
    runs that on its loop. Returns False when the send was dropped.
    """
    try:
        _ensure_worker()
        _pending.put_nowait(send)
        return True
    except queue.Full:
        logger.debug(
            "[TokenTelemetry] %d sends pending; dropping one", MAX_PENDING_SENDS
        )
    except Exception as e:  # noqa: BLE001 — telemetry is best-effort
        logger.debug("Token telemetry not queued: %s", e)
    return False


def _resolve_user_token() -> Optional[str]:
    """User token for OBO telemetry, or None.

    Contextvars do not propagate into the worker threads LLM calls run on in
    subprocess execution, hence the module-level fallback that the subprocess
    populates via ``set_subprocess_user_token``.
    """
    from src.core.llm.subprocess_token import get_subprocess_user_token
    from src.utils.user_context import UserContext

    return UserContext.get_user_token() or get_subprocess_user_token()


def _should_send(usage: Optional[dict]) -> TypeGuard[dict]:
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


def _on_llm_call_completed(source: Any, event: BaseEvent) -> None:
    """Queue one call's usage for the worker. Never raises and never waits —
    the bus logs, but telemetry must not be able to disturb a run either way."""
    try:
        usage = getattr(event, "usage", None)
        if not _should_send(usage):
            return

        # An LLMCallCompletedEvent (registered below); read defensively all the same.
        model = cast(
            str, getattr(event, "model", None) or getattr(source, "model", "unknown")
        )
        product_context = _product_context(source)
        logger.info(
            "[TokenTelemetry] model=%s context=%s tokens=%s",
            model,
            product_context,
            usage.get("total_tokens", 0),
        )

        from src.utils.telemetry import send_logfood_telemetry

        # Resolved HERE, on the calling thread: the user token lives in a
        # contextvar the worker thread cannot see.
        # skip_db_auth=True: telemetry must never open a database session; the
        # caller may be inside a request's transaction.
        submit_telemetry(
            functools.partial(
                send_logfood_telemetry,
                usage=dict(usage),
                model=model,
                product_context=product_context,
                user_token=_resolve_user_token(),
                skip_db_auth=True,
            )
        )
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
