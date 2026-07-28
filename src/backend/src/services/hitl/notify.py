"""
"Input is needed" notification — the live half of HITL.

Both the tool-approval gate (engine) and the task-review guardrail (services)
have to tell the UI that a run is waiting on a human, and both can be running
either in-process or inside the crew subprocess. The transport differs by path;
the caller should not have to care.

The DB row is always the durable source of truth. Every notification here is
best-effort and never raises: a dropped frame costs a client a live update, not
the gate itself (`GET /hitl/pending` still returns it).
"""

import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


async def notify_input_needed_sse(execution_id: str, payload: Dict[str, Any]) -> None:
    """Broadcast the hitl_request event to a job's SSE subscribers."""
    from src.core.sse_manager import SSEEvent, sse_manager

    # skip_replay: a replayed hitl_request after a reconnect would pop stale
    # (often already-expired) gates. The DB row + GET /hitl/pending are the
    # durable source of truth for clients that missed the live event.
    await sse_manager.broadcast_to_job(
        execution_id,
        SSEEvent(data=payload, event="hitl_request"),
        skip_replay=True,
    )


def notify_input_needed(execution_id: str, payload: Dict[str, Any]) -> None:
    """Notify the UI that input is needed — path-appropriate transport."""
    if os.environ.get("CREW_SUBPROCESS_MODE", "").lower() == "true":
        # Subprocess: the parent's relay turns this frame into the same SSE
        # event. The DB row stays the source of truth (pipe drops on full).
        try:
            from src.services import execution_event_pipe

            writer = execution_event_pipe._active_writer
            if writer is not None:
                writer._put({"kind": "hitl_request", **payload})
        except Exception as pipe_err:  # noqa: BLE001
            logger.debug(f"[hitl_notify] pipe notify skipped: {pipe_err}")
        return
    try:
        from src.services.tools.async_bridge import run_async_with_context

        run_async_with_context(notify_input_needed_sse(execution_id, payload), timeout=10)
    except Exception as sse_err:  # noqa: BLE001
        logger.debug(f"[hitl_notify] SSE notify skipped: {sse_err}")
