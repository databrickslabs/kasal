"""
Server-Sent Events (SSE) API router for real-time updates.

Provides SSE endpoints for:
- Execution status updates
- Trace streaming
- HITL notifications
"""

from typing import Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import NotFoundError
from src.core.logger import LoggerManager
from src.core.sse_manager import event_stream_generator, sse_manager
from src.repositories.execution_history_repository import ExecutionHistoryRepository

logger = LoggerManager.get_instance().system

router = APIRouter(prefix="/sse", tags=["Server-Sent Events"])

# HTTP/2-safe headers for SSE streaming responses.
# IMPORTANT: Do NOT include "Connection: keep-alive" — it is a hop-by-hop
# header forbidden in HTTP/2 (RFC 7540 §8.1.2.2). Sending it through an
# HTTP/2 reverse proxy (e.g., Databricks Apps) causes
# ERR_HTTP2_PROTOCOL_ERROR and kills the stream immediately.
#
# "Content-Encoding: none" is borrowed from Databricks AppKit — it tells
# the HTTP/2 proxy NOT to buffer/compress the response, which is critical
# for SSE to stream through without being held back.
SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Content-Encoding": "none",          # Prevent proxy buffering/compression
    "X-Accel-Buffering": "no",           # Disable buffering in nginx / envoy
    "X-Content-Type-Options": "nosniff",
}


def _parse_last_event_id(request: Request) -> Optional[int]:
    """Extract Last-Event-ID from request headers (sent by EventSource on reconnect)."""
    raw = request.headers.get("last-event-id")
    if raw:
        try:
            return int(raw)
        except (ValueError, TypeError):
            pass
    return None


@router.get("/executions/{job_id}/stream")
async def stream_execution_updates(
    request: Request,
    job_id: str,
    group_context: GroupContextDep,
    session: SessionDep,
    timeout: int = Query(3600, ge=30, le=7200, description="Stream timeout in seconds"),
    heartbeat: int = Query(
        15, ge=5, le=120, description="Heartbeat interval in seconds"
    ),
):
    """
    Stream real-time updates for a specific execution via Server-Sent Events.

    Supports automatic reconnection with event replay: when the connection
    drops (common behind HTTP/2 proxies like Databricks Apps), the browser
    reconnects with ``Last-Event-ID`` and the server replays missed events.
    """
    # SECURITY: the per-job stream carries the execution's live traces/outputs
    # (which include group_id/group_email/output). Verify the execution belongs
    # to one of the caller's groups before streaming. Deny on a positive
    # cross-tenant mismatch (a not-yet-persisted job is allowed, mirroring the
    # flow-execution read path).
    group_ids = group_context.group_ids or []
    execution = await ExecutionHistoryRepository(session).get_execution_by_job_id(job_id)
    if (
        execution
        and getattr(execution, "group_id", None)
        and execution.group_id not in group_ids
    ):
        logger.warning(
            f"[SSE_STREAM] cross-tenant access denied | job={job_id} | caller_groups={group_ids}"
        )
        raise NotFoundError(f"Execution {job_id} not found")

    last_event_id = _parse_last_event_id(request)

    logger.info(
        f"[SSE_STREAM] per-job endpoint hit | job={job_id} | "
        f"timeout={timeout}s | heartbeat={heartbeat}s | last_event_id={last_event_id}"
    )

    return StreamingResponse(
        event_stream_generator(
            job_id,
            timeout=timeout,
            heartbeat_interval=heartbeat,
            last_event_id=last_event_id,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/executions/stream-all")
async def stream_all_executions(
    request: Request,
    group_context: GroupContextDep,
    timeout: int = Query(3600, ge=30, le=7200),
    heartbeat: int = Query(15, ge=5, le=120),
):
    """
    Stream updates for all executions in the user's groups.

    Supports automatic reconnection with event replay (see per-job endpoint).
    """
    last_event_id = _parse_last_event_id(request)
    group_ids = group_context.group_ids or []
    stream_id = f"all_groups_{'-'.join(sorted(group_ids))}"

    logger.info(
        f"[SSE_STREAM] stream-all endpoint hit | groups={group_ids} | "
        f"stream_id={stream_id} | timeout={timeout}s | heartbeat={heartbeat}s | "
        f"last_event_id={last_event_id} | "
        f"headers={dict(request.headers)}"
    )

    return StreamingResponse(
        event_stream_generator(
            stream_id,
            timeout=timeout,
            heartbeat_interval=heartbeat,
            last_event_id=last_event_id,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/generations/{generation_id}/stream")
async def stream_generation_updates(
    request: Request,
    generation_id: str,
    group_context: GroupContextDep,
    timeout: int = Query(300, ge=30, le=600, description="Stream timeout in seconds"),
    heartbeat: int = Query(10, ge=5, le=60, description="Heartbeat interval in seconds"),
):
    """
    Stream real-time updates for a progressive crew generation via SSE.

    Supports automatic reconnection with event replay.
    """
    last_event_id = _parse_last_event_id(request)

    logger.info(
        f"[SSE_STREAM] generation endpoint hit | id={generation_id} | "
        f"timeout={timeout}s | heartbeat={heartbeat}s | last_event_id={last_event_id}"
    )

    return StreamingResponse(
        event_stream_generator(
            generation_id,
            timeout=timeout,
            heartbeat_interval=heartbeat,
            last_event_id=last_event_id,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/generations/{generation_id}/result")
async def get_generation_result(
    generation_id: str,
    group_context: GroupContextDep,
):
    """
    Non-streaming fallback to recover a generation's terminal outcome.

    The chat fast path completes in well under a second and the Databricks
    Apps proxy drops the first SSE connect of a page often enough that the
    client can miss the ``generation_complete`` event — which is the sole
    carrier of the ``execution_id``. Without it the run is orphaned and the
    chat appears dead until the user submits again. The frontend polls this
    endpoint when the stream fails or stalls; it reads the same replay buffer
    the stream replays from, so it returns the buffered terminal event over
    plain HTTP.

    Returns:
        ``{"status": "completed"|"failed", ...event data...}`` once the
        terminal event is buffered, or ``{"status": "pending"}`` while the
        generation is still in flight / not yet known.
    """
    event = sse_manager.get_terminal_event(generation_id)
    if event is None:
        return {"status": "pending", "generation_id": generation_id}

    payload = dict(event.data) if isinstance(event.data, dict) else {"data": event.data}
    payload.setdefault("generation_id", generation_id)
    # Normalize a top-level status so the client can branch without inspecting
    # the SSE event name.
    if event.event == "generation_failed":
        payload.setdefault("status", "failed")
    else:
        payload.setdefault("status", "completed")
    return payload


@router.get("/stats")
async def get_sse_stats():
    """
    Get SSE connection statistics.

    Useful for monitoring and debugging SSE connections.

    Returns:
        Statistics about active SSE connections
    """
    return sse_manager.get_statistics()


@router.get("/health")
async def sse_health():
    """
    Health check endpoint for SSE infrastructure.

    Returns:
        Health status
    """
    stats = sse_manager.get_statistics()
    return {
        "status": "healthy",
        "active_connections": stats["total_connections"],
        "active_streams": len(stats["active_jobs"]),
    }
