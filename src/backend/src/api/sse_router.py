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

from src.core.dependencies import GroupContextDep
from src.core.logger import LoggerManager
from src.core.sse_manager import event_stream_generator, sse_manager

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
