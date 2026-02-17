"""
Server-Sent Events (SSE) API router for real-time updates.

Provides SSE endpoints for:
- Execution status updates
- Trace streaming
- HITL notifications
"""

from typing import Optional

from fastapi import APIRouter, Header, Query
from fastapi.responses import StreamingResponse

from src.core.dependencies import GroupContextDep
from src.core.logger import LoggerManager
from src.core.sse_manager import event_stream_generator, sse_manager

logger = LoggerManager.get_instance().system

router = APIRouter(prefix="/sse", tags=["Server-Sent Events"])


@router.get("/executions/{job_id}/stream")
async def stream_execution_updates(
    job_id: str,
    group_context: GroupContextDep,
    timeout: int = Query(3600, ge=30, le=7200, description="Stream timeout in seconds"),
    heartbeat: int = Query(
        15, ge=5, le=120, description="Heartbeat interval in seconds"
    ),
):
    """
    Stream real-time updates for a specific execution via Server-Sent Events.

    This endpoint provides a continuous stream of events including:
    - Execution status changes (queued, running, completed, failed)
    - Execution traces
    - HITL approval requests
    - Error notifications

    The connection will automatically close when:
    - The execution completes (status = completed/failed/stopped)
    - The timeout is reached
    - The client disconnects

    **Event Types:**
    - `execution_update`: Status change event
    - `trace`: New execution trace
    - `hitl_request`: Human-in-the-loop approval needed
    - `error`: Error occurred
    - `connected`: Initial connection established

    **Event Data Format:**
    ```json
    {
        "job_id": "...",
        "status": "running",
        "updated_at": "2024-01-01T12:00:00",
        ...
    }
    ```

    Args:
        job_id: The execution job ID to stream updates for
        group_context: Group context for security filtering
        timeout: Maximum time to keep stream alive (30-3600 seconds)
        heartbeat: Interval for keepalive messages (10-120 seconds)

    Returns:
        StreamingResponse with text/event-stream content type
    """
    # TODO: Add security check - verify job belongs to user's groups
    # For now, we rely on group_context being passed through

    logger.info(
        f"SSE stream requested for job {job_id}, "
        f"timeout={timeout}s, heartbeat={heartbeat}s"
    )

    return StreamingResponse(
        event_stream_generator(job_id, timeout=timeout, heartbeat_interval=heartbeat),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable buffering in nginx
        },
    )


@router.get("/executions/stream-all")
async def stream_all_executions(
    group_context: GroupContextDep,
    timeout: int = Query(3600, ge=30, le=7200),
    heartbeat: int = Query(15, ge=5, le=120),
):
    """
    Stream updates for all executions in the user's groups.

    This endpoint streams events for any execution that belongs to the
    user's accessible groups. Useful for dashboard views that need to
    monitor multiple executions simultaneously.

    The stream includes the same event types as the single-job endpoint,
    but with events from all jobs in the user's groups.

    Args:
        group_context: Group context for filtering
        timeout: Maximum time to keep stream alive
        heartbeat: Interval for keepalive messages

    Returns:
        StreamingResponse with text/event-stream content type
    """
    # For "stream all", we'll use a special job_id pattern
    # This will be expanded in future to handle multiple job subscriptions
    stream_id = f"all_groups_{'-'.join(sorted(group_context.group_ids))}"

    logger.info(
        f"SSE stream-all requested for groups {group_context.group_ids}, "
        f"timeout={timeout}s"
    )

    return StreamingResponse(
        event_stream_generator(
            stream_id, timeout=timeout, heartbeat_interval=heartbeat
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/generations/{generation_id}/stream")
async def stream_generation_updates(
    generation_id: str,
    group_context: GroupContextDep,
    timeout: int = Query(300, ge=30, le=600, description="Stream timeout in seconds"),
    heartbeat: int = Query(10, ge=5, le=60, description="Heartbeat interval in seconds"),
):
    """
    Stream real-time updates for a progressive crew generation via SSE.

    Events:
    - `plan_ready`: Crew outline with agent/task names
    - `agent_detail`: Full agent details after generation + DB persist
    - `task_detail`: Full task details after generation + DB persist
    - `entity_error`: Error generating a specific entity
    - `generation_complete`: All entities created
    - `generation_failed`: Fatal error during generation
    """
    logger.info(
        f"SSE generation stream requested for {generation_id}, "
        f"timeout={timeout}s, heartbeat={heartbeat}s"
    )

    return StreamingResponse(
        event_stream_generator(
            generation_id, timeout=timeout, heartbeat_interval=heartbeat
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
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
