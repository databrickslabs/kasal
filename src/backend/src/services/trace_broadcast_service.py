"""
Trace Broadcast Service for real-time SSE updates.

This service runs in the main process and polls the database for new traces
written by subprocess executions, then broadcasts them via SSE to connected clients.

This bridges the gap between:
- Subprocess: Writes traces to database (can't broadcast SSE - no clients connected)
- Main process: Has SSE clients connected but doesn't know about new traces

The service:
1. Tracks the last broadcasted trace ID per job
2. Periodically polls for new traces for jobs with active SSE connections
3. Broadcasts new traces via the SSE manager
"""

import asyncio
import logging
from typing import Dict, Optional, Set

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.sse_manager import sse_manager, SSEEvent
from src.db.session import async_session_factory
from src.models.execution_trace import ExecutionTrace
from src.models.execution_history import ExecutionHistory

logger = logging.getLogger(__name__)


class TraceBroadcastService:
    """
    Service that polls for new traces and broadcasts them via SSE.

    This runs as a background task in the main process to enable
    real-time trace updates from subprocess executions.
    """

    def __init__(self, poll_interval: float = 1.0):
        """
        Initialize the trace broadcast service.

        Args:
            poll_interval: How often to poll for new traces (seconds)
        """
        self.poll_interval = poll_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Track last broadcasted trace ID per job
        self._last_trace_ids: Dict[str, int] = {}

    def start(self):
        """Start the background polling task."""
        if self._running:
            logger.warning("[TraceBroadcastService] Already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("[TraceBroadcastService] Started trace broadcast polling")

    def stop(self):
        """Stop the background polling task."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        logger.info("[TraceBroadcastService] Stopped trace broadcast polling")

    def _has_global_stream_listeners(self) -> bool:
        """Check if any global stream (all_groups_*) has active listeners."""
        stats = sse_manager.get_statistics()
        for job_id in stats.get("active_jobs", []):
            if job_id.startswith("all_groups_"):
                return True
        return False

    def _get_active_job_ids(self) -> Set[str]:
        """
        Get job IDs that have active SSE connections.

        Returns:
            Set of job_ids with active SSE listeners
        """
        stats = sse_manager.get_statistics()
        active_jobs = set()

        for job_id in stats.get("active_jobs", []):
            # Skip the "all_groups_*" stream IDs - those are global streams
            if not job_id.startswith("all_groups_"):
                active_jobs.add(job_id)

        return active_jobs

    async def _get_running_job_ids(self, session: AsyncSession) -> Set[str]:
        """
        Query DB for currently running job IDs.
        Used when global SSE stream is connected but no per-job connections exist.

        When Lakebase is active, execution_history lives there, so we use the
        smart session (execute_db_operation_smart). Falls back to the provided
        local session otherwise.
        """
        try:
            from src.utils.asyncio_utils import execute_db_operation_smart

            async def _query(s: AsyncSession) -> Set[str]:
                query = (
                    select(ExecutionHistory.job_id)
                    .where(ExecutionHistory.status.in_(["RUNNING", "running"]))
                )
                result = await s.execute(query)
                return {row[0] for row in result.fetchall() if row[0]}

            return await execute_db_operation_smart(_query)
        except Exception as e:
            logger.error(f"[TraceBroadcastService] Error querying running jobs: {e}")
            return set()

    async def _poll_loop(self):
        """Main polling loop that checks for new traces."""
        logger.info("[TraceBroadcastService] Poll loop started")

        while self._running:
            try:
                await self._poll_for_traces()
            except asyncio.CancelledError:
                logger.info("[TraceBroadcastService] Poll loop cancelled")
                break
            except Exception as e:
                logger.error(f"[TraceBroadcastService] Error in poll loop: {e}")

            await asyncio.sleep(self.poll_interval)

        logger.info("[TraceBroadcastService] Poll loop ended")

    async def _poll_for_traces(self):
        """Poll database for new traces and broadcast them."""
        # Get jobs with active per-job SSE connections
        active_jobs = self._get_active_job_ids()

        # When the global SSE stream has listeners (but no per-job connections),
        # we still need to poll for running jobs so trace events reach the frontend.
        has_global = self._has_global_stream_listeners()

        if not active_jobs and not has_global:
            return

        async with async_session_factory() as session:
            if has_global:
                running_jobs = await self._get_running_job_ids(session)
                active_jobs = active_jobs | running_jobs

            if not active_jobs:
                return
            # Initialize tracking for new jobs - start from current max ID
            # This avoids re-broadcasting traces that the initial fetch already loaded
            for job_id in active_jobs:
                if job_id not in self._last_trace_ids:
                    # Query for the current max trace ID for this job
                    from sqlalchemy import func
                    max_id_query = select(func.max(ExecutionTrace.id)).where(
                        ExecutionTrace.job_id == job_id
                    )
                    result = await session.execute(max_id_query)
                    max_id = result.scalar() or 0
                    self._last_trace_ids[job_id] = max_id
                    logger.info(f"[TraceBroadcastService] Started tracking job {job_id} from trace_id={max_id}")

            # Clean up jobs that are no longer active
            tracked_jobs = set(self._last_trace_ids.keys())
            for job_id in tracked_jobs - active_jobs:
                del self._last_trace_ids[job_id]
                logger.info(f"[TraceBroadcastService] Stopped tracking job {job_id}")

            # Query for new traces
            for job_id in active_jobs:
                await self._broadcast_new_traces_for_job(session, job_id)

    async def _broadcast_new_traces_for_job(self, session: AsyncSession, job_id: str):
        """
        Check for new traces for a specific job and broadcast them.

        Args:
            session: Database session
            job_id: Job ID to check for new traces
        """
        last_id = self._last_trace_ids.get(job_id, 0)

        try:
            # Query for traces newer than last_id
            # Using id > last_id is sufficient to avoid re-broadcasting
            # No time filter needed - avoids timezone issues between subprocess and main process
            query = (
                select(ExecutionTrace)
                .where(
                    and_(
                        ExecutionTrace.job_id == job_id,
                        ExecutionTrace.id > last_id
                    )
                )
                .order_by(ExecutionTrace.id.asc())
                .limit(50)  # Batch limit to avoid overwhelming
            )

            result = await session.execute(query)
            traces = result.scalars().all()

            if traces:
                logger.info(f"[TraceBroadcastService] Found {len(traces)} new traces for job {job_id}")

                for trace in traces:
                    # Update last seen ID
                    self._last_trace_ids[job_id] = trace.id

                    # Create trace data for SSE
                    trace_data = {
                        "id": trace.id,
                        "run_id": trace.run_id,
                        "job_id": trace.job_id,
                        "event_source": trace.event_source,
                        "event_context": trace.event_context,
                        "event_type": trace.event_type,
                        "output": trace.output,
                        "trace_metadata": trace.trace_metadata,
                        "created_at": trace.created_at.isoformat() if trace.created_at else None,
                        "group_id": trace.group_id,
                        "group_email": trace.group_email,
                    }

                    # Broadcast via SSE
                    event = SSEEvent(
                        data=trace_data,
                        event="trace",
                        id=f"{job_id}_trace_{trace.id}"
                    )

                    sent_count = await sse_manager.broadcast_to_job(job_id, event)
                    logger.debug(
                        f"[TraceBroadcastService] Broadcasted trace {trace.id} "
                        f"for job {job_id} to {sent_count} clients"
                    )

        except Exception as e:
            logger.error(f"[TraceBroadcastService] Error querying traces for job {job_id}: {e}")


# Global instance
trace_broadcast_service = TraceBroadcastService(poll_interval=1.0)
