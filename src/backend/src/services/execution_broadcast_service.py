"""
Execution Broadcast Service for real-time SSE updates.

This service runs in the main process and polls the database for execution status changes
written by subprocess executions, then broadcasts them via SSE to connected clients.

This bridges the gap between:
- Subprocess: Updates execution status in database (can't broadcast SSE - no clients connected)
- Main process: Has SSE clients connected but doesn't know about status changes

The service:
1. Tracks the last known status per job
2. Periodically polls for status changes for jobs with active SSE connections
3. Broadcasts status changes via the SSE manager
"""

import asyncio
import logging
from typing import Dict, Optional, Set

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.sse_manager import sse_manager, SSEEvent
from src.db.session import async_session_factory
from src.models.execution_history import ExecutionHistory

logger = logging.getLogger(__name__)


class ExecutionBroadcastService:
    """
    Service that polls for execution status changes and broadcasts them via SSE.

    This runs as a background task in the main process to enable
    real-time status updates from subprocess executions.
    """

    def __init__(self, poll_interval: float = 1.0):
        """
        Initialize the execution broadcast service.

        Args:
            poll_interval: How often to poll for status changes (seconds)
        """
        self.poll_interval = poll_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Track last known status per job
        self._last_statuses: Dict[str, str] = {}
        self._last_completed_at: Dict[str, Optional[str]] = {}

    def start(self):
        """Start the background polling task."""
        if self._running:
            logger.warning("[ExecutionBroadcastService] Already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("[ExecutionBroadcastService] Started execution broadcast polling")

    def stop(self):
        """Stop the background polling task."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        logger.info("[ExecutionBroadcastService] Stopped execution broadcast polling")

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

    async def _poll_loop(self):
        """Main polling loop that checks for status changes."""
        logger.info("[ExecutionBroadcastService] Poll loop started")

        while self._running:
            try:
                await self._poll_for_status_changes()
            except asyncio.CancelledError:
                logger.info("[ExecutionBroadcastService] Poll loop cancelled")
                break
            except Exception as e:
                logger.error(f"[ExecutionBroadcastService] Error in poll loop: {e}")

            await asyncio.sleep(self.poll_interval)

        logger.info("[ExecutionBroadcastService] Poll loop ended")

    async def _poll_for_status_changes(self):
        """Poll database for execution status changes and broadcast them."""
        # Get jobs with active SSE connections
        active_jobs = self._get_active_job_ids()

        if not active_jobs:
            return

        async with async_session_factory() as session:
            # Clean up jobs that are no longer active
            tracked_jobs = set(self._last_statuses.keys())
            for job_id in tracked_jobs - active_jobs:
                del self._last_statuses[job_id]
                if job_id in self._last_completed_at:
                    del self._last_completed_at[job_id]
                logger.debug(f"[ExecutionBroadcastService] Stopped tracking job {job_id}")

            # Check for status changes
            for job_id in active_jobs:
                await self._check_and_broadcast_status(session, job_id)

    async def _check_and_broadcast_status(self, session: AsyncSession, job_id: str):
        """
        Check for status changes for a specific job and broadcast them.

        Args:
            session: Database session
            job_id: Job ID to check for status changes
        """
        try:
            # Query for current execution status
            query = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await session.execute(query)
            execution = result.scalar_one_or_none()

            if not execution:
                return

            current_status = execution.status
            current_completed_at = execution.completed_at.isoformat() if execution.completed_at else None

            # Check if status changed or completed_at was set
            last_status = self._last_statuses.get(job_id)
            last_completed_at = self._last_completed_at.get(job_id)

            status_changed = last_status is None or last_status != current_status
            completed_at_changed = last_completed_at != current_completed_at

            if status_changed or completed_at_changed:
                # Update tracking
                self._last_statuses[job_id] = current_status
                self._last_completed_at[job_id] = current_completed_at

                # Only broadcast if this is not the initial tracking (last_status was None means first poll)
                if last_status is not None:
                    logger.info(f"[ExecutionBroadcastService] Status changed for job {job_id}: {last_status} -> {current_status}")

                    # Create event data
                    from datetime import datetime
                    event_data = {
                        "job_id": job_id,
                        "status": current_status,
                        "message": execution.error or "",
                        "updated_at": datetime.now().isoformat(),
                        "group_id": execution.group_id,
                        "completed_at": current_completed_at,
                    }

                    # Include result if available
                    if execution.result is not None:
                        event_data["result"] = execution.result

                    # Broadcast via SSE
                    event = SSEEvent(
                        data=event_data,
                        event="execution_update",
                        id=f"{job_id}_status_{current_status}"
                    )

                    sent_count = await sse_manager.broadcast_to_job(job_id, event)
                    logger.info(
                        f"[ExecutionBroadcastService] Broadcasted status update for job {job_id} "
                        f"({current_status}) to {sent_count} clients"
                    )
                else:
                    logger.debug(f"[ExecutionBroadcastService] Started tracking job {job_id} with status {current_status}")

        except Exception as e:
            logger.error(f"[ExecutionBroadcastService] Error checking status for job {job_id}: {e}")


# Global instance
execution_broadcast_service = ExecutionBroadcastService(poll_interval=1.0)
