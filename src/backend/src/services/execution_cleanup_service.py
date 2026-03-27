"""
Execution Cleanup Service.

This service handles cleanup of orphaned/stale job executions
that may occur when the service is restarted while jobs are running,
or when the status update after a successful execution silently fails.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List

from sqlalchemy import text

from src.models.execution_status import ExecutionStatus
from src.services.execution_status_service import ExecutionStatusService
from src.repositories.execution_repository import ExecutionRepository
from src.db.session import async_session_factory

logger = logging.getLogger(__name__)

# Jobs stuck in RUNNING for longer than this are eligible for zombie recovery
_ZOMBIE_THRESHOLD_MINUTES = 2


class ExecutionCleanupService:
    """Simple cleanup service for orphaned jobs."""
    
    @staticmethod
    async def cleanup_stale_jobs_on_startup() -> int:
        """
        On startup, mark any RUNNING/PREPARING/PENDING jobs as CANCELLED.
        Since the service just started, these jobs can't actually be running.
        
        Returns:
            Number of jobs cleaned up
        """
        try:
            active_statuses = [
                ExecutionStatus.PENDING.value,
                ExecutionStatus.PREPARING.value,
                ExecutionStatus.RUNNING.value
            ]
            
            cleaned_count = 0
            
            async with async_session_factory() as db:
                repo = ExecutionRepository(db)
                
                # Get all "active" jobs - they can't be truly active since we just started
                stale_jobs, _ = await repo.get_execution_history(
                    limit=1000,
                    offset=0,
                    status_filter=active_statuses,
                    system_level=True  # System-level cleanup needs access to all executions
                )
            
            # Process jobs outside the session context to avoid nested sessions
            for job in stale_jobs:
                logger.info(f"Cleaning up stale job on startup: {job.job_id} (was {job.status})")
                
                success = await ExecutionStatusService.update_status(
                    job_id=job.job_id,
                    status=ExecutionStatus.CANCELLED.value,
                    message="Job cancelled - service was restarted while job was running"
                )
                
                if success:
                    cleaned_count += 1
                else:
                    logger.error(f"Failed to clean up stale job: {job.job_id}")
                    
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} stale jobs on startup")
            else:
                logger.info("No stale jobs found on startup")
                
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Error during startup job cleanup: {e}", exc_info=True)
            return 0
    
    @staticmethod
    async def cleanup_zombie_jobs() -> int:
        """
        Periodically called to recover jobs that completed successfully but whose
        status update silently failed (e.g. DB session timeout after a long run).

        Strategy:
        - Find RUNNING jobs older than _ZOMBIE_THRESHOLD_MINUTES
        - If execution_trace has a 'crew_completed' event → mark COMPLETED
        - If no trace at all and job is very old → mark FAILED
        - Skip jobs that still have an active subprocess (process_crew_executor)

        Returns:
            Number of jobs recovered
        """
        recovered = 0
        try:
            cutoff = datetime.utcnow() - timedelta(minutes=_ZOMBIE_THRESHOLD_MINUTES)

            async with async_session_factory() as db:
                # Jobs stuck in RUNNING past the threshold
                result = await db.execute(text(
                    "SELECT job_id, created_at FROM executionhistory "
                    "WHERE status = 'RUNNING' AND created_at < :cutoff"
                ), {"cutoff": cutoff})
                zombie_jobs = result.fetchall()

                for job_id, created_at in zombie_jobs:
                    # Check if the subprocess is still actually running
                    try:
                        from src.services.process_crew_executor import process_crew_executor
                        if job_id in process_crew_executor._running_processes:
                            proc = process_crew_executor._running_processes[job_id]
                            if proc.is_alive():
                                logger.debug(f"[ZombieCleanup] {job_id} — process still alive, skipping")
                                continue
                    except Exception:
                        pass

                    # Check execution_trace for completion event
                    trace_result = await db.execute(text(
                        "SELECT event_type FROM execution_trace "
                        "WHERE job_id = :job_id AND event_type = 'crew_completed' LIMIT 1"
                    ), {"job_id": job_id})
                    has_completion = trace_result.fetchone() is not None

                    if has_completion:
                        final_status = ExecutionStatus.COMPLETED.value
                        message = "Status recovered by periodic cleanup — crew_completed trace found"
                    else:
                        # No completion trace — mark as failed
                        final_status = ExecutionStatus.FAILED.value
                        message = "Status recovered by periodic cleanup — no completion trace found, marking failed"

                    logger.warning(
                        f"[ZombieCleanup] Recovering zombie job {job_id} "
                        f"(created {created_at}) → {final_status}"
                    )
                    success = await ExecutionStatusService.update_status(
                        job_id=job_id,
                        status=final_status,
                        message=message,
                    )
                    if success:
                        recovered += 1
                    else:
                        logger.error(f"[ZombieCleanup] Failed to recover {job_id}")

            if recovered:
                logger.info(f"[ZombieCleanup] Recovered {recovered} zombie job(s)")

        except Exception as e:
            logger.error(f"[ZombieCleanup] Error during zombie cleanup: {e}", exc_info=True)

        return recovered

    @staticmethod
    async def get_stale_jobs() -> List[str]:
        """
        Get list of job IDs that are in active states.
        Useful for debugging and monitoring.
        
        Returns:
            List of job IDs in active states
        """
        try:
            active_statuses = [
                ExecutionStatus.PENDING.value,
                ExecutionStatus.PREPARING.value,
                ExecutionStatus.RUNNING.value
            ]
            
            stale_job_ids = []
            
            async with async_session_factory() as db:
                repo = ExecutionRepository(db)
                
                stale_jobs, _ = await repo.get_execution_history(
                    limit=1000,
                    offset=0,
                    status_filter=active_statuses,
                    system_level=True  # System-level cleanup needs access to all executions
                )
                
                # Extract job IDs before closing the session
                stale_job_ids = [job.job_id for job in stale_jobs]
                
            return stale_job_ids
            
        except Exception as e:
            logger.error(f"Error getting stale jobs: {e}", exc_info=True)
            return []