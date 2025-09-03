"""
Execution Cleanup Service.

This service handles cleanup of orphaned/stale job executions
that may occur when the service is restarted while jobs are running.
"""

import logging
from typing import List

from src.models.execution_status import ExecutionStatus
from src.services.execution_status_service import ExecutionStatusService
from src.repositories.execution_repository import ExecutionRepository
from src.db.session import async_session_factory

logger = logging.getLogger(__name__)


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
                    status_filter=active_statuses
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
                    status_filter=active_statuses
                )
                
                # Extract job IDs before closing the session
                stale_job_ids = [job.job_id for job in stale_jobs]
                
            return stale_job_ids
            
        except Exception as e:
            logger.error(f"Error getting stale jobs: {e}", exc_info=True)
            return []