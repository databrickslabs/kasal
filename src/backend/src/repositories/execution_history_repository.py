"""
Repository for execution history data access.

This module provides database operations for execution history models.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_history import ExecutionHistory, TaskStatus, ErrorTrace
# Removed async_session_factory import - using injected session only


class ExecutionHistoryRepository:
    """Repository for execution history data access operations."""
    
    def __init__(self, session: AsyncSession):
        """Initialize with required session."""
        self.session = session
    
    async def get_execution_history(
        self,
        limit: int = 50,
        offset: int = 0,
        group_ids: List[str] = None
    ) -> tuple[List[ExecutionHistory], int]:
        """
        Get paginated execution history with group filtering.

        Args:
            limit: Maximum number of items to return
            offset: Number of items to skip
            group_ids: List of group IDs for filtering

        Returns:
            Tuple of (list of Run objects, total count)
        """
        # Use the session from the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        session = self.session

        # Build base query with group filtering
        if group_ids and len(group_ids) > 0:
            base_filter = ExecutionHistory.group_id.in_(group_ids)
        else:
            # No filtering (fallback for admin/system access)
            base_filter = True

        # Get total count
        count_stmt = select(func.count()).select_from(ExecutionHistory).where(base_filter)
        total_count_result = await session.execute(count_stmt)
        total_count = total_count_result.scalar() or 0

        # Get paginated runs
        stmt = (select(ExecutionHistory)
               .where(base_filter)
               .order_by(ExecutionHistory.created_at.desc())
               .offset(offset)
               .limit(limit))
        result = await session.execute(stmt)
        runs = result.scalars().all()

        return runs, total_count
    
    async def get_execution_by_id(self, execution_id: int, group_ids: List[str] = None) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by ID with group filtering.

        Args:
            execution_id: ID of the execution
            group_ids: List of group IDs for filtering

        Returns:
            Run object if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        session = self.session

        filters = [ExecutionHistory.id == execution_id]

        # Add group filtering
        if group_ids and len(group_ids) > 0:
            filters.append(ExecutionHistory.group_id.in_(group_ids))

        stmt = select(ExecutionHistory).where(*filters)
        result = await session.execute(stmt)
        return result.scalars().first()
    
    async def get_execution_by_job_id(self, job_id: str, group_ids: List[str] = None) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by job_id with group filtering.

        Args:
            job_id: Job ID of the execution
            group_ids: List of group IDs for filtering

        Returns:
            Run object if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        session = self.session

        filters = [ExecutionHistory.job_id == job_id]

        # Add group filtering
        if group_ids and len(group_ids) > 0:
            filters.append(ExecutionHistory.group_id.in_(group_ids))

        stmt = select(ExecutionHistory).where(*filters)
        result = await session.execute(stmt)
        return result.scalars().first()
    
    async def find_by_id(self, execution_id: int) -> Optional[ExecutionHistory]:
        """
        Find execution by ID.

        Args:
            execution_id: ID of the execution

        Returns:
            ExecutionHistory object if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        # Use existing session
        return await self._get_execution_by_id_internal(self.session, execution_id)
    
    async def check_execution_exists(self, execution_id: int) -> bool:
        """
        Check if an execution exists.

        Args:
            execution_id: ID of the execution

        Returns:
            True if exists, False otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        stmt = select(func.count()).select_from(ExecutionHistory).where(ExecutionHistory.id == execution_id)
        result = await self.session.execute(stmt)
        count = result.scalar() or 0
        return count > 0
    
    async def delete_execution(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """
        Delete a specific execution and its associated data.

        Args:
            execution_id: ID of the execution

        Returns:
            Dictionary with deletion counts or None if execution not found
        """
        # ALWAYS use the session passed to the repository
        # Never create our own session - this breaks transaction boundaries
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        # Use the provided session without committing
        # The database router or service layer will handle commits
        return await self._delete_execution_with_session(self.session, execution_id, commit=False)

    async def _delete_execution_with_session(self, session: AsyncSession, execution_id: int, commit: bool = False) -> Optional[Dict[str, Any]]:
        """Internal method to handle deletion with a given session."""
        import logging
        logger = logging.getLogger(__name__)

        try:
            logger.debug(f"[DELETE] Starting deletion of execution {execution_id}, commit={commit}, session={id(session)}")

            # Get the run first to check existence and get job_id
            run = await self._get_execution_by_id_internal(session, execution_id)
            if not run:
                logger.warning(f"[DELETE] Execution {execution_id} not found")
                return None

            job_id = run.job_id
            logger.debug(f"[DELETE] Found execution {execution_id} with job_id={job_id}")
            result = {}

            # Delete associated task statuses
            task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id == job_id)
            task_status_result = await session.execute(task_status_stmt)
            result['task_status_count'] = task_status_result.rowcount

            # Delete associated error traces
            error_trace_stmt = delete(ErrorTrace).where(ErrorTrace.run_id == execution_id)
            error_trace_result = await session.execute(error_trace_stmt)
            result['error_trace_count'] = error_trace_result.rowcount

            # Delete the run
            run_stmt = delete(ExecutionHistory).where(ExecutionHistory.id == execution_id)
            delete_result = await session.execute(run_stmt)
            logger.debug(f"[DELETE] Deleted execution record, affected rows: {delete_result.rowcount}")

            # Flush to ensure operations are sent to database
            await session.flush()
            logger.debug(f"[DELETE] Flushed delete operations to database")

            # Only commit if we created our own session
            if commit:
                logger.debug(f"[DELETE] Committing transaction for session {id(session)}")
                await session.commit()
                logger.debug(f"[DELETE] Transaction committed successfully")
            else:
                logger.debug(f"[DELETE] Not committing - external session management")

            logger.debug(f"[DELETE] Successfully deleted execution {execution_id}")
            return {
                'execution_id': execution_id,
                'job_id': job_id,
                'task_status_count': result['task_status_count'],
                'error_trace_count': result['error_trace_count']
            }
        except Exception as e:
            if commit:
                await session.rollback()
            raise e
    
    async def _get_execution_by_id_internal(self, session: AsyncSession, execution_id: int) -> Optional[ExecutionHistory]:
        """Internal method to get execution by ID using provided session."""
        stmt = select(ExecutionHistory).where(ExecutionHistory.id == execution_id)
        result = await session.execute(stmt)
        return result.scalars().first()
    
    async def delete_execution_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Delete a specific execution and its associated data by job_id.

        Args:
            job_id: Job ID of the execution

        Returns:
            Dictionary with deletion counts or None if execution not found
        """
        # ALWAYS use the session passed to the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        return await self._delete_execution_by_job_id_with_session(self.session, job_id, commit=False)

    async def _delete_execution_by_job_id_with_session(self, session: AsyncSession, job_id: str, commit: bool = False) -> Optional[Dict[str, Any]]:
        """Internal method to handle deletion by job_id with a given session."""
        try:
            # Get the run first to check existence
            run = await self._get_execution_by_job_id_internal(session, job_id)
            if not run:
                return None

            execution_id = run.id
            result = {}

            # Delete associated task statuses
            task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id == job_id)
            task_status_result = await session.execute(task_status_stmt)
            result['task_status_count'] = task_status_result.rowcount

            # Delete associated error traces
            error_trace_stmt = delete(ErrorTrace).where(ErrorTrace.run_id == execution_id)
            error_trace_result = await session.execute(error_trace_stmt)
            result['error_trace_count'] = error_trace_result.rowcount

            # Delete the run
            run_stmt = delete(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            await session.execute(run_stmt)

            # Flush to ensure operations are sent to database
            await session.flush()

            # Only commit if we created our own session
            if commit:
                await session.commit()

            return {
                'execution_id': execution_id,
                'job_id': job_id,
                'task_status_count': result['task_status_count'],
                'error_trace_count': result['error_trace_count']
            }
        except Exception as e:
            if commit:
                await session.rollback()
            raise e
    
    async def _get_execution_by_job_id_internal(self, session: AsyncSession, job_id: str) -> Optional[ExecutionHistory]:
        """Internal method to get execution by job ID using provided session."""
        stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
        result = await session.execute(stmt)
        return result.scalars().first()

    async def update_mlflow_evaluation_run_id(self, job_id: str, evaluation_run_id: str) -> bool:
        """
        Update the MLflow evaluation run ID for an execution.

        Args:
            job_id: Job ID of the execution
            evaluation_run_id: MLflow evaluation run ID to set

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging
        logger = logging.getLogger(__name__)

        try:
            # Find the execution by job_id
            stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"No execution found with job_id: {job_id}")
                return False

            # Update the MLflow evaluation run ID
            execution.mlflow_evaluation_run_id = evaluation_run_id

            # Flush changes to database
            await self.session.flush()
            logger.info(f"Successfully updated MLflow evaluation run ID for job_id: {job_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating MLflow evaluation run ID for job_id {job_id}: {str(e)}", exc_info=True)
            return False
    
    async def delete_all_executions(self) -> Dict[str, int]:
        """
        Delete all executions and associated data.

        Returns:
            Dictionary with deletion counts
        """
        # ALWAYS use the session passed to the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        return await self._delete_all_executions_with_session(self.session, commit=False)

    async def _delete_all_executions_with_session(self, session: AsyncSession, commit: bool = False) -> Dict[str, int]:
        """Internal method to handle deletion of all executions with a given session."""
        try:
            result = {}

            # Delete all task statuses
            task_status_stmt = delete(TaskStatus)
            task_status_result = await session.execute(task_status_stmt)
            result['task_status_count'] = task_status_result.rowcount

            # Delete all error traces
            error_trace_stmt = delete(ErrorTrace)
            error_trace_result = await session.execute(error_trace_stmt)
            result['error_trace_count'] = error_trace_result.rowcount

            # Delete all runs and count them
            count_stmt = select(func.count()).select_from(ExecutionHistory)
            count_result = await session.execute(count_stmt)
            run_count = count_result.scalar() or 0

            run_stmt = delete(ExecutionHistory)
            await session.execute(run_stmt)

            # Flush to ensure operations are sent to database
            await session.flush()

            # Only commit if we created our own session
            if commit:
                await session.commit()

            return {
                'run_count': run_count,
                'task_status_count': result['task_status_count'],
                'error_trace_count': result['error_trace_count']
            }
        except Exception as e:
            if commit:
                await session.rollback()
            raise e


# Don't create a singleton instance - repositories should be created with sessions
# execution_history_repository = ExecutionHistoryRepository()  # Removed - causes session issues 