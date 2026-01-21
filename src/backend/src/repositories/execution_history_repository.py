"""
Repository for execution history data access.

This module provides database operations for execution history models.
"""

import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_history import ExecutionHistory, TaskStatus, ErrorTrace
# Removed async_session_factory import - using injected session only

logger = logging.getLogger(__name__)


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
    
    async def delete_all_executions(self, group_ids: List[str] = None) -> Dict[str, int]:
        """
        Delete all executions and associated data for specified groups.

        Args:
            group_ids: List of group IDs to filter deletions. If provided, only
                      executions belonging to these groups will be deleted.
                      If None, deletes ALL executions (admin/system access only).

        Returns:
            Dictionary with deletion counts
        """
        # ALWAYS use the session passed to the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        return await self._delete_all_executions_with_session(self.session, group_ids=group_ids, commit=False)

    async def _delete_all_executions_with_session(self, session: AsyncSession, group_ids: List[str] = None, commit: bool = False) -> Dict[str, int]:
        """Internal method to handle deletion of all executions with a given session."""
        try:
            result = {}

            # If group_ids provided, only delete executions for those groups
            if group_ids and len(group_ids) > 0:
                # First get all job_ids and execution_ids for the group
                stmt = select(ExecutionHistory.id, ExecutionHistory.job_id).where(
                    ExecutionHistory.group_id.in_(group_ids)
                )
                exec_result = await session.execute(stmt)
                executions = exec_result.fetchall()

                execution_ids = [row[0] for row in executions]
                job_ids = [row[1] for row in executions]

                if not execution_ids:
                    return {
                        'run_count': 0,
                        'task_status_count': 0,
                        'error_trace_count': 0
                    }

                # Delete task statuses for these job_ids
                task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id.in_(job_ids))
                task_status_result = await session.execute(task_status_stmt)
                result['task_status_count'] = task_status_result.rowcount

                # Delete error traces for these execution_ids
                error_trace_stmt = delete(ErrorTrace).where(ErrorTrace.run_id.in_(execution_ids))
                error_trace_result = await session.execute(error_trace_stmt)
                result['error_trace_count'] = error_trace_result.rowcount

                # Delete executions for the group
                run_count = len(execution_ids)
                run_stmt = delete(ExecutionHistory).where(ExecutionHistory.group_id.in_(group_ids))
                await session.execute(run_stmt)
            else:
                # No group filtering - delete ALL (admin/system access)
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


    async def get_checkpoints_for_flow(
        self,
        flow_id,
        group_id: Optional[str] = None,
        status_filter: Optional[str] = "active"
    ) -> List[ExecutionHistory]:
        """
        Get available checkpoints for a specific flow.

        Args:
            flow_id: UUID of the flow to get checkpoints for
            group_id: Group ID for filtering (multi-tenant isolation)
            status_filter: Filter by checkpoint status ('active', 'resumed', 'expired', or None for all)

        Returns:
            List of ExecutionHistory records with checkpoint information
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging
        logger = logging.getLogger(__name__)

        try:
            # Build filters - must have flow_uuid (checkpoint enabled) and match flow_id
            filters = [
                ExecutionHistory.flow_id == flow_id,
                ExecutionHistory.flow_uuid.isnot(None)  # Only executions with checkpoints
            ]

            # Add group filtering for multi-tenant isolation
            if group_id:
                filters.append(ExecutionHistory.group_id == group_id)

            # Add status filter if provided
            if status_filter:
                filters.append(ExecutionHistory.checkpoint_status == status_filter)

            # Query for checkpoints ordered by most recent
            stmt = (
                select(ExecutionHistory)
                .where(*filters)
                .order_by(ExecutionHistory.created_at.desc())
            )
            result = await self.session.execute(stmt)
            checkpoints = result.scalars().all()

            logger.debug(f"Found {len(checkpoints)} checkpoints for flow {flow_id}")
            return list(checkpoints)

        except Exception as e:
            logger.error(f"Error getting checkpoints for flow {flow_id}: {str(e)}", exc_info=True)
            raise

    async def update_checkpoint_status(
        self,
        execution_id: int,
        status: str,
        group_id: Optional[str] = None
    ) -> bool:
        """
        Update the checkpoint status for an execution.

        Args:
            execution_id: ID of the execution to update
            status: New checkpoint status ('active', 'resumed', 'expired')
            group_id: Group ID for filtering (multi-tenant isolation)

        Returns:
            True if successful, False if execution not found
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging
        logger = logging.getLogger(__name__)

        try:
            # Build filters
            filters = [ExecutionHistory.id == execution_id]
            if group_id:
                filters.append(ExecutionHistory.group_id == group_id)

            # Find the execution
            stmt = select(ExecutionHistory).where(*filters)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"No execution found with id: {execution_id}")
                return False

            # Update the checkpoint status
            execution.checkpoint_status = status

            # Flush changes to database
            await self.session.flush()
            logger.info(f"Updated checkpoint status to '{status}' for execution {execution_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating checkpoint status for execution {execution_id}: {str(e)}", exc_info=True)
            return False

    async def set_checkpoint_info(
        self,
        execution_id: int,
        flow_uuid: str,
        checkpoint_status: str = "active",
        checkpoint_method: Optional[str] = None
    ) -> bool:
        """
        Set checkpoint information for an execution.

        Called when a flow execution with checkpoint enabled completes or checkpoints.

        Args:
            execution_id: ID of the execution
            flow_uuid: CrewAI state.id for resuming the flow
            checkpoint_status: Initial status (default: 'active')
            checkpoint_method: Name of the last checkpointed method

        Returns:
            True if successful, False if execution not found
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging
        logger = logging.getLogger(__name__)

        try:
            # Find the execution
            stmt = select(ExecutionHistory).where(ExecutionHistory.id == execution_id)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"No execution found with id: {execution_id}")
                return False

            # Set checkpoint information
            execution.flow_uuid = flow_uuid
            execution.checkpoint_status = checkpoint_status
            execution.checkpoint_method = checkpoint_method

            # Flush changes to database
            await self.session.flush()
            logger.info(f"Set checkpoint info for execution {execution_id}: flow_uuid={flow_uuid}, status={checkpoint_status}")
            return True

        except Exception as e:
            logger.error(f"Error setting checkpoint info for execution {execution_id}: {str(e)}", exc_info=True)
            return False

    async def add_crew_checkpoint(
        self,
        job_id: str,
        crew_node_id: str,
        crew_name: str,
        sequence: int,
        status: str,
        output_preview: str,
        completed_at: str
    ) -> bool:
        """
        Add a crew checkpoint to the execution's checkpoint_data.

        This allows granular resume functionality - users can choose which crew to resume from.

        Args:
            job_id: Job ID of the execution
            crew_node_id: Node ID of the crew in the flow
            crew_name: Human-readable name of the crew
            sequence: Order in which the crew executed (1, 2, 3...)
            status: Status of the crew ('completed' or 'failed')
            output_preview: First 500 chars of the crew output
            completed_at: ISO timestamp when the crew completed

        Returns:
            True if successful, False otherwise
        """
        try:
            # Find the execution
            result = await self.session.execute(
                select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            )
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"Execution not found for job_id: {job_id}")
                return False

            # Get existing checkpoint_data or initialize
            checkpoint_data = execution.checkpoint_data or {}
            crew_checkpoints = checkpoint_data.get("crew_checkpoints", [])

            # Add the new crew checkpoint
            new_checkpoint = {
                "crew_node_id": crew_node_id,
                "crew_name": crew_name,
                "sequence": sequence,
                "status": status,
                "output_preview": output_preview[:500] if output_preview else "",
                "completed_at": completed_at
            }
            crew_checkpoints.append(new_checkpoint)

            # Update checkpoint_data
            checkpoint_data["crew_checkpoints"] = crew_checkpoints
            execution.checkpoint_data = checkpoint_data

            # Flush changes
            await self.session.flush()
            logger.info(f"Added crew checkpoint for job {job_id}: crew={crew_name}, sequence={sequence}")
            return True

        except Exception as e:
            logger.error(f"Error adding crew checkpoint for job {job_id}: {str(e)}", exc_info=True)
            return False

    async def get_crew_checkpoints(self, job_id: str) -> list:
        """
        Get crew checkpoints for an execution.

        Args:
            job_id: Job ID of the execution

        Returns:
            List of crew checkpoint dictionaries
        """
        try:
            result = await self.session.execute(
                select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            )
            execution = result.scalar_one_or_none()

            if not execution or not execution.checkpoint_data:
                return []

            return execution.checkpoint_data.get("crew_checkpoints", [])

        except Exception as e:
            logger.error(f"Error getting crew checkpoints for job {job_id}: {str(e)}", exc_info=True)
            return []


# Don't create a singleton instance - repositories should be created with sessions
# execution_history_repository = ExecutionHistoryRepository()  # Removed - causes session issues