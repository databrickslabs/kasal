"""
Service for accessing and managing execution traces.

This module provides functions for retrieving and managing execution traces
from the database.
"""

from typing import List, Optional, Dict, Any
import logging
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.execution_trace_repository import ExecutionTraceRepository
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.schemas.execution_trace import (
    ExecutionTraceItem,
    ExecutionTraceList,
    ExecutionTraceResponseByRunId,
    ExecutionTraceResponseByJobId,
    DeleteTraceResponse
)
from src.utils.user_context import GroupContext

from src.core.logger import LoggerManager

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

class ExecutionTraceService:
    """Service for accessing and managing execution traces."""

    def __init__(self, session):
        """
        Initialize the service with session.

        Args:
            session: Database session from FastAPI DI (from core.dependencies)
        """
        self.session = session
        self.repository = ExecutionTraceRepository(session)
        self.execution_history_repository = ExecutionHistoryRepository(session)

    async def get_traces_by_run_id(
        self,
        group_context=None,
        run_id: int = None,
        limit: int = 100,
        offset: int = 0
    ) -> ExecutionTraceResponseByRunId:
        """
        Get traces for an execution by run_id with pagination and authorization.
        
        Args:
            group_context: Group context for authorization (contains group_ids and email)
            run_id: Database ID of the execution
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            ExecutionTraceResponseByRunId with traces for the execution if authorized
        """
        try:
            # First check if the execution exists and belongs to the user's group
            
            # Get group IDs from context for filtering
            group_ids = group_context.group_ids if group_context else None
            
            # Check if the execution exists and the user has access to it
            execution = await self.execution_history_repository.get_execution_by_id(
                run_id, 
                group_ids=group_ids
            )
            
            if not execution:
                # Either doesn't exist or user doesn't have access
                return None
            
            # Get job_id from the execution
            job_id = execution.job_id
            
            # Get traces using repository
            traces = await self.repository.get_by_run_id(
                run_id,
                limit,
                offset
            )
            
            # Get job_id for these traces if needed
            if traces and not all(trace.job_id for trace in traces):
                # Update any missing job_id values
                for trace in traces:
                    if not trace.job_id:
                        trace.job_id = job_id
            
            # Convert to schema objects
            trace_items = [ExecutionTraceItem.model_validate(trace) for trace in traces]
            
            return ExecutionTraceResponseByRunId(
                run_id=run_id,
                traces=trace_items
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for execution {run_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving traces for execution {run_id}: {str(e)}")
            raise
    
    async def get_traces_by_job_id(
        self,
        group_context=None, 
        job_id: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> ExecutionTraceResponseByJobId:
        """
        Get traces for an execution by job_id with pagination and authorization.
        
        Args:
            group_context: Group context for authorization (contains group_ids and email)
            job_id: String ID of the execution (job_id in database)
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            ExecutionTraceResponseByJobId with traces for the execution if authorized
        """
        try:
            # First check if the execution exists and belongs to the user's group
            
            # Get group IDs from context for filtering
            group_ids = group_context.group_ids if group_context else None
            
            # Check if the execution exists and the user has access to it
            execution = await self.execution_history_repository.get_execution_by_job_id(
                job_id, 
                group_ids=group_ids
            )
            
            if not execution:
                # Either doesn't exist or user doesn't have access
                return None
            
            # Get the run_id from the execution
            run_id = execution.id
            
            # Get traces using repository - direct lookup by job_id
            traces = await self.repository.get_by_job_id(
                job_id,
                limit,
                offset
            )
            
            # If no traces found using the direct job_id field, try via the run_id (for backward compatibility)
            if not traces:
                logger.debug(f"No traces found directly with job_id {job_id}, trying via run_id lookup")
                traces = await self.repository.get_by_run_id(
                    run_id,
                    limit,
                    offset
                )
                
                # Update job_id for these traces if it's missing
                for trace in traces:
                    if not trace.job_id:
                        trace.job_id = job_id
            
            # Convert to schema objects
            trace_items = [ExecutionTraceItem.model_validate(trace) for trace in traces]
            
            return ExecutionTraceResponseByJobId(
                job_id=job_id,
                traces=trace_items
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for execution with job_id {job_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving traces for execution with job_id {job_id}: {str(e)}")
            raise
    
    async def get_all_traces(self,
        limit: int = 100,
        offset: int = 0
    ) -> ExecutionTraceList:
        """
        Get all traces with pagination.
        
        Args:
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            ExecutionTraceList with paginated traces
        """
        try:
            # Get all traces using repository
            traces, total_count = await self.repository.get_all_traces(
                limit,
                offset
            )
            
            # Convert to schema objects
            trace_items = [ExecutionTraceItem.model_validate(trace) for trace in traces]
            
            return ExecutionTraceList(
                traces=trace_items,
                total=total_count,
                limit=limit,
                offset=offset
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving all traces: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving all traces: {str(e)}")
            raise
    
    async def get_all_traces_for_group(
        self,
        group_context: GroupContext,
        limit: int = 100,
        offset: int = 0
    ) -> ExecutionTraceList:
        """
        Get all traces for a specific group with pagination.
        
        Args:
            group_context: Group context for authorization
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            ExecutionTraceList with paginated traces for the group
        """
        try:
            if not group_context or not group_context.group_ids:
                return ExecutionTraceList(traces=[], total=0, limit=limit, offset=offset)
            
            # Get all executions for the group first
            executions = await self.execution_history_repository.get_all_executions_for_groups(
                group_ids=group_context.group_ids
            )
            
            if not executions:
                return ExecutionTraceList(traces=[], total=0, limit=limit, offset=offset)
            
            # Get job_ids from executions
            job_ids = [exec.job_id for exec in executions if exec.job_id]
            
            if not job_ids:
                return ExecutionTraceList(traces=[], total=0, limit=limit, offset=offset)
            
            # Get traces for these job_ids
            traces = []
            for job_id in job_ids:
                job_traces = await self.repository.get_by_job_id(job_id, limit=100, offset=0)
                traces.extend(job_traces)
            
            # Apply pagination
            total_count = len(traces)
            traces = traces[offset:offset + limit]
            
            # Convert to schema objects
            trace_items = [ExecutionTraceItem.model_validate(trace) for trace in traces]
            
            return ExecutionTraceList(
                traces=trace_items,
                total=total_count,
                limit=limit,
                offset=offset
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving group traces: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving group traces: {str(e)}")
            raise
    
    async def get_trace_by_id(self, trace_id: int) -> Optional[ExecutionTraceItem]:
        """
        Get a specific trace by ID.
        
        Args:
            trace_id: ID of the trace to retrieve
            
        Returns:
            ExecutionTraceItem if found, None otherwise
        """
        try:
            trace = await self.repository.get_by_id(trace_id)
                
            if not trace:
                return None
                
            return ExecutionTraceItem.model_validate(trace)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving trace {trace_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving trace {trace_id}: {str(e)}")
            raise
    
    async def get_trace_by_id_with_group_check(self,
        trace_id: int,
        group_context: GroupContext
    ) -> Optional[ExecutionTraceItem]:
        """
        Get a specific trace by ID with group authorization.
        
        Args:
            trace_id: ID of the trace to retrieve
            group_context: Group context for authorization
            
        Returns:
            ExecutionTraceItem if found and authorized, None otherwise
        """
        try:
            trace = await self.repository.get_by_id(trace_id)
                
            if not trace:
                return None
            
            # Check if trace belongs to user's group via job_id
            if trace.job_id and group_context and group_context.group_ids:
                execution = await self.execution_history_repository.get_execution_by_job_id(
                    trace.job_id,
                    group_ids=group_context.group_ids
                )
                if not execution:
                    return None  # Not authorized
                
            return ExecutionTraceItem.model_validate(trace)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving trace {trace_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving trace {trace_id}: {str(e)}")
            raise
    
    async def create_trace(self, trace_data: Dict[str, Any]) -> ExecutionTraceItem:
        """
        Create a new trace.

        Args:
            trace_data: Dictionary with trace data

        Returns:
            Created ExecutionTraceItem

        Raises:
            ValueError: If job_id doesn't exist in ExecutionHistory
        """
        try:
            trace = await self.repository.create(trace_data)

            return ExecutionTraceItem.model_validate(trace)

        except ValueError as e:
            # Log the error but don't re-raise for missing jobs
            logger.warning(f"Trace creation skipped: {str(e)}")
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error creating trace: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error creating trace: {str(e)}")
            raise
    
    async def create_trace_with_group(
        self,
        trace_data: Dict[str, Any],
        group_context: GroupContext
    ) -> ExecutionTraceItem:
        """
        Create a new trace with group verification.
        
        Args:
            trace_data: Dictionary with trace data
            group_context: Group context for authorization
            
        Returns:
            Created ExecutionTraceItem
        """
        try:
            # If job_id is provided, verify it belongs to the group
            if 'job_id' in trace_data and trace_data['job_id'] and group_context and group_context.group_ids:
                execution = await self.execution_history_repository.get_execution_by_job_id(
                    trace_data['job_id'],
                    group_ids=group_context.group_ids
                )
                if not execution:
                    raise ValueError("Not authorized to create trace for this job")
            
            trace = await self.repository.create(trace_data)
                
            return ExecutionTraceItem.model_validate(trace)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error creating trace: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error creating trace: {str(e)}")
            raise
    
    async def delete_trace(self, trace_id: int) -> Optional[DeleteTraceResponse]:
        """
        Delete a specific trace by ID.
        
        Args:
            trace_id: ID of the trace to delete
            
        Returns:
            DeleteTraceResponse with information about the deleted trace
        """
        try:
            # Check if the trace exists first
            trace = await self.repository.get_by_id(trace_id)
            
            if not trace:
                return None
            
            # Delete the trace
            deleted_count = await self.repository.delete_by_id(trace_id)
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted trace {trace_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting trace {trace_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting trace {trace_id}: {str(e)}")
            raise
    
    async def delete_trace_with_group_check(self,
        trace_id: int,
        group_context: GroupContext
    ) -> Optional[DeleteTraceResponse]:
        """
        Delete a specific trace by ID with group authorization.
        
        Args:
            trace_id: ID of the trace to delete
            group_context: Group context for authorization
            
        Returns:
            DeleteTraceResponse with information about the deleted trace
        """
        try:
            # Check if the trace exists and belongs to group
            trace = await self.repository.get_by_id(trace_id)
            
            if not trace:
                return None
            
            # Check authorization via job_id
            if trace.job_id and group_context and group_context.group_ids:
                execution = await self.execution_history_repository.get_execution_by_job_id(
                    trace.job_id,
                    group_ids=group_context.group_ids
                )
                if not execution:
                    return None  # Not authorized
            
            # Delete the trace
            deleted_count = await self.repository.delete_by_id(trace_id)
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted trace {trace_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting trace {trace_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting trace {trace_id}: {str(e)}")
            raise
    
    async def delete_traces_by_run_id(self, run_id: int) -> DeleteTraceResponse:
        """
        Delete all traces for a specific execution.
        
        Args:
            run_id: Database ID of the execution
            
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Delete the traces
            deleted_count = await self.repository.delete_by_run_id(run_id)
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted {deleted_count} traces for execution {run_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting traces for execution {run_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting traces for execution {run_id}: {str(e)}")
            raise
    
    async def delete_traces_by_run_id_with_group_check(self,
        run_id: int,
        group_context: GroupContext
    ) -> DeleteTraceResponse:
        """
        Delete all traces for a specific execution with group authorization.
        
        Args:
            run_id: Database ID of the execution
            group_context: Group context for authorization
            
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Check if execution belongs to group
            if group_context and group_context.group_ids:
                execution = await self.execution_history_repository.get_execution_by_id(
                    run_id,
                    group_ids=group_context.group_ids
                )
                if not execution:
                    return DeleteTraceResponse(
                        deleted_traces=0,
                        message="Execution not found or not authorized"
                    )
            
            # Delete the traces
            deleted_count = await self.repository.delete_by_run_id(run_id)
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted {deleted_count} traces for execution {run_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting traces for execution {run_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting traces for execution {run_id}: {str(e)}")
            raise
    
    async def delete_traces_by_job_id(self, job_id: str) -> DeleteTraceResponse:
        """
        Delete all traces for a specific job.
        
        Args:
            job_id: String ID of the execution (job_id)
            
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Delete the traces
            deleted_count = await self.repository.delete_by_job_id(job_id)
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted {deleted_count} traces for job {job_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting traces for job_id {job_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting traces for job_id {job_id}: {str(e)}")
            raise
    
    async def delete_traces_by_job_id_with_group_check(self,
        job_id: str,
        group_context: GroupContext
    ) -> DeleteTraceResponse:
        """
        Delete all traces for a specific job with group authorization.
        
        Args:
            job_id: String ID of the execution (job_id)
            group_context: Group context for authorization
            
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Check if job belongs to group
            if group_context and group_context.group_ids:
                execution = await self.execution_history_repository.get_execution_by_job_id(
                    job_id,
                    group_ids=group_context.group_ids
                )
                if not execution:
                    return DeleteTraceResponse(
                        deleted_traces=0,
                        message="Job not found or not authorized"
                    )
            
            # Delete the traces
            deleted_count = await self.repository.delete_by_job_id(job_id)
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted {deleted_count} traces for job {job_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting traces for job_id {job_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting traces for job_id {job_id}: {str(e)}")
            raise
    
    async def delete_all_traces(self) -> DeleteTraceResponse:
        """
        Delete all execution traces.
        
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Delete all traces
            deleted_count = await self.repository.delete_all()
            
            return DeleteTraceResponse(
                deleted_traces=deleted_count,
                message=f"Successfully deleted all traces ({deleted_count} total)"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting all traces: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting all traces: {str(e)}")
            raise
    
    async def delete_all_traces_for_group(self,
        group_context: GroupContext
    ) -> DeleteTraceResponse:
        """
        Delete all execution traces for a specific group.
        
        Args:
            group_context: Group context for authorization
        
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            if not group_context or not group_context.group_ids:
                return DeleteTraceResponse(
                    deleted_traces=0,
                    message="No group context provided"
                )
            
            # Get all executions for the group
            executions = await self.execution_history_repository.get_all_executions_for_groups(
                group_ids=group_context.group_ids
            )
            
            if not executions:
                return DeleteTraceResponse(
                    deleted_traces=0,
                    message="No executions found for group"
                )
            
            # Delete traces for each execution
            total_deleted = 0
            for execution in executions:
                if execution.job_id:
                    deleted_count = await self.repository.delete_by_job_id(execution.job_id)
                    total_deleted += deleted_count
            
            return DeleteTraceResponse(
                deleted_traces=total_deleted,
                message=f"Successfully deleted {total_deleted} traces for group"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting group traces: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting group traces: {str(e)}")
            raise 