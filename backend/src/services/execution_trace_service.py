"""
Service for accessing and managing execution traces.

This module provides functions for retrieving and managing execution traces
from the database.
"""

from typing import List, Optional, Dict, Any
import logging
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.execution_trace_repository import execution_trace_repository
from src.schemas.execution_trace import (
    ExecutionTraceItem,
    ExecutionTraceList,
    ExecutionTraceResponseByRunId,
    ExecutionTraceResponseByJobId,
    DeleteTraceResponse
)

from src.core.logger import LoggerManager

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

class ExecutionTraceService:
    """Service for accessing and managing execution traces."""
    
    @staticmethod
    async def get_traces_by_run_id(
        db, 
        run_id: int,
        limit: int = 100,
        offset: int = 0
    ) -> ExecutionTraceResponseByRunId:
        """
        Get traces for an execution by run_id with pagination.
        
        Args:
            db: No longer used, kept for backward compatibility
            run_id: Database ID of the execution
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            ExecutionTraceResponseByRunId with traces for the execution
        """
        try:
            # Check if the execution exists using repository
            job_id = await execution_trace_repository.get_execution_job_id_by_run_id(run_id)
            
            if not job_id:
                return None
            
            # Get traces using repository
            traces = await execution_trace_repository.get_by_run_id(
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
    
    @staticmethod
    async def get_traces_by_job_id(
        db, 
        job_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> ExecutionTraceResponseByJobId:
        """
        Get traces for an execution by job_id with pagination.
        
        Args:
            db: No longer used, kept for backward compatibility
            job_id: String ID of the execution (job_id in database)
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            ExecutionTraceResponseByJobId with traces for the execution
        """
        try:
            # Check if the execution exists using repository
            run_id = await execution_trace_repository.get_execution_run_id_by_job_id(job_id)
            
            if not run_id:
                return None
            
            # Get traces using repository - direct lookup by job_id
            traces = await execution_trace_repository.get_by_job_id(
                job_id,
                limit,
                offset
            )
            
            # If no traces found using the direct job_id field, try via the run_id (for backward compatibility)
            if not traces:
                logger.info(f"No traces found directly with job_id {job_id}, trying via run_id lookup")
                traces = await execution_trace_repository.get_by_run_id(
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
    
    @staticmethod
    async def get_all_traces(
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
            traces, total_count = await execution_trace_repository.get_all_traces(
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
    
    @staticmethod
    async def get_trace_by_id(trace_id: int) -> Optional[ExecutionTraceItem]:
        """
        Get a specific trace by ID.
        
        Args:
            trace_id: ID of the trace to retrieve
            
        Returns:
            ExecutionTraceItem if found, None otherwise
        """
        try:
            trace = await execution_trace_repository.get_by_id(trace_id)
                
            if not trace:
                return None
                
            return ExecutionTraceItem.model_validate(trace)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving trace {trace_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving trace {trace_id}: {str(e)}")
            raise
    
    @staticmethod
    async def create_trace(trace_data: Dict[str, Any]) -> ExecutionTraceItem:
        """
        Create a new trace.
        
        Args:
            trace_data: Dictionary with trace data
            
        Returns:
            Created ExecutionTraceItem
        """
        try:
            trace = await execution_trace_repository.create(trace_data)
                
            return ExecutionTraceItem.model_validate(trace)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error creating trace: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error creating trace: {str(e)}")
            raise
    
    @staticmethod
    async def delete_trace(trace_id: int) -> Optional[DeleteTraceResponse]:
        """
        Delete a specific trace by ID.
        
        Args:
            trace_id: ID of the trace to delete
            
        Returns:
            DeleteTraceResponse with information about the deleted trace
        """
        try:
            # Check if the trace exists first
            trace = await execution_trace_repository.get_by_id(trace_id)
            
            if not trace:
                return None
            
            # Delete the trace
            deleted_count = await execution_trace_repository.delete_by_id(trace_id)
            
            return DeleteTraceResponse(
                deleted_count=deleted_count,
                message=f"Successfully deleted trace {trace_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting trace {trace_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting trace {trace_id}: {str(e)}")
            raise
    
    @staticmethod
    async def delete_traces_by_run_id(run_id: int) -> DeleteTraceResponse:
        """
        Delete all traces for a specific execution.
        
        Args:
            run_id: Database ID of the execution
            
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Delete the traces
            deleted_count = await execution_trace_repository.delete_by_run_id(run_id)
            
            return DeleteTraceResponse(
                deleted_count=deleted_count,
                message=f"Successfully deleted {deleted_count} traces for execution {run_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting traces for execution {run_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting traces for execution {run_id}: {str(e)}")
            raise
    
    @staticmethod
    async def delete_traces_by_job_id(job_id: str) -> DeleteTraceResponse:
        """
        Delete all traces for a specific job.
        
        Args:
            job_id: String ID of the execution (job_id)
            
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Delete the traces
            deleted_count = await execution_trace_repository.delete_by_job_id(job_id)
            
            return DeleteTraceResponse(
                deleted_count=deleted_count,
                message=f"Successfully deleted {deleted_count} traces for job {job_id}"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting traces for job_id {job_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting traces for job_id {job_id}: {str(e)}")
            raise
    
    @staticmethod
    async def delete_all_traces() -> DeleteTraceResponse:
        """
        Delete all execution traces.
        
        Returns:
            DeleteTraceResponse with information about deleted traces
        """
        try:
            # Delete all traces
            deleted_count = await execution_trace_repository.delete_all()
            
            return DeleteTraceResponse(
                deleted_count=deleted_count,
                message=f"Successfully deleted all traces ({deleted_count} total)"
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting all traces: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting all traces: {str(e)}")
            raise 