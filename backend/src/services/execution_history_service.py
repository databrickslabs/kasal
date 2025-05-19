"""
Service for accessing and managing execution history.

This module provides functions for retrieving and managing execution history
from the database.
"""

from typing import Optional
import logging
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.execution_logs_repository import execution_logs_repository
from src.repositories.execution_trace_repository import execution_trace_repository
from src.repositories.execution_history_repository import execution_history_repository
from src.schemas.execution_history import (
    ExecutionHistoryItem, 
    ExecutionHistoryList,
    ExecutionOutput,
    ExecutionOutputList,
    ExecutionOutputDebug,
    ExecutionOutputDebugList,
    DeleteResponse
)

logger = logging.getLogger(__name__)

class ExecutionHistoryService:
    """Service for accessing and managing execution history."""
    
    def __init__(
        self, 
        execution_history_repository,
        execution_logs_repository,
        execution_trace_repository
    ):
        """
        Initialize the service with repositories.
        
        Args:
            execution_history_repository: Repository for execution history
            execution_logs_repository: Repository for execution logs
            execution_trace_repository: Repository for execution traces
        """
        self.history_repo = execution_history_repository
        self.logs_repo = execution_logs_repository
        self.trace_repo = execution_trace_repository
    
    async def get_execution_history(self, limit: int = 50, offset: int = 0) -> ExecutionHistoryList:
        """
        Get paginated execution history.
        
        Args:
            limit: Maximum number of items to return
            offset: Number of items to skip
            
        Returns:
            ExecutionHistoryList with paginated execution history items and metadata
        """
        try:
            # Use the repository to get the paginated data and total count
            runs, total_count = await self.history_repo.get_execution_history(
                limit=limit,
                offset=offset
            )
                
            # Convert each run to a pydantic model, handling string results properly
            execution_items = []
            for run in runs:
                if hasattr(run, 'result') and isinstance(run.result, str):
                    # Convert string result to a dictionary with the content field
                    run_dict = run.__dict__.copy()
                    run_dict['result'] = {"content": run.result}
                    # Create a model-compatible dictionary for validation
                    execution_items.append(ExecutionHistoryItem.model_validate(run_dict))
                else:
                    execution_items.append(ExecutionHistoryItem.model_validate(run))
            
            return ExecutionHistoryList(
                executions=execution_items,
                total=total_count,
                limit=limit,
                offset=offset
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving execution history: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving execution history: {str(e)}", exc_info=True)
            raise
    
    async def get_execution_by_id(self, execution_id: int) -> Optional[ExecutionHistoryItem]:
        """
        Get a specific execution by ID.
        
        Args:
            execution_id: ID of the execution to retrieve
            
        Returns:
            ExecutionHistoryItem or None if not found
        """
        try:
            # Use the repository to get the data
            run = await self.history_repo.get_execution_by_id(execution_id)
                
            if not run:
                return None
            
            # Check if result field is a string but should be a dictionary
            if hasattr(run, 'result') and isinstance(run.result, str):
                # Convert string result to a dictionary with the content field
                run_dict = run.__dict__.copy()
                run_dict['result'] = {"content": run.result}
                # Create a model-compatible dictionary for validation
                return ExecutionHistoryItem.model_validate(run_dict)
            else:
                return ExecutionHistoryItem.model_validate(run)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving execution {execution_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving execution {execution_id}: {str(e)}", exc_info=True)
            raise
    
    async def check_execution_exists(self, execution_id: int) -> bool:
        """
        Check if an execution exists.
        
        Args:
            execution_id: ID of the execution to check
            
        Returns:
            True if the execution exists, False otherwise
        """
        try:
            # Use the repository to check existence
            exists = await self.history_repo.check_execution_exists(execution_id)
                
            return exists
            
        except SQLAlchemyError as e:
            logger.error(f"Database error checking execution {execution_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error checking execution {execution_id}: {str(e)}")
            raise
    
    async def get_execution_outputs(
        self, 
        execution_id: str, 
        limit: int = 1000, 
        offset: int = 0
    ) -> ExecutionOutputList:
        """
        Get outputs for an execution.
        
        Args:
            execution_id: String ID of the execution (job_id in database)
            limit: Maximum number of items to return
            offset: Number of items to skip
            
        Returns:
            ExecutionOutputList with paginated execution outputs
        """
        try:
            # Get logs from our repository
            logs = await self.logs_repo.get_by_execution_id(
                execution_id=execution_id,
                limit=limit,
                offset=offset,
                newest_first=True
            )
                
            # Get total count
            total_count = await self.logs_repo.count_by_execution_id(
                execution_id=execution_id
            )
            
            # Convert to schema objects
            output_items = [
                ExecutionOutput(
                    id=log.id,
                    job_id=log.execution_id,
                    output=log.content,
                    timestamp=log.timestamp
                ) for log in logs
            ]
            
            return ExecutionOutputList(
                execution_id=execution_id,
                outputs=output_items,
                limit=limit,
                offset=offset,
                total=total_count
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving outputs for execution {execution_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving outputs for execution {execution_id}: {str(e)}")
            raise
    
    async def get_debug_outputs(self, execution_id: str) -> ExecutionOutputDebugList:
        """
        Get debug information about outputs for an execution.
        
        Args:
            execution_id: String ID of the execution (job_id in database)
            
        Returns:
            ExecutionOutputDebugList with debug information
        """
        try:
            # Check if the run exists
            run = await self.history_repo.get_execution_by_job_id(execution_id)
                
            if not run:
                return None
            
            # Get logs from our repository
            logs = await self.logs_repo.get_by_execution_id(
                execution_id=execution_id
            )
            
            # Create debug items
            debug_items = []
            for log in logs:
                debug_items.append(ExecutionOutputDebug(
                    id=log.id,
                    timestamp=log.timestamp,
                    task_name=None,  # These fields aren't in our new model
                    agent_name=None,
                    output_preview=log.content[:200] if log.content else None
                ))
            
            return ExecutionOutputDebugList(
                execution_id=execution_id,
                debug_items=debug_items
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving debug outputs for execution {execution_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving debug outputs for execution {execution_id}: {str(e)}")
            raise
    
    async def delete_all_executions(self) -> DeleteResponse:
        """
        Delete all executions and their associated data.
        
        Returns:
            DeleteResponse with information about the deleted data
        """
        try:
            logger.info("Attempting to delete all executions and their associated data")
            
            # Delete all traces first to avoid foreign key constraint violations
            trace_count = await self.trace_repo.delete_all()
            
            # Delete all logs
            log_count = await self.logs_repo.delete_all_with_managed_session()
            
            # Delete all executions and associated data last (after dependent records are gone)
            result = await self.history_repo.delete_all_executions()
            
            return DeleteResponse(
                success=True,
                message=f"Deleted {result['run_count']} executions, {result['task_status_count']} task statuses, "
                        f"{result['error_trace_count']} error traces, {log_count} logs, and {trace_count} traces."
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting all executions: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting all executions: {str(e)}")
            raise
    
    async def delete_execution(self, execution_id: int) -> DeleteResponse:
        """
        Delete a specific execution and its associated data.
        
        Args:
            execution_id: ID of the execution to delete
            
        Returns:
            DeleteResponse with information about the deleted data
        """
        try:
            logger.info(f"Attempting to delete execution {execution_id} and its associated data")
            
            # First get the job_id
            run = await self.history_repo.get_execution_by_id(execution_id)
            if not run:
                return None
                
            job_id = run.job_id
            
            # Delete associated traces first (to avoid foreign key constraint violations)
            trace_count = await self.trace_repo.delete_by_job_id(job_id)
            
            # Delete execution logs - use the managed session version
            output_count = await self.logs_repo.delete_by_execution_id_with_managed_session(job_id)
            
            # Delete execution using repository (after dependent records are gone)
            result = await self.history_repo.delete_execution(execution_id)
            
            return DeleteResponse(
                success=True,
                message=f"Deleted execution {execution_id} (job_id: {job_id}), "
                        f"{result['task_status_count']} task statuses, "
                        f"{result['error_trace_count']} error traces, "
                        f"{trace_count} traces, and {output_count} logs."
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting execution {execution_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting execution {execution_id}: {str(e)}")
            raise
    
    async def delete_execution_by_job_id(self, job_id: str) -> DeleteResponse:
        """
        Delete a specific execution and its associated data by job_id (UUID).
        
        Args:
            job_id: The job_id (UUID) of the execution
            
        Returns:
            DeleteResponse with information about the deleted data
        """
        try:
            logger.info(f"Attempting to delete execution with job_id {job_id} and its associated data")
            
            # Check if the execution exists
            run = await self.history_repo.get_execution_by_job_id(job_id)
            if not run:
                return None
                
            execution_id = run.id
            
            # Delete associated traces first (to avoid foreign key constraint violations)
            trace_count = await self.trace_repo.delete_by_job_id(job_id)
            
            # Delete execution logs - use the managed session version
            output_count = await self.logs_repo.delete_by_execution_id_with_managed_session(job_id)
            
            # Delete execution using repository (after dependent records are gone)
            result = await self.history_repo.delete_execution_by_job_id(job_id)
            
            return DeleteResponse(
                success=True,
                message=f"Deleted execution {execution_id} (job_id: {job_id}), "
                        f"{result['task_status_count']} task statuses, "
                        f"{result['error_trace_count']} error traces, "
                        f"{trace_count} traces, and {output_count} logs."
            )
            
        except SQLAlchemyError as e:
            logger.error(f"Database error deleting execution with job_id {job_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error deleting execution with job_id {job_id}: {str(e)}")
            raise
    
    async def get_execution_by_job_id(self, job_id: str) -> Optional[ExecutionHistoryItem]:
        """
        Get execution history by job_id (UUID).
        
        Args:
            job_id: String ID of the execution (job_id in database)
            
        Returns:
            ExecutionHistoryItem if found, None otherwise
        """
        try:
            # Use the repository to get the data
            run = await self.history_repo.get_execution_by_job_id(job_id)
                
            if not run:
                return None
            
            # Check if result field is a string but should be a dictionary
            if hasattr(run, 'result') and isinstance(run.result, str):
                # Convert string result to a dictionary with the content field
                run_dict = run.__dict__.copy()
                run_dict['result'] = {"content": run.result}
                # Create a model-compatible dictionary for validation
                return ExecutionHistoryItem.model_validate(run_dict)
            else:
                return ExecutionHistoryItem.model_validate(run)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving execution with job_id {job_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving execution with job_id {job_id}: {str(e)}", exc_info=True)
            raise

def get_execution_history_service() -> ExecutionHistoryService:
    """
    Factory function to create an instance of ExecutionHistoryService.
    
    Returns:
        ExecutionHistoryService instance initialized with repositories
    """
    return ExecutionHistoryService(
        execution_history_repository=execution_history_repository,
        execution_logs_repository=execution_logs_repository,
        execution_trace_repository=execution_trace_repository
    ) 