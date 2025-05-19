"""
Execution Status Service.

This service manages execution status operations:
- Updating execution status in the database
- Retrieving execution status
"""

import logging
from typing import Dict, Any, Optional

from src.models.execution_status import ExecutionStatus
from src.repositories.execution_repository import ExecutionRepository
from src.utils.asyncio_utils import execute_db_operation_with_fresh_engine

logger = logging.getLogger(__name__)

class ExecutionStatusService:
    """
    Service for managing execution status operations.
    """
    
    @staticmethod
    async def update_status(
        job_id: str,
        status: str,
        message: str,
        result: Any = None
    ) -> bool:
        """
        Update the status of an execution in the database.
        
        Args:
            job_id: Execution ID (string UUID, maps to job_id field)
            status: New status string value
            message: Status message
            result: Optional result data
            
        Returns:
            True if successful, False otherwise
        """
        # Validate job_id
        if not job_id or not isinstance(job_id, str):
            logger.error(f"[ExecutionStatusService] Invalid job_id: {job_id}")
            return False
            
        try:
            # Define the database operation
            async def _update_operation(session):
                repo = ExecutionRepository(session)
                
                # Find the execution record by job_id (string UUID)
                logger.debug(f"[ExecutionStatusService] Finding execution by job_id: {job_id} to update status.")
                execution_record = await repo.get_execution_by_job_id(job_id=job_id)
                
                if not execution_record:
                    logger.error(f"[ExecutionStatusService] Execution record not found for job_id: {job_id}. Cannot update status.")
                    return False
                
                # Get the integer primary key (id) from the record
                record_id = execution_record.id
                logger.debug(f"[ExecutionStatusService] Found record_id: {record_id} for job_id: {job_id}. Preparing update data.")

                # Prepare complete update data with all fields
                update_data = {
                    "status": status,
                    "error": message  # Changed from "message" to "error" to match the database column
                }
                
                # Add result if provided - properly handle JSON serialization
                if result is not None:
                    logger.info(f"[ExecutionStatusService] Processing result of type {type(result)} for job_id: {job_id}")
                    
                    # The result field is defined as JSON in the model
                    try:
                        # Check if we need to serialize to JSON
                        if isinstance(result, (dict, list)):
                            # For dict or list, store as is (SQLAlchemy handles JSON conversion)
                            update_data["result"] = result
                        else:
                            # For other types, convert to string representation
                            update_data["result"] = str(result)
                            
                        logger.info(f"[ExecutionStatusService] Successfully processed result for job_id: {job_id}")
                    except Exception as json_err:
                        logger.error(f"[ExecutionStatusService] Error processing result for job_id: {job_id}: {str(json_err)}")
                        # Still add the result as a string if JSON serialization fails
                        update_data["result"] = str(result)
                
                # Set completed_at if status is a terminal status
                if status in [ExecutionStatus.COMPLETED.value, ExecutionStatus.FAILED.value, ExecutionStatus.CANCELLED.value]:
                    from datetime import datetime
                    # Always set completed_at to current time for terminal statuses
                    update_data["completed_at"] = datetime.now()  # Use timezone-naive datetime
                    logger.info(f"[ExecutionStatusService] Setting completed_at for terminal status {status} on job {job_id}")
                
                logger.info(f"[ExecutionStatusService] Update data keys: {', '.join(update_data.keys())}")

                # Call the repository update method using the integer record_id
                logger.debug(f"[ExecutionStatusService] Calling repo.update_execution for record_id: {record_id} with status: {status}")
                updated_execution = await repo.update_execution(
                    execution_id=record_id, # Use the integer ID here
                    data=update_data
                )
                
                # Explicitly flush and commit the session to catch potential DB errors early
                if updated_execution:
                    logger.debug(f"[ExecutionStatusService] Flushing session after updating record_id: {record_id} for job_id: {job_id}")
                    await session.flush() # Send the UPDATE to the DB
                    logger.debug(f"[ExecutionStatusService] Committing transaction after flushing update for record_id: {record_id}")
                    await session.commit() # Attempt to COMMIT the transaction
                    logger.info(f"[ExecutionStatusService] Successfully committed status update for job_id: {job_id} (record_id: {record_id}) to {status}.")
                    return True
                else:
                    logger.error(f"[ExecutionStatusService] Failed to update execution for job_id: {job_id} (record_id: {record_id}). Update method returned None.")
                    # Rollback might be appropriate here if update returned None unexpectedly
                    await session.rollback()
                    return False

            # Execute the operation with a fresh engine/session
            return await execute_db_operation_with_fresh_engine(_update_operation)
                
        except Exception as e:
            logger.error(f"[ExecutionStatusService] Error during update/flush/commit for job_id {job_id}: {str(e)}", exc_info=True)
            return False
    
    @staticmethod
    async def get_status(execution_id: str) -> Optional[Any]:
        """
        Get the status of an execution from the database.
        
        Args:
            execution_id: Execution ID
            
        Returns:
            Execution object or None if not found
        """
        # Validate execution_id
        if not execution_id or not isinstance(execution_id, str):
            logger.error(f"[ExecutionStatusService] Invalid execution_id: {execution_id}")
            return None
            
        try:
            # Define the database operation
            async def _get_operation(session):
                repo = ExecutionRepository(session)
                return await repo.get_execution_by_job_id(job_id=execution_id)
            
            # Execute the operation with a fresh engine/session
            return await execute_db_operation_with_fresh_engine(_get_operation)
            
        except Exception as e:
            logger.error(f"Error getting execution status: {str(e)}")
            return None

    @staticmethod
    async def create_execution(execution_data: Dict[str, Any]) -> bool:
        """
        Create a new execution record in the database.
        
        Args:
            execution_data: Dictionary with execution data
            
        Returns:
            True if successful, False otherwise
        """
        from src.db.session import async_session_factory
        from src.repositories.execution_repository import ExecutionRepository
        
        # Validate job_id
        job_id = execution_data.get('job_id')
        if not job_id or not isinstance(job_id, str):
            logger.error(f"[ExecutionStatusService] Invalid job_id in execution data: {job_id}")
            return False
            
        try:
            # Create database session internally
            async with async_session_factory() as session:
                # Create repository instance
                repo = ExecutionRepository(session)
                
                # Check if record already exists
                existing = await repo.get_execution_by_job_id(job_id=job_id)
                if existing:
                    logger.info(f"[ExecutionStatusService] Execution record with job_id: {job_id} already exists, skipping creation")
                    return True
                
                # Create execution record
                logger.debug(f"[ExecutionStatusService] Creating execution record with job_id: {job_id}")
                execution = await repo.create_execution(data=execution_data)
                
                # Explicitly commit transaction
                await session.commit()
                
                logger.info(f"[ExecutionStatusService] Successfully created execution record with job_id: {job_id}")
                return True
        except Exception as e:
            logger.error(f"[ExecutionStatusService] Error creating execution record: {e}", exc_info=True)
            return False 