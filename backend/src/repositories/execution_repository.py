"""
Repository for execution data access.

This module provides database operations for execution models.
"""

from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete, update
from datetime import datetime, UTC
import logging

from src.models.execution_history import ExecutionHistory
from src.core.base_repository import BaseRepository
from src.schemas.execution import ExecutionStatus


class ExecutionRepository(BaseRepository[ExecutionHistory]):
    """Repository for execution data access operations."""
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ExecutionHistory, session)
    
    async def get_execution_history(
        self, 
        limit: int = 50, 
        offset: int = 0
    ) -> Tuple[List[ExecutionHistory], int]:
        """
        Get paginated execution history.
        
        Args:
            limit: Maximum number of items to return
            offset: Number of items to skip
            
        Returns:
            Tuple of (list of executions, total count)
        """
        # Get total count
        count_stmt = select(func.count()).select_from(ExecutionHistory)
        total_count_result = await self.session.execute(count_stmt)
        total_count = total_count_result.scalar() or 0
        
        # Get paginated executions
        stmt = select(ExecutionHistory).order_by(ExecutionHistory.created_at.desc()).offset(offset).limit(limit)
        result = await self.session.execute(stmt)
        executions = result.scalars().all()
        
        return executions, total_count
    
    async def get_execution_by_job_id(self, job_id: str) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by job_id.
        
        Args:
            job_id: Job ID of the execution
            
        Returns:
            Execution object if found, None otherwise
        """
        stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()
    
    async def create_execution(self, data: Dict[str, Any]) -> ExecutionHistory:
        """
        Create a new execution record.
        
        Args:
            data: Dictionary with execution data
            
        Returns:
            Created execution instance
        """
        # Ensure required fields are present
        required_fields = ['job_id', 'status']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field '{field}' in execution data")
        
        # Create execution object
        execution = ExecutionHistory(**data)
        self.session.add(execution)
        await self.session.flush()
        return execution
    
    async def update_execution(self, execution_id: int, data: Dict[str, Any]) -> Optional[ExecutionHistory]:
        """
        Update an existing execution.
        
        Args:
            execution_id: ID of the execution to update
            data: Dictionary with updated values
            
        Returns:
            Updated execution instance or None if not found
        """
        return await self.update(execution_id, data)
    
    async def update_execution_by_job_id(self, job_id: str, data: Dict[str, Any]) -> Optional[ExecutionHistory]:
        """
        Update an existing execution by job_id.
        
        Args:
            job_id: Job ID of the execution to update
            data: Dictionary with updated values
            
        Returns:
            Updated execution instance or None if not found
        """
        execution = await self.get_execution_by_job_id(job_id)
        if not execution:
            return None
            
        for key, value in data.items():
            setattr(execution, key, value)
            
        await self.session.flush()
        return execution
    
    async def update_execution_status(
        self, 
        job_id: str,  # Renamed parameter for clarity
        status: str, 
        message: str,
        result: Any = None
    ) -> Optional[ExecutionHistory]:
        """
        Update the status of an execution using its job_id.

        Args:
            job_id: Job ID (UUID) of the execution
            status: New status
            message: Status message
            result: Optional result data

        Returns:
            Updated execution or None if not found
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.debug(f"Updating execution status for job_id {job_id} to {status}")
            
            # Get the current execution to check created_at
            execution = await self.get_execution_by_job_id(job_id)
            if not execution:
                logger.warning(f"No execution found with job_id {job_id} during status update.")
                return None
            
            # Prepare update data
            update_data = {
                "status": status,
                "error": message,  # Store the message in the error field
                "updated_at": datetime.now(UTC)
            }
            
            # Add result if provided
            if result is not None:
                if isinstance(result, (dict, list)):
                    import json
                    try:
                        update_data["result"] = json.dumps(result)
                    except Exception as e:
                        logger.warning(f"Failed to JSON serialize result for {job_id}: {str(e)}")
                        update_data["result"] = str(result)
                else:
                    update_data["result"] = str(result)
                
            # Set completed_at if status is terminal
            if status in [
                ExecutionStatus.COMPLETED.value,
                ExecutionStatus.FAILED.value, 
                ExecutionStatus.CANCELLED.value
            ]:
                # Always set completed_at to current time to ensure it differs from created_at
                update_data["completed_at"] = datetime.now(UTC)
                logger.debug(f"Set completed_at for terminal status {status} on job {job_id}")

            # Perform the update directly using job_id
            stmt = (
                update(self.model)
                .where(self.model.job_id == job_id)
                .values(**update_data)
                .returning(self.model) # Return the updated row
            )
            
            result = await self.session.execute(stmt)
            updated_execution = result.scalars().first()
            
            if not updated_execution:
                logger.warning(f"No execution found with job_id {job_id} during status update.")
                return None

            # Explicitly commit the transaction
            await self.session.commit()
            logger.debug(f"Successfully committed status update for job_id {job_id} to {status}")
            
            return updated_execution
            
        except Exception as e:
            logger.error(f"Error updating execution status for {job_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    async def mark_execution_completed(self, execution_id: int, result: Optional[Dict[str, Any]] = None) -> Optional[ExecutionHistory]:
        """
        Mark an execution as completed.
        
        Args:
            execution_id: ID of the execution to update
            result: Optional result data
            
        Returns:
            Updated execution instance or None if not found
        """
        update_data = {
            "status": ExecutionStatus.COMPLETED.value,
            "completed_at": datetime.now(UTC)
        }
        
        if result:
            update_data["result"] = result
            
        return await self.update(execution_id, update_data)
    
    async def mark_execution_failed(self, execution_id: int, error: str) -> Optional[ExecutionHistory]:
        """
        Mark an execution as failed.
        
        Args:
            execution_id: ID of the execution to update
            error: Error message
            
        Returns:
            Updated execution instance or None if not found
        """
        update_data = {
            "status": ExecutionStatus.FAILED.value,
            "error": error,
            "completed_at": datetime.now(UTC)
        }
            
        return await self.update(execution_id, update_data)

def get_execution_repository(session: AsyncSession) -> ExecutionRepository:
    """
    Factory function to create and return an ExecutionRepository instance.
    
    Args:
        session: SQLAlchemy async session
        
    Returns:
        An instance of ExecutionRepository
    """
    return ExecutionRepository(session) 