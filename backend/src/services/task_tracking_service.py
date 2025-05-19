import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from fastapi import HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.repositories.task_tracking_repository import TaskTrackingRepository
from src.schemas.task_tracking import (
    JobExecutionStatusResponse, 
    TaskStatusSchema,
    TaskStatusEnum,
    TaskStatusCreate,
    TaskStatusUpdate,
    TaskStatusResponse
)
from src.core.logger import LoggerManager
from src.models.execution_history import TaskStatus as DBTaskStatus, ExecutionHistory, ErrorTrace

logger = logging.getLogger(__name__)
# Get crew logger from the centralized logging system
crew_logger = LoggerManager.get_instance().crew

class TaskTrackingService:
    """
    Service for task tracking and job execution status.
    Acts as an intermediary between the API router and the repository.
    """
    
    def __init__(self, repository: TaskTrackingRepository):
        """
        Initialize service with repository.
        
        Args:
            repository: TaskTrackingRepository instance
        """
        self.repository = repository
    
    async def get_job_status(self, job_id: str) -> JobExecutionStatusResponse:
        """
        Get job execution status including all task statuses.
        
        Args:
            job_id: Job identifier
            
        Returns:
            JobExecutionStatusResponse with job status and task statuses
            
        Raises:
            HTTPException: If job not found or status retrieval fails
        """
        try:
            # Get job execution status from repository
            job_status = await self.repository.get_job_execution_status(job_id)
            
            # Convert to response model
            return JobExecutionStatusResponse.model_validate(job_status)
        except ValueError as e:
            logger.warning(f"Job not found: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Job not found with ID: {job_id}"
            )
        except Exception as e:
            logger.error(f"Error retrieving job execution status: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve job execution status: {str(e)}"
            ) 
            
    async def get_all_tasks(self) -> List[TaskStatusResponse]:
        """
        Get all task statuses.
        
        Returns:
            List of task statuses
        """
        try:
            all_tasks = await self.repository.get_all_tasks()
            return [TaskStatusResponse.model_validate(task) for task in all_tasks]
        except Exception as e:
            logger.error(f"Error retrieving all tasks: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve tasks: {str(e)}"
            )
            
    async def create_task(self, task: TaskStatusCreate) -> TaskStatusResponse:
        """
        Create a new task status.
        
        Args:
            task: Task status data to create
            
        Returns:
            Created task status
        """
        try:
            db_task = await self.repository.create_task(task)
            return TaskStatusResponse.model_validate(db_task)
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create task: {str(e)}"
            )
            
    async def update_task(self, task_id: int, task: TaskStatusUpdate) -> TaskStatusResponse:
        """
        Update a task status.
        
        Args:
            task_id: ID of the task to update
            task: Updated task status data
            
        Returns:
            Updated task status
        """
        try:
            db_task = await self.repository.update_task(task_id, task)
            if not db_task:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Task with ID {task_id} not found"
                )
            return TaskStatusResponse.model_validate(db_task)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating task: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update task: {str(e)}"
            )

    # Add synchronous methods for crew functionality
    @classmethod
    def for_crew(cls, db: Session) -> 'TaskTrackingService':
        """
        Create a TaskTrackingService instance for crew operations with a synchronous session.
        
        Args:
            db: SQLAlchemy synchronous session
            
        Returns:
            TaskTrackingService instance configured for synchronous operations
        """
        repository = TaskTrackingRepository(db)
        service = cls(repository)
        service.db = db  # Keep db reference for crew functionality
        return service
        
    @classmethod
    def for_crew_with_repo(cls, repository: TaskTrackingRepository) -> 'TaskTrackingService':
        """
        Create a TaskTrackingService instance for crew operations with an existing repository.
        
        Args:
            repository: Existing TaskTrackingRepository instance
            
        Returns:
            TaskTrackingService instance configured for synchronous operations
        """
        service = cls(repository)
        service.db = repository.db if hasattr(repository, 'db') else None
        return service
    
    def create_task_status(self, job_id: str, task_id: str, agent_name: Optional[str] = None) -> TaskStatusResponse:
        """
        Create a new task status entry with RUNNING status.
        
        Args:
            job_id: The ID of the job
            task_id: The ID/key of the task
            agent_name: The name of the agent assigned to this task (optional)
            
        Returns:
            The created task status record
        """
        #crew_logger.info(f"Creating task status for job {job_id}, task {task_id}, agent {agent_name}")
        
        task_status = TaskStatusCreate(
            job_id=job_id,
            task_id=task_id,
            status=TaskStatusEnum.RUNNING,
            agent_name=agent_name
        )
        
        db_task_status = self.repository.create_task_status(task_status)
        #crew_logger.info(f"Created task status record: {db_task_status.id}")
        
        return TaskStatusResponse.model_validate(db_task_status)
    
    def update_task_status(self, job_id: str, task_id: str, status: TaskStatusEnum) -> Optional[TaskStatusResponse]:
        """
        Update the status of a task.
        
        Args:
            job_id: The ID of the job
            task_id: The ID/key of the task
            status: The new status (use TaskStatusEnum)
            
        Returns:
            The updated task status record or None if not found
        """
        crew_logger.info(f"Updating task status for job {job_id}, task {task_id} to {status}")
        
        task_status = TaskStatusUpdate(status=status)
        db_task_status = self.repository.update_task_status(job_id, task_id, task_status)
        
        if not db_task_status:
            crew_logger.warning(f"No task status found for job {job_id}, task {task_id}")
            return None
        
        #crew_logger.info(f"Updated task status to {status} for job {job_id}, task {task_id}")
        return TaskStatusResponse.model_validate(db_task_status)
    
    def get_task_status(self, job_id: str, task_id: str) -> Optional[TaskStatusResponse]:
        """
        Get the current status of a task.
        
        Args:
            job_id: The ID of the job
            task_id: The ID/key of the task
            
        Returns:
            The task status record or None if not found
        """
        db_task_status = self.repository.get_task_status(job_id, task_id)
        
        if not db_task_status:
            return None
            
        return TaskStatusResponse.model_validate(db_task_status)
    
    def get_task_status_by_task_id(self, task_id: str) -> Optional[TaskStatusResponse]:
        """
        Get the current status of a task using only the task_id.
        This is used when we don't have the job_id readily available.
        
        Args:
            task_id: The ID/key of the task
            
        Returns:
            The task status record or None if not found
        """
        db_task_status = self.repository.get_task_status_by_task_id(task_id)
        
        if not db_task_status:
            return None
            
        return TaskStatusResponse.model_validate(db_task_status)
    
    def get_all_task_statuses(self, job_id: str) -> List[TaskStatusResponse]:
        """
        Get all task statuses for a job.
        
        Args:
            job_id: The ID of the job
            
        Returns:
            List of task status records
        """
        db_task_statuses = self.repository.get_all_task_statuses(job_id)
        return [TaskStatusResponse.model_validate(status) for status in db_task_statuses]
    
    def create_task_statuses_for_job(self, job_id: str, tasks_config: Dict[str, Dict]) -> List[TaskStatusResponse]:
        """
        Create task status entries for all tasks in a job with RUNNING status.
        
        Args:
            job_id: The ID of the job
            tasks_config: Dictionary of tasks with their configurations
            
        Returns:
            List of created task status records
        """
        #crew_logger.info(f"Creating task statuses for all tasks in job {job_id}")
        
        db_task_statuses = self.repository.create_task_statuses_for_job(job_id, tasks_config)
        
        #crew_logger.info(f"Created {len(db_task_statuses)} task status records for job {job_id}")
        return [TaskStatusResponse.model_validate(status) for status in db_task_statuses]
    
    def create_task_callbacks(self, job_id: str, task_id: str) -> Dict[str, Callable]:
        """
        Create a set of callback functions for updating task status.
        
        Args:
            job_id: The ID of the job
            task_id: The ID/key of the task
            
        Returns:
            Dict containing 'on_start', 'on_end', and 'on_error' functions
        """
        def on_start():
            """Update task status to RUNNING when it starts"""
            try:
                #crew_logger.info(f"Task {task_id} in job {job_id} is starting")
                self.update_task_status(job_id, task_id, TaskStatusEnum.RUNNING)
            except Exception as e:
                crew_logger.error(f"Error updating task status to RUNNING: {str(e)}")
        
        def on_end(output):
            """Update task status to COMPLETED when it finishes"""
            try:
                crew_logger.info(f"Task {task_id} in job {job_id} completed with output")
                self.update_task_status(job_id, task_id, TaskStatusEnum.COMPLETED)
                return output
            except Exception as e:
                crew_logger.error(f"Error updating task status to COMPLETED: {str(e)}")
                return output
        
        def on_error(error):
            """Update task status to FAILED when it encounters an error"""
            try:
                crew_logger.error(f"Task {task_id} in job {job_id} failed: {str(error)}")
                self.update_task_status(job_id, task_id, TaskStatusEnum.FAILED)
                
                # Record error trace if possible
                try:
                    if hasattr(self, 'db'):
                        run = self.db.query(ExecutionHistory).filter(ExecutionHistory.job_id == job_id).first()
                        if run:
                            error_metadata = {
                                'callback_name': 'task_error_handler',
                                'error': str(error)
                            }
                            self.repository.record_error_trace(
                                run_id=run.id,
                                task_key=task_id,
                                error_type=type(error).__name__,
                                error_message=str(error),
                                error_metadata=error_metadata
                            )
                except Exception as trace_error:
                    crew_logger.error(f"Failed to record error trace: {str(trace_error)}")
                
                return error
            except Exception as e:
                crew_logger.error(f"Error updating task status to FAILED: {str(e)}")
                return error
        
        return {
            'on_start': on_start,
            'on_end': on_end,
            'on_error': on_error
        }

# Dependency for FastAPI
async def get_task_tracking_service() -> TaskTrackingService:
    """
    Dependency to get a TaskTrackingService instance.
    Repository handles session management internally.
    
    Returns:
        TaskTrackingService instance
    """
    # Create repository that manages its own session
    repository = TaskTrackingRepository()
    
    # Create service with repository
    service = TaskTrackingService(repository)
    
    # Yield service for FastAPI dependency injection
    try:
        yield service
    finally:
        # Any cleanup if needed
        pass 