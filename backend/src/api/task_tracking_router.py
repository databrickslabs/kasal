from typing import Annotated, List, Dict, Any
import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query, Path

from src.services.task_tracking_service import TaskTrackingService, get_task_tracking_service
from src.schemas.task_tracking import (
    JobExecutionStatusResponse,
    TaskStatusSchema,
    TaskStatusCreate,
    TaskStatusUpdate,
    TaskStatusResponse
)

# Create router instance
router = APIRouter(
    prefix="/task-tracking",
    tags=["task tracking"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)

@router.get("/status/{job_id}", response_model=JobExecutionStatusResponse)
async def get_job_status(
    job_id: str,
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)]
) -> JobExecutionStatusResponse:
    """
    Get the status of a job execution.
    
    Args:
        job_id: The ID of the job to get status for
        
    Returns:
        JobExecutionStatusResponse with job status information
    """
    try:
        return await service.get_job_status(job_id)
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/tasks", response_model=List[TaskStatusResponse])
async def get_all_tasks(
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)]
) -> List[TaskStatusResponse]:
    """
    Get all task statuses.
    
    Returns:
        List of task statuses
    """
    try:
        return await service.get_all_tasks()
    except Exception as e:
        logger.error(f"Error getting tasks: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/tasks", response_model=TaskStatusResponse)
async def create_task(
    task: TaskStatusCreate,
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)]
) -> TaskStatusResponse:
    """
    Create a new task status.
    
    Args:
        task: Task status data to create
        
    Returns:
        Created task status
    """
    try:
        return await service.create_task(task)
    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.put("/tasks/{task_id}", response_model=TaskStatusResponse)
async def update_task(
    task_id: int,
    task: TaskStatusUpdate,
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)]
) -> TaskStatusResponse:
    """
    Update a task status.
    
    Args:
        task_id: ID of the task to update
        task: Updated task status data
        
    Returns:
        Updated task status
    """
    try:
        return await service.update_task(task_id, task)
    except Exception as e:
        logger.error(f"Error updating task: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) 