from typing import Annotated, Any, Dict, List

from fastapi import APIRouter, Depends, Path, Query, status

from src.core.dependencies import GroupContextDep
from src.schemas.task_tracking import (
    JobExecutionStatusResponse,
    TaskStatusCreate,
    TaskStatusResponse,
    TaskStatusSchema,
    TaskStatusUpdate,
)
from src.services.task_tracking_service import (
    TaskTrackingService,
    get_task_tracking_service,
)

# Create router instance
router = APIRouter(
    prefix="/task-tracking",
    tags=["task tracking"],
    responses={404: {"description": "Not found"}},
)


@router.get("/status/{job_id}", response_model=JobExecutionStatusResponse)
async def get_job_status(
    job_id: str,
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)],
    group_context: GroupContextDep,
) -> JobExecutionStatusResponse:
    """
    Get the status of a job execution.

    Args:
        job_id: The ID of the job to get status for

    Returns:
        JobExecutionStatusResponse with job status information
    """
    return await service.get_job_status(job_id)


@router.get("/tasks", response_model=List[TaskStatusResponse])
async def get_all_tasks(
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)],
    group_context: GroupContextDep,
) -> List[TaskStatusResponse]:
    """
    Get all task statuses.

    Returns:
        List of task statuses
    """
    return await service.get_all_tasks()


@router.post("/tasks", response_model=TaskStatusResponse)
async def create_task(
    task: TaskStatusCreate,
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)],
    group_context: GroupContextDep,
) -> TaskStatusResponse:
    """
    Create a new task status.

    Args:
        task: Task status data to create

    Returns:
        Created task status
    """
    return await service.create_task(task)


@router.put("/tasks/{task_id}", response_model=TaskStatusResponse)
async def update_task(
    task_id: int,
    task: TaskStatusUpdate,
    service: Annotated[TaskTrackingService, Depends(get_task_tracking_service)],
    group_context: GroupContextDep,
) -> TaskStatusResponse:
    """
    Update a task status.

    Args:
        task_id: ID of the task to update
        task: Updated task status data

    Returns:
        Updated task status
    """
    return await service.update_task(task_id, task)
