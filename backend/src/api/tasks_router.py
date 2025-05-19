from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
import logging

from src.core.dependencies import SessionDep, get_service
from src.models.task import Task
from src.repositories.task_repository import TaskRepository
from src.schemas.task import Task as TaskSchema
from src.schemas.task import TaskCreate, TaskUpdate
from src.services.task_service import TaskService

router = APIRouter(
    prefix="/tasks",
    tags=["tasks"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get TaskService
get_task_service = get_service(TaskService, TaskRepository, Task)


@router.post("", response_model=TaskSchema, status_code=status.HTTP_201_CREATED)
async def create_task(
    task_in: TaskCreate,
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Create a new task.
    
    Args:
        task_in: Task data for creation
        service: Task service injected by dependency
        
    Returns:
        Created task
    """
    try:
        return await service.create(task_in)
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[TaskSchema])
async def list_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Retrieve all tasks.
    
    Args:
        service: Task service injected by dependency
        
    Returns:
        List of tasks
    """
    try:
        return await service.find_all()
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{task_id}", response_model=TaskSchema)
async def get_task(
    task_id: Annotated[str, Path(title="The ID of the task to get")],
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Get a specific task by ID.
    
    Args:
        task_id: ID of the task to get
        service: Task service injected by dependency
        
    Returns:
        Task if found
        
    Raises:
        HTTPException: If task not found
    """
    try:
        task = await service.get(task_id)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        return task
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{task_id}/full", response_model=TaskSchema)
async def update_task_full(
    task_id: Annotated[str, Path(title="The ID of the task to update")],
    task_in: dict,
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Update all fields of an existing task.
    
    Args:
        task_id: ID of the task to update
        task_in: Full task data for update
        service: Task service injected by dependency
        
    Returns:
        Updated task
        
    Raises:
        HTTPException: If task not found
    """
    try:
        task = await service.update_full(task_id, task_in)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        return task
    except Exception as e:
        logger.error(f"Error updating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{task_id}", response_model=TaskSchema)
async def update_task(
    task_id: Annotated[str, Path(title="The ID of the task to update")],
    task_in: TaskUpdate,
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Update an existing task with partial data.
    
    Args:
        task_id: ID of the task to update
        task_in: Task data for update
        service: Task service injected by dependency
        
    Returns:
        Updated task
        
    Raises:
        HTTPException: If task not found
    """
    try:
        task = await service.update_with_partial_data(task_id, task_in)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        return task
    except Exception as e:
        logger.error(f"Error updating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task(
    task_id: Annotated[str, Path(title="The ID of the task to delete")],
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Delete a task.
    
    Args:
        task_id: ID of the task to delete
        service: Task service injected by dependency
        
    Raises:
        HTTPException: If task not found
    """
    try:
        deleted = await service.delete(task_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
    except Exception as e:
        logger.error(f"Error deleting task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
):
    """
    Delete all tasks.
    
    Args:
        service: Task service injected by dependency
    """
    try:
        await service.delete_all()
    except Exception as e:
        logger.error(f"Error deleting all tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 