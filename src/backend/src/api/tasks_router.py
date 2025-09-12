from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
import logging

from src.core.dependencies import SessionDep, GroupContextDep, get_service
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
    group_context: GroupContextDep,
):
    """
    Create a new task with group isolation.
    
    Args:
        task_in: Task data for creation
        service: Task service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Created task
    """
    try:
        return await service.create_with_group(task_in, group_context)
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[TaskSchema])
async def list_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
    group_context: GroupContextDep,
):
    """
    Retrieve all tasks for the current group.
    
    Args:
        service: Task service injected by dependency
        group_context: Group context from headers
        
    Returns:
        List of tasks for the current group
    """
    try:
        return await service.find_by_group(group_context)
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{task_id}", response_model=TaskSchema)
async def get_task(
    task_id: Annotated[str, Path(title="The ID of the task to get")],
    service: Annotated[TaskService, Depends(get_task_service)],
    group_context: GroupContextDep,
):
    """
    Get a specific task by ID with group isolation.
    
    Args:
        task_id: ID of the task to get
        service: Task service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Task if found and belongs to user's group
        
    Raises:
        HTTPException: If task not found or not authorized
    """
    try:
        task = await service.get_with_group_check(task_id, group_context)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        
        # Debug logging for tool_configs
        if hasattr(task, 'tool_configs'):
            logger.info(f"GET task {task_id} - tool_configs value: {task.tool_configs}")
        else:
            logger.warning(f"GET task {task_id} - no tool_configs attribute found")
        
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
    group_context: GroupContextDep,
):
    """
    Update all fields of an existing task with group isolation.
    
    Args:
        task_id: ID of the task to update
        task_in: Full task data for update
        service: Task service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Updated task
        
    Raises:
        HTTPException: If task not found or not authorized
    """
    try:
        task = await service.update_full_with_group_check(task_id, task_in, group_context)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        return task
    except HTTPException:
        # Re-raise HTTP exceptions (like 404) without modification
        raise
    except Exception as e:
        logger.error(f"Error updating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{task_id}", response_model=TaskSchema)
async def update_task(
    task_id: Annotated[str, Path(title="The ID of the task to update")],
    task_in: TaskUpdate,
    service: Annotated[TaskService, Depends(get_task_service)],
    group_context: GroupContextDep,
):
    """
    Update an existing task with partial data and group isolation.
    
    Args:
        task_id: ID of the task to update
        task_in: Task data for update
        service: Task service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Updated task
        
    Raises:
        HTTPException: If task not found or not authorized
    """
    try:
        task = await service.update_with_group_check(task_id, task_in, group_context)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        return task
    except HTTPException:
        # Re-raise HTTP exceptions (like 404) without modification
        raise
    except Exception as e:
        logger.error(f"Error updating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task(
    task_id: Annotated[str, Path(title="The ID of the task to delete")],
    service: Annotated[TaskService, Depends(get_task_service)],
    group_context: GroupContextDep,
):
    """
    Delete a task with group isolation.
    
    Args:
        task_id: ID of the task to delete
        service: Task service injected by dependency
        group_context: Group context from headers
        
    Raises:
        HTTPException: If task not found or not authorized
    """
    try:
        deleted = await service.delete_with_group_check(task_id, group_context)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
    except HTTPException:
        # Re-raise HTTP exceptions (like 404) without modification
        raise
    except Exception as e:
        logger.error(f"Error deleting task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
    group_context: GroupContextDep,
):
    """
    Delete all tasks for the current group.
    
    Args:
        service: Task service injected by dependency
        group_context: Group context from headers
    """
    try:
        await service.delete_all_for_group(group_context)
    except Exception as e:
        logger.error(f"Error deleting all tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 