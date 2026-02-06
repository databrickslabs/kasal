import logging
from typing import Annotated, Any, Dict, List

from fastapi import APIRouter, Depends, Path, Query, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.models.task import Task
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


async def get_task_service(session: SessionDep) -> TaskService:
    """
    Dependency provider for TaskService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        TaskService instance with injected session
    """
    return TaskService(session=session)


# Type alias for cleaner function signatures
TaskServiceDep = Annotated[TaskService, Depends(get_task_service)]


@router.post("", response_model=TaskSchema, status_code=status.HTTP_201_CREATED)
async def create_task(
    task_in: TaskCreate,
    service: TaskServiceDep,
    group_context: GroupContextDep,
):
    """
    Create a new task with group isolation.
    Only Editors and Admins can create tasks.

    Args:
        task_in: Task data for creation
        service: Task service injected by dependency
        group_context: Group context from headers

    Returns:
        Created task
    """
    # Check permissions - only editors and admins can create tasks
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can create tasks")

    return await service.create_with_group(task_in, group_context)


@router.get("", response_model=List[TaskSchema])
async def list_tasks(
    service: TaskServiceDep,
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
    return await service.find_by_group(group_context)


@router.get("/{task_id}", response_model=TaskSchema)
async def get_task(
    task_id: Annotated[str, Path(title="The ID of the task to get")],
    service: TaskServiceDep,
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
    task = await service.get_with_group_check(task_id, group_context)
    if not task:
        raise NotFoundError("Task not found")

    # Debug logging for tool_configs
    if hasattr(task, "tool_configs"):
        logger.info(f"GET task {task_id} - tool_configs value: {task.tool_configs}")
    else:
        logger.warning(f"GET task {task_id} - no tool_configs attribute found")

    return task


@router.put("/{task_id}/full", response_model=TaskSchema)
async def update_task_full(
    task_id: Annotated[str, Path(title="The ID of the task to update")],
    task_in: dict,
    service: TaskServiceDep,
    group_context: GroupContextDep,
):
    """
    Update all fields of an existing task with group isolation.
    Only Editors and Admins can update tasks.

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
    # Check permissions - only editors and admins can update tasks
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can update tasks")

    task = await service.update_full_with_group_check(task_id, task_in, group_context)
    if not task:
        raise NotFoundError("Task not found")
    return task


@router.put("/{task_id}", response_model=TaskSchema)
async def update_task(
    task_id: Annotated[str, Path(title="The ID of the task to update")],
    task_in: TaskUpdate,
    service: TaskServiceDep,
    group_context: GroupContextDep,
):
    """
    Update an existing task with partial data and group isolation.
    Only Editors and Admins can update tasks.

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
    # Check permissions - only editors and admins can update tasks
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can update tasks")

    task = await service.update_with_group_check(task_id, task_in, group_context)
    if not task:
        raise NotFoundError("Task not found")
    return task


@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task(
    task_id: Annotated[str, Path(title="The ID of the task to delete")],
    service: TaskServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete a task with group isolation.
    Only Editors and Admins can delete tasks.

    Args:
        task_id: ID of the task to delete
        service: Task service injected by dependency
        group_context: Group context from headers

    Raises:
        HTTPException: If task not found or not authorized
    """
    # Check permissions - only editors and admins can delete tasks
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can delete tasks")

    deleted = await service.delete_with_group_check(task_id, group_context)
    if not deleted:
        raise NotFoundError("Task not found")


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_tasks(
    service: TaskServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete all tasks for the current group.
    Only Admins can delete all tasks.

    Args:
        service: Task service injected by dependency
        group_context: Group context from headers
    """
    # Check permissions - only admins can delete all tasks
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can delete all tasks")

    await service.delete_all_for_group(group_context)
