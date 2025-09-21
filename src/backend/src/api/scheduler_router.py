from typing import Annotated, List, Dict
import logging

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status, Path
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.schedule import ScheduleCreate, ScheduleCreateFromExecution, ScheduleUpdate, ScheduleResponse, ScheduleListResponse, ToggleResponse
from src.services.scheduler_service import SchedulerService
from src.utils.user_context import GroupContext
from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.scheduler import (
    SchedulerJobSchema,
    SchedulerJobCreate,
    SchedulerJobUpdate,
    SchedulerJobResponse
)

# Create router instance
router = APIRouter(
    prefix="/schedules",
    tags=["schedules"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)

async def get_scheduler_service(session: SessionDep) -> SchedulerService:
    """
    Dependency provider for SchedulerService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        SchedulerService instance with injected dependencies
    """
    return SchedulerService(session)

# Type alias for cleaner function signatures
SchedulerServiceDep = Annotated[SchedulerService, Depends(get_scheduler_service)]


@router.post("", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    schedule: ScheduleCreate,
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> ScheduleResponse:
    """
    Create a new schedule.
    
    This endpoint creates a new schedule based on the provided cron expression and job configuration.
    
    Args:
        schedule: Schedule data to create
        
    Returns:
        Created schedule information
    """
    logger.info(f"Creating schedule: {schedule.name} with cron expression: {schedule.cron_expression}")
    try:
        response = await service.create_schedule(schedule, group_context)
        logger.info(f"Created schedule with ID {response.id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule creation failed: {str(e)}")
        raise


@router.post("/from-execution", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule_from_execution(
    schedule: ScheduleCreateFromExecution,
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> ScheduleResponse:
    """
    Create a new schedule based on an existing execution.
    
    This endpoint creates a new schedule using the agents and tasks configuration
    from a previously executed job.
    
    Args:
        schedule: Schedule data including execution_id to use as template
        
    Returns:
        Created schedule information
    """
    logger.info(f"Creating schedule from execution {schedule.execution_id}: {schedule.name}")
    try:
        response = await service.create_schedule_from_execution(schedule, group_context)
        logger.info(f"Created schedule with ID {response.id} from execution {schedule.execution_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule creation from execution failed: {str(e)}")
        raise


@router.get("", response_model=List[ScheduleResponse])
async def list_schedules(
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> List[ScheduleResponse]:
    """
    List all schedules.
    
    Returns:
        List of all schedules
    """
    logger.info("Listing all schedules")
    response = await service.get_all_schedules(group_context)
    logger.info(f"Found {response.count} schedules")
    return response.schedules


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to get")],
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> ScheduleResponse:
    """
    Get a specific schedule by ID.
    
    Args:
        schedule_id: ID of the schedule to retrieve
        
    Returns:
        Schedule information
    """
    logger.info(f"Getting schedule with ID {schedule_id}")
    try:
        response = await service.get_schedule_by_id_with_group_check(schedule_id, group_context)
        logger.info(f"Retrieved schedule with ID {schedule_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule retrieval failed: {str(e)}")
        raise


@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to update")],
    schedule_update: ScheduleUpdate,
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> ScheduleResponse:
    """
    Update an existing schedule.
    
    Args:
        schedule_id: ID of the schedule to update
        schedule_update: Schedule data for update
        
    Returns:
        Updated schedule information
    """
    logger.info(f"Updating schedule with ID {schedule_id}")
    try:
        response = await service.update_schedule_with_group_check(schedule_id, schedule_update, group_context)
        logger.info(f"Updated schedule with ID {schedule_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule update failed: {str(e)}")
        raise


@router.delete("/{schedule_id}", status_code=status.HTTP_200_OK)
async def delete_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to delete")],
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> Dict[str, str]:
    """
    Delete a schedule.
    
    Args:
        schedule_id: ID of the schedule to delete
        
    Returns:
        Success message
    """
    logger.info(f"Deleting schedule with ID {schedule_id}")
    try:
        response = await service.delete_schedule_with_group_check(schedule_id, group_context)
        logger.info(f"Deleted schedule with ID {schedule_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule deletion failed: {str(e)}")
        raise


@router.post("/{schedule_id}/toggle", response_model=ToggleResponse)
async def toggle_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to toggle")],
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> ToggleResponse:
    """
    Toggle a schedule's active state.
    
    This endpoint toggles a schedule between active and inactive states.
    When a schedule is inactive, it will not be executed.
    
    Args:
        schedule_id: ID of the schedule to toggle
        
    Returns:
        Updated schedule information
    """
    logger.info(f"Toggling schedule with ID {schedule_id}")
    try:
        response = await service.toggle_schedule_with_group_check(schedule_id, group_context)
        active_status = "enabled" if response.is_active else "disabled"
        logger.info(f"Toggled schedule with ID {schedule_id}, now {active_status}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule toggle failed: {str(e)}")
        raise


@router.get("/jobs", response_model=List[SchedulerJobResponse])
async def get_all_jobs(
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> List[SchedulerJobResponse]:
    """
    Get all scheduler jobs.
    
    Returns:
        List of scheduler jobs
    """
    try:
        return await service.get_all_jobs_for_group(group_context)
    except Exception as e:
        logger.error(f"Error getting jobs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/jobs", response_model=SchedulerJobResponse)
async def create_job(
    job: SchedulerJobCreate,
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> SchedulerJobResponse:
    """
    Create a new scheduler job.
    
    Args:
        job: Job data to create
        
    Returns:
        Created job
    """
    try:
        return await service.create_job_with_group(job, group_context)
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.put("/jobs/{job_id}", response_model=SchedulerJobResponse)
async def update_job(
    job_id: int,
    job: SchedulerJobUpdate,
    service: SchedulerServiceDep,
    group_context: GroupContextDep
) -> SchedulerJobResponse:
    """
    Update a scheduler job.
    
    Args:
        job_id: ID of the job to update
        job: Updated job data
        
    Returns:
        Updated job
    """
    try:
        return await service.update_job_with_group_check(job_id, job, group_context)
    except Exception as e:
        logger.error(f"Error updating job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) 