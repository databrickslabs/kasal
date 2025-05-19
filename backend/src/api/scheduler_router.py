from typing import Annotated, List, Dict
import logging

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status, Path
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.schedule import ScheduleCreate, ScheduleUpdate, ScheduleResponse, ScheduleListResponse, ToggleResponse
from src.db.session import get_db
from src.services.scheduler_service import SchedulerService
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

# Create service dependency
async def get_scheduler_service(db: AsyncSession = Depends(get_db)) -> SchedulerService:
    return SchedulerService(db)


@router.post("", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    schedule: ScheduleCreate,
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
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
        response = await service.create_schedule(schedule)
        logger.info(f"Created schedule with ID {response.id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule creation failed: {str(e)}")
        raise


@router.get("", response_model=List[ScheduleResponse])
async def list_schedules(
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
) -> List[ScheduleResponse]:
    """
    List all schedules.
    
    Returns:
        List of all schedules
    """
    logger.info("Listing all schedules")
    response = await service.get_all_schedules()
    logger.info(f"Found {response.count} schedules")
    return response.schedules


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to get")],
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
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
        response = await service.get_schedule_by_id(schedule_id)
        logger.info(f"Retrieved schedule with ID {schedule_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule retrieval failed: {str(e)}")
        raise


@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to update")],
    schedule_update: ScheduleUpdate,
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
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
        response = await service.update_schedule(schedule_id, schedule_update)
        logger.info(f"Updated schedule with ID {schedule_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule update failed: {str(e)}")
        raise


@router.delete("/{schedule_id}", status_code=status.HTTP_200_OK)
async def delete_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to delete")],
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
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
        response = await service.delete_schedule(schedule_id)
        logger.info(f"Deleted schedule with ID {schedule_id}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule deletion failed: {str(e)}")
        raise


@router.post("/{schedule_id}/toggle", response_model=ToggleResponse)
async def toggle_schedule(
    schedule_id: Annotated[int, Path(title="The ID of the schedule to toggle")],
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
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
        response = await service.toggle_schedule(schedule_id)
        active_status = "enabled" if response.is_active else "disabled"
        logger.info(f"Toggled schedule with ID {schedule_id}, now {active_status}")
        return response
    except HTTPException as e:
        logger.warning(f"Schedule toggle failed: {str(e)}")
        raise


@router.get("/jobs", response_model=List[SchedulerJobResponse])
async def get_all_jobs(
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
) -> List[SchedulerJobResponse]:
    """
    Get all scheduler jobs.
    
    Returns:
        List of scheduler jobs
    """
    try:
        return await service.get_all_jobs()
    except Exception as e:
        logger.error(f"Error getting jobs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/jobs", response_model=SchedulerJobResponse)
async def create_job(
    job: SchedulerJobCreate,
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
) -> SchedulerJobResponse:
    """
    Create a new scheduler job.
    
    Args:
        job: Job data to create
        
    Returns:
        Created job
    """
    try:
        return await service.create_job(job)
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
    service: Annotated[SchedulerService, Depends(get_scheduler_service)]
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
        return await service.update_job(job_id, job)
    except Exception as e:
        logger.error(f"Error updating job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) 