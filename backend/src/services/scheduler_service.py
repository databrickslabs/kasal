import logging
import asyncio
import uuid
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from src.repositories.schedule_repository import ScheduleRepository
from src.schemas.schedule import ScheduleCreate, ScheduleUpdate, ScheduleResponse, ScheduleListResponse, ToggleResponse, CrewConfig
from src.schemas.scheduler import SchedulerJobCreate, SchedulerJobUpdate, SchedulerJobResponse
from src.utils.cron_utils import ensure_utc, calculate_next_run_from_last
from src.services.crewai_execution_service import CrewAIExecutionService, JobStatus
from src.db.session import async_session_factory
from src.models.execution_history import ExecutionHistory as Run
from src.config.settings import settings
from src.engines.crewai.callbacks import JobOutputCallback
from src.core.logger import LoggerManager
from src.services.execution_service import ExecutionService
from src.schemas.execution import ExecutionNameGenerationRequest

logger = logging.getLogger(__name__)
logger_manager = LoggerManager.get_instance()

# Define DB_PATH from settings
DB_PATH = str(settings.DATABASE_URI).replace('sqlite:///', '')

class SchedulerService:
    """
    Service for scheduler operations.
    Acts as an intermediary between the API router and the repository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize service with database session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.repository = ScheduleRepository(session)
        self.session = session
        self._running_tasks: Set[asyncio.Task] = set()
    
    async def create_schedule(self, schedule_data: ScheduleCreate) -> ScheduleResponse:
        """
        Create a new schedule.
        
        Args:
            schedule_data: Schedule data for creation
            
        Returns:
            ScheduleResponse of created schedule
            
        Raises:
            HTTPException: If schedule creation fails
        """
        try:
            # Calculate next run time
            next_run = calculate_next_run_from_last(schedule_data.cron_expression)
            
            # Create schedule
            schedule = await self.repository.create({
                **schedule_data.model_dump(),
                "next_run_at": next_run
            })
            
            return ScheduleResponse.model_validate(schedule)
        except ValueError as e:
            logger.error(f"Invalid cron expression: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid cron expression: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Failed to create schedule: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create schedule: {str(e)}"
            )
    
    async def get_all_schedules(self) -> ScheduleListResponse:
        """
        Get all schedules.
        
        Returns:
            ScheduleListResponse with list of schedules
        """
        schedules = await self.repository.find_all()
        return ScheduleListResponse(
            schedules=[ScheduleResponse.model_validate(schedule) for schedule in schedules],
            count=len(schedules)
        )
    
    async def get_schedule_by_id(self, schedule_id: int) -> ScheduleResponse:
        """
        Get a schedule by ID.
        
        Args:
            schedule_id: ID of the schedule to retrieve
            
        Returns:
            ScheduleResponse if schedule found
            
        Raises:
            HTTPException: If schedule not found
        """
        schedule = await self.repository.find_by_id(schedule_id)
        if not schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule with ID {schedule_id} not found"
            )
        return ScheduleResponse.model_validate(schedule)
    
    async def update_schedule(self, schedule_id: int, schedule_data: ScheduleUpdate) -> ScheduleResponse:
        """
        Update a schedule.
        
        Args:
            schedule_id: ID of the schedule to update
            schedule_data: New schedule data
            
        Returns:
            ScheduleResponse of updated schedule
            
        Raises:
            HTTPException: If schedule not found or update fails
        """
        try:
            # Update schedule
            schedule = await self.repository.update(schedule_id, schedule_data.model_dump())
            if not schedule:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schedule with ID {schedule_id} not found"
                )
            
            return ScheduleResponse.model_validate(schedule)
        except HTTPException:
            raise
        except ValueError as e:
            logger.error(f"Invalid cron expression: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid cron expression: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Failed to update schedule: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update schedule: {str(e)}"
            )
    
    async def delete_schedule(self, schedule_id: int) -> Dict[str, str]:
        """
        Delete a schedule.
        
        Args:
            schedule_id: ID of the schedule to delete
            
        Returns:
            Success message
            
        Raises:
            HTTPException: If schedule not found or deletion fails
        """
        try:
            # Delete schedule
            deleted = await self.repository.delete(schedule_id)
            if not deleted:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schedule with ID {schedule_id} not found"
                )
            
            return {"message": "Schedule deleted successfully"}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to delete schedule: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete schedule: {str(e)}"
            )
    
    async def toggle_schedule(self, schedule_id: int) -> ToggleResponse:
        """
        Toggle a schedule's active state.
        
        Args:
            schedule_id: ID of the schedule to toggle
            
        Returns:
            ToggleResponse of updated schedule
            
        Raises:
            HTTPException: If schedule not found or toggle fails
        """
        try:
            # Toggle schedule
            schedule = await self.repository.toggle_active(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schedule with ID {schedule_id} not found"
                )
            
            return ToggleResponse.model_validate(schedule)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle schedule: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to toggle schedule: {str(e)}"
            )
    
    async def run_schedule_job(self, schedule_id: int, config: CrewConfig, execution_time: datetime) -> None:
        """
        Run a scheduled job.
        
        Args:
            schedule_id: ID of the schedule to run
            config: Job configuration
            execution_time: Time when the job was triggered
        """
        try:
            # Generate job ID and run name
            job_id = str(uuid.uuid4())
            model = config.model or "gpt-3.5-turbo"
            
            # Setup async session
            async with async_session_factory() as session:
                execution_service = ExecutionService()
                request = ExecutionNameGenerationRequest(
                    agents_yaml=config.agents_yaml,
                    tasks_yaml=config.tasks_yaml,
                    model=model
                )
                response = await execution_service.generate_execution_name(request)
                run_name = response.name
                
                # Prepare job configuration
                config_dict = {
                    "agents_yaml": config.agents_yaml,
                    "tasks_yaml": config.tasks_yaml,
                    "inputs": config.inputs,
                    "model": config.model
                }
                
                # Create run record
                db_run = Run(
                    job_id=job_id,
                    status="pending",
                    inputs=config_dict,
                    created_at=execution_time,
                    trigger_type="scheduled",
                    planning=config.planning,
                    run_name=run_name
                )
                session.add(db_run)
                await session.commit()
                await session.refresh(db_run)
                
                # Create an instance of CrewExecutionService
                crew_execution_service = CrewAIExecutionService(session)
                
                # Add execution to memory
                CrewAIExecutionService.add_execution_to_memory(
                    execution_id=job_id,
                    status=JobStatus.PENDING.value,
                    run_name=run_name,
                    created_at=execution_time
                )
                
                # Run the job
                await crew_execution_service.run_crew_execution(
                    execution_id=job_id,
                    config=config
                )
                
                # Update schedule after execution
                repo = ScheduleRepository(session)
                await repo.update_after_execution(schedule_id, execution_time)
                
                logger_manager.scheduler.info(
                    f"Successfully ran schedule {schedule_id}."
                )
                
        except Exception as job_error:
            logger_manager.scheduler.error(f"Error running job for schedule {schedule_id}: {job_error}")
            try:
                # Update schedule even if job fails
                async with async_session_factory() as error_session:
                    repo = ScheduleRepository(error_session)
                    await repo.update_after_execution(schedule_id, execution_time)
            except Exception as update_error:
                logger_manager.scheduler.error(f"Error updating schedule {schedule_id} after job failure: {update_error}")
    
    async def check_and_run_schedules(self) -> None:
        """
        Check for due schedules and run them.
        This is the main scheduler loop that runs continuously.
        """
        logger_manager.scheduler.info("Schedule checker started and running")
        
        while True:
            try:
                # Clean up completed tasks
                self._running_tasks = {task for task in self._running_tasks if not task.done()}
                
                # Get current time
                now_utc = datetime.now(timezone.utc)
                now_local = datetime.now().astimezone()
                logger_manager.scheduler.info(f"Checking for due schedules at {now_local} (local) / {now_utc} (UTC)")
                logger_manager.scheduler.info(f"Currently running tasks: {len(self._running_tasks)}")
                
                # Find due schedules
                async with async_session_factory() as session:
                    repo = ScheduleRepository(session)
                    due_schedules = await repo.find_due_schedules(now_utc)
                    all_schedules = await repo.find_all()
                    
                    # Log status of all schedules
                    logger_manager.scheduler.info("Current schedules status:")
                    for schedule in all_schedules:
                        next_run = ensure_utc(schedule.next_run_at)
                        last_run = ensure_utc(schedule.last_run_at)
                        is_due = schedule.is_active and next_run is not None and next_run <= now_utc
                        
                        next_run_local = next_run.astimezone() if next_run else None
                        last_run_local = last_run.astimezone() if last_run else None
                        
                        logger_manager.scheduler.info(
                            f"Schedule {schedule.id} - {schedule.name}:"
                            f" active={schedule.is_active},"
                            f" next_run={next_run_local} (local) / {next_run} (UTC),"
                            f" last_run={last_run_local} (local) / {last_run} (UTC),"
                            f" cron={schedule.cron_expression},"
                            f" planning={schedule.planning},"
                            f" model={schedule.model},"
                            f" is_due={is_due}"
                            f" (now={now_local} local / {now_utc} UTC)"
                        )
                    
                    # Start tasks for due schedules
                    logger_manager.scheduler.info(f"Found {len(due_schedules)} schedules due to run")
                    
                    for schedule in due_schedules:
                        logger_manager.scheduler.info(f"Starting task for schedule {schedule.id} - {schedule.name}")
                        logger_manager.scheduler.info(f"Schedule configuration: agents_yaml={schedule.agents_yaml}, tasks_yaml={schedule.tasks_yaml}, inputs={schedule.inputs}, planning={schedule.planning}, model={schedule.model}")
                        
                        config = CrewConfig(
                            agents_yaml=schedule.agents_yaml,
                            tasks_yaml=schedule.tasks_yaml,
                            inputs=schedule.inputs,
                            planning=schedule.planning,
                            model=schedule.model
                        )
                        
                        # Create task for the job
                        task = asyncio.create_task(
                            self.run_schedule_job(schedule.id, config, now_utc),
                            name=f"schedule_{schedule.id}_{now_utc.isoformat()}"
                        )
                        self._running_tasks.add(task)
                        
                        # Update next run time immediately
                        schedule.next_run_at = calculate_next_run_from_last(
                            schedule.cron_expression,
                            now_utc
                        )
                        await session.commit()
                
                # Check for task errors
                for task in list(self._running_tasks):
                    if task.done():
                        try:
                            await task
                        except Exception as e:
                            logger_manager.scheduler.error(f"Task {task.get_name()} failed with error: {e}")
                
                # Sleep before next check
                logger_manager.scheduler.info("Sleeping for 60 seconds before next check")
                await asyncio.sleep(60)
            except Exception as e:
                logger_manager.scheduler.error(f"Error in schedule checker: {e}")
                await asyncio.sleep(60)
    
    async def start_scheduler(self, interval_seconds: int = 60) -> None:
        """
        Start the scheduler with a background task.
        
        Args:
            interval_seconds: Interval in seconds between schedule checks
        """
        logger.info("Starting scheduler background task...")
        
        async def scheduler_loop():
            while True:
                try:
                    await self.check_and_run_schedules()
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(interval_seconds)
        
        # Create and store the task
        task = asyncio.create_task(scheduler_loop())
        self._running_tasks.add(task)
        
        # Add done callback to remove task from set when done
        def task_done_callback(task):
            self._running_tasks.discard(task)
            if task.done() and not task.cancelled():
                exc = task.exception()
                if exc:
                    logger.error(f"Scheduler task failed: {exc}")
        
        task.add_done_callback(task_done_callback)
        logger.info("Scheduler background task started successfully.")
    
    async def get_all_jobs(self) -> List[SchedulerJobResponse]:
        """
        Get all scheduler jobs.
        
        Returns:
            List of scheduler jobs
        """
        # This is a placeholder implementation - you'll need to implement actual job repository
        # or adapt this to use existing schedules if that's the intended behavior
        schedules = await self.repository.find_all()
        
        # Convert schedules to job responses
        jobs = []
        for schedule in schedules:
            job = SchedulerJobResponse(
                id=schedule.id,
                name=schedule.name,
                description=f"Scheduled job from {schedule.name}",
                schedule=schedule.cron_expression,
                enabled=schedule.is_active,
                job_data={
                    "agents": schedule.agents_yaml,
                    "tasks": schedule.tasks_yaml,
                    "inputs": schedule.inputs,
                    "planning": schedule.planning,
                    "model": schedule.model
                },
                created_at=schedule.created_at,
                updated_at=schedule.updated_at,
                last_run_at=schedule.last_run_at,
                next_run_at=schedule.next_run_at
            )
            jobs.append(job)
            
        return jobs
        
    async def create_job(self, job_create: SchedulerJobCreate) -> SchedulerJobResponse:
        """
        Create a new scheduler job.
        
        Args:
            job_create: Job data to create
            
        Returns:
            Created job
        """
        # Convert job to schedule
        schedule_data = ScheduleCreate(
            name=job_create.name,
            cron_expression=job_create.schedule,
            agents_yaml=job_create.job_data.get("agents", {}),
            tasks_yaml=job_create.job_data.get("tasks", {}),
            inputs=job_create.job_data.get("inputs", {}),
            is_active=job_create.enabled,
            planning=job_create.job_data.get("planning", False),
            model=job_create.job_data.get("model", "gpt-4o-mini")
        )
        
        # Create schedule
        schedule = await self.repository.create(schedule_data.model_dump())
        
        # Convert back to job response
        return SchedulerJobResponse(
            id=schedule.id,
            name=schedule.name,
            description=job_create.description,
            schedule=schedule.cron_expression,
            enabled=schedule.is_active,
            job_data={
                "agents": schedule.agents_yaml,
                "tasks": schedule.tasks_yaml,
                "inputs": schedule.inputs,
                "planning": schedule.planning,
                "model": schedule.model
            },
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
            last_run_at=schedule.last_run_at,
            next_run_at=schedule.next_run_at
        )
        
    async def update_job(self, job_id: int, job_update: SchedulerJobUpdate) -> SchedulerJobResponse:
        """
        Update a scheduler job.
        
        Args:
            job_id: ID of the job to update
            job_update: Updated job data
            
        Returns:
            Updated job
        """
        # Get existing schedule
        existing_schedule = await self.repository.find_by_id(job_id)
        if not existing_schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found"
            )
        
        # Prepare update data
        update_data = {}
        if job_update.name is not None:
            update_data["name"] = job_update.name
        if job_update.schedule is not None:
            update_data["cron_expression"] = job_update.schedule
        if job_update.enabled is not None:
            update_data["is_active"] = job_update.enabled
            
        # Update job_data if provided
        if job_update.job_data is not None:
            if "agents" in job_update.job_data:
                update_data["agents_yaml"] = job_update.job_data["agents"]
            if "tasks" in job_update.job_data:
                update_data["tasks_yaml"] = job_update.job_data["tasks"]
            if "inputs" in job_update.job_data:
                update_data["inputs"] = job_update.job_data["inputs"]
            if "planning" in job_update.job_data:
                update_data["planning"] = job_update.job_data["planning"]
            if "model" in job_update.job_data:
                update_data["model"] = job_update.job_data["model"]
        
        # Update schedule
        updated_schedule = await self.repository.update(job_id, update_data)
        
        # Convert to job response
        return SchedulerJobResponse(
            id=updated_schedule.id,
            name=updated_schedule.name,
            description=job_update.description or f"Scheduled job from {updated_schedule.name}",
            schedule=updated_schedule.cron_expression,
            enabled=updated_schedule.is_active,
            job_data={
                "agents": updated_schedule.agents_yaml,
                "tasks": updated_schedule.tasks_yaml,
                "inputs": updated_schedule.inputs,
                "planning": updated_schedule.planning,
                "model": updated_schedule.model
            },
            created_at=updated_schedule.created_at,
            updated_at=updated_schedule.updated_at,
            last_run_at=updated_schedule.last_run_at,
            next_run_at=updated_schedule.next_run_at
        )
        
    async def shutdown(self) -> None:
        """
        Shutdown the scheduler and cancel all running tasks.
        """
        logger.info("Shutting down scheduler...")
        if not self._running_tasks:
            logger.info("No running tasks to cancel.")
            return
            
        logger.info(f"Cancelling {len(self._running_tasks)} running tasks...")
        # Create a copy of the set to avoid "Set changed size during iteration" error
        tasks_to_cancel = self._running_tasks.copy()
        
        # Cancel all tasks
        for task in tasks_to_cancel:
            task.cancel()
            
        # Wait for all tasks to complete
        for task in tasks_to_cancel:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error cancelling task: {e}")
        
        self._running_tasks.clear()
        logger.info("Scheduler shutdown complete.") 