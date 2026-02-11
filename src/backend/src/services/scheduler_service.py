import logging
import asyncio
import uuid
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from src.core.exceptions import KasalError, NotFoundError, BadRequestError
from src.repositories.schedule_repository import ScheduleRepository
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.schemas.schedule import ScheduleCreate, ScheduleCreateFromExecution, ScheduleUpdate, ScheduleResponse, ScheduleListResponse, ToggleResponse
from src.schemas.execution import CrewConfig
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
from src.utils.user_context import GroupContext

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
        self.execution_history_repository = ExecutionHistoryRepository(session)
        self.session = session
        self._running_tasks: Set[asyncio.Task] = set()
    
    async def create_schedule(self, schedule_data: ScheduleCreate, group_context: GroupContext = None) -> ScheduleResponse:
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
            
            # Create schedule with tenant context
            schedule_dict = schedule_data.model_dump()
            schedule_dict["next_run_at"] = next_run
            
            # Add group context if provided
            if group_context:
                schedule_dict["group_id"] = group_context.primary_group_id
                schedule_dict["created_by_email"] = group_context.group_email
            
            schedule = await self.repository.create(schedule_dict)
            
            return ScheduleResponse.model_validate(schedule)
        except ValueError as e:
            logger.error(f"Invalid cron expression: {str(e)}")
            raise BadRequestError(detail=f"Invalid cron expression: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to create schedule: {str(e)}")
            raise KasalError(detail=f"Failed to create schedule: {str(e)}")

    async def create_schedule_from_execution(self, schedule_data: ScheduleCreateFromExecution, group_context: GroupContext = None) -> ScheduleResponse:
        """
        Create a new schedule based on an existing execution.
        Supports both crew and flow executions.

        Args:
            schedule_data: Schedule data for creation including execution_id
            group_context: Group context for group isolation

        Returns:
            ScheduleResponse of created schedule

        Raises:
            HTTPException: If execution not found or schedule creation fails
        """
        try:
            # Get the execution history to extract the configuration
            execution = await self.execution_history_repository.find_by_id(schedule_data.execution_id)
            if not execution:
                raise NotFoundError(detail=f"Execution with ID {schedule_data.execution_id} not found")

            # Parse the inputs from execution
            inputs = execution.inputs if execution.inputs else {}
            execution_inputs = inputs.get("inputs", {})
            planning = inputs.get("planning", False)

            # Determine execution type from the execution record
            execution_type = getattr(execution, 'execution_type', None) or inputs.get("execution_type", "crew")

            # Try to get model from inputs first, then from agent configs
            model = inputs.get("model")

            # Create base schedule dictionary
            schedule_dict = {
                "name": schedule_data.name,
                "cron_expression": schedule_data.cron_expression,
                "execution_type": execution_type,
                "inputs": execution_inputs,
                "is_active": schedule_data.is_active,
                "planning": planning,
                "next_run_at": calculate_next_run_from_last(schedule_data.cron_expression)
            }

            if execution_type == "flow":
                # Flow execution - extract flow-specific fields
                nodes = inputs.get("nodes", [])
                edges = inputs.get("edges", [])
                flow_config = inputs.get("flow_config", {})
                flow_id = inputs.get("flow_id") or getattr(execution, 'flow_id', None)

                if not flow_id and not (nodes and edges):
                    raise BadRequestError(detail=f"Execution {schedule_data.execution_id} is a flow execution but does not contain flow_id or nodes/edges configuration")

                schedule_dict["flow_id"] = flow_id
                schedule_dict["nodes"] = nodes
                schedule_dict["edges"] = edges
                schedule_dict["flow_config"] = flow_config
                # For flows, agents_yaml and tasks_yaml can be empty
                schedule_dict["agents_yaml"] = inputs.get("agents_yaml", {})
                schedule_dict["tasks_yaml"] = inputs.get("tasks_yaml", {})

                logger_manager.scheduler.info(f"Creating flow schedule from execution {schedule_data.execution_id} with flow_id={flow_id}, {len(nodes)} nodes, {len(edges)} edges")
            else:
                # Crew execution - extract crew-specific fields
                agents_yaml = inputs.get("agents_yaml", {})
                tasks_yaml = inputs.get("tasks_yaml", {})

                if not agents_yaml or not tasks_yaml:
                    raise BadRequestError(detail=f"Execution {schedule_data.execution_id} does not contain valid agents_yaml or tasks_yaml configuration")

                # Extract model from first agent's llm configuration if not in inputs
                if not model and agents_yaml:
                    for agent_key, agent_config in agents_yaml.items():
                        if isinstance(agent_config, dict) and agent_config.get("llm"):
                            model = agent_config["llm"]
                            break

                schedule_dict["agents_yaml"] = agents_yaml
                schedule_dict["tasks_yaml"] = tasks_yaml

                logger_manager.scheduler.info(f"Creating crew schedule from execution {schedule_data.execution_id} with {len(agents_yaml)} agents, {len(tasks_yaml)} tasks")

            # Fallback to default model if no model found
            if not model:
                model = "gpt-4o-mini"
            schedule_dict["model"] = model

            # Add group context if provided
            if group_context:
                schedule_dict["group_id"] = group_context.primary_group_id
                schedule_dict["created_by_email"] = group_context.group_email

            schedule = await self.repository.create(schedule_dict)

            return ScheduleResponse.model_validate(schedule)
        except KasalError:
            raise
        except ValueError as e:
            logger.error(f"Invalid cron expression: {str(e)}")
            raise BadRequestError(detail=f"Invalid cron expression: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to create schedule from execution: {str(e)}")
            raise KasalError(detail=f"Failed to create schedule from execution: {str(e)}")
    
    async def get_all_schedules(self, group_context: GroupContext = None) -> ScheduleListResponse:
        """
        Get all schedules.
        
        Returns:
            ScheduleListResponse with list of schedules
        """
        logger.debug(f"get_all_schedules called with group_context: {group_context}")
        if group_context and group_context.primary_group_id:
            logger.debug(f"Filtering by group_id: {group_context.primary_group_id}")
            schedules = await self.repository.find_by_group(group_context.primary_group_id)
        else:
            logger.debug(f"No valid group context (context={group_context}, group_id={getattr(group_context, 'primary_group_id', None)}), getting all schedules")
            schedules = await self.repository.find_all()
        
        logger.debug(f"Found {len(schedules)} schedules")
        for schedule in schedules:
            logger.debug(f"  Schedule ID: {schedule.id}, Name: {schedule.name}, Group: {schedule.group_id}")
        
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
            raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")
        return ScheduleResponse.model_validate(schedule)

    async def get_schedule_by_id_with_group_check(self, schedule_id: int, group_context: GroupContext = None) -> ScheduleResponse:
        """
        Get a schedule by ID with group isolation.
        
        Args:
            schedule_id: ID of the schedule to retrieve
            group_context: Group context for isolation
            
        Returns:
            ScheduleResponse if schedule found and belongs to group
            
        Raises:
            HTTPException: If schedule not found or doesn't belong to group
        """
        schedule = await self.repository.find_by_id(schedule_id)
        if not schedule:
            raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

        # Check group access
        if group_context and group_context.primary_group_id:
            if schedule.group_id != group_context.primary_group_id:
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

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
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            return ScheduleResponse.model_validate(schedule)
        except KasalError:
            raise
        except ValueError as e:
            logger.error(f"Invalid cron expression: {str(e)}")
            raise BadRequestError(detail=f"Invalid cron expression: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to update schedule: {str(e)}")
            raise KasalError(detail=f"Failed to update schedule: {str(e)}")

    async def update_schedule_with_group_check(self, schedule_id: int, schedule_data: ScheduleUpdate, group_context: GroupContext = None) -> ScheduleResponse:
        """
        Update a schedule with group isolation.
        
        Args:
            schedule_id: ID of the schedule to update
            schedule_data: New schedule data
            group_context: Group context for isolation
            
        Returns:
            ScheduleResponse of updated schedule
            
        Raises:
            HTTPException: If schedule not found, doesn't belong to group, or update fails
        """
        try:
            # First check if schedule exists and belongs to group
            schedule = await self.repository.find_by_id(schedule_id)
            if not schedule:
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            # Check group access
            if group_context and group_context.primary_group_id:
                if schedule.group_id != group_context.primary_group_id:
                    raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            # Update schedule
            schedule = await self.repository.update(schedule_id, schedule_data.model_dump())
            return ScheduleResponse.model_validate(schedule)
        except KasalError:
            raise
        except ValueError as e:
            logger.error(f"Invalid cron expression: {str(e)}")
            raise BadRequestError(detail=f"Invalid cron expression: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to update schedule: {str(e)}")
            raise KasalError(detail=f"Failed to update schedule: {str(e)}")
    
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
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            return {"message": "Schedule deleted successfully"}
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete schedule: {str(e)}")
            raise KasalError(detail=f"Failed to delete schedule: {str(e)}")

    async def delete_schedule_with_group_check(self, schedule_id: int, group_context: GroupContext = None) -> Dict[str, str]:
        """
        Delete a schedule with group isolation.
        
        Args:
            schedule_id: ID of the schedule to delete
            group_context: Group context for isolation
            
        Returns:
            Success message
            
        Raises:
            HTTPException: If schedule not found, doesn't belong to group, or deletion fails
        """
        try:
            # First check if schedule exists and belongs to group
            schedule = await self.repository.find_by_id(schedule_id)
            if not schedule:
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            # Check group access
            if group_context and group_context.primary_group_id:
                if schedule.group_id != group_context.primary_group_id:
                    raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            # Delete schedule
            deleted = await self.repository.delete(schedule_id)
            return {"message": "Schedule deleted successfully"}
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete schedule: {str(e)}")
            raise KasalError(detail=f"Failed to delete schedule: {str(e)}")
    
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
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            return ToggleResponse.model_validate(schedule)
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle schedule: {str(e)}")
            raise KasalError(detail=f"Failed to toggle schedule: {str(e)}")

    async def toggle_schedule_with_group_check(self, schedule_id: int, group_context: GroupContext = None) -> ToggleResponse:
        """
        Toggle a schedule's active state with group isolation.
        
        Args:
            schedule_id: ID of the schedule to toggle
            group_context: Group context for isolation
            
        Returns:
            ToggleResponse of updated schedule
            
        Raises:
            HTTPException: If schedule not found, doesn't belong to group, or toggle fails
        """
        try:
            # First check if schedule exists and belongs to group
            schedule = await self.repository.find_by_id(schedule_id)
            if not schedule:
                raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            # Check group access
            if group_context and group_context.primary_group_id:
                if schedule.group_id != group_context.primary_group_id:
                    raise NotFoundError(detail=f"Schedule with ID {schedule_id} not found")

            # Toggle schedule
            schedule = await self.repository.toggle_active(schedule_id)
            return ToggleResponse.model_validate(schedule)
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle schedule: {str(e)}")
            raise KasalError(detail=f"Failed to toggle schedule: {str(e)}")
    
    async def run_schedule_job(self, schedule_id: int, config: CrewConfig, execution_time: datetime) -> None:
        """
        Run a scheduled job. Supports both crew and flow executions.

        Args:
            schedule_id: ID of the schedule to run
            config: Job configuration (supports both crew and flow)
            execution_time: Time when the job was triggered
        """
        try:
            # Generate job ID and determine execution type
            job_id = str(uuid.uuid4())
            model = config.model or "gpt-4o-mini"
            execution_type = getattr(config, 'execution_type', 'crew') or 'crew'

            # Setup async session
            async with async_session_factory() as session:
                # Get the schedule to retrieve group information
                repo = ScheduleRepository(session)
                schedule = await repo.find_by_id(schedule_id)
                if not schedule:
                    logger_manager.scheduler.error(f"Schedule {schedule_id} not found")
                    return

                # Generate run name based on execution type
                execution_service = ExecutionService()
                if execution_type == "flow":
                    # For flows, generate a name based on flow configuration
                    run_name = f"Scheduled Flow {job_id[:8]}"
                    logger_manager.scheduler.info(f"Running scheduled flow job {job_id} for schedule {schedule_id}")
                else:
                    # For crews, use the name generation service
                    request = ExecutionNameGenerationRequest(
                        agents_yaml=config.agents_yaml or {},
                        tasks_yaml=config.tasks_yaml or {},
                        model=model
                    )
                    response = await execution_service.generate_execution_name(request)
                    run_name = response.get("name", f"Scheduled Run {job_id[:8]}")
                    logger_manager.scheduler.info(f"Running scheduled crew job {job_id} for schedule {schedule_id}")

                # Prepare job configuration based on execution type
                config_dict = {
                    "agents_yaml": config.agents_yaml or {},
                    "tasks_yaml": config.tasks_yaml or {},
                    "inputs": config.inputs,
                    "model": config.model,
                    "execution_type": execution_type
                }

                # Add flow-specific fields if flow execution
                if execution_type == "flow":
                    if config.flow_id:
                        config_dict["flow_id"] = str(config.flow_id)
                    if config.nodes:
                        config_dict["nodes"] = config.nodes
                    if config.edges:
                        config_dict["edges"] = config.edges
                    if config.flow_config:
                        config_dict["flow_config"] = config.flow_config

                # Convert to local time for database consistency with regular jobs
                if hasattr(execution_time, 'tzinfo') and execution_time.tzinfo is not None:
                    execution_time_naive = execution_time.astimezone().replace(tzinfo=None)
                else:
                    execution_time_naive = execution_time

                # Create run record with group information from schedule
                db_run = Run(
                    job_id=job_id,
                    status="pending",
                    inputs=config_dict,
                    created_at=execution_time_naive,
                    trigger_type="scheduled",
                    planning=config.planning,
                    run_name=run_name,
                    group_id=schedule.group_id,
                    group_email=schedule.created_by_email,
                    execution_type=execution_type
                )

                # Add flow_id if it's a flow execution with a saved flow
                if execution_type == "flow" and config.flow_id:
                    db_run.flow_id = config.flow_id

                session.add(db_run)
                await session.commit()
                await session.refresh(db_run)

                # Ensure Databricks auth is available via unified auth for scheduled jobs
                import os
                try:
                    from src.utils.databricks_auth import get_auth_context
                    auth = await get_auth_context()
                    if auth:
                        if auth.workspace_url:
                            os.environ["DATABRICKS_HOST"] = auth.workspace_url
                            logger_manager.scheduler.info(f"Loaded DATABRICKS_HOST from unified {auth.auth_method} auth for scheduled job")
                        if auth.token:
                            os.environ["DATABRICKS_TOKEN"] = auth.token
                            os.environ["DATABRICKS_API_KEY"] = auth.token
                            logger_manager.scheduler.info(f"Loaded DATABRICKS_TOKEN from unified {auth.auth_method} auth for scheduled job")
                    else:
                        logger_manager.scheduler.warning("No unified auth available for scheduled job")
                except Exception as e:
                    logger_manager.scheduler.warning(f"Could not load Databricks auth from unified auth: {e}")

                # Create group context from schedule information
                from src.utils.user_context import GroupContext
                group_context = GroupContext(
                    group_ids=[schedule.group_id] if schedule.group_id else [],
                    group_email=schedule.created_by_email
                )

                # Add execution to memory
                CrewAIExecutionService.add_execution_to_memory(
                    execution_id=job_id,
                    status=JobStatus.PENDING.value,
                    run_name=run_name,
                    created_at=execution_time_naive
                )

                # Run the job using ExecutionService which handles both crew and flow
                await ExecutionService.run_crew_execution(
                    execution_id=job_id,
                    config=config,
                    execution_type=execution_type,
                    group_context=group_context,
                    session=session
                )

                # Update schedule after execution
                repo = ScheduleRepository(session)
                await repo.update_after_execution(schedule_id, execution_time)

                logger_manager.scheduler.info(
                    f"Successfully ran {execution_type} schedule {schedule_id}."
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
                logger_manager.scheduler.debug(f"Checking for due schedules at {now_local} (local) / {now_utc} (UTC)")
                logger_manager.scheduler.debug(f"Currently running tasks: {len(self._running_tasks)}")
                
                # Find due schedules
                async with async_session_factory() as session:
                    repo = ScheduleRepository(session)
                    # Convert timezone-aware now_utc to timezone-naive for database comparison
                    due_schedules = await repo.find_due_schedules(now_utc.replace(tzinfo=None))
                    all_schedules = await repo.find_all()
                    
                    # Log status of all schedules
                    logger_manager.scheduler.debug("Current schedules status:")
                    for schedule in all_schedules:
                        # Handle timezone-naive datetimes from database
                        if schedule.next_run_at and schedule.next_run_at.tzinfo is None:
                            next_run = schedule.next_run_at.replace(tzinfo=timezone.utc)
                        else:
                            next_run = ensure_utc(schedule.next_run_at)
                            
                        if schedule.last_run_at and schedule.last_run_at.tzinfo is None:
                            last_run = schedule.last_run_at.replace(tzinfo=timezone.utc)
                        else:
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
                    if len(due_schedules) > 0:
                        logger_manager.scheduler.info(f"Found {len(due_schedules)} schedules due to run")
                    else:
                        logger_manager.scheduler.debug(f"Found {len(due_schedules)} schedules due to run")
                    
                    for schedule in due_schedules:
                        execution_type = getattr(schedule, 'execution_type', 'crew') or 'crew'
                        logger_manager.scheduler.info(f"Starting task for schedule {schedule.id} - {schedule.name} (type: {execution_type})")
                        logger_manager.scheduler.info(f"Schedule configuration: execution_type={execution_type}, agents_yaml={schedule.agents_yaml}, tasks_yaml={schedule.tasks_yaml}, inputs={schedule.inputs}, planning={schedule.planning}, model={schedule.model}")

                        # Build CrewConfig with proper defaults for None values (important for flow schedules)
                        config = CrewConfig(
                            agents_yaml=schedule.agents_yaml or {},
                            tasks_yaml=schedule.tasks_yaml or {},
                            inputs=schedule.inputs or {},
                            planning=schedule.planning or False,
                            model=schedule.model,
                            reasoning=False,  # Default value for scheduled jobs
                            execution_type=execution_type,
                            schema_detection_enabled=True,  # Default value
                            # Flow-specific fields
                            flow_id=str(schedule.flow_id) if getattr(schedule, 'flow_id', None) else None,
                            nodes=getattr(schedule, 'nodes', None),
                            edges=getattr(schedule, 'edges', None),
                            flow_config=getattr(schedule, 'flow_config', None),
                        )
                        
                        # Create task for the job
                        task = asyncio.create_task(
                            self.run_schedule_job(schedule.id, config, now_utc),
                            name=f"schedule_{schedule.id}_{now_utc.isoformat()}"
                        )
                        self._running_tasks.add(task)
                        
                        # Update next run time immediately
                        # Convert timezone-aware now_utc to timezone-naive for consistency
                        schedule.next_run_at = calculate_next_run_from_last(
                            schedule.cron_expression,
                            now_utc.replace(tzinfo=None)
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
                logger_manager.scheduler.debug("Sleeping for 60 seconds before next check")
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
    
    async def get_all_jobs_for_group(self, group_context: GroupContext = None) -> List[SchedulerJobResponse]:
        """
        Get all scheduler jobs for a specific group.
        
        Args:
            group_context: Group context for isolation
            
        Returns:
            List of scheduler jobs for the group
        """
        # Get schedules for the group
        if group_context and group_context.primary_group_id:
            schedules = await self.repository.find_by_group(group_context.primary_group_id)
        else:
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
        agents_yaml = job_create.job_data.get("agents", {})
        
        # Extract model from job data or agent configurations
        model = job_create.job_data.get("model")
        if not model and agents_yaml:
            # Extract model from first agent's llm configuration
            for agent_key, agent_config in agents_yaml.items():
                if isinstance(agent_config, dict) and agent_config.get("llm"):
                    model = agent_config["llm"]
                    break
        # Fallback to default if no model found
        if not model:
            model = "gpt-4o-mini"
            
        schedule_data = ScheduleCreate(
            name=job_create.name,
            cron_expression=job_create.schedule,
            agents_yaml=agents_yaml,
            tasks_yaml=job_create.job_data.get("tasks", {}),
            inputs=job_create.job_data.get("inputs", {}),
            is_active=job_create.enabled,
            planning=job_create.job_data.get("planning", False),
            model=model
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
    
    async def create_job_with_group(self, job_create: SchedulerJobCreate, group_context: GroupContext = None) -> SchedulerJobResponse:
        """
        Create a new scheduler job with group isolation.
        
        Args:
            job_create: Job data to create
            group_context: Group context for isolation
            
        Returns:
            Created job
        """
        # Convert job to schedule
        agents_yaml = job_create.job_data.get("agents", {})
        
        # Extract model from job data or agent configurations
        model = job_create.job_data.get("model")
        if not model and agents_yaml:
            # Extract model from first agent's llm configuration
            for agent_key, agent_config in agents_yaml.items():
                if isinstance(agent_config, dict) and agent_config.get("llm"):
                    model = agent_config["llm"]
                    break
        # Fallback to default if no model found
        if not model:
            model = "gpt-4o-mini"
            
        schedule_dict = {
            "name": job_create.name,
            "cron_expression": job_create.schedule,
            "agents_yaml": agents_yaml,
            "tasks_yaml": job_create.job_data.get("tasks", {}),
            "inputs": job_create.job_data.get("inputs", {}),
            "is_active": job_create.enabled,
            "planning": job_create.job_data.get("planning", False),
            "model": model,
            "next_run_at": calculate_next_run_from_last(job_create.schedule)
        }
        
        # Add group context if provided
        if group_context:
            schedule_dict["group_id"] = group_context.primary_group_id
            schedule_dict["created_by_email"] = group_context.group_email
        
        # Create schedule
        schedule = await self.repository.create(schedule_dict)
        
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
            raise NotFoundError(detail=f"Job with ID {job_id} not found")

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

    async def update_job_with_group_check(self, job_id: int, job_update: SchedulerJobUpdate, group_context: GroupContext = None) -> SchedulerJobResponse:
        """
        Update a scheduler job with group isolation.
        
        Args:
            job_id: ID of the job to update
            job_update: Updated job data
            group_context: Group context for isolation
            
        Returns:
            Updated job
            
        Raises:
            HTTPException: If job not found, doesn't belong to group, or update fails
        """
        # Get existing schedule
        existing_schedule = await self.repository.find_by_id(job_id)
        if not existing_schedule:
            raise NotFoundError(detail=f"Job with ID {job_id} not found")

        # Check group access
        if group_context and group_context.primary_group_id:
            if existing_schedule.group_id != group_context.primary_group_id:
                raise NotFoundError(detail=f"Job with ID {job_id} not found")
        
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