"""
Unit tests for SchedulerService.

Tests the functionality of scheduler operations including
schedule CRUD operations, job management, and scheduler execution.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime, timezone

from fastapi import HTTPException, status

from src.services.scheduler_service import SchedulerService
from src.schemas.schedule import ScheduleCreate, ScheduleCreateFromExecution, ScheduleUpdate, ScheduleResponse, ScheduleListResponse, ToggleResponse, CrewConfig
from src.schemas.scheduler import SchedulerJobCreate, SchedulerJobUpdate, SchedulerJobResponse
from src.utils.user_context import GroupContext


# Mock models
class MockSchedule:
    def __init__(self, id=1, name="test_schedule", cron_expression="0 0 * * *",
                 agents_yaml=None, tasks_yaml=None, inputs=None, is_active=True,
                 planning=False, model="gpt-4o-mini", group_id=None, created_by_email=None,
                 created_at=None, updated_at=None, last_run_at=None, next_run_at=None):
        self.id = id
        self.name = name
        self.cron_expression = cron_expression
        self.agents_yaml = agents_yaml or {}
        self.tasks_yaml = tasks_yaml or {}
        self.inputs = inputs or {}
        self.is_active = is_active
        self.planning = planning
        self.model = model
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.last_run_at = last_run_at
        self.next_run_at = next_run_at or datetime.utcnow()


class MockExecutionHistory:
    def __init__(self, id="exec-123", inputs=None):
        self.id = id
        self.inputs = inputs or {
            "agents_yaml": {"agent1": {"role": "researcher"}},
            "tasks_yaml": {"task1": {"description": "research"}},
            "inputs": {"query": "test"},
            "planning": False,
            "model": "gpt-4o-mini"
        }


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def scheduler_service(mock_session):
    """Create a SchedulerService instance with mock session."""
    service = SchedulerService(mock_session)
    service.repository = AsyncMock()
    service.execution_history_repository = AsyncMock()
    return service


@pytest.fixture
def mock_schedule():
    """Create a mock schedule."""
    return MockSchedule()


@pytest.fixture
def mock_execution_history():
    """Create a mock execution history."""
    return MockExecutionHistory()


@pytest.fixture
def group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com"
    )


class TestSchedulerService:
    """Test cases for SchedulerService."""
    
    @pytest.mark.asyncio
    async def test_create_schedule_success(self, scheduler_service, group_context):
        """Test successful schedule creation."""
        schedule_data = ScheduleCreate(
            name="test_schedule",
            cron_expression="0 0 * * *",
            agents_yaml={"agent1": {"role": "researcher"}},
            tasks_yaml={"task1": {"description": "research"}},
            inputs={"query": "test"},
            is_active=True,
            planning=False,
            model="gpt-4o-mini"
        )
        
        mock_schedule = MockSchedule()
        scheduler_service.repository.create.return_value = mock_schedule
        
        with patch('src.services.scheduler_service.calculate_next_run_from_last') as mock_calc:
            mock_calc.return_value = datetime.utcnow()
            
            result = await scheduler_service.create_schedule(schedule_data, group_context)
            
            assert isinstance(result, ScheduleResponse)
            scheduler_service.repository.create.assert_called_once()
            
            # Verify group context was added
            call_args = scheduler_service.repository.create.call_args[0][0]
            assert call_args["group_id"] == "group-123"
            assert call_args["created_by_email"] == "test@example.com"
    
    @pytest.mark.asyncio
    async def test_create_schedule_invalid_cron(self, scheduler_service):
        """Test schedule creation with invalid cron expression."""
        schedule_data = ScheduleCreate(
            name="test_schedule",
            cron_expression="invalid_cron",
            agents_yaml={},
            tasks_yaml={},
            inputs={},
            is_active=True
        )
        
        with patch('src.services.scheduler_service.calculate_next_run_from_last') as mock_calc:
            mock_calc.side_effect = ValueError("Invalid cron expression")
            
            with pytest.raises(HTTPException) as exc_info:
                await scheduler_service.create_schedule(schedule_data)
            
            assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
            assert "Invalid cron expression" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_schedule_from_execution_success(self, scheduler_service, mock_execution_history, group_context):
        """Test successful schedule creation from execution."""
        schedule_data = ScheduleCreateFromExecution(
            name="test_schedule",
            cron_expression="0 0 * * *",
            execution_id=123,
            is_active=True
        )
        
        mock_schedule = MockSchedule()
        scheduler_service.execution_history_repository.find_by_id.return_value = mock_execution_history
        scheduler_service.repository.create.return_value = mock_schedule
        
        with patch('src.services.scheduler_service.calculate_next_run_from_last') as mock_calc:
            mock_calc.return_value = datetime.utcnow()
            
            result = await scheduler_service.create_schedule_from_execution(schedule_data, group_context)
            
            assert isinstance(result, ScheduleResponse)
            scheduler_service.execution_history_repository.find_by_id.assert_called_once_with(123)
            scheduler_service.repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_schedule_from_execution_not_found(self, scheduler_service):
        """Test schedule creation from execution when execution not found."""
        schedule_data = ScheduleCreateFromExecution(
            name="test_schedule",
            cron_expression="0 0 * * *",
            execution_id=999,
            is_active=True
        )
        
        scheduler_service.execution_history_repository.find_by_id.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.create_schedule_from_execution(schedule_data)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_schedule_from_execution_invalid_data(self, scheduler_service):
        """Test schedule creation from execution with invalid execution data."""
        schedule_data = ScheduleCreateFromExecution(
            name="test_schedule",
            cron_expression="0 0 * * *",
            execution_id=123,
            is_active=True
        )
        
        # Mock execution with invalid data
        mock_execution = MockExecutionHistory(
            inputs={"agents_yaml": None, "tasks_yaml": None}
        )
        scheduler_service.execution_history_repository.find_by_id.return_value = mock_execution
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.create_schedule_from_execution(schedule_data)
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "does not contain valid" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_schedule_from_execution_invalid_cron_error(self, scheduler_service, mock_execution_history):
        """Test schedule creation from execution with cron validation error."""
        schedule_data = ScheduleCreateFromExecution(
            name="test_schedule",
            cron_expression="invalid cron",
            execution_id=123,
            is_active=True
        )
        
        scheduler_service.execution_history_repository.find_by_id.return_value = mock_execution_history
        scheduler_service.repository.create.side_effect = ValueError("Invalid cron expression")
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.create_schedule_from_execution(schedule_data)
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid cron expression" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_schedule_from_execution_general_error(self, scheduler_service, mock_execution_history):
        """Test schedule creation from execution with general error."""
        schedule_data = ScheduleCreateFromExecution(
            name="test_schedule",
            cron_expression="0 0 * * *",
            execution_id=123,
            is_active=True
        )
        
        scheduler_service.execution_history_repository.find_by_id.return_value = mock_execution_history
        scheduler_service.repository.create.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.create_schedule_from_execution(schedule_data)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to create schedule from execution" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_schedules_with_group_context(self, scheduler_service, group_context):
        """Test getting all schedules with group context."""
        mock_schedules = [
            MockSchedule(id=1, name="schedule1"),
            MockSchedule(id=2, name="schedule2")
        ]
        scheduler_service.repository.find_by_group.return_value = mock_schedules
        
        result = await scheduler_service.get_all_schedules(group_context)
        
        assert isinstance(result, ScheduleListResponse)
        assert result.count == 2
        assert len(result.schedules) == 2
        scheduler_service.repository.find_by_group.assert_called_once_with("group-123")
    
    @pytest.mark.asyncio
    async def test_get_all_schedules_without_group_context(self, scheduler_service):
        """Test getting all schedules without group context."""
        mock_schedules = [MockSchedule(id=1, name="schedule1")]
        scheduler_service.repository.find_all.return_value = mock_schedules
        
        result = await scheduler_service.get_all_schedules()
        
        assert isinstance(result, ScheduleListResponse)
        assert result.count == 1
        scheduler_service.repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_schedule_by_id_success(self, scheduler_service, mock_schedule):
        """Test successful schedule retrieval by ID."""
        scheduler_service.repository.find_by_id.return_value = mock_schedule
        
        result = await scheduler_service.get_schedule_by_id(1)
        
        assert isinstance(result, ScheduleResponse)
        scheduler_service.repository.find_by_id.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_get_schedule_by_id_not_found(self, scheduler_service):
        """Test schedule retrieval when schedule not found."""
        scheduler_service.repository.find_by_id.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.get_schedule_by_id(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_schedule_success(self, scheduler_service, mock_schedule):
        """Test successful schedule update."""
        schedule_data = ScheduleUpdate(
            name="updated_schedule",
            cron_expression="0 1 * * *",
            agents_yaml={"agent1": {"role": "researcher"}},
            tasks_yaml={"task1": {"description": "research"}},
            inputs={"query": "test"}
        )
        
        scheduler_service.repository.update.return_value = mock_schedule
        
        result = await scheduler_service.update_schedule(1, schedule_data)
        
        assert isinstance(result, ScheduleResponse)
        scheduler_service.repository.update.assert_called_once_with(1, schedule_data.model_dump())
    
    @pytest.mark.asyncio
    async def test_update_schedule_not_found(self, scheduler_service):
        """Test schedule update when schedule not found."""
        schedule_data = ScheduleUpdate(
            name="updated_schedule",
            cron_expression="0 0 * * *",
            agents_yaml={},
            tasks_yaml={}
        )
        scheduler_service.repository.update.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.update_schedule(999, schedule_data)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    
    @pytest.mark.asyncio
    async def test_delete_schedule_success(self, scheduler_service):
        """Test successful schedule deletion."""
        scheduler_service.repository.delete.return_value = True
        
        result = await scheduler_service.delete_schedule(1)
        
        assert result == {"message": "Schedule deleted successfully"}
        scheduler_service.repository.delete.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_delete_schedule_not_found(self, scheduler_service):
        """Test schedule deletion when schedule not found."""
        scheduler_service.repository.delete.return_value = False
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.delete_schedule(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    
    @pytest.mark.asyncio
    async def test_toggle_schedule_success(self, scheduler_service, mock_schedule):
        """Test successful schedule toggle."""
        scheduler_service.repository.toggle_active.return_value = mock_schedule
        
        result = await scheduler_service.toggle_schedule(1)
        
        assert isinstance(result, ToggleResponse)
        scheduler_service.repository.toggle_active.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_toggle_schedule_not_found(self, scheduler_service):
        """Test schedule toggle when schedule not found."""
        scheduler_service.repository.toggle_active.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.toggle_schedule(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    
    @pytest.mark.asyncio
    async def test_run_schedule_job_success(self, scheduler_service):
        """Test successful schedule job execution."""
        config = CrewConfig(
            agents_yaml={"agent1": {"role": "researcher"}},
            tasks_yaml={"task1": {"description": "research"}},
            inputs={"query": "test"},
            planning=False,
            model="gpt-4o-mini"
        )
        execution_time = datetime.now(timezone.utc)
        
        # Mock all the dependencies
        with patch('src.services.scheduler_service.async_session_factory') as mock_session_factory, \
             patch('src.services.scheduler_service.ExecutionService') as mock_exec_service, \
             patch('src.services.scheduler_service.CrewAIExecutionService') as mock_crew_service, \
             patch('src.services.scheduler_service.ScheduleRepository') as mock_repo, \
             patch('src.services.scheduler_service.Run') as mock_run, \
             patch('src.services.scheduler_service.uuid.uuid4') as mock_uuid:
            
            mock_uuid.return_value = "test-job-id"
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            
            mock_exec_instance = AsyncMock()
            mock_exec_service.return_value = mock_exec_instance
            mock_exec_instance.generate_execution_name.return_value.name = "test_run"
            
            mock_crew_instance = AsyncMock()
            mock_crew_service.return_value = mock_crew_instance
            mock_crew_service.add_execution_to_memory = MagicMock()
            
            mock_repo_instance = AsyncMock()
            mock_repo.return_value = mock_repo_instance
            
            # Run the job
            await scheduler_service.run_schedule_job(1, config, execution_time)
            
            # Verify execution service was called
            mock_exec_instance.generate_execution_name.assert_called_once()
            mock_crew_instance.run_crew_execution.assert_called_once()
            mock_repo_instance.update_after_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_schedule_job_error_handling(self, scheduler_service):
        """Test schedule job execution with error handling."""
        config = CrewConfig(
            agents_yaml={},
            tasks_yaml={},
            inputs={},
            planning=False,
            model="gpt-4o-mini"
        )
        execution_time = datetime.now(timezone.utc)
        
        with patch('src.services.scheduler_service.async_session_factory') as mock_session_factory:
            mock_session_factory.side_effect = Exception("Database error")
            
            # Should not raise exception but handle it gracefully
            await scheduler_service.run_schedule_job(1, config, execution_time)
    
    @pytest.mark.asyncio
    async def test_get_all_jobs(self, scheduler_service):
        """Test getting all scheduler jobs."""
        mock_schedules = [
            MockSchedule(id=1, name="schedule1"),
            MockSchedule(id=2, name="schedule2")
        ]
        scheduler_service.repository.find_all.return_value = mock_schedules
        
        result = await scheduler_service.get_all_jobs()
        
        assert len(result) == 2
        assert all(isinstance(job, SchedulerJobResponse) for job in result)
        assert result[0].name == "schedule1"
        assert result[1].name == "schedule2"
    
    @pytest.mark.asyncio
    async def test_create_job(self, scheduler_service, mock_schedule):
        """Test creating a scheduler job."""
        job_create = SchedulerJobCreate(
            name="test_job",
            description="Test job description",
            schedule="0 0 * * *",
            enabled=True,
            job_data={
                "agents": {"agent1": {"role": "researcher"}},
                "tasks": {"task1": {"description": "research"}},
                "inputs": {"query": "test"},
                "planning": False,
                "model": "gpt-4o-mini"
            }
        )
        
        scheduler_service.repository.create.return_value = mock_schedule
        
        result = await scheduler_service.create_job(job_create)
        
        assert isinstance(result, SchedulerJobResponse)
        assert result.name == "test_schedule"
        scheduler_service.repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_job_success(self, scheduler_service, mock_schedule):
        """Test successful job update."""
        job_update = SchedulerJobUpdate(
            name="updated_job",
            description="Updated description",
            schedule="0 1 * * *",
            enabled=False,
            job_data={
                "agents": {"agent2": {"role": "writer"}},
                "model": "gpt-4"
            }
        )
        
        scheduler_service.repository.find_by_id.return_value = mock_schedule
        scheduler_service.repository.update.return_value = mock_schedule
        
        result = await scheduler_service.update_job(1, job_update)
        
        assert isinstance(result, SchedulerJobResponse)
        scheduler_service.repository.find_by_id.assert_called_once_with(1)
        scheduler_service.repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_job_not_found(self, scheduler_service):
        """Test job update when job not found."""
        job_update = SchedulerJobUpdate(name="updated_job")
        scheduler_service.repository.find_by_id.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.update_job(999, job_update)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    
    @pytest.mark.asyncio
    async def test_start_scheduler(self, scheduler_service):
        """Test starting the scheduler."""
        with patch('asyncio.create_task') as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task
            
            await scheduler_service.start_scheduler(30)
            
            mock_create_task.assert_called_once()
            assert mock_task in scheduler_service._running_tasks
    
    @pytest.mark.asyncio
    async def test_shutdown(self, scheduler_service):
        """Test scheduler shutdown."""
        # Create mock tasks
        mock_task1 = AsyncMock()
        mock_task2 = AsyncMock()
        mock_task1.cancel = MagicMock()
        mock_task2.cancel = MagicMock()
        
        scheduler_service._running_tasks = {mock_task1, mock_task2}
        
        await scheduler_service.shutdown()
        
        # Verify tasks were cancelled
        mock_task1.cancel.assert_called_once()
        mock_task2.cancel.assert_called_once()
        assert len(scheduler_service._running_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_shutdown_no_tasks(self, scheduler_service):
        """Test scheduler shutdown with no running tasks."""
        scheduler_service._running_tasks = set()
        
        # Should not raise any errors
        await scheduler_service.shutdown()
    
    @pytest.mark.asyncio
    async def test_check_and_run_schedules_no_due_schedules(self, scheduler_service):
        """Test check_and_run_schedules with no due schedules."""
        with patch('src.services.scheduler_service.async_session_factory') as mock_session_factory, \
             patch('asyncio.sleep') as mock_sleep:
            
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            mock_repo = AsyncMock()
            
            with patch('src.services.scheduler_service.ScheduleRepository', return_value=mock_repo):
                mock_repo.find_due_schedules.return_value = []
                mock_repo.find_all.return_value = []
                
                # Mock sleep to break the infinite loop after first iteration
                call_count = 0
                async def mock_sleep_func(seconds):
                    nonlocal call_count
                    call_count += 1
                    if call_count >= 2:  # Break after second call
                        raise Exception("Break loop")
                    await asyncio.sleep(0)
                
                mock_sleep.side_effect = mock_sleep_func
                
                with pytest.raises(Exception, match="Break loop"):
                    await scheduler_service.check_and_run_schedules()
    
    @pytest.mark.asyncio
    async def test_create_schedule_exception_handling(self, scheduler_service):
        """Test schedule creation with general exception handling."""
        schedule_data = ScheduleCreate(
            name="test_schedule",
            cron_expression="0 0 * * *",
            agents_yaml={},
            tasks_yaml={},
            inputs={},
            is_active=True
        )
        
        scheduler_service.repository.create.side_effect = Exception("Database error")
        
        with patch('src.services.scheduler_service.calculate_next_run_from_last'):
            with pytest.raises(HTTPException) as exc_info:
                await scheduler_service.create_schedule(schedule_data)
            
            assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert "Failed to create schedule" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_schedule_exception_handling(self, scheduler_service):
        """Test schedule update with general exception handling."""
        schedule_data = ScheduleUpdate(
            name="updated_schedule",
            cron_expression="0 0 * * *",
            agents_yaml={},
            tasks_yaml={}
        )
        scheduler_service.repository.update.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.update_schedule(1, schedule_data)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to update schedule" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_delete_schedule_exception_handling(self, scheduler_service):
        """Test schedule deletion with general exception handling."""
        scheduler_service.repository.delete.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.delete_schedule(1)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to delete schedule" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_toggle_schedule_exception_handling(self, scheduler_service):
        """Test schedule toggle with general exception handling."""
        scheduler_service.repository.toggle_active.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await scheduler_service.toggle_schedule(1)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to toggle schedule" in str(exc_info.value.detail)