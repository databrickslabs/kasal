"""
Unit tests for SchedulerService.

Tests the functionality of the scheduler service including
creating, updating, deleting, and managing scheduled tasks.
"""
import pytest
import uuid
from datetime import datetime, UTC, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.scheduler_service import SchedulerService
from src.schemas.schedule import ScheduleCreate, ScheduleUpdate
from src.models.schedule import Schedule
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    uow.session = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_schedule_repository():
    """Create a mock schedule repository."""
    repo = AsyncMock()
    
    # Create mock schedule objects
    mock_schedule = MagicMock(spec=Schedule)
    mock_schedule.id = uuid.uuid4()
    mock_schedule.name = "Daily Backup"
    mock_schedule.description = "Daily backup task"
    mock_schedule.cron_expression = "0 2 * * *"
    mock_schedule.crew_id = uuid.uuid4()
    mock_schedule.is_active = True
    mock_schedule.next_run_time = datetime.now(UTC) + timedelta(hours=1)
    mock_schedule.last_run_time = None
    mock_schedule.created_at = datetime.now(UTC)
    mock_schedule.updated_at = datetime.now(UTC)
    
    # Setup repository method returns
    repo.get.return_value = mock_schedule
    repo.list.return_value = [mock_schedule]
    repo.create.return_value = mock_schedule
    repo.update.return_value = mock_schedule
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_schedule
    repo.get_active_schedules.return_value = [mock_schedule]
    repo.get_due_schedules.return_value = [mock_schedule]
    
    return repo


@pytest.fixture
def schedule_create_data():
    """Create test data for schedule creation."""
    return ScheduleCreate(
        name="Test Schedule",
        description="A test schedule for unit testing",
        cron_expression="0 9 * * 1-5",  # 9 AM Monday to Friday
        crew_id=uuid.uuid4(),
        timezone="UTC",
        max_retries=3,
        retry_delay=300
    )


@pytest.fixture
def schedule_update_data():
    """Create test data for schedule updates."""
    return ScheduleUpdate(
        name="Updated Schedule",
        description="Updated schedule description",
        cron_expression="0 10 * * 1-5"  # 10 AM Monday to Friday
    )


class TestSchedulerService:
    """Test cases for SchedulerService."""
    
    @pytest.mark.asyncio
    async def test_create_schedule_success(self, mock_uow, mock_schedule_repository, schedule_create_data):
        """Test successful schedule creation."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.create_schedule(schedule_create_data)
            
            assert result is not None
            assert result.name == "Daily Backup"
            mock_schedule_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_schedule_invalid_cron(self, mock_uow, mock_schedule_repository):
        """Test schedule creation with invalid cron expression."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            invalid_data = ScheduleCreate(
                name="Invalid Schedule",
                description="Test",
                cron_expression="invalid_cron",  # Invalid cron expression
                crew_id=uuid.uuid4()
            )
            
            with pytest.raises(ValueError, match="Invalid cron expression"):
                await service.create_schedule(invalid_data)
    
    @pytest.mark.asyncio
    async def test_get_schedule_by_id(self, mock_uow, mock_schedule_repository):
        """Test getting a schedule by ID."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.get_schedule(schedule_id)
            
            assert result is not None
            assert result.name == "Daily Backup"
            mock_schedule_repository.get.assert_called_once_with(schedule_id)
    
    @pytest.mark.asyncio
    async def test_get_schedule_not_found(self, mock_uow, mock_schedule_repository):
        """Test getting a non-existent schedule."""
        schedule_id = uuid.uuid4()
        mock_schedule_repository.get.return_value = None
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.get_schedule(schedule_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_schedule_success(self, mock_uow, mock_schedule_repository, schedule_update_data):
        """Test successful schedule update."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.update_schedule(schedule_id, schedule_update_data)
            
            assert result is not None
            mock_schedule_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_schedule_success(self, mock_uow, mock_schedule_repository):
        """Test successful schedule deletion."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.delete_schedule(schedule_id)
            
            assert result is True
            mock_schedule_repository.delete.assert_called_once_with(schedule_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_schedules(self, mock_uow, mock_schedule_repository):
        """Test listing all schedules."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.list_schedules()
            
            assert len(result) == 1
            assert result[0].name == "Daily Backup"
            mock_schedule_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_schedules(self, mock_uow, mock_schedule_repository):
        """Test getting active schedules."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.get_active_schedules()
            
            assert len(result) == 1
            assert result[0].is_active is True
            mock_schedule_repository.get_active_schedules.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_due_schedules(self, mock_uow, mock_schedule_repository):
        """Test getting schedules that are due for execution."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            result = await service.get_due_schedules()
            
            assert len(result) == 1
            mock_schedule_repository.get_due_schedules.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_cron_expression(self, mock_uow):
        """Test cron expression validation."""
        service = SchedulerService(mock_uow)
        
        # Test valid cron expressions
        valid_crons = [
            "0 9 * * 1-5",  # 9 AM Monday to Friday
            "*/15 * * * *",  # Every 15 minutes
            "0 0 1 * *",     # First day of each month
            "0 12 * * 0",    # Every Sunday at noon
        ]
        
        for cron in valid_crons:
            assert service._validate_cron_expression(cron) is True
        
        # Test invalid cron expressions
        invalid_crons = [
            "invalid",
            "60 * * * *",    # Invalid minute (>59)
            "* 25 * * *",    # Invalid hour (>23)
            "* * 32 * *",    # Invalid day (>31)
            "* * * 13 *",    # Invalid month (>12)
            "* * * * 8",     # Invalid day of week (>7)
        ]
        
        for cron in invalid_crons:
            assert service._validate_cron_expression(cron) is False
    
    @pytest.mark.asyncio
    async def test_calculate_next_run_time(self, mock_uow):
        """Test calculation of next run time."""
        service = SchedulerService(mock_uow)
        
        cron_expression = "0 9 * * 1-5"  # 9 AM Monday to Friday
        current_time = datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC)  # Monday 8 AM
        
        next_run = service._calculate_next_run_time(cron_expression, current_time)
        
        # Should be 9 AM on the same day (Monday)
        expected = datetime(2024, 1, 15, 9, 0, 0, tzinfo=UTC)
        assert next_run == expected
    
    @pytest.mark.asyncio
    async def test_schedule_activation_deactivation(self, mock_uow, mock_schedule_repository):
        """Test schedule activation and deactivation."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            # Test activation
            result = await service.activate_schedule(schedule_id)
            assert result is not None
            
            # Test deactivation
            result = await service.deactivate_schedule(schedule_id)
            assert result is not None
            
            # Verify repository calls
            assert mock_schedule_repository.update.call_count == 2
    
    @pytest.mark.asyncio
    async def test_execute_scheduled_task(self, mock_uow, mock_schedule_repository):
        """Test execution of a scheduled task."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository), \
             patch("src.services.scheduler_service.ExecutionService") as mock_execution_service:
            
            mock_exec_instance = AsyncMock()
            mock_exec_instance.create_execution.return_value = {"execution_id": "exec-123"}
            mock_execution_service.return_value = mock_exec_instance
            
            service = SchedulerService(mock_uow)
            
            result = await service.execute_scheduled_task(schedule_id)
            
            assert result is not None
            assert "execution_id" in result
            mock_exec_instance.create_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_last_run_time(self, mock_uow, mock_schedule_repository):
        """Test updating last run time after execution."""
        schedule_id = uuid.uuid4()
        execution_time = datetime.now(UTC)
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            await service.update_last_run_time(schedule_id, execution_time)
            
            mock_schedule_repository.update.assert_called_once()
            # Verify last_run_time was updated
            update_args = mock_schedule_repository.update.call_args[0][1]
            assert "last_run_time" in update_args
    
    @pytest.mark.asyncio
    async def test_schedule_retry_mechanism(self, mock_uow, mock_schedule_repository):
        """Test schedule retry mechanism on failure."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            # Mock schedule with retry configuration
            retry_schedule = MagicMock()
            retry_schedule.max_retries = 3
            retry_schedule.retry_delay = 300
            retry_schedule.retry_count = 1
            mock_schedule_repository.get.return_value = retry_schedule
            
            result = await service.handle_execution_failure(schedule_id, "Execution failed")
            
            assert result is not None
            # Should increment retry count
            assert retry_schedule.retry_count == 2
    
    @pytest.mark.asyncio
    async def test_schedule_metrics(self, mock_uow, mock_schedule_repository):
        """Test schedule metrics collection."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            metrics = await service.get_schedule_metrics()
            
            assert "total_schedules" in metrics
            assert "active_schedules" in metrics
            assert "due_schedules" in metrics
            assert "failed_schedules" in metrics
            assert metrics["total_schedules"] >= 0
    
    @pytest.mark.asyncio
    async def test_timezone_handling(self, mock_uow):
        """Test timezone handling in schedules."""
        service = SchedulerService(mock_uow)
        
        # Test different timezones
        cron_expression = "0 9 * * 1-5"
        current_time = datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC)
        
        # Test UTC timezone
        next_run_utc = service._calculate_next_run_time(
            cron_expression, current_time, timezone="UTC"
        )
        
        # Test different timezone
        next_run_est = service._calculate_next_run_time(
            cron_expression, current_time, timezone="America/New_York"
        )
        
        # Times should be different due to timezone offset
        assert next_run_utc != next_run_est
    
    @pytest.mark.asyncio
    async def test_schedule_conflict_detection(self, mock_uow, mock_schedule_repository):
        """Test detection of schedule conflicts."""
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            # Mock existing schedules
            existing_schedules = [
                MagicMock(cron_expression="0 9 * * 1-5", crew_id=uuid.uuid4()),
                MagicMock(cron_expression="0 10 * * 1-5", crew_id=uuid.uuid4())
            ]
            mock_schedule_repository.list.return_value = existing_schedules
            
            # Test conflicting schedule (same time, same crew)
            new_schedule = MagicMock()
            new_schedule.cron_expression = "0 9 * * 1-5"
            new_schedule.crew_id = existing_schedules[0].crew_id
            
            has_conflict = await service.check_schedule_conflict(new_schedule)
            assert has_conflict is True
            
            # Test non-conflicting schedule
            new_schedule.crew_id = uuid.uuid4()  # Different crew
            has_conflict = await service.check_schedule_conflict(new_schedule)
            assert has_conflict is False
    
    @pytest.mark.asyncio
    async def test_schedule_history_tracking(self, mock_uow, mock_schedule_repository):
        """Test schedule execution history tracking."""
        schedule_id = uuid.uuid4()
        
        with patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_schedule_repository):
            service = SchedulerService(mock_uow)
            
            execution_history = {
                "execution_id": "exec-123",
                "status": "completed",
                "execution_time": 120.5,
                "timestamp": datetime.now(UTC)
            }
            
            await service.record_execution_history(schedule_id, execution_history)
            
            # Verify history was recorded
            mock_schedule_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_schedule_dependency_management(self, mock_uow):
        """Test schedule dependency management."""
        service = SchedulerService(mock_uow)
        
        # Create schedules with dependencies
        schedule_a = MagicMock()
        schedule_a.id = uuid.uuid4()
        schedule_a.dependencies = []
        
        schedule_b = MagicMock()
        schedule_b.id = uuid.uuid4()
        schedule_b.dependencies = [schedule_a.id]
        
        schedule_c = MagicMock()
        schedule_c.id = uuid.uuid4()
        schedule_c.dependencies = [schedule_b.id]
        
        schedules = [schedule_c, schedule_a, schedule_b]  # Out of order
        
        # Calculate execution order based on dependencies
        execution_order = service._calculate_execution_order(schedules)
        
        # Should be ordered by dependencies
        assert execution_order[0].id == schedule_a.id
        assert execution_order[1].id == schedule_b.id
        assert execution_order[2].id == schedule_c.id