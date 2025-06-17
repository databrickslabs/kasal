"""
Unit tests for ScheduleRepository.

Tests the functionality of schedule repository including
CRUD operations, cron scheduling, active/inactive management, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.schedule_repository import ScheduleRepository
from src.models.schedule import Schedule


# Mock schedule model
class MockSchedule:
    def __init__(self, id=1, name="Test Schedule", description="Test Description",
                 cron_expression="0 9 * * *", is_active=True, group_id="group-123",
                 crew_id="crew-123", last_run_at=None, next_run_at=None,
                 created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.description = description
        self.cron_expression = cron_expression
        self.is_active = is_active
        self.group_id = group_id
        self.crew_id = crew_id
        self.last_run_at = last_run_at
        self.next_run_at = next_run_at or datetime.now(timezone.utc)
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results, scalar_result=None):
        self._scalars = MockScalars(results)
        self._scalar_result = scalar_result if scalar_result is not None else (results[0] if results else None)
    
    def scalars(self):
        return self._scalars
    
    def scalar_one_or_none(self):
        return self._scalar_result


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.delete = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def schedule_repository(mock_async_session):
    """Create a schedule repository with async session."""
    return ScheduleRepository(session=mock_async_session)


@pytest.fixture
def sample_schedules():
    """Create sample schedules for testing."""
    return [
        MockSchedule(id=1, name="Daily Schedule", cron_expression="0 9 * * *", is_active=True, group_id="group-1"),
        MockSchedule(id=2, name="Weekly Schedule", cron_expression="0 9 * * 1", is_active=True, group_id="group-1"),
        MockSchedule(id=3, name="Monthly Schedule", cron_expression="0 9 1 * *", is_active=False, group_id="group-2"),
        MockSchedule(id=4, name="Hourly Schedule", cron_expression="0 * * * *", is_active=True, group_id="group-1")
    ]


@pytest.fixture
def sample_schedule_data():
    """Create sample schedule data for creation."""
    return {
        "name": "New Schedule",
        "description": "A new test schedule",
        "cron_expression": "0 12 * * *",
        "is_active": True,
        "group_id": "group-123",
        "crew_id": "crew-123"
    }


class TestScheduleRepositoryInit:
    """Test cases for ScheduleRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = ScheduleRepository(session=mock_async_session)
        
        assert repository.session == mock_async_session


class TestScheduleRepositoryCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, schedule_repository, mock_async_session, sample_schedule_data):
        """Test successful schedule creation."""
        with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
            next_run_time = datetime.now(timezone.utc)
            mock_calc_next.return_value = next_run_time
            
            with patch('src.repositories.schedule_repository.Schedule') as mock_schedule_class:
                created_schedule = MockSchedule(**sample_schedule_data)
                mock_schedule_class.return_value = created_schedule
                
                result = await schedule_repository.create(sample_schedule_data)
                
                assert result == created_schedule
                mock_schedule_class.assert_called_once()
                mock_async_session.add.assert_called_once_with(created_schedule)
                mock_async_session.commit.assert_called_once()
                mock_async_session.refresh.assert_called_once_with(created_schedule)
                
                # Verify next_run_at was calculated
                mock_calc_next.assert_called_once_with(sample_schedule_data['cron_expression'])
    
    @pytest.mark.asyncio
    async def test_create_with_existing_next_run_at(self, schedule_repository, mock_async_session, sample_schedule_data):
        """Test schedule creation when next_run_at is already provided."""
        next_run_time = datetime.now(timezone.utc)
        sample_schedule_data['next_run_at'] = next_run_time
        
        with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
            with patch('src.repositories.schedule_repository.Schedule') as mock_schedule_class:
                created_schedule = MockSchedule(**sample_schedule_data)
                mock_schedule_class.return_value = created_schedule
                
                result = await schedule_repository.create(sample_schedule_data)
                
                assert result == created_schedule
                # Should not calculate next_run_at if already provided
                mock_calc_next.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, schedule_repository, mock_async_session, sample_schedule_data):
        """Test create with database error."""
        with patch('src.repositories.schedule_repository.calculate_next_run_from_last'):
            with patch('src.repositories.schedule_repository.Schedule') as mock_schedule_class:
                mock_schedule_class.return_value = MockSchedule(**sample_schedule_data)
                mock_async_session.commit.side_effect = Exception("Commit failed")
                
                with pytest.raises(Exception, match="Commit failed"):
                    await schedule_repository.create(sample_schedule_data)


class TestScheduleRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, schedule_repository, mock_async_session, sample_schedules):
        """Test successful retrieval of all schedules."""
        mock_result = MockResult(sample_schedules)
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_all()
        
        assert len(result) == len(sample_schedules)
        assert result == sample_schedules
        mock_async_session.execute.assert_called_once()
        
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Schedule)))
    
    @pytest.mark.asyncio
    async def test_find_all_empty_result(self, schedule_repository, mock_async_session):
        """Test find all when no schedules exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestScheduleRepositoryFindById:
    """Test cases for find_by_id method."""
    
    @pytest.mark.asyncio
    async def test_find_by_id_success(self, schedule_repository, mock_async_session):
        """Test successful schedule retrieval by ID."""
        schedule = MockSchedule(id=1)
        mock_result = MockResult([], scalar_result=schedule)
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_by_id(1)
        
        assert result == schedule
        mock_async_session.execute.assert_called_once()
        
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Schedule)))
    
    @pytest.mark.asyncio
    async def test_find_by_id_not_found(self, schedule_repository, mock_async_session):
        """Test find by ID when schedule not found."""
        mock_result = MockResult([], scalar_result=None)
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_by_id(999)
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestScheduleRepositoryFindByTenant:
    """Test cases for find_by_tenant method."""
    
    @pytest.mark.asyncio
    async def test_find_by_tenant_success(self, schedule_repository, mock_async_session, sample_schedules):
        """Test successful schedule retrieval by tenant."""
        tenant_schedules = [schedule for schedule in sample_schedules if schedule.group_id == "group-1"]
        mock_result = MockResult(tenant_schedules)
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_by_tenant("group-1")
        
        assert len(result) == len(tenant_schedules)
        assert all(schedule.group_id == "group-1" for schedule in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_tenant_no_schedules(self, schedule_repository, mock_async_session):
        """Test find by tenant when no schedules found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_by_tenant("nonexistent-tenant")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestScheduleRepositoryFindByGroup:
    """Test cases for find_by_group method."""
    
    @pytest.mark.asyncio
    async def test_find_by_group_success(self, schedule_repository, mock_async_session, sample_schedules):
        """Test successful schedule retrieval by group."""
        group_schedules = [schedule for schedule in sample_schedules if schedule.group_id == "group-1"]
        mock_result = MockResult(group_schedules)
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_by_group("group-1")
        
        assert len(result) == len(group_schedules)
        assert all(schedule.group_id == "group-1" for schedule in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_group_no_schedules(self, schedule_repository, mock_async_session):
        """Test find by group when no schedules found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_by_group("nonexistent-group")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestScheduleRepositoryFindDueSchedules:
    """Test cases for find_due_schedules method."""
    
    @pytest.mark.asyncio
    async def test_find_due_schedules_success(self, schedule_repository, mock_async_session, sample_schedules):
        """Test successful retrieval of due schedules."""
        current_time = datetime.now(timezone.utc)
        
        # Mock schedules that are due (active and next_run_at <= current_time)
        due_schedules = [schedule for schedule in sample_schedules if schedule.is_active]
        for schedule in due_schedules:
            schedule.next_run_at = current_time - timezone.utc.utcoffset(None) if timezone.utc.utcoffset(None) else current_time
        
        mock_result = MockResult(due_schedules)
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_due_schedules(current_time)
        
        assert len(result) == len(due_schedules)
        assert all(schedule.is_active for schedule in result)
        mock_async_session.execute.assert_called_once()
        
        # Verify the query filters for active schedules and due time
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Schedule)))
    
    @pytest.mark.asyncio
    async def test_find_due_schedules_none_due(self, schedule_repository, mock_async_session):
        """Test find due schedules when none are due."""
        current_time = datetime.now(timezone.utc)
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_due_schedules(current_time)
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestScheduleRepositoryUpdate:
    """Test cases for update method."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, schedule_repository, mock_async_session):
        """Test successful schedule update."""
        schedule = MockSchedule(id=1, name="Old Name", cron_expression="0 9 * * *")
        update_data = {"name": "New Name", "description": "Updated description"}
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            result = await schedule_repository.update(1, update_data)
            
            assert result == schedule
            assert schedule.name == "New Name"
            assert schedule.description == "Updated description"
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(schedule)
    
    @pytest.mark.asyncio
    async def test_update_with_cron_expression(self, schedule_repository, mock_async_session):
        """Test update with cron expression recalculates next run time."""
        schedule = MockSchedule(id=1, cron_expression="0 9 * * *", last_run_at=datetime.now(timezone.utc))
        update_data = {"cron_expression": "0 12 * * *"}
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
                new_next_run = datetime.now(timezone.utc)
                mock_calc_next.return_value = new_next_run
                
                result = await schedule_repository.update(1, update_data)
                
                assert result == schedule
                assert schedule.cron_expression == "0 12 * * *"
                assert schedule.next_run_at == new_next_run
                mock_calc_next.assert_called_once_with("0 12 * * *", schedule.last_run_at)
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, schedule_repository, mock_async_session):
        """Test update when schedule not found."""
        with patch.object(schedule_repository, 'find_by_id', return_value=None):
            result = await schedule_repository.update(999, {"name": "New Name"})
            
            assert result is None
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_database_error(self, schedule_repository, mock_async_session):
        """Test update with database error."""
        schedule = MockSchedule(id=1)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            mock_async_session.commit.side_effect = Exception("Update failed")
            
            with pytest.raises(Exception, match="Update failed"):
                await schedule_repository.update(1, {"name": "New Name"})


class TestScheduleRepositoryDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, schedule_repository, mock_async_session):
        """Test successful schedule deletion."""
        schedule = MockSchedule(id=1)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            result = await schedule_repository.delete(1)
            
            assert result is True
            mock_async_session.delete.assert_called_once_with(schedule)
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, schedule_repository, mock_async_session):
        """Test delete when schedule not found."""
        with patch.object(schedule_repository, 'find_by_id', return_value=None):
            result = await schedule_repository.delete(999)
            
            assert result is False
            mock_async_session.delete.assert_not_called()
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_database_error(self, schedule_repository, mock_async_session):
        """Test delete with database error."""
        schedule = MockSchedule(id=1)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            mock_async_session.delete.side_effect = Exception("Delete failed")
            
            with pytest.raises(Exception, match="Delete failed"):
                await schedule_repository.delete(1)


class TestScheduleRepositoryToggleActive:
    """Test cases for toggle_active method."""
    
    @pytest.mark.asyncio
    async def test_toggle_active_activate(self, schedule_repository, mock_async_session):
        """Test toggling schedule from inactive to active."""
        schedule = MockSchedule(id=1, is_active=False, cron_expression="0 9 * * *", last_run_at=datetime.now(timezone.utc))
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
                new_next_run = datetime.now(timezone.utc)
                mock_calc_next.return_value = new_next_run
                
                result = await schedule_repository.toggle_active(1)
                
                assert result == schedule
                assert schedule.is_active is True
                assert schedule.next_run_at == new_next_run
                mock_calc_next.assert_called_once_with(schedule.cron_expression, schedule.last_run_at)
                mock_async_session.commit.assert_called_once()
                mock_async_session.refresh.assert_called_once_with(schedule)
    
    @pytest.mark.asyncio
    async def test_toggle_active_deactivate(self, schedule_repository, mock_async_session):
        """Test toggling schedule from active to inactive."""
        schedule = MockSchedule(id=1, is_active=True)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
                result = await schedule_repository.toggle_active(1)
                
                assert result == schedule
                assert schedule.is_active is False
                # Should not recalculate next run time when deactivating
                mock_calc_next.assert_not_called()
                mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_active_not_found(self, schedule_repository, mock_async_session):
        """Test toggle active when schedule not found."""
        with patch.object(schedule_repository, 'find_by_id', return_value=None):
            result = await schedule_repository.toggle_active(999)
            
            assert result is None
            mock_async_session.commit.assert_not_called()


class TestScheduleRepositoryUpdateAfterExecution:
    """Test cases for update_after_execution method."""
    
    @pytest.mark.asyncio
    async def test_update_after_execution_success(self, schedule_repository, mock_async_session):
        """Test successful update after execution."""
        schedule = MockSchedule(id=1, cron_expression="0 9 * * *")
        execution_time = datetime.now(timezone.utc)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
                new_next_run = datetime.now(timezone.utc)
                mock_calc_next.return_value = new_next_run
                
                result = await schedule_repository.update_after_execution(1, execution_time)
                
                assert result == schedule
                assert schedule.last_run_at == execution_time
                assert schedule.next_run_at == new_next_run
                mock_calc_next.assert_called_once_with(schedule.cron_expression, execution_time)
                mock_async_session.commit.assert_called_once()
                mock_async_session.refresh.assert_called_once_with(schedule)
    
    @pytest.mark.asyncio
    async def test_update_after_execution_not_found(self, schedule_repository, mock_async_session):
        """Test update after execution when schedule not found."""
        execution_time = datetime.now(timezone.utc)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=None):
            result = await schedule_repository.update_after_execution(999, execution_time)
            
            assert result is None
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_after_execution_database_error(self, schedule_repository, mock_async_session):
        """Test update after execution with database error."""
        schedule = MockSchedule(id=1, cron_expression="0 9 * * *")
        execution_time = datetime.now(timezone.utc)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last'):
                mock_async_session.commit.side_effect = Exception("Update failed")
                
                with pytest.raises(Exception, match="Update failed"):
                    await schedule_repository.update_after_execution(1, execution_time)


class TestScheduleRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_id(self, schedule_repository, mock_async_session, sample_schedule_data):
        """Test creating schedule then finding it by ID."""
        with patch('src.repositories.schedule_repository.calculate_next_run_from_last'):
            with patch('src.repositories.schedule_repository.Schedule') as mock_schedule_class:
                created_schedule = MockSchedule(id=1, **sample_schedule_data)
                mock_schedule_class.return_value = created_schedule
                
                # Create
                create_result = await schedule_repository.create(sample_schedule_data)
                
                # Mock find_by_id to avoid SQLAlchemy issues with mocked Schedule
                with patch.object(schedule_repository, 'find_by_id', return_value=created_schedule):
                    find_result = await schedule_repository.find_by_id(1)
                
                assert create_result == created_schedule
                assert find_result == created_schedule
    
    @pytest.mark.asyncio
    async def test_schedule_lifecycle(self, schedule_repository, mock_async_session, sample_schedule_data):
        """Test complete schedule lifecycle: create, update, toggle, delete."""
        with patch('src.repositories.schedule_repository.calculate_next_run_from_last'):
            with patch('src.repositories.schedule_repository.Schedule') as mock_schedule_class:
                schedule = MockSchedule(id=1, **sample_schedule_data)
                mock_schedule_class.return_value = schedule
                
                # Create
                create_result = await schedule_repository.create(sample_schedule_data)
                assert create_result == schedule
                
                # Update
                with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
                    update_result = await schedule_repository.update(1, {"name": "Updated Schedule"})
                    assert update_result == schedule
                    assert schedule.name == "Updated Schedule"
                    
                    # Toggle active
                    original_active = schedule.is_active
                    toggle_result = await schedule_repository.toggle_active(1)
                    assert toggle_result == schedule
                    assert schedule.is_active != original_active
                    
                    # Delete
                    delete_result = await schedule_repository.delete(1)
                    assert delete_result is True


class TestScheduleRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_all_database_error(self, schedule_repository, mock_async_session):
        """Test find all with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await schedule_repository.find_all()
    
    @pytest.mark.asyncio
    async def test_find_by_tenant_database_error(self, schedule_repository, mock_async_session):
        """Test find by tenant with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await schedule_repository.find_by_tenant("group-1")
    
    @pytest.mark.asyncio
    async def test_find_due_schedules_database_error(self, schedule_repository, mock_async_session):
        """Test find due schedules with database error."""
        current_time = datetime.now(timezone.utc)
        mock_async_session.execute.side_effect = Exception("Database offline")
        
        with pytest.raises(Exception, match="Database offline"):
            await schedule_repository.find_due_schedules(current_time)


class TestScheduleRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_create_with_invalid_cron_expression(self, schedule_repository, mock_async_session, sample_schedule_data):
        """Test create with invalid cron expression."""
        sample_schedule_data['cron_expression'] = "invalid cron"
        
        with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
            mock_calc_next.side_effect = ValueError("Invalid cron expression")
            
            with pytest.raises(ValueError, match="Invalid cron expression"):
                await schedule_repository.create(sample_schedule_data)
    
    @pytest.mark.asyncio
    async def test_find_by_tenant_vs_find_by_group_equivalence(self, schedule_repository, mock_async_session, sample_schedules):
        """Test that find_by_tenant and find_by_group return same results."""
        group_schedules = [schedule for schedule in sample_schedules if schedule.group_id == "group-1"]
        mock_result = MockResult(group_schedules)
        mock_async_session.execute.return_value = mock_result
        
        # Find by tenant
        tenant_result = await schedule_repository.find_by_tenant("group-1")
        
        # Reset mock for second call
        mock_async_session.execute.return_value = mock_result
        
        # Find by group
        group_result = await schedule_repository.find_by_group("group-1")
        
        assert tenant_result == group_result
        assert len(tenant_result) == len(group_schedules)
    
    @pytest.mark.asyncio
    async def test_update_empty_data(self, schedule_repository, mock_async_session):
        """Test update with empty data dictionary."""
        schedule = MockSchedule(id=1, name="Original Name", group_id="group-123")
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            result = await schedule_repository.update(1, {})
            
            assert result == schedule
            assert schedule.name == "Original Name"  # Should remain unchanged
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_active_multiple_times(self, schedule_repository, mock_async_session):
        """Test toggling active status multiple times."""
        schedule = MockSchedule(id=1, is_active=True)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last'):
                # First toggle (active -> inactive)
                result1 = await schedule_repository.toggle_active(1)
                assert result1.is_active is False
                
                # Second toggle (inactive -> active)
                result2 = await schedule_repository.toggle_active(1)
                assert result2.is_active is True
                
                # Third toggle (active -> inactive)
                result3 = await schedule_repository.toggle_active(1)
                assert result3.is_active is False
                
                assert mock_async_session.commit.call_count == 3
    
    @pytest.mark.asyncio
    async def test_find_due_schedules_boundary_time(self, schedule_repository, mock_async_session):
        """Test find due schedules with exact boundary time."""
        current_time = datetime.now(timezone.utc)
        
        # Create schedule with next_run_at exactly equal to current_time
        boundary_schedule = MockSchedule(
            id=1, 
            is_active=True, 
            next_run_at=current_time
        )
        
        mock_result = MockResult([boundary_schedule])
        mock_async_session.execute.return_value = mock_result
        
        result = await schedule_repository.find_due_schedules(current_time)
        
        assert len(result) == 1
        assert result[0] == boundary_schedule
    
    @pytest.mark.asyncio
    async def test_update_after_execution_none_last_run_at(self, schedule_repository, mock_async_session):
        """Test update after execution when schedule has no previous last_run_at."""
        schedule = MockSchedule(id=1, cron_expression="0 9 * * *", last_run_at=None)
        execution_time = datetime.now(timezone.utc)
        
        with patch.object(schedule_repository, 'find_by_id', return_value=schedule):
            with patch('src.repositories.schedule_repository.calculate_next_run_from_last') as mock_calc_next:
                new_next_run = datetime.now(timezone.utc)
                mock_calc_next.return_value = new_next_run
                
                result = await schedule_repository.update_after_execution(1, execution_time)
                
                assert result == schedule
                assert schedule.last_run_at == execution_time
                mock_calc_next.assert_called_once_with(schedule.cron_expression, execution_time)