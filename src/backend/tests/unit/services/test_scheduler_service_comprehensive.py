"""
Comprehensive unit tests for services/scheduler_service.py
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone
import asyncio

from src.services.scheduler_service import SchedulerService
from src.core.exceptions import KasalError, NotFoundError, BadRequestError


def _make_service():
    mock_session = Mock()
    service = SchedulerService(mock_session)
    service.repository = AsyncMock()
    service.execution_history_repository = AsyncMock()
    return service


def _make_schedule(id=1, name="Test", cron="0 * * * *", is_active=True, group_id="grp-1"):
    s = MagicMock()
    s.id = id
    s.name = name
    s.cron_expression = cron
    s.is_active = is_active
    s.group_id = group_id
    s.execution_type = "crew"
    s.agents_yaml = {}
    s.tasks_yaml = {}
    s.inputs = {}
    s.model = "gpt-4o-mini"
    s.next_run_at = datetime.now(timezone.utc)
    s.created_at = datetime.now(timezone.utc)
    s.updated_at = datetime.now(timezone.utc)
    # Return dict for model_validate
    s.model_dump = MagicMock(return_value={
        "id": id, "name": name, "cron_expression": cron,
        "is_active": is_active, "group_id": group_id,
    })
    return s


def _make_group_context(group_id="grp-1", email="user@example.com"):
    gc = MagicMock()
    gc.primary_group_id = group_id
    gc.group_email = email
    return gc


class TestSchedulerServiceInit:
    """Test SchedulerService initialization."""

    def test_init_basic(self):
        mock_session = Mock()
        service = SchedulerService(mock_session)
        assert service.session == mock_session
        assert hasattr(service, 'repository')
        assert hasattr(service, 'execution_history_repository')
        assert hasattr(service, '_running_tasks')
        assert isinstance(service._running_tasks, set)
        assert len(service._running_tasks) == 0

    def test_init_creates_repositories(self):
        mock_session = Mock()
        with patch('src.services.scheduler_service.ScheduleRepository') as mock_schedule_repo:
            with patch('src.services.scheduler_service.ExecutionHistoryRepository') as mock_exec_repo:
                mock_schedule_repo.return_value = Mock()
                mock_exec_repo.return_value = Mock()
                service = SchedulerService(mock_session)
                mock_schedule_repo.assert_called_once_with(mock_session)
                mock_exec_repo.assert_called_once_with(mock_session)


class TestSchedulerServiceShutdown:
    """Test shutdown method."""

    @pytest.mark.asyncio
    async def test_shutdown_empty_tasks(self):
        service = _make_service()
        await service.shutdown()
        assert len(service._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_shutdown_with_running_tasks(self):
        service = _make_service()
        mock_task1 = Mock()
        mock_task1.cancel = Mock()
        mock_task1.done.return_value = False

        mock_task2 = Mock()
        mock_task2.cancel = Mock()
        mock_task2.done.return_value = False

        service._running_tasks.add(mock_task1)
        service._running_tasks.add(mock_task2)

        await service.shutdown()
        mock_task1.cancel.assert_called_once()
        mock_task2.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_with_done_task(self):
        # Verify shutdown handles tasks regardless of done() status
        service = _make_service()
        mock_task = Mock()
        mock_task.done.return_value = True
        mock_task.cancel = Mock()
        service._running_tasks.add(mock_task)

        # Should not raise even if task is done
        await service.shutdown()


class TestCreateSchedule:
    """Tests for create_schedule."""

    @pytest.mark.asyncio
    async def test_create_schedule_success(self):
        from src.schemas.schedule import ScheduleCreate, ScheduleResponse

        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.cron_expression = "0 * * * *"
        schedule_data.model_dump = MagicMock(return_value={
            "name": "Test", "cron_expression": "0 * * * *"
        })

        mock_schedule = MagicMock()
        service.repository.create = AsyncMock(return_value=mock_schedule)

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
                result = await service.create_schedule(schedule_data)

        service.repository.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schedule_invalid_cron_raises_bad_request(self):
        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.cron_expression = "invalid"

        with patch('src.services.scheduler_service.calculate_next_run_from_last', side_effect=ValueError("bad cron")):
            with pytest.raises(BadRequestError):
                await service.create_schedule(schedule_data)

    @pytest.mark.asyncio
    async def test_create_schedule_db_error_raises_kasal_error(self):
        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.cron_expression = "0 * * * *"
        schedule_data.model_dump = MagicMock(return_value={"name": "T", "cron_expression": "0 * * * *"})
        service.repository.create = AsyncMock(side_effect=RuntimeError("db error"))

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with pytest.raises(KasalError):
                await service.create_schedule(schedule_data)

    @pytest.mark.asyncio
    async def test_create_schedule_with_group_context(self):
        service = _make_service()
        gc = _make_group_context("grp-42", "test@example.com")
        schedule_data = MagicMock()
        schedule_data.cron_expression = "0 * * * *"
        schedule_data.model_dump = MagicMock(return_value={
            "name": "T", "cron_expression": "0 * * * *"
        })

        mock_schedule = MagicMock()
        service.repository.create = AsyncMock(return_value=mock_schedule)

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
                await service.create_schedule(schedule_data, group_context=gc)

        # Verify group_id was passed to create
        created_dict = service.repository.create.call_args[0][0]
        assert created_dict.get("group_id") == "grp-42"


class TestGetAllSchedules:
    """Tests for get_all_schedules."""

    @pytest.mark.asyncio
    async def test_get_all_no_group_context(self):
        service = _make_service()
        service.repository.find_all = AsyncMock(return_value=[])

        with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
            result = await service.get_all_schedules()

        service.repository.find_all.assert_called_once()
        assert result.count == 0

    @pytest.mark.asyncio
    async def test_get_all_with_group_context(self):
        service = _make_service()
        gc = _make_group_context("grp-1")
        service.repository.find_by_group = AsyncMock(return_value=[])

        with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
            result = await service.get_all_schedules(group_context=gc)

        service.repository.find_by_group.assert_called_once_with("grp-1")

    @pytest.mark.asyncio
    async def test_returns_empty_count(self):
        service = _make_service()
        service.repository.find_all = AsyncMock(return_value=[])

        with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
            result = await service.get_all_schedules()

        assert result.count == 0


class TestGetScheduleById:
    """Tests for get_schedule_by_id."""

    @pytest.mark.asyncio
    async def test_returns_schedule_when_found(self):
        service = _make_service()
        mock_schedule = MagicMock()
        service.repository.find_by_id = AsyncMock(return_value=mock_schedule)

        with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
            result = await service.get_schedule_by_id(1)

        service.repository.find_by_id.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_raises_not_found_when_missing(self):
        service = _make_service()
        service.repository.find_by_id = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await service.get_schedule_by_id(999)


class TestGetScheduleByIdWithGroupCheck:
    """Tests for get_schedule_by_id_with_group_check."""

    @pytest.mark.asyncio
    async def test_returns_schedule_for_correct_group(self):
        service = _make_service()
        gc = _make_group_context("grp-1")
        mock_schedule = MagicMock()
        mock_schedule.group_id = "grp-1"
        service.repository.find_by_id = AsyncMock(return_value=mock_schedule)

        with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
            result = await service.get_schedule_by_id_with_group_check(1, gc)

    @pytest.mark.asyncio
    async def test_raises_not_found_for_different_group(self):
        service = _make_service()
        gc = _make_group_context("grp-A")
        mock_schedule = MagicMock()
        mock_schedule.group_id = "grp-B"
        service.repository.find_by_id = AsyncMock(return_value=mock_schedule)

        with pytest.raises(NotFoundError):
            await service.get_schedule_by_id_with_group_check(1, gc)

    @pytest.mark.asyncio
    async def test_raises_not_found_when_missing(self):
        service = _make_service()
        service.repository.find_by_id = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await service.get_schedule_by_id_with_group_check(99)


class TestUpdateSchedule:
    """Tests for update_schedule."""

    @pytest.mark.asyncio
    async def test_update_success(self):
        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.model_dump = MagicMock(return_value={"name": "New Name"})
        mock_schedule = MagicMock()
        service.repository.update = AsyncMock(return_value=mock_schedule)

        with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
            await service.update_schedule(1, schedule_data)

        service.repository.update.assert_called_once_with(1, {"name": "New Name"})

    @pytest.mark.asyncio
    async def test_update_not_found_raises_not_found_error(self):
        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.model_dump = MagicMock(return_value={})
        service.repository.update = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await service.update_schedule(99, schedule_data)

    @pytest.mark.asyncio
    async def test_update_value_error_raises_bad_request(self):
        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.model_dump = MagicMock(return_value={})
        service.repository.update = AsyncMock(side_effect=ValueError("bad cron"))

        with pytest.raises(BadRequestError):
            await service.update_schedule(1, schedule_data)

    @pytest.mark.asyncio
    async def test_update_generic_error_raises_kasal_error(self):
        service = _make_service()
        schedule_data = MagicMock()
        schedule_data.model_dump = MagicMock(return_value={})
        service.repository.update = AsyncMock(side_effect=RuntimeError("db error"))

        with pytest.raises(KasalError):
            await service.update_schedule(1, schedule_data)


class TestDeleteSchedule:
    """Tests for delete_schedule."""

    @pytest.mark.asyncio
    async def test_delete_success(self):
        service = _make_service()
        service.repository.delete = AsyncMock(return_value=True)

        result = await service.delete_schedule(1)
        assert "deleted" in result.get("message", "").lower()

    @pytest.mark.asyncio
    async def test_delete_not_found_raises(self):
        service = _make_service()
        service.repository.delete = AsyncMock(return_value=False)

        with pytest.raises(NotFoundError):
            await service.delete_schedule(99)

    @pytest.mark.asyncio
    async def test_delete_generic_error_raises_kasal_error(self):
        service = _make_service()
        service.repository.delete = AsyncMock(side_effect=RuntimeError("db error"))

        with pytest.raises(KasalError):
            await service.delete_schedule(1)


class TestDeleteScheduleWithGroupCheck:
    """Tests for delete_schedule_with_group_check."""

    @pytest.mark.asyncio
    async def test_delete_success_correct_group(self):
        service = _make_service()
        gc = _make_group_context("grp-1")
        mock_schedule = MagicMock()
        mock_schedule.group_id = "grp-1"
        service.repository.find_by_id = AsyncMock(return_value=mock_schedule)
        service.repository.delete = AsyncMock(return_value=True)

        result = await service.delete_schedule_with_group_check(1, gc)
        assert "deleted" in result.get("message", "").lower()

    @pytest.mark.asyncio
    async def test_delete_wrong_group_raises_not_found(self):
        service = _make_service()
        gc = _make_group_context("grp-A")
        mock_schedule = MagicMock()
        mock_schedule.group_id = "grp-B"
        service.repository.find_by_id = AsyncMock(return_value=mock_schedule)

        with pytest.raises(NotFoundError):
            await service.delete_schedule_with_group_check(1, gc)

    @pytest.mark.asyncio
    async def test_delete_not_found_raises(self):
        service = _make_service()
        service.repository.find_by_id = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await service.delete_schedule_with_group_check(99)


class TestToggleSchedule:
    """Tests for toggle_schedule."""

    @pytest.mark.asyncio
    async def test_toggle_success(self):
        service = _make_service()
        mock_schedule = MagicMock()
        service.repository.toggle_active = AsyncMock(return_value=mock_schedule)

        with patch('src.schemas.schedule.ToggleResponse.model_validate', return_value=MagicMock()):
            await service.toggle_schedule(1)

    @pytest.mark.asyncio
    async def test_toggle_not_found_raises(self):
        service = _make_service()
        service.repository.toggle_active = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await service.toggle_schedule(99)

    @pytest.mark.asyncio
    async def test_toggle_generic_error_raises_kasal_error(self):
        service = _make_service()
        service.repository.toggle_active = AsyncMock(side_effect=RuntimeError("db error"))

        with pytest.raises(KasalError):
            await service.toggle_schedule(1)


class TestCreateScheduleFromExecution:
    """Tests for create_schedule_from_execution."""

    @pytest.mark.asyncio
    async def test_raises_not_found_when_execution_missing(self):
        from src.schemas.schedule import ScheduleCreateFromExecution
        service = _make_service()
        service.execution_history_repository.find_by_id = AsyncMock(return_value=None)

        schedule_data = MagicMock()
        schedule_data.execution_id = "exec-1"
        schedule_data.cron_expression = "0 * * * *"

        with pytest.raises(NotFoundError):
            await service.create_schedule_from_execution(schedule_data)

    @pytest.mark.asyncio
    async def test_crew_execution_missing_yaml_raises_bad_request(self):
        service = _make_service()
        mock_execution = MagicMock()
        mock_execution.execution_type = "crew"
        mock_execution.inputs = {
            "agents_yaml": {},
            "tasks_yaml": {},
        }
        service.execution_history_repository.find_by_id = AsyncMock(return_value=mock_execution)

        schedule_data = MagicMock()
        schedule_data.execution_id = "exec-1"
        schedule_data.cron_expression = "0 * * * *"
        schedule_data.name = "Test"
        schedule_data.is_active = True

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with pytest.raises(BadRequestError):
                await service.create_schedule_from_execution(schedule_data)

    @pytest.mark.asyncio
    async def test_flow_execution_missing_config_raises_bad_request(self):
        service = _make_service()
        mock_execution = MagicMock()
        mock_execution.execution_type = "flow"
        mock_execution.flow_id = None
        mock_execution.inputs = {
            "execution_type": "flow",
            "nodes": [],
            "edges": [],
        }
        service.execution_history_repository.find_by_id = AsyncMock(return_value=mock_execution)

        schedule_data = MagicMock()
        schedule_data.execution_id = "exec-flow-1"
        schedule_data.cron_expression = "0 * * * *"
        schedule_data.name = "Flow Schedule"
        schedule_data.is_active = True

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with pytest.raises(BadRequestError):
                await service.create_schedule_from_execution(schedule_data)

    @pytest.mark.asyncio
    async def test_crew_execution_success(self):
        service = _make_service()
        mock_execution = MagicMock()
        mock_execution.execution_type = "crew"
        mock_execution.inputs = {
            "agents_yaml": {"agent1": {"role": "R", "llm": "gpt-4"}},
            "tasks_yaml": {"task1": {"description": "D"}},
            "inputs": {},
        }
        service.execution_history_repository.find_by_id = AsyncMock(return_value=mock_execution)
        service.repository.create = AsyncMock(return_value=MagicMock())

        schedule_data = MagicMock()
        schedule_data.execution_id = "exec-1"
        schedule_data.cron_expression = "0 * * * *"
        schedule_data.name = "Crew Sched"
        schedule_data.is_active = True

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
                result = await service.create_schedule_from_execution(schedule_data)

        service.repository.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_flow_execution_success(self):
        service = _make_service()
        mock_execution = MagicMock()
        mock_execution.execution_type = "flow"
        mock_execution.flow_id = "flow-abc"
        mock_execution.inputs = {
            "execution_type": "flow",
            "flow_id": "flow-abc",
            "nodes": [{"id": "n1"}],
            "edges": [{"id": "e1"}],
            "inputs": {},
        }
        service.execution_history_repository.find_by_id = AsyncMock(return_value=mock_execution)
        service.repository.create = AsyncMock(return_value=MagicMock())

        schedule_data = MagicMock()
        schedule_data.execution_id = "exec-flow"
        schedule_data.cron_expression = "0 0 * * *"
        schedule_data.name = "Flow Sched"
        schedule_data.is_active = True

        with patch('src.services.scheduler_service.calculate_next_run_from_last', return_value=datetime.now(timezone.utc)):
            with patch('src.schemas.schedule.ScheduleResponse.model_validate', return_value=MagicMock()):
                result = await service.create_schedule_from_execution(schedule_data)

        service.repository.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_cron_raises_bad_request(self):
        service = _make_service()
        mock_execution = MagicMock()
        mock_execution.execution_type = "crew"
        mock_execution.inputs = {
            "agents_yaml": {"a": {}},
            "tasks_yaml": {"t": {}},
        }
        service.execution_history_repository.find_by_id = AsyncMock(return_value=mock_execution)

        schedule_data = MagicMock()
        schedule_data.execution_id = "exec-1"
        schedule_data.cron_expression = "invalid-cron"
        schedule_data.name = "T"
        schedule_data.is_active = True

        with patch('src.services.scheduler_service.calculate_next_run_from_last', side_effect=ValueError("bad cron")):
            with pytest.raises(BadRequestError):
                await service.create_schedule_from_execution(schedule_data)
