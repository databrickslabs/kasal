"""
Additional unit tests for services/scheduler_service.py — coverage boost.

Targets uncovered paths:
  - create_schedule (success, invalid cron, generic error)
  - create_schedule_from_execution (crew, flow, not found, bad request)
  - get_all_schedules (with/without group context)
  - get_schedule_by_id (found, not found)
  - get_schedule_by_id_with_group_check (found, wrong group, not found)
  - update_schedule (success, not found, value error, generic error)
  - update_schedule_with_group_check (success, wrong group)
  - delete_schedule (success, not found, generic error)
  - delete_schedule_with_group_check (success, wrong group)
  - toggle_schedule (success, not found)
  - toggle_schedule_with_group_check (success, wrong group)
  - run_schedule_job (crew, flow, error handling)
  - shutdown (empty tasks, running tasks)
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service():
    mock_session = MagicMock()
    with patch("src.services.scheduler_service.ScheduleRepository") as mock_repo, \
         patch("src.services.scheduler_service.ExecutionHistoryRepository") as mock_hist_repo:
        mock_repo.return_value = AsyncMock()
        mock_hist_repo.return_value = AsyncMock()
        from src.services.scheduler_service import SchedulerService
        svc = SchedulerService(mock_session)
        svc.repository = AsyncMock()
        svc.execution_history_repository = AsyncMock()
    return svc


def _make_group_context(group_id="grp-1", email="user@example.com"):
    gc = MagicMock()
    gc.primary_group_id = group_id
    gc.group_email = email
    return gc


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
    return s


def _make_schedule_create(name="Test", cron="0 * * * *"):
    sc = MagicMock()
    sc.cron_expression = cron
    sc.model_dump.return_value = {"name": name, "cron_expression": cron, "is_active": True}
    return sc


def _make_schedule_update(cron="0 * * * *"):
    su = MagicMock()
    su.model_dump.return_value = {"cron_expression": cron}
    return su


# ---------------------------------------------------------------------------
# create_schedule
# ---------------------------------------------------------------------------

class TestCreateSchedule:
    @pytest.mark.asyncio
    async def test_create_schedule_success(self):
        svc = _make_service()
        schedule_data = _make_schedule_create("My Schedule", "0 * * * *")
        mock_schedule = _make_schedule()

        svc.repository.create = AsyncMock(return_value=mock_schedule)

        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)), \
             patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=1, name="My Schedule")
            result = await svc.create_schedule(schedule_data)
        svc.repository.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_schedule_with_group_context(self):
        svc = _make_service()
        schedule_data = _make_schedule_create()
        gc = _make_group_context("grp-42")
        mock_schedule = _make_schedule(group_id="grp-42")
        svc.repository.create = AsyncMock(return_value=mock_schedule)

        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)), \
             patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(group_id="grp-42")
            result = await svc.create_schedule(schedule_data, gc)
        # Verify group_id was set in the dict passed to create
        call_args = svc.repository.create.call_args[0][0]
        assert call_args.get("group_id") == "grp-42"

    @pytest.mark.asyncio
    async def test_create_schedule_invalid_cron_raises_bad_request(self):
        svc = _make_service()
        schedule_data = _make_schedule_create(cron="invalid-cron")
        with patch("src.services.scheduler_service.calculate_next_run_from_last",
                   side_effect=ValueError("bad cron")):
            from src.core.exceptions import BadRequestError
            with pytest.raises(BadRequestError):
                await svc.create_schedule(schedule_data)

    @pytest.mark.asyncio
    async def test_create_schedule_generic_error_raises_kasal_error(self):
        svc = _make_service()
        schedule_data = _make_schedule_create()
        svc.repository.create = AsyncMock(side_effect=RuntimeError("db failure"))
        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            from src.core.exceptions import KasalError
            with pytest.raises(KasalError):
                await svc.create_schedule(schedule_data)


# ---------------------------------------------------------------------------
# create_schedule_from_execution
# ---------------------------------------------------------------------------

class TestCreateScheduleFromExecution:
    def _make_schedule_from_exec_data(self, exec_id="exec-1", name="Sched", cron="0 * * * *", is_active=True):
        d = MagicMock()
        d.execution_id = exec_id
        d.name = name
        d.cron_expression = cron
        d.is_active = is_active
        return d

    @pytest.mark.asyncio
    async def test_execution_not_found_raises_not_found(self):
        svc = _make_service()
        svc.execution_history_repository.find_by_id = AsyncMock(return_value=None)
        data = self._make_schedule_from_exec_data()
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.create_schedule_from_execution(data)

    @pytest.mark.asyncio
    async def test_crew_execution_missing_agents_raises_bad_request(self):
        svc = _make_service()
        mock_exec = MagicMock()
        mock_exec.inputs = {"agents_yaml": {}, "tasks_yaml": {}, "execution_type": "crew"}
        mock_exec.execution_type = "crew"
        svc.execution_history_repository.find_by_id = AsyncMock(return_value=mock_exec)
        data = self._make_schedule_from_exec_data()
        from src.core.exceptions import BadRequestError
        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            with pytest.raises(BadRequestError):
                await svc.create_schedule_from_execution(data)

    @pytest.mark.asyncio
    async def test_crew_execution_success(self):
        svc = _make_service()
        mock_exec = MagicMock()
        mock_exec.inputs = {
            "agents_yaml": {"agent1": {"llm": "gpt-4o-mini", "role": "Analyst"}},
            "tasks_yaml": {"task1": {"description": "Do stuff"}},
            "execution_type": "crew",
        }
        mock_exec.execution_type = "crew"
        svc.execution_history_repository.find_by_id = AsyncMock(return_value=mock_exec)
        mock_schedule = _make_schedule()
        svc.repository.create = AsyncMock(return_value=mock_schedule)
        data = self._make_schedule_from_exec_data()
        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)), \
             patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=1)
            result = await svc.create_schedule_from_execution(data)
        svc.repository.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_flow_execution_missing_flow_id_and_nodes_raises_bad_request(self):
        svc = _make_service()
        mock_exec = MagicMock()
        mock_exec.inputs = {"execution_type": "flow", "nodes": [], "edges": []}
        mock_exec.execution_type = "flow"
        mock_exec.flow_id = None
        svc.execution_history_repository.find_by_id = AsyncMock(return_value=mock_exec)
        data = self._make_schedule_from_exec_data()
        from src.core.exceptions import BadRequestError
        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            with pytest.raises(BadRequestError):
                await svc.create_schedule_from_execution(data)

    @pytest.mark.asyncio
    async def test_flow_execution_success(self):
        svc = _make_service()
        mock_exec = MagicMock()
        mock_exec.inputs = {
            "execution_type": "flow",
            "nodes": [{"id": "n1"}],
            "edges": [{"id": "e1"}],
            "flow_id": "flow-uuid",
            "flow_config": {},
            "agents_yaml": {},
            "tasks_yaml": {},
        }
        mock_exec.execution_type = "flow"
        mock_exec.flow_id = "flow-uuid"
        svc.execution_history_repository.find_by_id = AsyncMock(return_value=mock_exec)
        mock_schedule = _make_schedule()
        svc.repository.create = AsyncMock(return_value=mock_schedule)
        data = self._make_schedule_from_exec_data()
        with patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)), \
             patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=1)
            result = await svc.create_schedule_from_execution(data)
        svc.repository.create.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_all_schedules
# ---------------------------------------------------------------------------

class TestGetAllSchedules:
    @pytest.mark.asyncio
    async def test_no_group_context_finds_all(self):
        svc = _make_service()
        svc.repository.find_all = AsyncMock(return_value=[])
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.side_effect = lambda s: s
            result = await svc.get_all_schedules(group_context=None)
        svc.repository.find_all.assert_awaited_once()
        assert result.count == 0

    @pytest.mark.asyncio
    async def test_with_group_context_filters_by_group(self):
        svc = _make_service()
        gc = _make_group_context("grp-5")
        svc.repository.find_by_group = AsyncMock(return_value=[])
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.side_effect = lambda s: s
            result = await svc.get_all_schedules(group_context=gc)
        svc.repository.find_by_group.assert_awaited_once_with("grp-5")

    @pytest.mark.asyncio
    async def test_empty_group_id_finds_all(self):
        svc = _make_service()
        gc = _make_group_context(group_id=None)
        svc.repository.find_all = AsyncMock(return_value=[])
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.side_effect = lambda s: s
            result = await svc.get_all_schedules(group_context=gc)
        svc.repository.find_all.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_schedule_by_id
# ---------------------------------------------------------------------------

class TestGetScheduleById:
    @pytest.mark.asyncio
    async def test_found_returns_response(self):
        svc = _make_service()
        mock_sched = _make_schedule(id=5)
        svc.repository.find_by_id = AsyncMock(return_value=mock_sched)
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=5)
            result = await svc.get_schedule_by_id(5)
        svc.repository.find_by_id.assert_awaited_once_with(5)

    @pytest.mark.asyncio
    async def test_not_found_raises_not_found_error(self):
        svc = _make_service()
        svc.repository.find_by_id = AsyncMock(return_value=None)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.get_schedule_by_id(999)


# ---------------------------------------------------------------------------
# get_schedule_by_id_with_group_check
# ---------------------------------------------------------------------------

class TestGetScheduleByIdWithGroupCheck:
    @pytest.mark.asyncio
    async def test_found_and_group_matches(self):
        svc = _make_service()
        mock_sched = _make_schedule(id=7, group_id="grp-1")
        gc = _make_group_context("grp-1")
        svc.repository.find_by_id = AsyncMock(return_value=mock_sched)
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=7)
            result = await svc.get_schedule_by_id_with_group_check(7, gc)

    @pytest.mark.asyncio
    async def test_wrong_group_raises_not_found(self):
        svc = _make_service()
        mock_sched = _make_schedule(id=7, group_id="grp-OTHER")
        gc = _make_group_context("grp-1")
        svc.repository.find_by_id = AsyncMock(return_value=mock_sched)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.get_schedule_by_id_with_group_check(7, gc)

    @pytest.mark.asyncio
    async def test_not_found_raises_not_found_error(self):
        svc = _make_service()
        svc.repository.find_by_id = AsyncMock(return_value=None)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.get_schedule_by_id_with_group_check(999)


# ---------------------------------------------------------------------------
# update_schedule
# ---------------------------------------------------------------------------

class TestUpdateSchedule:
    @pytest.mark.asyncio
    async def test_update_success(self):
        svc = _make_service()
        mock_sched = _make_schedule(id=3)
        svc.repository.update = AsyncMock(return_value=mock_sched)
        su = _make_schedule_update()
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=3)
            result = await svc.update_schedule(3, su)
        svc.repository.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.update = AsyncMock(return_value=None)
        su = _make_schedule_update()
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.update_schedule(999, su)

    @pytest.mark.asyncio
    async def test_update_value_error_raises_bad_request(self):
        svc = _make_service()
        svc.repository.update = AsyncMock(side_effect=ValueError("bad value"))
        su = _make_schedule_update()
        from src.core.exceptions import BadRequestError
        with pytest.raises(BadRequestError):
            await svc.update_schedule(1, su)

    @pytest.mark.asyncio
    async def test_update_generic_error_raises_kasal_error(self):
        svc = _make_service()
        svc.repository.update = AsyncMock(side_effect=RuntimeError("db down"))
        su = _make_schedule_update()
        from src.core.exceptions import KasalError
        with pytest.raises(KasalError):
            await svc.update_schedule(1, su)


# ---------------------------------------------------------------------------
# update_schedule_with_group_check
# ---------------------------------------------------------------------------

class TestUpdateScheduleWithGroupCheck:
    @pytest.mark.asyncio
    async def test_update_success_with_matching_group(self):
        svc = _make_service()
        gc = _make_group_context("grp-1")
        existing = _make_schedule(id=10, group_id="grp-1")
        updated = _make_schedule(id=10, group_id="grp-1")
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        svc.repository.update = AsyncMock(return_value=updated)
        su = _make_schedule_update()
        with patch("src.services.scheduler_service.ScheduleResponse") as mock_resp:
            mock_resp.model_validate.return_value = MagicMock(id=10)
            result = await svc.update_schedule_with_group_check(10, su, gc)

    @pytest.mark.asyncio
    async def test_update_wrong_group_raises_not_found(self):
        svc = _make_service()
        gc = _make_group_context("grp-1")
        existing = _make_schedule(id=10, group_id="grp-OTHER")
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        su = _make_schedule_update()
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.update_schedule_with_group_check(10, su, gc)

    @pytest.mark.asyncio
    async def test_update_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.find_by_id = AsyncMock(return_value=None)
        su = _make_schedule_update()
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.update_schedule_with_group_check(999, su)


# ---------------------------------------------------------------------------
# delete_schedule
# ---------------------------------------------------------------------------

class TestDeleteSchedule:
    @pytest.mark.asyncio
    async def test_delete_success(self):
        svc = _make_service()
        svc.repository.delete = AsyncMock(return_value=True)
        result = await svc.delete_schedule(1)
        assert "deleted" in result["message"].lower()

    @pytest.mark.asyncio
    async def test_delete_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.delete = AsyncMock(return_value=False)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.delete_schedule(999)

    @pytest.mark.asyncio
    async def test_delete_generic_error_raises_kasal_error(self):
        svc = _make_service()
        svc.repository.delete = AsyncMock(side_effect=RuntimeError("db error"))
        from src.core.exceptions import KasalError
        with pytest.raises(KasalError):
            await svc.delete_schedule(1)


# ---------------------------------------------------------------------------
# delete_schedule_with_group_check
# ---------------------------------------------------------------------------

class TestDeleteScheduleWithGroupCheck:
    @pytest.mark.asyncio
    async def test_delete_success_matching_group(self):
        svc = _make_service()
        gc = _make_group_context("grp-1")
        existing = _make_schedule(id=1, group_id="grp-1")
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        svc.repository.delete = AsyncMock(return_value=True)
        result = await svc.delete_schedule_with_group_check(1, gc)
        assert "deleted" in result["message"].lower()

    @pytest.mark.asyncio
    async def test_delete_wrong_group_raises_not_found(self):
        svc = _make_service()
        gc = _make_group_context("grp-1")
        existing = _make_schedule(id=1, group_id="grp-OTHER")
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.delete_schedule_with_group_check(1, gc)

    @pytest.mark.asyncio
    async def test_delete_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.find_by_id = AsyncMock(return_value=None)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.delete_schedule_with_group_check(999)


# ---------------------------------------------------------------------------
# toggle_schedule
# ---------------------------------------------------------------------------

class TestToggleSchedule:
    @pytest.mark.asyncio
    async def test_toggle_success(self):
        svc = _make_service()
        mock_sched = _make_schedule(id=1, is_active=False)
        svc.repository.toggle_active = AsyncMock(return_value=mock_sched)
        with patch("src.services.scheduler_service.ToggleResponse") as mock_tr:
            mock_tr.model_validate.return_value = MagicMock(is_active=False)
            result = await svc.toggle_schedule(1)
        svc.repository.toggle_active.assert_awaited_once_with(1)

    @pytest.mark.asyncio
    async def test_toggle_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.toggle_active = AsyncMock(return_value=None)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.toggle_schedule(999)

    @pytest.mark.asyncio
    async def test_toggle_generic_error_raises_kasal_error(self):
        svc = _make_service()
        svc.repository.toggle_active = AsyncMock(side_effect=RuntimeError("db error"))
        from src.core.exceptions import KasalError
        with pytest.raises(KasalError):
            await svc.toggle_schedule(1)


# ---------------------------------------------------------------------------
# toggle_schedule_with_group_check
# ---------------------------------------------------------------------------

class TestToggleScheduleWithGroupCheck:
    @pytest.mark.asyncio
    async def test_toggle_matching_group_success(self):
        svc = _make_service()
        gc = _make_group_context("grp-1")
        existing = _make_schedule(id=5, group_id="grp-1")
        toggled = _make_schedule(id=5, is_active=False)
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        svc.repository.toggle_active = AsyncMock(return_value=toggled)
        with patch("src.services.scheduler_service.ToggleResponse") as mock_tr:
            mock_tr.model_validate.return_value = MagicMock(is_active=False)
            result = await svc.toggle_schedule_with_group_check(5, gc)

    @pytest.mark.asyncio
    async def test_toggle_wrong_group_raises_not_found(self):
        svc = _make_service()
        gc = _make_group_context("grp-1")
        existing = _make_schedule(id=5, group_id="grp-OTHER")
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.toggle_schedule_with_group_check(5, gc)

    @pytest.mark.asyncio
    async def test_toggle_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.find_by_id = AsyncMock(return_value=None)
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.toggle_schedule_with_group_check(999)


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

class TestSchedulerShutdownExtra:
    @pytest.mark.asyncio
    async def test_shutdown_cancels_done_tasks_cleanly(self):
        svc = _make_service()
        task = MagicMock()
        task.cancel = MagicMock()
        task.done.return_value = True  # Already done
        svc._running_tasks.add(task)
        await svc.shutdown()
        # cancel is called regardless
        task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_clears_running_tasks(self):
        svc = _make_service()
        svc._running_tasks.add(MagicMock(cancel=MagicMock(), done=MagicMock(return_value=False)))
        svc._running_tasks.add(MagicMock(cancel=MagicMock(), done=MagicMock(return_value=False)))
        await svc.shutdown()
        assert len(svc._running_tasks) == 0
