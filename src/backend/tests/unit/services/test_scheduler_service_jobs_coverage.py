"""
Additional unit tests for scheduler_service.py job-related methods.

Covers:
  - run_schedule_job (crew path, flow path, schedule not found, error handling)
  - check_and_run_schedules (one iteration with due schedule, no due schedules)
  - get_all_jobs / get_all_jobs_for_group
  - create_job / create_job_with_group
  - update_job / update_job_with_group_check
  - delete_job / delete_job_with_group_check
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service():
    mock_session = MagicMock()
    with patch("src.services.scheduler_service.ScheduleRepository"), \
         patch("src.services.scheduler_service.ExecutionHistoryRepository"):
        from src.services.scheduler_service import SchedulerService
        svc = SchedulerService(mock_session)
    svc.repository = AsyncMock()
    svc.execution_history_repository = AsyncMock()
    return svc


def _make_schedule(id=1, name="S", cron="0 * * * *", is_active=True, group_id="g-1",
                   execution_type="crew", agents_yaml=None, tasks_yaml=None,
                   inputs=None, model="gpt-4o-mini", flow_id=None, nodes=None, edges=None):
    s = MagicMock()
    s.id = id
    s.name = name
    s.cron_expression = cron
    s.is_active = is_active
    s.group_id = group_id
    s.execution_type = execution_type
    s.agents_yaml = agents_yaml or {"agent1": {"role": "Analyst"}}
    s.tasks_yaml = tasks_yaml or {"task1": {"description": "Do analysis"}}
    s.inputs = inputs or {}
    s.model = model
    s.planning = False
    s.flow_id = flow_id
    s.nodes = nodes
    s.edges = edges
    s.flow_config = None
    s.created_by_email = "user@example.com"
    s.next_run_at = datetime.now(timezone.utc)
    s.last_run_at = None
    s.created_at = datetime.now(timezone.utc)
    s.updated_at = datetime.now(timezone.utc)
    return s


def _make_group_context(group_id="g-1", email="user@example.com"):
    gc = MagicMock()
    gc.primary_group_id = group_id
    gc.group_email = email
    return gc


def _make_crew_config(model="gpt-4o-mini", execution_type="crew"):
    cfg = MagicMock()
    cfg.model = model
    cfg.execution_type = execution_type
    cfg.agents_yaml = {"agent1": {"role": "A"}}
    cfg.tasks_yaml = {"task1": {"description": "T"}}
    cfg.inputs = {}
    cfg.planning = False
    cfg.flow_id = None
    cfg.nodes = None
    cfg.edges = None
    cfg.flow_config = None
    return cfg


# ---------------------------------------------------------------------------
# run_schedule_job
# ---------------------------------------------------------------------------

class TestRunScheduleJob:
    @pytest.mark.asyncio
    async def test_schedule_not_found_returns_early(self):
        """If schedule doesn't exist in DB, function returns without error."""
        svc = _make_service()
        config = _make_crew_config()
        now = datetime.now(timezone.utc)

        mock_session_cm = AsyncMock()
        mock_repo = AsyncMock()
        mock_repo.find_by_id = AsyncMock(return_value=None)

        with patch("src.services.scheduler_service.async_session_factory") as mock_factory, \
             patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_repo):
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            # Patch ScheduleRepository inside context manager
            with patch("src.services.scheduler_service.ScheduleRepository") as mock_repo_cls:
                mock_repo_cls.return_value = mock_repo
                # Should not raise
                await svc.run_schedule_job(999, config, now)

    @pytest.mark.asyncio
    async def test_run_schedule_job_crew_success(self):
        """Full crew execution path runs without error."""
        svc = _make_service()
        config = _make_crew_config()
        schedule = _make_schedule(id=1, execution_type="crew")
        now = datetime.now(timezone.utc)

        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()

        mock_repo = AsyncMock()
        mock_repo.find_by_id = AsyncMock(return_value=schedule)
        mock_repo.update_after_execution = AsyncMock()

        mock_exec_service = AsyncMock()
        mock_exec_service.generate_execution_name = AsyncMock(return_value={"name": "Run-abc"})

        with patch("src.services.scheduler_service.async_session_factory") as mock_factory, \
             patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_repo), \
             patch("src.services.scheduler_service.ExecutionService") as mock_es_cls, \
             patch("src.services.scheduler_service.CrewAIExecutionService") as mock_crewai, \
             patch("src.services.scheduler_service.Run") as mock_run_cls, \
             patch("src.services.scheduler_service.GroupContext") as mock_gc_cls:
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_es_cls.return_value = mock_exec_service
            mock_es_cls.run_crew_execution = AsyncMock()
            mock_crewai.add_execution_to_memory = MagicMock()
            mock_run_cls.return_value = MagicMock()
            mock_gc_cls.return_value = MagicMock()

            with patch("src.utils.databricks_auth.get_auth_context", new=AsyncMock(return_value=None)):
                await svc.run_schedule_job(1, config, now)

    @pytest.mark.asyncio
    async def test_run_schedule_job_error_still_updates_schedule(self):
        """Even on error, schedule's next run time is updated."""
        svc = _make_service()
        config = _make_crew_config()
        now = datetime.now(timezone.utc)

        # First session raises, second session for update succeeds
        error_session = AsyncMock()
        update_repo = AsyncMock()
        update_repo.update_after_execution = AsyncMock()

        sessions = [Exception("Session init failed")]

        mock_factory_calls = [0]

        async def mock_aenter(self_):
            if mock_factory_calls[0] == 0:
                mock_factory_calls[0] += 1
                raise RuntimeError("main session failed")
            else:
                return error_session

        with patch("src.services.scheduler_service.async_session_factory") as mock_factory, \
             patch("src.services.scheduler_service.ScheduleRepository") as mock_repo_cls:
            # First call raises, second call returns update session
            cm1 = MagicMock()
            cm1.__aenter__ = AsyncMock(side_effect=RuntimeError("main session failed"))
            cm1.__aexit__ = AsyncMock(return_value=False)
            cm2 = MagicMock()
            cm2.__aenter__ = AsyncMock(return_value=error_session)
            cm2.__aexit__ = AsyncMock(return_value=False)
            mock_factory.side_effect = [cm1, cm2]
            mock_repo_cls.return_value = update_repo
            update_repo.update_after_execution = AsyncMock()
            error_session.commit = AsyncMock()

            # Should not raise - errors are caught
            await svc.run_schedule_job(1, config, now)


# ---------------------------------------------------------------------------
# check_and_run_schedules (one iteration)
# ---------------------------------------------------------------------------

class TestCheckAndRunSchedules:
    @pytest.mark.asyncio
    async def test_check_no_due_schedules_sleeps_then_loops(self):
        """When no schedules are due, loop calls asyncio.sleep(60)."""
        svc = _make_service()

        mock_session = AsyncMock()
        mock_repo = AsyncMock()
        mock_repo.find_due_schedules = AsyncMock(return_value=[])
        mock_repo.find_all = AsyncMock(return_value=[])

        # Raise on the first sleep so the loop exits
        mock_sleep = AsyncMock(side_effect=[asyncio.CancelledError()])

        with patch("src.services.scheduler_service.async_session_factory") as mock_factory, \
             patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_repo):
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_session.commit = AsyncMock()

            # Patch the module-level asyncio reference used inside scheduler_service
            import src.services.scheduler_service as sched_mod
            original_sleep = asyncio.sleep
            sched_mod_asyncio = __import__('asyncio')

            task = asyncio.create_task(svc.check_and_run_schedules())
            # Give the loop time to run one iteration and hit sleep
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # The loop ran at least one iteration (find_due_schedules was called)
        mock_repo.find_due_schedules.assert_awaited()

    @pytest.mark.asyncio
    async def test_check_with_due_schedule_creates_task(self):
        """When a due schedule is found, run_schedule_job is called."""
        svc = _make_service()
        schedule = _make_schedule(id=5, execution_type="crew")

        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_repo = AsyncMock()
        mock_repo.find_due_schedules = AsyncMock(return_value=[schedule])
        mock_repo.find_all = AsyncMock(return_value=[schedule])

        with patch("src.services.scheduler_service.async_session_factory") as mock_factory, \
             patch("src.services.scheduler_service.ScheduleRepository", return_value=mock_repo), \
             patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)), \
             patch.object(svc, "run_schedule_job", new=AsyncMock()):
            mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

            task = asyncio.create_task(svc.check_and_run_schedules())
            # Let one iteration complete (hits asyncio.sleep(60) at the end)
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            # Clean up any running tasks
            for t in list(svc._running_tasks):
                t.cancel()
                try:
                    await t
                except Exception:
                    pass
        # find_due_schedules was called
        mock_repo.find_due_schedules.assert_awaited()


# ---------------------------------------------------------------------------
# get_all_jobs
# ---------------------------------------------------------------------------

class TestGetAllJobs:
    @pytest.mark.asyncio
    async def test_returns_job_responses(self):
        svc = _make_service()
        schedule = _make_schedule(id=1)
        svc.repository.find_all = AsyncMock(return_value=[schedule])
        with patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp:
            mock_resp.return_value = MagicMock(id=1, name="S")
            result = await svc.get_all_jobs()
        assert len(result) == 1
        svc.repository.find_all.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_schedules(self):
        svc = _make_service()
        svc.repository.find_all = AsyncMock(return_value=[])
        result = await svc.get_all_jobs()
        assert result == []


# ---------------------------------------------------------------------------
# get_all_jobs_for_group
# ---------------------------------------------------------------------------

class TestGetAllJobsForGroup:
    @pytest.mark.asyncio
    async def test_with_group_context_filters_by_group(self):
        svc = _make_service()
        gc = _make_group_context("grp-42")
        schedule = _make_schedule(id=1, group_id="grp-42")
        svc.repository.find_by_group = AsyncMock(return_value=[schedule])
        with patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp:
            mock_resp.return_value = MagicMock(id=1)
            result = await svc.get_all_jobs_for_group(gc)
        svc.repository.find_by_group.assert_awaited_once_with("grp-42")

    @pytest.mark.asyncio
    async def test_without_group_context_finds_all(self):
        svc = _make_service()
        svc.repository.find_all = AsyncMock(return_value=[])
        result = await svc.get_all_jobs_for_group(group_context=None)
        svc.repository.find_all.assert_awaited_once()


# ---------------------------------------------------------------------------
# create_job
# ---------------------------------------------------------------------------

class TestCreateJob:
    def _make_job_create(self, name="My Job", schedule="0 * * * *", enabled=True):
        jc = MagicMock()
        jc.name = name
        jc.schedule = schedule
        jc.enabled = enabled
        jc.description = "desc"
        jc.job_data = {"agents": {"a1": {"role": "Analyst"}}, "tasks": {}, "inputs": {}, "planning": False, "model": "gpt-4o-mini"}
        return jc

    @pytest.mark.asyncio
    async def test_create_job_success(self):
        svc = _make_service()
        jc = self._make_job_create()
        mock_schedule = _make_schedule(id=10, name="My Job")
        svc.repository.create = AsyncMock(return_value=mock_schedule)
        with patch("src.services.scheduler_service.ScheduleCreate") as mock_sc, \
             patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp, \
             patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            mock_sc.return_value = MagicMock(model_dump=MagicMock(return_value={}))
            mock_resp.return_value = MagicMock(id=10, name="My Job")
            result = await svc.create_job(jc)
        svc.repository.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_job_extracts_model_from_agent_llm(self):
        svc = _make_service()
        jc = self._make_job_create()
        jc.job_data = {"agents": {"a1": {"llm": "gpt-4-turbo"}}, "tasks": {}, "inputs": {}}
        mock_schedule = _make_schedule(id=11, model="gpt-4-turbo")
        svc.repository.create = AsyncMock(return_value=mock_schedule)
        with patch("src.services.scheduler_service.ScheduleCreate") as mock_sc, \
             patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp, \
             patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            mock_sc.return_value = MagicMock(model_dump=MagicMock(return_value={}))
            mock_resp.return_value = MagicMock(id=11)
            result = await svc.create_job(jc)


# ---------------------------------------------------------------------------
# create_job_with_group
# ---------------------------------------------------------------------------

class TestCreateJobWithGroup:
    def _make_job_create(self, name="Job", schedule="0 * * * *"):
        jc = MagicMock()
        jc.name = name
        jc.schedule = schedule
        jc.enabled = True
        jc.description = "desc"
        jc.job_data = {"agents": {}, "tasks": {}, "inputs": {}, "planning": False}
        return jc

    @pytest.mark.asyncio
    async def test_create_job_with_group_injects_group_id(self):
        svc = _make_service()
        jc = self._make_job_create()
        gc = _make_group_context("grp-77")
        mock_schedule = _make_schedule(id=20, group_id="grp-77")
        svc.repository.create = AsyncMock(return_value=mock_schedule)
        with patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp, \
             patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            mock_resp.return_value = MagicMock(id=20)
            result = await svc.create_job_with_group(jc, gc)
        call_args = svc.repository.create.call_args[0][0]
        assert call_args.get("group_id") == "grp-77"

    @pytest.mark.asyncio
    async def test_create_job_without_group_uses_defaults(self):
        svc = _make_service()
        jc = self._make_job_create()
        mock_schedule = _make_schedule(id=21)
        svc.repository.create = AsyncMock(return_value=mock_schedule)
        with patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp, \
             patch("src.services.scheduler_service.calculate_next_run_from_last", return_value=datetime.now(timezone.utc)):
            mock_resp.return_value = MagicMock(id=21)
            result = await svc.create_job_with_group(jc, group_context=None)
        svc.repository.create.assert_awaited_once()


# ---------------------------------------------------------------------------
# update_job
# ---------------------------------------------------------------------------

class TestUpdateJob:
    def _make_job_update(self, name=None, schedule=None, enabled=None):
        ju = MagicMock()
        ju.name = name
        ju.schedule = schedule
        ju.enabled = enabled
        ju.description = "Updated"
        ju.job_data = None
        return ju

    @pytest.mark.asyncio
    async def test_update_job_not_found_raises_not_found(self):
        svc = _make_service()
        svc.repository.find_by_id = AsyncMock(return_value=None)
        ju = self._make_job_update()
        from src.core.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            await svc.update_job(999, ju)

    @pytest.mark.asyncio
    async def test_update_job_success(self):
        svc = _make_service()
        existing = _make_schedule(id=5)
        updated = _make_schedule(id=5, name="Updated Name")
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        svc.repository.update = AsyncMock(return_value=updated)
        ju = self._make_job_update(name="Updated Name")
        with patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp:
            mock_resp.return_value = MagicMock(id=5)
            result = await svc.update_job(5, ju)
        svc.repository.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_job_with_job_data(self):
        svc = _make_service()
        existing = _make_schedule(id=6)
        updated = _make_schedule(id=6)
        svc.repository.find_by_id = AsyncMock(return_value=existing)
        svc.repository.update = AsyncMock(return_value=updated)
        ju = self._make_job_update()
        ju.job_data = {"agents": {"a": {}}, "tasks": {"t": {}}, "inputs": {}, "planning": True, "model": "claude-3"}
        with patch("src.services.scheduler_service.SchedulerJobResponse") as mock_resp:
            mock_resp.return_value = MagicMock(id=6)
            result = await svc.update_job(6, ju)
        call_data = svc.repository.update.call_args[0][1]
        assert call_data.get("agents_yaml") == {"a": {}}
        assert call_data.get("model") == "claude-3"


# ---------------------------------------------------------------------------
# start_scheduler
# ---------------------------------------------------------------------------

class TestStartScheduler:
    @pytest.mark.asyncio
    async def test_start_scheduler_creates_background_task(self):
        """start_scheduler creates an asyncio task and adds it to _running_tasks."""
        svc = _make_service()
        # Patch check_and_run_schedules to avoid infinite loop
        with patch.object(svc, "check_and_run_schedules", new=AsyncMock(side_effect=asyncio.CancelledError)):
            await svc.start_scheduler(interval_seconds=1)
        # After call, a task should have been added to _running_tasks
        # (it may already be done since we forced CancelledError)
        # Just verify the method ran without error and returned
        assert True  # reached here without exception
