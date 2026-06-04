"""
Unit tests for TaskTrackingRepository.

Tests task execution tracking, status creation/update, error trace recording,
and job-level status aggregation.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.task_tracking_repository import TaskTrackingRepository
from src.models.execution_history import ExecutionHistory, TaskStatus as DBTaskStatus, ErrorTrace
from src.schemas.task_tracking import TaskStatusEnum, TaskStatusCreate, TaskStatusUpdate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(job_id="job-1", status="running"):
    job = MagicMock(spec=ExecutionHistory)
    job.job_id = job_id
    job.status = status
    return job


def _make_task_status(
    id=1,
    job_id="job-1",
    task_id="task-1",
    status="running",
    agent_name="Agent A",
    started_at=None,
    completed_at=None,
):
    ts = MagicMock(spec=DBTaskStatus)
    ts.id = id
    ts.job_id = job_id
    ts.task_id = task_id
    ts.status = status
    ts.agent_name = agent_name
    ts.started_at = started_at or datetime.utcnow()
    ts.completed_at = completed_at
    return ts


def _make_error_trace(id=1, run_id=1, task_key="task-1", error_type="ValueError", error_message="oops"):
    et = MagicMock(spec=ErrorTrace)
    et.id = id
    et.run_id = run_id
    et.task_key = task_key
    et.error_type = error_type
    et.error_message = error_message
    et.timestamp = datetime.now(timezone.utc)
    et.error_metadata = {}
    return et


def _scalar_result(item):
    scalars = MagicMock()
    scalars.first.return_value = item
    scalars.all.return_value = [item] if item is not None else []
    result = MagicMock()
    result.scalars.return_value = scalars
    return result


def _scalar_list_result(items):
    scalars = MagicMock()
    scalars.all.return_value = items
    scalars.first.return_value = items[0] if items else None
    result = MagicMock()
    result.scalars.return_value = scalars
    return result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    return session


@pytest.fixture
def repo(mock_session):
    return TaskTrackingRepository(db=mock_session)


# ---------------------------------------------------------------------------
# find_job_by_id
# ---------------------------------------------------------------------------

class TestFindJobById:
    @pytest.mark.asyncio
    async def test_returns_job_when_found(self, repo, mock_session):
        job = _make_job("job-42")
        mock_session.execute.return_value = _scalar_result(job)

        result = await repo.find_job_by_id("job-42")

        assert result is job
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_result(None)

        result = await repo.find_job_by_id("nonexistent")

        assert result is None


# ---------------------------------------------------------------------------
# find_task_statuses_by_job_id
# ---------------------------------------------------------------------------

class TestFindTaskStatusesByJobId:
    @pytest.mark.asyncio
    async def test_returns_ordered_list(self, repo, mock_session):
        ts1 = _make_task_status(id=1, task_id="t1")
        ts2 = _make_task_status(id=2, task_id="t2")
        mock_session.execute.return_value = _scalar_list_result([ts1, ts2])

        result = await repo.find_task_statuses_by_job_id("job-1")

        assert len(result) == 2
        assert result[0].task_id == "t1"

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_tasks(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_list_result([])

        result = await repo.find_task_statuses_by_job_id("job-1")

        assert result == []


# ---------------------------------------------------------------------------
# get_job_execution_status
# ---------------------------------------------------------------------------

class TestGetJobExecutionStatus:
    @pytest.mark.asyncio
    async def test_returns_dict_with_job_and_tasks(self, repo, mock_session):
        job = _make_job("job-1", status="completed")
        ts = _make_task_status(id=1, job_id="job-1", task_id="t1", status="completed")
        # first execute = find_job_by_id, second = find_task_statuses_by_job_id
        mock_session.execute.side_effect = [
            _scalar_result(job),
            _scalar_list_result([ts]),
        ]

        result = await repo.get_job_execution_status("job-1")

        assert result["job_id"] == "job-1"
        assert result["status"] == "completed"
        assert len(result["tasks"]) == 1
        assert result["tasks"][0]["task_id"] == "t1"

    @pytest.mark.asyncio
    async def test_raises_value_error_when_job_not_found(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_result(None)

        with pytest.raises(ValueError, match="Job not found"):
            await repo.get_job_execution_status("missing-job")

    @pytest.mark.asyncio
    async def test_task_fields_included_in_response(self, repo, mock_session):
        job = _make_job("job-1")
        ts = _make_task_status(
            id=5,
            job_id="job-1",
            task_id="t-special",
            status="completed",
            agent_name="AgentX",
        )
        mock_session.execute.side_effect = [
            _scalar_result(job),
            _scalar_list_result([ts]),
        ]

        result = await repo.get_job_execution_status("job-1")

        task = result["tasks"][0]
        assert task["id"] == 5
        assert task["agent_name"] == "AgentX"
        assert task["status"] == "completed"


# ---------------------------------------------------------------------------
# get_all_tasks
# ---------------------------------------------------------------------------

class TestGetAllTasks:
    @pytest.mark.asyncio
    async def test_returns_all_tasks(self, repo, mock_session):
        tasks = [_make_task_status(id=i) for i in range(3)]
        mock_session.execute.return_value = _scalar_list_result(tasks)

        result = await repo.get_all_tasks()

        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_empty_when_no_tasks(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_list_result([])

        result = await repo.get_all_tasks()

        assert result == []


# ---------------------------------------------------------------------------
# create_task
# ---------------------------------------------------------------------------

class TestCreateTask:
    @pytest.mark.asyncio
    async def test_creates_new_task(self, repo, mock_session):
        # No existing task
        mock_session.execute.return_value = _scalar_result(None)
        db_task = _make_task_status(id=10, job_id="job-1", task_id="t-new")
        mock_session.refresh.side_effect = lambda obj: None

        task_create = TaskStatusCreate(
            job_id="job-1",
            task_id="t-new",
            status=TaskStatusEnum.RUNNING,
            agent_name="Bot",
        )

        result = await repo.create_task(task_create)

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_existing_task_without_creating(self, repo, mock_session):
        existing = _make_task_status(id=7, job_id="job-1", task_id="t-exist")
        mock_session.execute.return_value = _scalar_result(existing)

        task_create = TaskStatusCreate(
            job_id="job-1",
            task_id="t-exist",
            status=TaskStatusEnum.RUNNING,
        )

        result = await repo.create_task(task_create)

        assert result is existing
        mock_session.add.assert_not_called()


# ---------------------------------------------------------------------------
# update_task
# ---------------------------------------------------------------------------

class TestUpdateTask:
    @pytest.mark.asyncio
    async def test_updates_status_field(self, repo, mock_session):
        db_task = _make_task_status(id=1, status="running")
        mock_session.execute.return_value = _scalar_result(db_task)

        task_update = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        result = await repo.update_task(1, task_update)

        assert db_task.status == TaskStatusEnum.COMPLETED
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_sets_completed_at_on_completed(self, repo, mock_session):
        db_task = _make_task_status(id=1, status="running")
        db_task.completed_at = None
        mock_session.execute.return_value = _scalar_result(db_task)

        task_update = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        await repo.update_task(1, task_update)

        assert db_task.completed_at is not None

    @pytest.mark.asyncio
    async def test_sets_completed_at_on_failed(self, repo, mock_session):
        db_task = _make_task_status(id=1, status="running")
        db_task.completed_at = None
        mock_session.execute.return_value = _scalar_result(db_task)

        task_update = TaskStatusUpdate(status=TaskStatusEnum.FAILED)
        await repo.update_task(1, task_update)

        assert db_task.completed_at is not None

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_result(None)

        task_update = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        result = await repo.update_task(999, task_update)

        assert result is None


# ---------------------------------------------------------------------------
# get_task_status / get_task_status_by_task_id
# ---------------------------------------------------------------------------

class TestGetTaskStatus:
    @pytest.mark.asyncio
    async def test_get_by_job_and_task_found(self, repo, mock_session):
        ts = _make_task_status()
        mock_session.execute.return_value = _scalar_result(ts)

        result = await repo.get_task_status("job-1", "task-1")

        assert result is ts

    @pytest.mark.asyncio
    async def test_get_by_task_id_only(self, repo, mock_session):
        ts = _make_task_status(task_id="t-solo")
        mock_session.execute.return_value = _scalar_result(ts)

        result = await repo.get_task_status_by_task_id("t-solo")

        assert result is ts

    @pytest.mark.asyncio
    async def test_get_task_status_not_found(self, repo, mock_session):
        mock_session.execute.return_value = _scalar_result(None)

        result = await repo.get_task_status("job-1", "no-task")

        assert result is None


# ---------------------------------------------------------------------------
# record_error_trace
# ---------------------------------------------------------------------------

class TestRecordErrorTrace:
    @pytest.mark.asyncio
    async def test_creates_error_trace(self, repo, mock_session):
        et = _make_error_trace()
        mock_session.refresh.side_effect = lambda obj: None

        result = await repo.record_error_trace(
            run_id=1,
            task_key="task-1",
            error_type="ValueError",
            error_message="something went wrong",
            error_metadata={"extra": "info"},
        )

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_creates_error_trace_without_metadata(self, repo, mock_session):
        mock_session.refresh.side_effect = lambda obj: None

        await repo.record_error_trace(
            run_id=2,
            task_key="task-2",
            error_type="RuntimeError",
            error_message="crash",
        )

        call_arg = mock_session.add.call_args[0][0]
        assert call_arg.error_metadata == {}


# ---------------------------------------------------------------------------
# create_task_statuses_for_job
# ---------------------------------------------------------------------------

class TestCreateTaskStatusesForJob:
    @pytest.mark.asyncio
    async def test_creates_statuses_for_all_tasks(self, repo, mock_session):
        # Each create_task_status call → no existing → creates
        mock_session.execute.return_value = _scalar_result(None)
        mock_session.refresh.side_effect = lambda obj: None

        tasks_config = {
            "task_a": {"agent": "AgentA"},
            "task_b": {"agent": "AgentB"},
        }

        result = await repo.create_task_statuses_for_job("job-99", tasks_config)

        # Two tasks → two DB adds (or more due to job-existence check)
        assert mock_session.add.call_count >= 2

    @pytest.mark.asyncio
    async def test_empty_tasks_config_returns_empty_list(self, repo, mock_session):
        result = await repo.create_task_statuses_for_job("job-99", {})

        assert result == []
