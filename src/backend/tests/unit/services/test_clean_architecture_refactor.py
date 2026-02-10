"""
Tests for clean architecture refactoring: new service methods that replace
direct DB queries previously in routers.

Tests cover:
- ExecutionService.get_execution_status_detail()
- ExecutionHistoryService.get_execution_groups_with_counts()
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# ExecutionService.get_execution_status_detail
# ---------------------------------------------------------------------------

class TestGetExecutionStatusDetail:
    """Tests for ExecutionService.get_execution_status_detail()."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        from src.services.execution_service import ExecutionService
        svc = ExecutionService.__new__(ExecutionService)
        svc.session = mock_session
        return svc

    @pytest.mark.asyncio
    async def test_returns_none_when_no_session(self):
        from src.services.execution_service import ExecutionService
        svc = ExecutionService.__new__(ExecutionService)
        svc.session = None
        result = await svc.get_execution_status_detail("exec-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_execution_not_found(self, service):
        mock_repo = AsyncMock()
        mock_repo.get_execution_by_job_id.return_value = None

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=mock_repo,
        ):
            result = await service.get_execution_status_detail("exec-missing", group_ids=["g1"])
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_status_for_completed_execution(self, service):
        execution = SimpleNamespace(
            status="COMPLETED",
            is_stopping=False,
            stopped_at=None,
            stop_reason=None,
        )
        mock_repo = AsyncMock()
        mock_repo.get_execution_by_job_id.return_value = execution

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=mock_repo,
        ):
            result = await service.get_execution_status_detail("exec-1", group_ids=["g1"])

        assert result is not None
        assert result["execution_id"] == "exec-1"
        assert result["status"] == "COMPLETED"
        assert result["is_stopping"] is False
        assert result["progress"] is None  # No progress for completed

    @pytest.mark.asyncio
    async def test_returns_progress_for_running_execution(self, service, mock_session):
        execution = SimpleNamespace(
            status="RUNNING",
            is_stopping=False,
            stopped_at=None,
            stop_reason=None,
        )
        mock_repo = AsyncMock()
        mock_repo.get_execution_by_job_id.return_value = execution

        # Mock task query
        task1 = SimpleNamespace(status="completed", task_id="t1")
        task2 = SimpleNamespace(status="running", task_id="t2")
        task3 = SimpleNamespace(status="pending", task_id="t3")
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [task1, task2, task3]
        mock_session.execute.return_value = mock_result

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=mock_repo,
        ):
            result = await service.get_execution_status_detail("exec-1", group_ids=["g1"])

        assert result is not None
        assert result["status"] == "RUNNING"
        assert result["progress"] is not None
        assert result["progress"]["total_tasks"] == 3
        assert result["progress"]["completed_tasks"] == 1
        assert result["progress"]["running_tasks"] == 1
        assert result["progress"]["current_task"] == "t2"

    @pytest.mark.asyncio
    async def test_returns_progress_none_when_no_tasks(self, service, mock_session):
        execution = SimpleNamespace(
            status="RUNNING",
            is_stopping=False,
            stopped_at=None,
            stop_reason=None,
        )
        mock_repo = AsyncMock()
        mock_repo.get_execution_by_job_id.return_value = execution

        # Mock empty task query
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=mock_repo,
        ):
            result = await service.get_execution_status_detail("exec-1")

        assert result["progress"] is None

    @pytest.mark.asyncio
    async def test_returns_stopping_info(self, service, mock_session):
        execution = SimpleNamespace(
            status="STOPPING",
            is_stopping=True,
            stopped_at="2026-01-01T00:00:00",
            stop_reason="User requested stop",
        )
        mock_repo = AsyncMock()
        mock_repo.get_execution_by_job_id.return_value = execution

        # Mock empty task query for STOPPING status
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=mock_repo,
        ):
            result = await service.get_execution_status_detail("exec-1", group_ids=["g1"])

        assert result["is_stopping"] is True
        assert result["stopped_at"] == "2026-01-01T00:00:00"
        assert result["stop_reason"] == "User requested stop"

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self, service, mock_session):
        mock_session.execute.side_effect = Exception("DB error")
        mock_repo = AsyncMock()
        mock_repo.get_execution_by_job_id.side_effect = Exception("DB error")

        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=mock_repo,
        ):
            result = await service.get_execution_status_detail("exec-1")
        assert result is None


# ---------------------------------------------------------------------------
# ExecutionHistoryService.get_execution_groups_with_counts
# ---------------------------------------------------------------------------

class TestGetExecutionGroupsWithCounts:
    """Tests for ExecutionHistoryService.get_execution_groups_with_counts()."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        from src.services.execution_history_service import ExecutionHistoryService
        svc = ExecutionHistoryService.__new__(ExecutionHistoryService)
        svc.session = mock_session
        svc.history_repo = AsyncMock()
        svc.logs_repo = AsyncMock()
        return svc

    @pytest.mark.asyncio
    async def test_returns_group_counts(self, service, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("group-a", 5),
            ("group-b", 12),
            ("group-c", 1),
        ]
        mock_session.execute.return_value = mock_result

        result = await service.get_execution_groups_with_counts()

        assert len(result) == 3
        assert result[0] == ("group-a", 5)
        assert result[1] == ("group-b", 12)
        assert result[2] == ("group-c", 1)

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_groups(self, service, mock_session):
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        result = await service.get_execution_groups_with_counts()

        assert result == []

    @pytest.mark.asyncio
    async def test_raises_on_db_error(self, service, mock_session):
        mock_session.execute.side_effect = Exception("connection lost")

        with pytest.raises(Exception, match="connection lost"):
            await service.get_execution_groups_with_counts()
