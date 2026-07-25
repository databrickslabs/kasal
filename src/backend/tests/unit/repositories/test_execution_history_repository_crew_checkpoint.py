"""Unit tests for crew task checkpoint methods on ExecutionHistoryRepository."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.execution_history_repository import ExecutionHistoryRepository


@pytest.fixture
def mock_session():
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    return session


@pytest.fixture
def repository(mock_session):
    return ExecutionHistoryRepository(mock_session)


def make_execution(checkpoint_data=None):
    execution = MagicMock()
    execution.checkpoint_data = checkpoint_data
    return execution


def set_lookup_result(mock_session, execution):
    result = MagicMock()
    result.scalar_one_or_none.return_value = execution
    mock_session.execute.return_value = result


class TestUpsertCrewTaskCheckpoint:
    @pytest.mark.asyncio
    async def test_creates_checkpoint_on_first_write(self, repository, mock_session):
        execution = make_execution(None)
        set_lookup_result(mock_session, execution)

        entry = {"index": 0, "task_key": "k0", "output_raw": "out"}
        ok = await repository.upsert_crew_task_checkpoint(
            "job-1", entry, task_count=3, process="sequential"
        )

        assert ok is True
        checkpoint = execution.checkpoint_data["crew_task_checkpoint"]
        assert checkpoint["task_count"] == 3
        assert checkpoint["process"] == "sequential"
        assert checkpoint["completed"]["0"] == entry
        assert checkpoint["version"] == 1
        assert checkpoint["updated_at"]
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_merges_and_preserves_other_keys(self, repository, mock_session):
        existing = {
            "crew_checkpoints": [{"crew_name": "flow-crew"}],
            "crew_task_checkpoint": {
                "version": 1,
                "task_count": 3,
                "completed": {"0": {"index": 0, "output_raw": "a"}},
            },
        }
        execution = make_execution(existing)
        set_lookup_result(mock_session, execution)

        entry = {"index": 1, "task_key": "k1", "output_raw": "b"}
        ok = await repository.upsert_crew_task_checkpoint(
            "job-1", entry, task_count=3, process="sequential"
        )

        assert ok is True
        data = execution.checkpoint_data
        # flow crew checkpoints untouched
        assert data["crew_checkpoints"] == [{"crew_name": "flow-crew"}]
        completed = data["crew_task_checkpoint"]["completed"]
        assert set(completed.keys()) == {"0", "1"}
        # a brand-new dict was assigned (JSON column change detection)
        assert data is not existing

    @pytest.mark.asyncio
    async def test_rewrite_of_same_index_is_idempotent(self, repository, mock_session):
        execution = make_execution(
            {"crew_task_checkpoint": {"completed": {"0": {"index": 0, "output_raw": "old"}}}}
        )
        set_lookup_result(mock_session, execution)

        entry = {"index": 0, "task_key": "k0", "output_raw": "new"}
        await repository.upsert_crew_task_checkpoint("job-1", entry, task_count=2)
        completed = execution.checkpoint_data["crew_task_checkpoint"]["completed"]
        assert completed == {"0": entry}

    @pytest.mark.asyncio
    async def test_missing_execution_returns_false(self, repository, mock_session):
        set_lookup_result(mock_session, None)
        ok = await repository.upsert_crew_task_checkpoint(
            "missing", {"index": 0}, task_count=1
        )
        assert ok is False
        mock_session.flush.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_error_returns_false(self, repository, mock_session):
        mock_session.execute.side_effect = Exception("boom")
        ok = await repository.upsert_crew_task_checkpoint(
            "job-1", {"index": 0}, task_count=1
        )
        assert ok is False


class TestClearCrewTaskCheckpoint:
    @pytest.mark.asyncio
    async def test_removes_only_task_checkpoint_key(self, repository, mock_session):
        execution = make_execution(
            {
                "crew_checkpoints": [{"crew_name": "x"}],
                "crew_task_checkpoint": {"completed": {"0": {}}},
            }
        )
        set_lookup_result(mock_session, execution)

        ok = await repository.clear_crew_task_checkpoint("job-1")
        assert ok is True
        assert execution.checkpoint_data == {"crew_checkpoints": [{"crew_name": "x"}]}
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_clears_to_none_when_nothing_else_stored(self, repository, mock_session):
        execution = make_execution({"crew_task_checkpoint": {"completed": {}}})
        set_lookup_result(mock_session, execution)
        ok = await repository.clear_crew_task_checkpoint("job-1")
        assert ok is True
        assert execution.checkpoint_data is None

    @pytest.mark.asyncio
    async def test_noop_when_absent(self, repository, mock_session):
        execution = make_execution({"other": 1})
        set_lookup_result(mock_session, execution)
        ok = await repository.clear_crew_task_checkpoint("job-1")
        assert ok is True
        mock_session.flush.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_execution_returns_false(self, repository, mock_session):
        set_lookup_result(mock_session, None)
        assert await repository.clear_crew_task_checkpoint("missing") is False


class TestGetCrewTaskCheckpoint:
    @pytest.mark.asyncio
    async def test_returns_checkpoint(self, repository, mock_session):
        checkpoint = {"completed": {"0": {"index": 0}}}
        execution = make_execution({"crew_task_checkpoint": checkpoint})
        set_lookup_result(mock_session, execution)
        result = await repository.get_crew_task_checkpoint("job-1", group_ids=["g1"])
        assert result == checkpoint

    @pytest.mark.asyncio
    async def test_returns_none_when_absent(self, repository, mock_session):
        execution = make_execution(None)
        set_lookup_result(mock_session, execution)
        assert await repository.get_crew_task_checkpoint("job-1") is None

    @pytest.mark.asyncio
    async def test_returns_none_when_execution_missing(self, repository, mock_session):
        set_lookup_result(mock_session, None)
        assert await repository.get_crew_task_checkpoint("missing") is None
