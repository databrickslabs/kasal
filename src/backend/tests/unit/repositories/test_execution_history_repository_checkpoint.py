"""Unit tests for the checkpoint persistence primitives on the repository.

The repository is deliberately SHAPE-BLIND: it may not import a service, so it
never parses the record, never knows which key holds it, and never merges. It
reads and writes the column as given. The merge lives in
services/execution/checkpointing/store.py, tested in test_checkpoint_store.py.
"""

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


def make_execution(checkpoint_data=None, checkpoint_status=None):
    execution = MagicMock()
    execution.checkpoint_data = checkpoint_data
    execution.checkpoint_status = checkpoint_status
    return execution


def set_lookup_result(mock_session, execution):
    result = MagicMock()
    result.scalar_one_or_none.return_value = execution
    mock_session.execute.return_value = result


class TestGetCheckpointData:
    @pytest.mark.asyncio
    async def test_returns_the_column_verbatim(self, repository, mock_session):
        column = {"checkpoint": {"units": {}}, "edited_config": {"a": 1}}
        set_lookup_result(mock_session, make_execution(column))

        result = await repository.get_checkpoint_data("job-1", group_ids=["g1"])

        # Verbatim: no key selection, no normalisation, no migration.
        assert result == column

    @pytest.mark.asyncio
    async def test_returns_none_when_absent(self, repository, mock_session):
        set_lookup_result(mock_session, make_execution(None))
        assert await repository.get_checkpoint_data("job-1") is None

    @pytest.mark.asyncio
    async def test_returns_none_when_execution_missing(self, repository, mock_session):
        set_lookup_result(mock_session, None)
        assert await repository.get_checkpoint_data("missing") is None

    @pytest.mark.asyncio
    async def test_db_error_returns_none(self, repository, mock_session):
        mock_session.execute.side_effect = Exception("boom")
        assert await repository.get_checkpoint_data("job-1") is None


class TestSetCheckpointData:
    @pytest.mark.asyncio
    async def test_assigns_a_new_dict_for_json_change_detection(
        self, repository, mock_session
    ):
        execution = make_execution({"old": True})
        set_lookup_result(mock_session, execution)
        payload = {"checkpoint": {"units": {"0": {}}}}

        ok = await repository.set_checkpoint_data("job-1", payload)

        assert ok is True
        assert execution.checkpoint_data == payload
        # Reassigning the same object would not register on a JSON column.
        assert execution.checkpoint_data is not payload
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_none_clears_the_column(self, repository, mock_session):
        execution = make_execution({"checkpoint": {}})
        set_lookup_result(mock_session, execution)

        await repository.set_checkpoint_data("job-1", None)
        assert execution.checkpoint_data is None

    @pytest.mark.asyncio
    async def test_status_is_left_alone_when_not_passed(self, repository, mock_session):
        execution = make_execution({"a": 1}, checkpoint_status="active")
        set_lookup_result(mock_session, execution)

        await repository.set_checkpoint_data("job-1", {"b": 2})
        assert execution.checkpoint_status == "active"

    @pytest.mark.asyncio
    async def test_status_none_is_distinguishable_from_omitted(
        self, repository, mock_session
    ):
        execution = make_execution({"a": 1}, checkpoint_status="active")
        set_lookup_result(mock_session, execution)

        await repository.set_checkpoint_data("job-1", {"b": 2}, checkpoint_status=None)
        assert execution.checkpoint_status is None

    @pytest.mark.asyncio
    async def test_missing_execution_returns_false(self, repository, mock_session):
        set_lookup_result(mock_session, None)
        assert await repository.set_checkpoint_data("missing", {}) is False
        mock_session.flush.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_error_returns_false(self, repository, mock_session):
        mock_session.execute.side_effect = Exception("boom")
        assert await repository.set_checkpoint_data("job-1", {}) is False


class TestSetCheckpointStatus:
    @pytest.mark.asyncio
    async def test_updates_the_status(self, repository, mock_session):
        execution = make_execution({"a": 1}, checkpoint_status="active")
        set_lookup_result(mock_session, execution)

        ok = await repository.set_checkpoint_status("job-1", "expired")

        assert ok is True
        assert execution.checkpoint_status == "expired"
        # The units survive expiry so an operator can still inspect them.
        assert execution.checkpoint_data == {"a": 1}

    @pytest.mark.asyncio
    async def test_missing_execution_returns_false(self, repository, mock_session):
        set_lookup_result(mock_session, None)
        assert await repository.set_checkpoint_status("missing", "expired") is False

    @pytest.mark.asyncio
    async def test_db_error_returns_false(self, repository, mock_session):
        mock_session.execute.side_effect = Exception("boom")
        assert await repository.set_checkpoint_status("job-1", "expired") is False
