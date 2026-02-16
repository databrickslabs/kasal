"""Unit tests for AgentTraceRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.agent_trace_repository import (
    AgentTraceRepository,
    get_agent_trace_repository,
)
from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace


@pytest.fixture
def mock_async_session():
    from sqlalchemy.ext.asyncio import AsyncSession
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    return session


@pytest.fixture
def repo(mock_async_session):
    return AgentTraceRepository(db=mock_async_session)


class TestCreateTrace:

    @pytest.mark.asyncio
    @patch("src.repositories.agent_trace_repository.ExecutionTrace")
    async def test_creates_trace_successfully(self, MockTrace, repo, mock_async_session):
        mock_trace = MagicMock(id="trace-1")
        MockTrace.return_value = mock_trace

        run = MagicMock(id=1, job_id="job-1")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = run
        mock_async_session.execute.return_value = mock_result

        trace_id = await repo.create_trace(
            job_id="job-1",
            agent_name="Researcher",
            task_name="Search",
            event_type="agent_step",
            content="Found results",
        )

        mock_async_session.add.assert_called_once_with(mock_trace)
        assert trace_id == "trace-1"

    @pytest.mark.asyncio
    @patch("src.repositories.agent_trace_repository.ExecutionTrace")
    async def test_creates_trace_with_timestamp(self, MockTrace, repo, mock_async_session):
        MockTrace.return_value = MagicMock(id="trace-2")

        run = MagicMock(id=1, job_id="job-1")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = run
        mock_async_session.execute.return_value = mock_result

        await repo.create_trace(
            job_id="job-1",
            agent_name="Writer",
            task_name="Write",
            event_type="tool_start",
            content="Starting tool",
            timestamp="2024-01-15T10:30:00",
        )

        mock_async_session.add.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.repositories.agent_trace_repository.ExecutionTrace")
    async def test_creates_trace_with_invalid_timestamp(self, MockTrace, repo, mock_async_session):
        MockTrace.return_value = MagicMock(id="trace-3")

        run = MagicMock(id=1, job_id="job-1")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = run
        mock_async_session.execute.return_value = mock_result

        await repo.create_trace(
            job_id="job-1",
            agent_name="Writer",
            task_name="Write",
            event_type="tool_start",
            content="Starting tool",
            timestamp="not-a-timestamp",
        )

        mock_async_session.add.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.repositories.agent_trace_repository.ExecutionTrace")
    async def test_creates_trace_with_metadata(self, MockTrace, repo, mock_async_session):
        MockTrace.return_value = MagicMock(id="trace-4")

        run = MagicMock(id=1, job_id="job-1")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = run
        mock_async_session.execute.return_value = mock_result

        await repo.create_trace(
            job_id="job-1",
            agent_name="Researcher",
            task_name="Search",
            event_type="agent_step",
            content="result",
            trace_metadata={"key": "value"},
        )

        mock_async_session.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_when_no_run_found(self, repo, mock_async_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_async_session.execute.return_value = mock_result

        with pytest.raises(ValueError, match="No execution record found"):
            await repo.create_trace(
                job_id="missing-job",
                agent_name="Agent",
                task_name="Task",
                event_type="event",
                content="content",
            )

    @pytest.mark.asyncio
    async def test_raises_when_session_not_async(self):
        repo = AgentTraceRepository(db=MagicMock())

        with pytest.raises(ValueError, match="AsyncSession"):
            await repo.create_trace(
                job_id="job-1",
                agent_name="Agent",
                task_name="Task",
                event_type="event",
                content="content",
            )


class TestRecordTrace:

    @patch("src.repositories.agent_trace_repository.ExecutionTrace")
    def test_records_trace_successfully(self, MockTrace):
        mock_trace = MagicMock(id="trace-1")
        MockTrace.return_value = mock_trace

        mock_db = MagicMock()
        run = MagicMock(id=1, job_id="job-1")
        mock_db.query.return_value.filter.return_value.first.return_value = run

        repo = AgentTraceRepository(db=mock_db)
        result = repo.record_trace(
            job_id="job-1",
            agent_name="Agent",
            task_name="Task",
            output_content="Output",
        )

        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        assert result is not None

    def test_returns_none_when_no_run(self):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None

        repo = AgentTraceRepository(db=mock_db)
        result = repo.record_trace(
            job_id="missing",
            agent_name="Agent",
            task_name="Task",
            output_content="Output",
        )

        assert result is None

    def test_returns_none_on_db_error(self):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.side_effect = SQLAlchemyError("error")

        repo = AgentTraceRepository(db=mock_db)
        result = repo.record_trace(
            job_id="job-1",
            agent_name="Agent",
            task_name="Task",
            output_content="Output",
        )

        assert result is None
        mock_db.rollback.assert_called_once()

    def test_returns_none_when_no_db(self):
        repo = AgentTraceRepository(db=None)

        # The outer try/except catches ValueError and returns None
        result = repo.record_trace(
            job_id="job-1",
            agent_name="Agent",
            task_name="Task",
            output_content="Output",
        )

        assert result is None


class TestGetAgentTraceRepository:

    def test_returns_repository_with_db(self):
        mock_db = AsyncMock()
        result = get_agent_trace_repository(mock_db)

        assert isinstance(result, AgentTraceRepository)
        assert result.db == mock_db

    def test_returns_repository_without_db(self):
        result = get_agent_trace_repository()

        assert isinstance(result, AgentTraceRepository)
