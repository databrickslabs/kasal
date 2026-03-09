"""
Unit tests for _process_log_queue method in ProcessCrewExecutor and ProcessFlowExecutor.

Tests the changes that route execution logs through get_smart_db_session
and ExecutionLogsRepository for writing execution logs to the database.
"""
import asyncio
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch, mock_open

import pytest

from src.services.process_crew_executor import ProcessCrewExecutor
from src.services.process_flow_executor import ProcessFlowExecutor


@pytest.fixture
def mock_group_context():
    """Create a mock group context for testing."""
    context = MagicMock()
    context.primary_group_id = "test-group-123"
    context.group_email = "test@example.com"
    return context


@pytest.fixture
def temp_log_dir():
    """Create a temporary log directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def crew_log_content():
    """Sample crew.log content for testing."""
    return """2024-02-09 10:00:00 - [12345678] INFO - Starting crew execution
2024-02-09 10:00:01 - [12345678] INFO - Agent initialized
2024-02-09 10:00:02 - [12345678] INFO - Task execution started
2024-02-09 10:00:03 - [87654321] INFO - Different execution
2024-02-09 10:00:04 - [12345678] INFO - Task completed
"""


@pytest.fixture
def flow_log_content():
    """Sample flow.log content for testing."""
    return """2024-02-09 10:00:00 - [abcd1234] INFO - Starting flow execution
2024-02-09 10:00:01 - [abcd1234] INFO - Node 1 processing
2024-02-09 10:00:02 - [abcd1234] INFO - Node 2 processing
2024-02-09 10:00:03 - [efgh5678] INFO - Different flow
2024-02-09 10:00:04 - [abcd1234] INFO - Flow completed
"""


def _make_smart_db_mock(mock_repo):
    """Create a mock for get_smart_db_session that yields a session + repo."""
    mock_session = AsyncMock()

    async def fake_smart_db():
        yield mock_session

    return fake_smart_db, mock_session


class TestProcessCrewExecutorLogQueue:
    """Test ProcessCrewExecutor._process_log_queue method."""

    @pytest.mark.asyncio
    async def test_process_log_queue_uses_app_engine(
        self, temp_log_dir, crew_log_content, mock_group_context
    ):
        """Test that _process_log_queue uses smart DB session for writes."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessCrewExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=mock_group_context
                )

        # Verify create_log was called (header + matching logs)
        assert mock_repo.create_log.call_count >= 2
        # Verify session.commit was called
        mock_session.commit.assert_called_once()

        # Verify header log
        first_call = mock_repo.create_log.call_args_list[0]
        assert first_call.kwargs["execution_id"] == execution_id
        assert "[EXECUTION_START]" in first_call.kwargs["content"]
        assert first_call.kwargs["group_id"] == "test-group-123"
        assert first_call.kwargs["group_email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_process_log_queue_filters_by_execution_id(
        self, temp_log_dir, crew_log_content
    ):
        """Test that only logs matching the execution ID are written."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessCrewExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Should have header + 4 matching logs (lines with 12345678)
        assert mock_repo.create_log.call_count == 5

        # Verify no logs contain the filtered execution ID
        for call in mock_repo.create_log.call_args_list[1:]:
            content = call.kwargs["content"]
            assert "87654321" not in content
            assert "12345678" in content

    @pytest.mark.asyncio
    async def test_process_log_queue_missing_log_file(self, temp_log_dir):
        """Test handling when crew.log file does not exist."""
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()

        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            executor = ProcessCrewExecutor()
            await executor._process_log_queue(
                log_queue=None,
                execution_id=execution_id,
                group_context=None
            )

        # Repository should not be called if file doesn't exist
        mock_repo.create_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_log_queue_empty_log_file(self, temp_log_dir):
        """Test handling when crew.log file is empty."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text("")

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessCrewExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Should still write header log even with empty file
        assert mock_repo.create_log.call_count == 1

    @pytest.mark.asyncio
    async def test_process_log_queue_engine_connection_failure(
        self, temp_log_dir, crew_log_content
    ):
        """Test error handling when DB session fails."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)

        async def failing_smart_db():
            raise Exception("Database connection failed")
            yield  # pragma: no cover

        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', failing_smart_db):
                executor = ProcessCrewExecutor()
                # Should not raise, just log error
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )


class TestProcessFlowExecutorLogQueue:
    """Test ProcessFlowExecutor._process_log_queue method."""

    @pytest.mark.asyncio
    async def test_process_log_queue_uses_app_engine(
        self, temp_log_dir, flow_log_content, mock_group_context
    ):
        """Test that _process_log_queue uses smart DB session for writes."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=mock_group_context
                )

        assert mock_repo.create_log.call_count >= 2
        mock_session.commit.assert_called_once()

        first_call = mock_repo.create_log.call_args_list[0]
        assert first_call.kwargs["execution_id"] == execution_id
        assert "[EXECUTION_START]" in first_call.kwargs["content"]
        assert first_call.kwargs["group_id"] == "test-group-123"
        assert first_call.kwargs["group_email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_process_log_queue_filters_by_execution_id(
        self, temp_log_dir, flow_log_content
    ):
        """Test that only logs matching the execution ID are written."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        assert mock_repo.create_log.call_count == 5

        for call in mock_repo.create_log.call_args_list[1:]:
            content = call.kwargs["content"]
            assert "efgh5678" not in content
            assert "abcd1234" in content

    @pytest.mark.asyncio
    async def test_process_log_queue_missing_log_file(self, temp_log_dir):
        """Test handling when flow.log file does not exist."""
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            executor = ProcessFlowExecutor()
            await executor._process_log_queue(
                log_queue=None,
                execution_id=execution_id,
                group_context=None
            )

        mock_repo.create_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_log_queue_empty_log_file(self, temp_log_dir):
        """Test handling when flow.log file is empty."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text("")

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        assert mock_repo.create_log.call_count == 1

    @pytest.mark.asyncio
    async def test_process_log_queue_engine_connection_failure(
        self, temp_log_dir, flow_log_content
    ):
        """Test error handling when DB session fails."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        async def failing_smart_db():
            raise Exception("Database connection failed")
            yield  # pragma: no cover

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', failing_smart_db):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

    @pytest.mark.asyncio
    async def test_process_log_queue_null_group_context(
        self, temp_log_dir, flow_log_content
    ):
        """Test that null group context is handled correctly."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock()
        mock_session = AsyncMock()

        async def fake_smart_db():
            yield mock_session

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', fake_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=mock_repo):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        first_call = mock_repo.create_log.call_args_list[0]
        assert first_call.kwargs["group_id"] is None
        assert first_call.kwargs["group_email"] is None


class TestBothExecutorsIntegration:
    """Integration tests comparing both executors' behavior."""

    @pytest.mark.asyncio
    async def test_both_executors_use_same_engine_pattern(
        self, temp_log_dir, crew_log_content, flow_log_content
    ):
        """Verify both executors use the same smart DB session pattern."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        crew_repo = MagicMock()
        crew_repo.create_log = AsyncMock()
        crew_session = AsyncMock()

        async def crew_smart_db():
            yield crew_session

        flow_repo = MagicMock()
        flow_repo.create_log = AsyncMock()
        flow_session = AsyncMock()

        async def flow_smart_db():
            yield flow_session

        crew_execution_id = "12345678-1234-1234-1234-123456789012"
        flow_execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.database_router.get_smart_db_session', crew_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=crew_repo):
                crew_executor = ProcessCrewExecutor()
                await crew_executor._process_log_queue(
                    log_queue=None,
                    execution_id=crew_execution_id,
                    group_context=None
                )

            with patch('src.db.database_router.get_smart_db_session', flow_smart_db), \
                 patch('src.repositories.execution_logs_repository.ExecutionLogsRepository', return_value=flow_repo):
                flow_executor = ProcessFlowExecutor()
                await flow_executor._process_log_queue(
                    log_queue=None,
                    execution_id=flow_execution_id,
                    group_context=None
                )

        # Both should have committed
        crew_session.commit.assert_called_once()
        flow_session.commit.assert_called_once()

        # Both should have same number of logs (5 each: 1 header + 4 matching)
        assert crew_repo.create_log.call_count == 5
        assert flow_repo.create_log.call_count == 5
