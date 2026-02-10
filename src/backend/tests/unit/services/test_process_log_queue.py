"""
Unit tests for _process_log_queue method in ProcessCrewExecutor and ProcessFlowExecutor.

Tests the changes that replaced manual DB URL construction with using the app engine
from src.db.session for writing execution logs to the database.
"""
import asyncio
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch, mock_open

import pytest
from sqlalchemy import text

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


@pytest.fixture
def mock_app_engine():
    """Create a mock application engine with async context manager."""
    mock_engine = MagicMock()
    mock_conn = AsyncMock()

    # Setup async context manager for engine.begin()
    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_context_manager.__aexit__ = AsyncMock(return_value=None)
    mock_engine.begin.return_value = mock_context_manager

    return mock_engine, mock_conn


class TestProcessCrewExecutorLogQueue:
    """Test ProcessCrewExecutor._process_log_queue method."""

    @pytest.mark.asyncio
    async def test_process_log_queue_uses_app_engine(
        self, temp_log_dir, crew_log_content, mock_group_context, mock_app_engine
    ):
        """Test that _process_log_queue uses the application engine for DB writes."""
        # Create crew.log file
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)

        mock_engine, mock_conn = mock_app_engine
        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            # Patch the import at the point where it's used
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessCrewExecutor()

                # Call the method
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=mock_group_context
                )

        # Verify engine.begin() was called
        mock_engine.begin.assert_called_once()

        # Verify execute was called multiple times (header + matching logs)
        assert mock_conn.execute.call_count >= 2  # At least header + 1 log

        # Verify SQL query structure
        first_call = mock_conn.execute.call_args_list[0]
        assert "INSERT INTO execution_logs" in first_call[0][0].text
        assert ":execution_id" in first_call[0][0].text
        assert ":content" in first_call[0][0].text
        assert ":timestamp" in first_call[0][0].text
        assert ":group_id" in first_call[0][0].text
        assert ":group_email" in first_call[0][0].text

        # Verify log data structure
        first_log_data = first_call[0][1]
        assert first_log_data["execution_id"] == execution_id
        assert "[EXECUTION_START]" in first_log_data["content"]
        assert first_log_data["group_id"] == "test-group-123"
        assert first_log_data["group_email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_process_log_queue_filters_by_execution_id(
        self, temp_log_dir, crew_log_content, mock_app_engine
    ):
        """Test that only logs matching the execution ID are written."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)

        mock_engine, mock_conn = mock_app_engine
        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessCrewExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Should have header + 4 matching logs (lines with 12345678)
        # 87654321 line should be filtered out
        assert mock_conn.execute.call_count == 5  # 1 header + 4 matching logs

        # Verify no logs contain the filtered execution ID
        for call in mock_conn.execute.call_args_list[1:]:  # Skip header
            log_data = call[0][1]
            content = log_data["content"]
            assert "87654321" not in content
            assert "12345678" in content

    @pytest.mark.asyncio
    async def test_process_log_queue_missing_log_file(self, temp_log_dir, mock_app_engine):
        """Test handling when crew.log file does not exist."""
        mock_engine, mock_conn = mock_app_engine
        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessCrewExecutor()
                # Should not raise, just return early
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Engine should not be called if file doesn't exist
        mock_engine.begin.assert_not_called()
        mock_conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_log_queue_empty_log_file(self, temp_log_dir, mock_app_engine):
        """Test handling when crew.log file is empty."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text("")  # Empty file

        mock_engine, mock_conn = mock_app_engine
        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessCrewExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Should still write header log even with empty file
        mock_engine.begin.assert_called_once()
        assert mock_conn.execute.call_count == 1  # Only header log

    @pytest.mark.asyncio
    async def test_process_log_queue_engine_connection_failure(
        self, temp_log_dir, crew_log_content
    ):
        """Test error handling when engine connection fails."""
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)

        # Create mock engine that fails on connection
        mock_engine = MagicMock()
        mock_engine.begin.side_effect = Exception("Database connection failed")

        execution_id = "12345678-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessCrewExecutor()
                # Should not raise, just log error (non-critical operation)
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Verify it attempted to connect
        mock_engine.begin.assert_called_once()

class TestProcessFlowExecutorLogQueue:
    """Test ProcessFlowExecutor._process_log_queue method."""

    @pytest.mark.asyncio
    async def test_process_log_queue_uses_app_engine(
        self, temp_log_dir, flow_log_content, mock_group_context, mock_app_engine
    ):
        """Test that _process_log_queue uses the application engine for DB writes."""
        # Create flow.log file
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_engine, mock_conn = mock_app_engine
        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessFlowExecutor()

                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=mock_group_context
                )

        # Verify engine.begin() was called
        mock_engine.begin.assert_called_once()

        # Verify execute was called multiple times (header + matching logs)
        assert mock_conn.execute.call_count >= 2

        # Verify SQL query structure
        first_call = mock_conn.execute.call_args_list[0]
        assert "INSERT INTO execution_logs" in first_call[0][0].text
        assert ":execution_id" in first_call[0][0].text

        # Verify log data
        first_log_data = first_call[0][1]
        assert first_log_data["execution_id"] == execution_id
        assert "[EXECUTION_START]" in first_log_data["content"]
        assert first_log_data["group_id"] == "test-group-123"
        assert first_log_data["group_email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_process_log_queue_filters_by_execution_id(
        self, temp_log_dir, flow_log_content, mock_app_engine
    ):
        """Test that only logs matching the execution ID are written."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_engine, mock_conn = mock_app_engine
        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Should have header + 4 matching logs (lines with abcd1234)
        assert mock_conn.execute.call_count == 5

        # Verify no logs contain the filtered execution ID
        for call in mock_conn.execute.call_args_list[1:]:
            log_data = call[0][1]
            content = log_data["content"]
            assert "efgh5678" not in content
            assert "abcd1234" in content

    @pytest.mark.asyncio
    async def test_process_log_queue_missing_log_file(self, temp_log_dir, mock_app_engine):
        """Test handling when flow.log file does not exist."""
        mock_engine, mock_conn = mock_app_engine
        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Engine should not be called if file doesn't exist
        mock_engine.begin.assert_not_called()
        mock_conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_log_queue_empty_log_file(self, temp_log_dir, mock_app_engine):
        """Test handling when flow.log file is empty."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text("")

        mock_engine, mock_conn = mock_app_engine
        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Should still write header log
        mock_engine.begin.assert_called_once()
        assert mock_conn.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_process_log_queue_engine_connection_failure(
        self, temp_log_dir, flow_log_content
    ):
        """Test error handling when engine connection fails."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_engine = MagicMock()
        mock_engine.begin.side_effect = Exception("Database connection failed")

        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessFlowExecutor()
                # Should not raise, just log warning (non-critical operation)
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None
                )

        # Verify it attempted to connect
        mock_engine.begin.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_log_queue_null_group_context(
        self, temp_log_dir, flow_log_content, mock_app_engine
    ):
        """Test that null group context is handled correctly."""
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        mock_engine, mock_conn = mock_app_engine
        execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            with patch('src.db.session.engine', mock_engine):
                executor = ProcessFlowExecutor()
                await executor._process_log_queue(
                    log_queue=None,
                    execution_id=execution_id,
                    group_context=None  # Explicitly None
                )

        # Verify logs were written with None for group fields
        first_call = mock_conn.execute.call_args_list[0]
        log_data = first_call[0][1]
        assert log_data["group_id"] is None
        assert log_data["group_email"] is None


class TestBothExecutorsIntegration:
    """Integration tests comparing both executors' behavior."""

    @pytest.mark.asyncio
    async def test_both_executors_use_same_engine_pattern(
        self, temp_log_dir, crew_log_content, flow_log_content
    ):
        """Verify both executors use the same engine import pattern."""
        # Create both log files
        crew_log_path = Path(temp_log_dir) / "crew.log"
        crew_log_path.write_text(crew_log_content)
        flow_log_path = Path(temp_log_dir) / "flow.log"
        flow_log_path.write_text(flow_log_content)

        # Create separate mocks for each executor
        crew_engine = MagicMock()
        crew_conn = AsyncMock()
        crew_context = MagicMock()
        crew_context.__aenter__ = AsyncMock(return_value=crew_conn)
        crew_context.__aexit__ = AsyncMock(return_value=None)
        crew_engine.begin.return_value = crew_context

        flow_engine = MagicMock()
        flow_conn = AsyncMock()
        flow_context = MagicMock()
        flow_context.__aenter__ = AsyncMock(return_value=flow_conn)
        flow_context.__aexit__ = AsyncMock(return_value=None)
        flow_engine.begin.return_value = flow_context

        crew_execution_id = "12345678-1234-1234-1234-123456789012"
        flow_execution_id = "abcd1234-1234-1234-1234-123456789012"

        with patch.dict(os.environ, {"LOG_DIR": temp_log_dir}):
            # Test crew executor
            with patch('src.db.session.engine', crew_engine):
                crew_executor = ProcessCrewExecutor()
                await crew_executor._process_log_queue(
                    log_queue=None,
                    execution_id=crew_execution_id,
                    group_context=None
                )

            crew_call_count = crew_conn.execute.call_count

            # Test flow executor
            with patch('src.db.session.engine', flow_engine):
                flow_executor = ProcessFlowExecutor()
                await flow_executor._process_log_queue(
                    log_queue=None,
                    execution_id=flow_execution_id,
                    group_context=None
                )

            flow_call_count = flow_conn.execute.call_count

            # Both should have called engine.begin()
            crew_engine.begin.assert_called_once()
            flow_engine.begin.assert_called_once()

            # Both should have written header + matching logs
            assert crew_call_count >= 2
            assert flow_call_count >= 2

            # Both should have same number of logs (5 each: 1 header + 4 matching)
            assert crew_call_count == 5
            assert flow_call_count == 5
