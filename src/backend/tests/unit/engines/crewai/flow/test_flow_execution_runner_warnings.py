"""Tests for MCP warning surfacing in flow_execution_runner.run_flow_in_process().

These tests verify that when process_flow_executor.run_flow_isolated returns
a COMPLETED result with a 'warnings' list, the warnings are surfaced in
the final_message passed to update_execution_status_with_retry.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.paths.flow.flow_execution_runner import run_flow_in_process
from src.models.execution_status import ExecutionStatus


class TestFlowExecutionWarnings:
    """Test MCP warnings are surfaced in flow execution results."""

    @pytest.fixture
    def mock_config(self):
        """Create a minimal flow configuration."""
        return {
            'nodes': [{'id': 'node1', 'type': 'crewnode'}],
            'edges': [],
            'flow_config': {},
            'inputs': {},
        }

    @pytest.fixture
    def mock_running_jobs(self):
        """Create a mock running jobs dictionary."""
        return {}

    @pytest.mark.asyncio
    async def test_completed_with_warnings_surfaces_in_message(self, mock_config, mock_running_jobs):
        """When result contains warnings, they should appear in the completion message."""
        execution_id = 'flow-warn-001'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': {'output': 'flow output'},
                'warnings': ["MCP server 'tavily': 403 Forbidden"],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            # The final status update should contain warnings in the message
            mock_update.assert_called()
            call_kwargs = mock_update.call_args.kwargs

            assert call_kwargs['status'] == ExecutionStatus.COMPLETED.value
            assert "warnings" in call_kwargs['message'].lower()
            assert "MCP server 'tavily': 403 Forbidden" in call_kwargs['message']
            assert call_kwargs['result'] == {'output': 'flow output'}

    @pytest.mark.asyncio
    async def test_completed_with_multiple_warnings(self, mock_config, mock_running_jobs):
        """Multiple warnings should be joined with semicolons in the message."""
        execution_id = 'flow-warn-002'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': {'output': 'data'},
                'warnings': [
                    "MCP server 'tavily': 403 Forbidden",
                    "MCP server 'slack': Connection timeout",
                ],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            message = mock_update.call_args.kwargs['message']

            assert "MCP server 'tavily': 403 Forbidden" in message
            assert "MCP server 'slack': Connection timeout" in message
            assert "; " in message

    @pytest.mark.asyncio
    async def test_completed_without_warnings_no_warning_text(self, mock_config, mock_running_jobs):
        """When there are no warnings, message should be the standard success text."""
        execution_id = 'flow-warn-003'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': {'output': 'data'},
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            message = mock_update.call_args.kwargs['message']

            assert message == "Flow execution completed successfully"
            assert "warning" not in message.lower()

    @pytest.mark.asyncio
    async def test_completed_with_empty_warnings_list(self, mock_config, mock_running_jobs):
        """An empty warnings list should produce the standard success message."""
        execution_id = 'flow-warn-004'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': {'output': 'data'},
                'warnings': [],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            message = mock_update.call_args.kwargs['message']

            assert message == "Flow execution completed successfully"
            assert "warning" not in message.lower()

    @pytest.mark.asyncio
    async def test_completed_with_warnings_still_has_completed_status(self, mock_config, mock_running_jobs):
        """Warnings should NOT cause the status to be anything other than COMPLETED."""
        execution_id = 'flow-warn-005'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': {'output': 'data'},
                'warnings': ["Some warning"],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            status = mock_update.call_args.kwargs['status']

            assert status == ExecutionStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_completed_with_warnings_preserves_result(self, mock_config, mock_running_jobs):
        """Warnings should not affect the result payload."""
        execution_id = 'flow-warn-006'
        expected_result = {'key': 'value', 'nodes': [1, 2, 3]}

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': expected_result,
                'warnings': ["MCP server issue"],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            result = mock_update.call_args.kwargs['result']

            assert result == expected_result

    @pytest.mark.asyncio
    async def test_warning_message_format(self, mock_config, mock_running_jobs):
        """Verify the exact format of the warning message."""
        execution_id = 'flow-warn-007'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_update.return_value = True
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': {'output': 'data'},
                'warnings': ["warn1", "warn2"],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            message = mock_update.call_args.kwargs['message']

            expected_message = "Flow execution completed with warnings: warn1; warn2"
            assert message == expected_message

    @pytest.mark.asyncio
    async def test_failed_result_does_not_check_warnings(self, mock_config, mock_running_jobs):
        """When result status is FAILED, warnings should not be surfaced."""
        execution_id = 'flow-warn-008'

        with patch('src.engines.crewai.paths.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.paths.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_update.return_value = True
            mock_status_svc.get_status = AsyncMock(return_value=MagicMock(status='RUNNING'))
            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'FAILED',
                'error': 'Something broke',
                'warnings': ["MCP warning that should not appear"],
            })

            await run_flow_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called()
            call_kwargs = mock_update.call_args.kwargs
            assert call_kwargs['status'] == ExecutionStatus.FAILED.value
            assert "warning" not in call_kwargs['message'].lower()


# ---------------------------------------------------------------------------
# CI/CD artifact aggregation query — SQLite/Postgres portability (regression)
# ---------------------------------------------------------------------------
#
# The query previously used the Postgres-only ``output::text`` cast, which on
# SQLite (dev) raised "(sqlite3.OperationalError) unrecognized token: ':'" and
# silently skipped CI/CD artifact injection. It now uses CAST(... AS TEXT).

import sqlite3  # noqa: E402

from src.engines.crewai.paths.flow.flow_execution_runner import (  # noqa: E402
    CICD_ARTIFACT_QUERY,
)


def test_cicd_query_has_no_postgres_only_cast():
    """The query must not use the Postgres-only ``::text`` cast (SQLite chokes)."""
    assert "::text" not in CICD_ARTIFACT_QUERY
    assert "CAST(output AS TEXT)" in CICD_ARTIFACT_QUERY


def test_cicd_query_runs_on_sqlite_and_filters():
    """Regression: the query parses on SQLite and matches only rows whose output
    contains cicd_download_url."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE execution_trace (job_id TEXT, output TEXT, created_at TEXT)")
    conn.execute(
        "INSERT INTO execution_trace VALUES (?, ?, ?)",
        ("job-1", '{"cicd_download_url": "/api/x"}', "2026-01-01"),
    )
    conn.execute(
        "INSERT INTO execution_trace VALUES (?, ?, ?)",
        ("job-1", '{"something_else": true}', "2026-01-02"),
    )
    conn.execute(
        "INSERT INTO execution_trace VALUES (?, ?, ?)",
        ("job-2", '{"cicd_download_url": "/api/y"}', "2026-01-03"),
    )

    # SQLAlchemy's ``:jid`` bind param maps to SQLite's ``?`` — run the named SQL
    # via a tiny translation so we exercise the real query text on SQLite.
    sqlite_sql = CICD_ARTIFACT_QUERY.replace(":jid", "?")
    rows = conn.execute(sqlite_sql, ("job-1",)).fetchall()

    assert len(rows) == 1  # only job-1's cicd row, not the non-cicd row nor job-2
    assert rows[0][0] == '{"cicd_download_url": "/api/x"}'
    conn.close()
