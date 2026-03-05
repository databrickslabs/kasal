"""Tests for MCP warning surfacing in execution_runner.run_crew_in_process().

These tests verify that when process_crew_executor.run_crew_isolated returns
a COMPLETED result with a 'warnings' list, the warnings are surfaced in
the final_message passed to update_execution_status_with_retry.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.execution_runner import run_crew_in_process
from src.models.execution_status import ExecutionStatus


class TestCrewExecutionWarnings:
    """Test MCP warnings are surfaced in crew execution results."""

    @pytest.fixture
    def mock_config(self):
        """Create a minimal crew configuration."""
        return {
            'agents': [],
            'tasks': [],
            'inputs': {},
        }

    @pytest.fixture
    def mock_running_jobs(self):
        """Create a mock running jobs dictionary."""
        return {}

    @pytest.mark.asyncio
    async def test_completed_with_warnings_surfaces_in_message(self, mock_config, mock_running_jobs):
        """When result contains warnings, they should appear in the completion message."""
        execution_id = 'test-warn-001'

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': 'crew output text',
                'warnings': ["MCP server 'tavily': 403 Forbidden"],
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            # The final status update should contain warnings in the message
            mock_update.assert_called_once()
            call_args = mock_update.call_args
            # update_execution_status_with_retry is called with positional args
            # (execution_id, final_status, final_message, final_result)
            status_arg = call_args[0][1]
            message_arg = call_args[0][2]
            result_arg = call_args[0][3]

            assert status_arg == ExecutionStatus.COMPLETED.value
            assert "warnings" in message_arg.lower()
            assert "MCP server 'tavily': 403 Forbidden" in message_arg
            assert result_arg == 'crew output text'

    @pytest.mark.asyncio
    async def test_completed_with_multiple_warnings(self, mock_config, mock_running_jobs):
        """Multiple warnings should be joined with semicolons in the message."""
        execution_id = 'test-warn-002'

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': 'output',
                'warnings': [
                    "MCP server 'tavily': 403 Forbidden",
                    "MCP server 'slack': Connection timeout",
                ],
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called_once()
            message_arg = mock_update.call_args[0][2]

            assert "MCP server 'tavily': 403 Forbidden" in message_arg
            assert "MCP server 'slack': Connection timeout" in message_arg
            assert "; " in message_arg  # Semicolon separator between warnings

    @pytest.mark.asyncio
    async def test_completed_without_warnings_no_warning_text(self, mock_config, mock_running_jobs):
        """When there are no warnings, message should be the standard success text."""
        execution_id = 'test-warn-003'

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': 'output',
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called_once()
            message_arg = mock_update.call_args[0][2]

            assert message_arg == "CrewAI execution completed successfully"
            assert "warning" not in message_arg.lower()

    @pytest.mark.asyncio
    async def test_completed_with_empty_warnings_list(self, mock_config, mock_running_jobs):
        """An empty warnings list should produce the standard success message."""
        execution_id = 'test-warn-004'

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': 'output',
                'warnings': [],
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called_once()
            message_arg = mock_update.call_args[0][2]

            assert message_arg == "CrewAI execution completed successfully"
            assert "warning" not in message_arg.lower()

    @pytest.mark.asyncio
    async def test_completed_with_warnings_still_has_completed_status(self, mock_config, mock_running_jobs):
        """Warnings should NOT cause the status to be anything other than COMPLETED."""
        execution_id = 'test-warn-005'

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': 'output',
                'warnings': ["Some warning"],
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called_once()
            status_arg = mock_update.call_args[0][1]

            assert status_arg == ExecutionStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_completed_with_warnings_preserves_result(self, mock_config, mock_running_jobs):
        """Warnings should not affect the result payload."""
        execution_id = 'test-warn-006'
        expected_result = {'key': 'value', 'data': [1, 2, 3]}

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': expected_result,
                'warnings': ["MCP server issue"],
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called_once()
            result_arg = mock_update.call_args[0][3]

            assert result_arg == expected_result

    @pytest.mark.asyncio
    async def test_warning_message_format(self, mock_config, mock_running_jobs):
        """Verify the exact format of the warning message."""
        execution_id = 'test-warn-007'

        with patch('src.engines.crewai.execution_runner.process_crew_executor') as mock_executor, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_svc:

            mock_status_svc.update_status = AsyncMock()
            mock_update.return_value = True
            mock_executor.run_crew_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': 'output',
                'warnings': ["warn1", "warn2"],
            })

            await run_crew_in_process(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
            )

            mock_update.assert_called_once()
            message_arg = mock_update.call_args[0][2]

            expected_message = "CrewAI execution completed with warnings: warn1; warn2"
            assert message_arg == expected_message
