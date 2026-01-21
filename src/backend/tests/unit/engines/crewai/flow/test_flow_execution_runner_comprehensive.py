"""
Comprehensive unit tests for FlowExecutionRunner.

This module provides comprehensive test coverage for the flow execution runner
functions that handle running CrewAI flows and managing the flow execution lifecycle.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.engines.crewai.flow.flow_execution_runner import (
    update_execution_status_with_retry,
    run_flow_in_process,
    run_flow
)
from src.models.execution_status import ExecutionStatus


class TestUpdateExecutionStatusWithRetry:
    """Tests for update_execution_status_with_retry function."""

    @pytest.mark.asyncio
    async def test_update_status_success_first_attempt(self):
        """Test successful status update on first attempt."""
        # Patch at the source module since import happens inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(return_value=True)

            result = await update_execution_status_with_retry(
                execution_id='exec-123',
                status='COMPLETED',
                message='Test message'
            )

            assert result is True
            mock_service.update_status.assert_called_once_with(
                job_id='exec-123',
                status='COMPLETED',
                message='Test message'
            )

    @pytest.mark.asyncio
    async def test_update_status_retries_on_failure(self):
        """Test that status update retries on failure."""
        # Patch at the source module since import happens inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            # Fail twice, succeed on third attempt
            mock_service.update_status = AsyncMock(
                side_effect=[False, False, True]
            )

            result = await update_execution_status_with_retry(
                execution_id='exec-123',
                status='COMPLETED',
                max_retries=3,
                retry_delay=0.01  # Short delay for tests
            )

            assert result is True
            assert mock_service.update_status.call_count == 3

    @pytest.mark.asyncio
    async def test_update_status_returns_false_after_max_retries(self):
        """Test that False is returned after exhausting all retries."""
        # Patch at the source module since import happens inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(return_value=False)

            result = await update_execution_status_with_retry(
                execution_id='exec-123',
                status='COMPLETED',
                max_retries=3,
                retry_delay=0.01
            )

            assert result is False
            assert mock_service.update_status.call_count == 3

    @pytest.mark.asyncio
    async def test_update_status_handles_exceptions(self):
        """Test that exceptions are handled and retried."""
        # Patch at the source module since import happens inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(
                side_effect=[Exception("Network error"), Exception("Timeout"), True]
            )

            result = await update_execution_status_with_retry(
                execution_id='exec-123',
                status='COMPLETED',
                max_retries=3,
                retry_delay=0.01
            )

            assert result is True
            assert mock_service.update_status.call_count == 3

    @pytest.mark.asyncio
    async def test_update_status_returns_false_on_persistent_exceptions(self):
        """Test that False is returned when all attempts raise exceptions."""
        # Patch at the source module since import happens inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(
                side_effect=Exception("Persistent error")
            )

            result = await update_execution_status_with_retry(
                execution_id='exec-123',
                status='COMPLETED',
                max_retries=3,
                retry_delay=0.01
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_update_status_exponential_backoff(self):
        """Test that retry delay increases exponentially."""
        # Patch at the source module since import happens inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(return_value=False)

            with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                await update_execution_status_with_retry(
                    execution_id='exec-123',
                    status='COMPLETED',
                    max_retries=3,
                    retry_delay=0.5
                )

                # Check exponential backoff: 0.5, 1.0
                assert mock_sleep.call_count == 2
                calls = mock_sleep.call_args_list
                assert calls[0][0][0] == 0.5  # First retry: 0.5 * 1
                assert calls[1][0][0] == 1.0  # Second retry: 0.5 * 2


class TestRunFlowInProcess:
    """Tests for run_flow_in_process function."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock flow configuration."""
        return {
            'nodes': [{'id': 'node1', 'type': 'crewnode'}],
            'edges': [],
            'flow_config': {},
            'inputs': {'topic': 'AI'}
        }

    @pytest.fixture
    def mock_running_jobs(self):
        """Create a mock running jobs dictionary."""
        return {}

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.primary_group_id = 'group-123'
        return context

    @pytest.mark.asyncio
    async def test_run_flow_in_process_success(self, mock_config, mock_running_jobs):
        """Test successful flow execution in process."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED', 'result': {'output': 'test'}}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=mock_config,
                    running_jobs=mock_running_jobs
                )

                mock_executor.run_flow_isolated.assert_called_once()
                mock_update.assert_called_with(
                    execution_id=execution_id,
                    status=ExecutionStatus.COMPLETED.value,
                    message='Flow execution completed successfully'
                )

    @pytest.mark.asyncio
    async def test_run_flow_in_process_failure(self, mock_config, mock_running_jobs):
        """Test flow execution failure handling."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'FAILED', 'error': 'Task failed'}
            )

            # Patch at the source module since import happens inside the function
            with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
                mock_service.get_status = AsyncMock(return_value=MagicMock(status='RUNNING'))

                with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                    mock_update.return_value = True

                    await run_flow_in_process(
                        execution_id=execution_id,
                        config=mock_config,
                        running_jobs=mock_running_jobs
                    )

                    # Should update status to FAILED
                    mock_update.assert_called()
                    call_args = mock_update.call_args
                    assert call_args.kwargs['status'] == ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_run_flow_in_process_preserves_stopped_status(self, mock_config, mock_running_jobs):
        """Test that STOPPED status is preserved and not overwritten."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'FAILED', 'error': 'Process terminated'}
            )

            # Patch at the source module since import happens inside the function
            with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
                # Simulate that execution was stopped
                mock_service.get_status = AsyncMock(
                    return_value=MagicMock(status='STOPPED')
                )

                with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                    await run_flow_in_process(
                        execution_id=execution_id,
                        config=mock_config,
                        running_jobs=mock_running_jobs
                    )

                    # Status should NOT be updated since it's already STOPPED
                    # The function should skip status update
                    # Check that update was not called with FAILED status
                    for call in mock_update.call_args_list:
                        if call.kwargs.get('status'):
                            assert call.kwargs['status'] != ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_run_flow_in_process_with_user_token(self, mock_config, mock_running_jobs, mock_group_context):
        """Test flow execution passes user token for OBO authentication."""
        execution_id = 'exec-123'
        user_token = 'test-token-xyz'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=mock_config,
                    running_jobs=mock_running_jobs,
                    group_context=mock_group_context,
                    user_token=user_token
                )

                # Verify user_token was added to config
                call_args = mock_executor.run_flow_isolated.call_args
                passed_config = call_args.kwargs['flow_config']
                assert passed_config.get('user_token') == user_token

    @pytest.mark.asyncio
    async def test_run_flow_in_process_adds_group_id_from_context(self, mock_config, mock_running_jobs, mock_group_context):
        """Test that group_id is added from group_context."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=mock_config,
                    running_jobs=mock_running_jobs,
                    group_context=mock_group_context
                )

                # Verify group_id was added to config
                call_args = mock_executor.run_flow_isolated.call_args
                passed_config = call_args.kwargs['flow_config']
                assert passed_config.get('group_id') == 'group-123'

    @pytest.mark.asyncio
    async def test_run_flow_in_process_removes_from_running_jobs(self, mock_config):
        """Test that execution is removed from running_jobs after completion."""
        execution_id = 'exec-123'
        running_jobs = {execution_id: {'task': MagicMock()}}

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=mock_config,
                    running_jobs=running_jobs
                )

                # Execution should be removed from running_jobs
                assert execution_id not in running_jobs

    @pytest.mark.asyncio
    async def test_run_flow_in_process_cancelled_error(self, mock_config, mock_running_jobs):
        """Test handling of CancelledError (execution cancellation)."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                side_effect=asyncio.CancelledError()
            )
            mock_executor.terminate_execution = AsyncMock()

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=mock_config,
                    running_jobs=mock_running_jobs
                )

                # Should attempt to terminate the process
                mock_executor.terminate_execution.assert_called_once_with(execution_id)

                # Should update status to CANCELLED
                mock_update.assert_called_with(
                    execution_id=execution_id,
                    status=ExecutionStatus.CANCELLED.value,
                    message='Flow execution was cancelled'
                )

    @pytest.mark.asyncio
    async def test_run_flow_in_process_exception_handling(self, mock_config, mock_running_jobs):
        """Test handling of general exceptions."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                side_effect=RuntimeError('Unexpected error')
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=mock_config,
                    running_jobs=mock_running_jobs
                )

                # Should update status to FAILED
                mock_update.assert_called()
                call_args = mock_update.call_args
                assert call_args.kwargs['status'] == ExecutionStatus.FAILED.value
                assert 'Unexpected error' in call_args.kwargs['message']


class TestRunFlow:
    """Tests for run_flow function."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock flow configuration."""
        return {
            'nodes': [{'id': 'node1'}],
            'edges': [],
            'flow_config': {}
        }

    @pytest.fixture
    def mock_running_jobs(self):
        """Create a mock running jobs dictionary."""
        return {}

    @pytest.mark.asyncio
    async def test_run_flow_delegates_to_run_flow_in_process(self, mock_config, mock_running_jobs):
        """Test that run_flow delegates to run_flow_in_process."""
        execution_id = 'exec-123'

        with patch('src.engines.crewai.flow.flow_execution_runner.run_flow_in_process') as mock_run:
            mock_run.return_value = None

            await run_flow(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs
            )

            mock_run.assert_called_once_with(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
                group_context=None,
                user_token=None
            )

    @pytest.mark.asyncio
    async def test_run_flow_passes_group_context(self, mock_config, mock_running_jobs):
        """Test that run_flow passes group_context to run_flow_in_process."""
        execution_id = 'exec-123'
        mock_group_context = MagicMock()

        with patch('src.engines.crewai.flow.flow_execution_runner.run_flow_in_process') as mock_run:
            mock_run.return_value = None

            await run_flow(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
                group_context=mock_group_context
            )

            call_args = mock_run.call_args
            assert call_args.kwargs['group_context'] == mock_group_context

    @pytest.mark.asyncio
    async def test_run_flow_passes_user_token(self, mock_config, mock_running_jobs):
        """Test that run_flow passes user_token to run_flow_in_process."""
        execution_id = 'exec-123'
        user_token = 'test-token'

        with patch('src.engines.crewai.flow.flow_execution_runner.run_flow_in_process') as mock_run:
            mock_run.return_value = None

            await run_flow(
                execution_id=execution_id,
                config=mock_config,
                running_jobs=mock_running_jobs,
                user_token=user_token
            )

            call_args = mock_run.call_args
            assert call_args.kwargs['user_token'] == user_token


class TestRunFlowInProcessLogging:
    """Tests for logging behavior in run_flow_in_process."""

    @pytest.mark.asyncio
    async def test_run_flow_logs_function_entry(self):
        """Test that function entry is logged."""
        execution_id = 'exec-123'
        config = {'nodes': [], 'edges': []}
        running_jobs = {}

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                with patch('src.engines.crewai.flow.flow_execution_runner.logger') as mock_logger:
                    await run_flow_in_process(
                        execution_id=execution_id,
                        config=config,
                        running_jobs=running_jobs
                    )

                    # Verify logging was called
                    assert mock_logger.info.called

    @pytest.mark.asyncio
    async def test_run_flow_logs_completion_status(self):
        """Test that completion status is logged."""
        execution_id = 'exec-123'
        config = {'nodes': [], 'edges': []}
        running_jobs = {}

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                with patch('src.engines.crewai.flow.flow_execution_runner.logger') as mock_logger:
                    await run_flow_in_process(
                        execution_id=execution_id,
                        config=config,
                        running_jobs=running_jobs
                    )

                    # Check that completion was logged
                    log_messages = [str(call) for call in mock_logger.info.call_args_list]
                    assert any('COMPLETED' in msg or 'completed' in msg.lower() for msg in log_messages)


class TestRunFlowInProcessInputExtraction:
    """Tests for input extraction in run_flow_in_process."""

    @pytest.mark.asyncio
    async def test_extracts_inputs_from_config(self):
        """Test that inputs are extracted from config and passed to executor."""
        execution_id = 'exec-123'
        user_inputs = {'topic': 'AI', 'depth': 'detailed'}
        config = {
            'nodes': [],
            'edges': [],
            'inputs': user_inputs
        }
        running_jobs = {}

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=config,
                    running_jobs=running_jobs
                )

                # Verify inputs were passed to executor
                call_args = mock_executor.run_flow_isolated.call_args
                assert call_args.kwargs['inputs'] == user_inputs

    @pytest.mark.asyncio
    async def test_handles_missing_inputs(self):
        """Test handling when inputs are not in config."""
        execution_id = 'exec-123'
        config = {'nodes': [], 'edges': []}  # No inputs key
        running_jobs = {}

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor:
            mock_executor.run_flow_isolated = AsyncMock(
                return_value={'status': 'COMPLETED'}
            )

            with patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:
                mock_update.return_value = True

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=config,
                    running_jobs=running_jobs
                )

                # Should pass empty dict for inputs
                call_args = mock_executor.run_flow_isolated.call_args
                assert call_args.kwargs['inputs'] == {}
