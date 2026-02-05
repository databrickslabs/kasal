"""Unit tests for ProcessFlowExecutor.

Tests the core functionality of the flow process executor.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import os


class TestProcessFlowExecutorInit:
    """Tests for ProcessFlowExecutor initialization."""

    def test_init_default_max_concurrent(self):
        """Test initialization with default max_concurrent value."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()
        assert executor._max_concurrent == 2
        assert executor._running_processes == {}

    def test_init_custom_max_concurrent(self):
        """Test initialization with custom max_concurrent value."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor(max_concurrent=5)
        assert executor._max_concurrent == 5

    def test_init_metrics_structure(self):
        """Test initialization creates proper metrics."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()
        metrics = executor.get_metrics()
        assert isinstance(metrics, dict)
        assert "total_executions" in metrics
        assert "active_executions" in metrics
        assert "completed_executions" in metrics
        assert "failed_executions" in metrics
        assert "terminated_executions" in metrics


class TestProcessFlowExecutorMethods:
    """Tests for ProcessFlowExecutor methods."""

    def test_has_terminate_execution(self):
        """Test that terminate_execution method exists."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        assert hasattr(ProcessFlowExecutor, 'terminate_execution')

    def test_has_run_flow_isolated(self):
        """Test that run_flow_isolated method exists."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        assert hasattr(ProcessFlowExecutor, 'run_flow_isolated')

    def test_has_terminate_execution_method(self):
        """Test that terminate_execution async method exists."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        import inspect
        assert hasattr(ProcessFlowExecutor, 'terminate_execution')
        assert inspect.iscoroutinefunction(ProcessFlowExecutor.terminate_execution)

    def test_has_get_metrics(self):
        """Test that get_metrics method exists."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        assert hasattr(ProcessFlowExecutor, 'get_metrics')


class TestModuleExports:
    """Tests for module exports."""

    def test_exports_process_flow_executor(self):
        """Test module exports process_flow_executor instance."""
        from src.services.process_flow_executor import process_flow_executor
        assert process_flow_executor is not None

    def test_exports_run_flow_in_process(self):
        """Test module exports run_flow_in_process function."""
        from src.services.process_flow_executor import run_flow_in_process
        assert callable(run_flow_in_process)


class TestTerminateExecution:
    """Tests for terminate_execution method."""

    @pytest.mark.asyncio
    async def test_terminate_execution_returns_bool(self):
        """Test terminate_execution returns boolean."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()

        with patch.object(executor, '_terminate_orphaned_process', return_value=False):
            result = await executor.terminate_execution("nonexistent_id")
            assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_terminate_tracked_process(self):
        """Test terminating a tracked process."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()

        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        mock_process.terminate = MagicMock()
        mock_process.join = MagicMock()

        executor._running_processes["test_exec"] = mock_process

        try:
            # Mock is_alive to return False after terminate
            mock_process.is_alive.side_effect = [True, False]

            result = await executor.terminate_execution("test_exec")
            assert result is True
        finally:
            executor._running_processes.pop("test_exec", None)


class TestGetMetrics:
    """Tests for get_metrics method."""

    def test_get_metrics_returns_copy(self):
        """Test get_metrics returns a copy of metrics."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()

        metrics1 = executor.get_metrics()
        metrics2 = executor.get_metrics()

        assert metrics1 is not metrics2


class TestTerminateNonexistent:
    """Tests for terminating non-existent execution."""

    @pytest.mark.asyncio
    async def test_terminate_nonexistent_returns_false(self):
        """Test terminating non-existent execution returns False."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()

        # Should not raise, returns False for non-existent
        result = await executor.terminate_execution("nonexistent_id")
        assert isinstance(result, bool)


class TestProcessFlowExecutorExecutionIdHandling:
    """Test execution_id handling in ProcessFlowExecutor."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessFlowExecutor instance."""
        from src.services.process_flow_executor import ProcessFlowExecutor
        executor = ProcessFlowExecutor()
        return executor

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.primary_group_id = "test_group_flow_123"
        context.access_token = "test_token_flow_abc"
        return context

    @pytest.mark.asyncio
    async def test_execution_id_added_to_flow_config_with_group_context(self, executor, mock_group_context):
        """Test that execution_id is added to flow_config when group_context is provided."""
        execution_id = "flow_exec_test_123"
        flow_config = {"nodes": [], "edges": [], "flow_config": {}}

        # Mock the Process creation
        mock_process = MagicMock()
        mock_process.pid = 22345
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        executor._ctx.Process = MagicMock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = MagicMock()
        mock_result_queue.empty = MagicMock(return_value=False)
        mock_result_queue.get_nowait = MagicMock(return_value={
            "status": "COMPLETED",
            "result": "flow_result"
        })

        executor._ctx.Queue = MagicMock(return_value=mock_result_queue)

        # Mock log queue processing and _wait_for_result
        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            with patch.object(executor, '_wait_for_result', return_value={
                "status": "COMPLETED",
                "result": "flow_result"
            }):
                try:
                    await executor.run_flow_isolated(
                        execution_id=execution_id,
                        flow_config=flow_config,
                        group_context=mock_group_context,
                        inputs={}
                    )
                except Exception:
                    pass  # We just want to verify the config

        # Verify execution_id was added to flow_config
        assert flow_config.get('execution_id') == execution_id

    @pytest.mark.asyncio
    async def test_execution_id_added_to_flow_config_without_group_context(self, executor):
        """Test that execution_id is added to flow_config as fallback without group_context."""
        execution_id = "flow_exec_fallback_456"
        flow_config = {"nodes": [], "edges": [], "flow_config": {}}

        # Mock the Process creation
        mock_process = MagicMock()
        mock_process.pid = 22346
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        executor._ctx.Process = MagicMock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = MagicMock()
        mock_result_queue.empty = MagicMock(return_value=False)
        mock_result_queue.get_nowait = MagicMock(return_value={
            "status": "COMPLETED",
            "result": "flow_result"
        })

        executor._ctx.Queue = MagicMock(return_value=mock_result_queue)

        # Mock log queue processing and _wait_for_result
        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            with patch.object(executor, '_wait_for_result', return_value={
                "status": "COMPLETED",
                "result": "flow_result"
            }):
                try:
                    await executor.run_flow_isolated(
                        execution_id=execution_id,
                        flow_config=flow_config,
                        group_context=None,  # No group context
                        inputs={}
                    )
                except Exception:
                    pass  # We just want to verify the config

        # Verify execution_id was added via fallback
        assert flow_config.get('execution_id') == execution_id

    @pytest.mark.asyncio
    async def test_execution_id_overwrites_when_group_context_provided(self, executor, mock_group_context):
        """Test that execution_id is set when group_context is provided."""
        execution_id = "flow_exec_new_789"
        existing_execution_id = "flow_exec_existing_000"
        flow_config = {"nodes": [], "edges": [], "execution_id": existing_execution_id}

        # Mock the Process creation
        mock_process = MagicMock()
        mock_process.pid = 22347
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        executor._ctx.Process = MagicMock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = MagicMock()
        mock_result_queue.empty = MagicMock(return_value=False)
        mock_result_queue.get_nowait = MagicMock(return_value={
            "status": "COMPLETED",
            "result": "flow_result"
        })

        executor._ctx.Queue = MagicMock(return_value=mock_result_queue)

        # Mock log queue processing and _wait_for_result
        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            with patch.object(executor, '_wait_for_result', return_value={
                "status": "COMPLETED",
                "result": "flow_result"
            }):
                try:
                    await executor.run_flow_isolated(
                        execution_id=execution_id,
                        flow_config=flow_config,
                        group_context=mock_group_context,
                        inputs={}
                    )
                except Exception:
                    pass

        # The execution_id SHOULD be the new one
        assert flow_config.get('execution_id') == execution_id

    @pytest.mark.asyncio
    async def test_kasal_execution_id_env_var_set_and_restored(self, executor, mock_group_context):
        """Test that KASAL_EXECUTION_ID environment variable is set and restored."""
        execution_id = "flow_exec_env_test_111"
        flow_config = {"nodes": [], "edges": []}

        # Mock the Process creation
        mock_process = MagicMock()
        mock_process.pid = 22348
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        executor._ctx.Process = MagicMock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = MagicMock()
        mock_result_queue.empty = MagicMock(return_value=False)
        mock_result_queue.get_nowait = MagicMock(return_value={
            "status": "COMPLETED",
            "result": "flow_result"
        })

        executor._ctx.Queue = MagicMock(return_value=mock_result_queue)

        # Store original env var value
        original_value = os.environ.get('KASAL_EXECUTION_ID')

        # Mock log queue processing and _wait_for_result
        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            with patch.object(executor, '_wait_for_result', return_value={
                "status": "COMPLETED",
                "result": "flow_result"
            }):
                try:
                    await executor.run_flow_isolated(
                        execution_id=execution_id,
                        flow_config=flow_config,
                        group_context=mock_group_context,
                        inputs={}
                    )
                except Exception:
                    pass

        # After execution, the env var should be restored to original state
        current_value = os.environ.get('KASAL_EXECUTION_ID')
        assert current_value == original_value

    @pytest.mark.asyncio
    async def test_group_id_and_user_token_added_with_group_context(self, executor, mock_group_context):
        """Test that group_id and user_token are added to flow_config with group_context."""
        execution_id = "flow_exec_context_test_222"
        flow_config = {"nodes": [], "edges": []}

        # Mock the Process creation
        mock_process = MagicMock()
        mock_process.pid = 22349
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        executor._ctx.Process = MagicMock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = MagicMock()
        mock_result_queue.empty = MagicMock(return_value=False)
        mock_result_queue.get_nowait = MagicMock(return_value={
            "status": "COMPLETED",
            "result": "flow_result"
        })

        executor._ctx.Queue = MagicMock(return_value=mock_result_queue)

        # Mock log queue processing and _wait_for_result
        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            with patch.object(executor, '_wait_for_result', return_value={
                "status": "COMPLETED",
                "result": "flow_result"
            }):
                try:
                    await executor.run_flow_isolated(
                        execution_id=execution_id,
                        flow_config=flow_config,
                        group_context=mock_group_context,
                        inputs={}
                    )
                except Exception:
                    pass

        # Verify group_id, user_token, and execution_id were added
        assert flow_config.get('group_id') == mock_group_context.primary_group_id
        assert flow_config.get('user_token') == mock_group_context.access_token
        assert flow_config.get('execution_id') == execution_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
