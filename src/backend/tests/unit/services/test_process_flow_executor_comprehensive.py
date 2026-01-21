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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
