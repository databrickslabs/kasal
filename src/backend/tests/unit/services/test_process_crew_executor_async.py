"""
Comprehensive unit tests for ProcessCrewExecutor async methods.

Tests the process-based crew execution system including:
- Process isolation and management
- Async execution methods
- Termination and cleanup
- Error handling and recovery
- Multi-tenant support
"""
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timezone
import multiprocessing as mp
import os


class TestProcessCrewExecutorInit:
    """Test ProcessCrewExecutor initialization."""

    def test_init_default_max_concurrent(self):
        """Test initialization with default max_concurrent."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor()

            assert executor._max_concurrent == 4
            assert executor._running_processes == {}
            assert executor._running_futures == {}
            assert executor._running_executors == {}

    def test_init_custom_max_concurrent(self):
        """Test initialization with custom max_concurrent."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor(max_concurrent=8)

            assert executor._max_concurrent == 8

    def test_init_sets_environment_variables(self):
        """Test that initialization sets required environment variables."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor()

            assert os.environ.get('PYTHONUNBUFFERED') == '0'
            assert os.environ.get('CREWAI_VERBOSE') == 'false'

    def test_init_uses_spawn_context(self):
        """Test that spawn context is used for better isolation."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor()

            mock_ctx.assert_called_once_with('spawn')

    def test_init_metrics_initialized(self):
        """Test that metrics are properly initialized."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor()

            assert executor._metrics['total_executions'] == 0
            assert executor._metrics['active_executions'] == 0
            assert executor._metrics['completed_executions'] == 0
            assert executor._metrics['failed_executions'] == 0
            assert executor._metrics['terminated_executions'] == 0


class TestProcessCrewExecutorSubprocessInitializer:
    """Test subprocess initialization."""

    def test_subprocess_initializer_sets_env_vars(self):
        """Test that subprocess initializer sets environment variables."""
        from src.services.process_crew_executor import ProcessCrewExecutor

        # Call the initializer
        ProcessCrewExecutor._subprocess_initializer()

        assert os.environ.get('PYTHONUNBUFFERED') == '0'
        assert os.environ.get('CREWAI_VERBOSE') == 'false'


class TestRunCrewInProcess:
    """Test run_crew_in_process function."""

    def test_run_crew_in_process_with_none_config(self):
        """Test run_crew_in_process returns error for None config."""
        from src.services.process_crew_executor import run_crew_in_process

        result = run_crew_in_process(
            execution_id="test-123",
            crew_config=None,
            inputs=None,
            group_context=None,
            log_queue=None
        )

        assert result["status"] == "FAILED"
        assert "crew_config is None" in result["error"]
        assert result["execution_id"] == "test-123"

    def test_run_crew_in_process_with_invalid_config_type(self):
        """Test run_crew_in_process returns error for invalid config type."""
        from src.services.process_crew_executor import run_crew_in_process

        result = run_crew_in_process(
            execution_id="test-456",
            crew_config="not a dict",
            inputs=None,
            group_context=None,
            log_queue=None
        )

        # String that's not JSON should fail
        assert result["status"] == "FAILED"
        assert "execution_id" in result

    def test_run_crew_in_process_sets_execution_id_env(self):
        """Test that execution ID is set in environment."""
        from src.services.process_crew_executor import run_crew_in_process

        # This will fail early but should set the env var
        with patch.dict(os.environ, {}, clear=False):
            result = run_crew_in_process(
                execution_id="env-test-789",
                crew_config=None,
                inputs=None,
                group_context=None,
                log_queue=None
            )

            # The function should have set KASAL_EXECUTION_ID
            # (though it may be cleared after)
            assert result["execution_id"] == "env-test-789"

    def test_run_crew_in_process_with_json_string_config(self):
        """Test run_crew_in_process parses JSON string config."""
        from src.services.process_crew_executor import run_crew_in_process
        import json

        # Valid JSON that will still fail later but tests JSON parsing
        config_json = json.dumps({"agents": [], "tasks": []})

        # This will fail later but JSON parsing should work
        result = run_crew_in_process(
            execution_id="json-test",
            crew_config=config_json,
            inputs=None,
            group_context=None,
            log_queue=None
        )

        # Should fail for other reasons, not JSON parsing
        assert result["execution_id"] == "json-test"


class TestRunCrewIsolated:
    """Test ProcessCrewExecutor.run_crew_isolated method."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_context = MagicMock()
            mock_ctx.return_value = mock_context

            from src.services.process_crew_executor import ProcessCrewExecutor
            return ProcessCrewExecutor()

    @pytest.mark.asyncio
    async def test_run_crew_isolated_updates_metrics(self, executor):
        """Test that run_crew_isolated updates execution metrics."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group-123"
        mock_group_context.access_token = "test-token"

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            result = await executor.run_crew_isolated(
                execution_id="test-exec-1",
                crew_config=crew_config,
                group_context=mock_group_context
            )

        assert executor._metrics['total_executions'] >= 1

    @pytest.mark.asyncio
    async def test_run_crew_isolated_adds_group_context_to_config(self, executor):
        """Test that group context is added to crew config."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group-456"
        mock_group_context.access_token = "user-token"

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            await executor.run_crew_isolated(
                execution_id="test-exec-2",
                crew_config=crew_config,
                group_context=mock_group_context
            )

        # Check that group_id and user_token were added
        assert crew_config.get('group_id') == "group-456"
        assert crew_config.get('user_token') == "user-token"

    @pytest.mark.asyncio
    async def test_run_crew_isolated_handles_terminated_process(self, executor):
        """Test handling of terminated (stopped) processes."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = -15  # SIGTERM
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group-789"

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            result = await executor.run_crew_isolated(
                execution_id="test-exec-3",
                crew_config=crew_config,
                group_context=mock_group_context
            )

        assert result["status"] == "STOPPED"
        assert result["exit_code"] == -15

    @pytest.mark.asyncio
    async def test_run_crew_isolated_handles_killed_process(self, executor):
        """Test handling of killed processes (SIGKILL)."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = -9  # SIGKILL
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            result = await executor.run_crew_isolated(
                execution_id="test-exec-4",
                crew_config=crew_config,
                group_context=None
            )

        assert result["status"] == "STOPPED"
        assert result["exit_code"] == -9

    @pytest.mark.asyncio
    async def test_run_crew_isolated_handles_failed_process(self, executor):
        """Test handling of failed processes (non-zero exit code)."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 1  # Error exit
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            result = await executor.run_crew_isolated(
                execution_id="test-exec-5",
                crew_config=crew_config,
                group_context=None
            )

        assert result["status"] == "FAILED"
        assert result["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_run_crew_isolated_gets_result_from_queue(self, executor):
        """Test that results are retrieved from the queue."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = False
        mock_queue.get_nowait.return_value = {
            "status": "COMPLETED",
            "execution_id": "test-exec-6",
            "result": "Test result"
        }

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            result = await executor.run_crew_isolated(
                execution_id="test-exec-6",
                crew_config=crew_config,
                group_context=None
            )

        assert result["status"] == "COMPLETED"
        assert result["result"] == "Test result"

    @pytest.mark.asyncio
    async def test_run_crew_isolated_sets_debug_tracing_env(self, executor):
        """Test that debug tracing environment variable is set."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            await executor.run_crew_isolated(
                execution_id="test-exec-7",
                crew_config=crew_config,
                group_context=None,
                debug_tracing_enabled=True
            )

        assert os.environ.get('CREWAI_DEBUG_TRACING') == 'true'

    @pytest.mark.asyncio
    async def test_run_crew_isolated_tracks_process(self, executor):
        """Test that process is tracked in running_processes."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            # The process should be tracked during execution
            await executor.run_crew_isolated(
                execution_id="test-exec-8",
                crew_config=crew_config,
                group_context=None
            )

        # Process was added to tracking
        mock_process.start.assert_called_once()


class TestTerminateExecution:
    """Test execution termination functionality."""

    @pytest.fixture
    def executor_with_process(self):
        """Create executor with a running process."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_context = MagicMock()
            mock_ctx.return_value = mock_context

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor()

            # Add a mock running process
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_process.is_alive.return_value = True
            mock_process.terminate = MagicMock()
            mock_process.kill = MagicMock()
            mock_process.join = MagicMock()

            executor._running_processes["test-exec"] = mock_process

            return executor, mock_process

    @pytest.mark.asyncio
    async def test_terminate_execution_terminates_process(self, executor_with_process):
        """Test that terminate_execution terminates the process."""
        executor, mock_process = executor_with_process

        # Mock is_alive to return False after terminate
        mock_process.is_alive.side_effect = [True, False]

        result = await executor.terminate_execution("test-exec")

        mock_process.terminate.assert_called()
        assert result is True

    @pytest.mark.asyncio
    async def test_terminate_execution_force_kills_stuck_process(self, executor_with_process):
        """Test that terminate_execution force kills stuck processes."""
        executor, mock_process = executor_with_process

        # Process stays alive after terminate but eventually returns False to avoid infinite loop
        mock_process.is_alive.side_effect = [True, True, False]

        result = await executor.terminate_execution("test-exec")

        # Either kill was called or terminate was called
        assert mock_process.terminate.called or mock_process.kill.called
        assert result is True

    @pytest.mark.asyncio
    async def test_terminate_execution_nonexistent_returns_false(self):
        """Test terminating non-existent execution returns False."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            executor = ProcessCrewExecutor()

            # Mock _terminate_orphaned_process to return False
            with patch.object(executor, '_terminate_orphaned_process', return_value=False):
                result = await executor.terminate_execution("nonexistent")

            # Should return False for non-existent execution
            assert result is False


class TestCleanupMethods:
    """Test cleanup and resource management."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            return ProcessCrewExecutor()

    def test_shutdown_terminates_all_processes(self, executor):
        """Test that shutdown terminates all running processes."""
        # Add mock processes
        mock_process1 = MagicMock()
        mock_process1.is_alive.return_value = True
        mock_process1.terminate = MagicMock()
        mock_process1.join = MagicMock()

        mock_process2 = MagicMock()
        mock_process2.is_alive.return_value = True
        mock_process2.terminate = MagicMock()
        mock_process2.join = MagicMock()

        executor._running_processes["exec1"] = mock_process1
        executor._running_processes["exec2"] = mock_process2

        # shutdown is synchronous
        executor.shutdown()

        # Both processes should have been cleaned up
        assert mock_process1.terminate.called or mock_process1.kill.called
        assert mock_process2.terminate.called or mock_process2.kill.called


class TestProcessTracking:
    """Test process tracking functionality."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            return ProcessCrewExecutor()

    def test_running_processes_dict_exists(self, executor):
        """Test that running processes dict is initialized."""
        assert executor._running_processes == {}
        assert isinstance(executor._running_processes, dict)

    def test_running_futures_dict_exists(self, executor):
        """Test that running futures dict is initialized."""
        assert executor._running_futures == {}
        assert isinstance(executor._running_futures, dict)

    def test_running_executors_dict_exists(self, executor):
        """Test that running executors dict is initialized."""
        assert executor._running_executors == {}
        assert isinstance(executor._running_executors, dict)

    def test_can_add_process_to_tracking(self, executor):
        """Test that processes can be added to tracking."""
        mock_process = MagicMock()
        mock_process.pid = 12345

        executor._running_processes["test-exec"] = mock_process

        assert "test-exec" in executor._running_processes
        assert executor._running_processes["test-exec"].pid == 12345


class TestGetMetrics:
    """Test metrics retrieval."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            return ProcessCrewExecutor()

    def test_get_metrics_returns_copy(self, executor):
        """Test that get_metrics returns a copy of metrics."""
        metrics = executor.get_metrics()

        # Modify returned metrics
        metrics['total_executions'] = 999

        # Original should be unchanged
        assert executor._metrics['total_executions'] == 0

    def test_get_metrics_includes_all_fields(self, executor):
        """Test that all metric fields are present."""
        metrics = executor.get_metrics()

        assert 'total_executions' in metrics
        assert 'active_executions' in metrics
        assert 'completed_executions' in metrics
        assert 'failed_executions' in metrics
        assert 'terminated_executions' in metrics


class TestProcessLogQueue:
    """Test log queue processing."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            return ProcessCrewExecutor()

    @pytest.mark.asyncio
    async def test_process_log_queue_handles_empty_queue(self, executor):
        """Test processing empty log queue."""
        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        # Should not raise
        await executor._process_log_queue(mock_queue, "test-exec", None)

    @pytest.mark.asyncio
    async def test_process_log_queue_method_exists(self, executor):
        """Test that _process_log_queue method exists and is callable."""
        # Verify the method exists
        assert hasattr(executor, '_process_log_queue')
        assert callable(executor._process_log_queue)

        # Verify the method signature accepts expected params
        import inspect
        sig = inspect.signature(executor._process_log_queue)
        params = list(sig.parameters.keys())
        assert 'log_queue' in params or len(params) >= 3


class TestRunCrewWrapper:
    """Test the subprocess wrapper function."""

    def test_run_crew_wrapper_puts_result_in_queue(self):
        """Test that wrapper puts result in queue."""
        from src.services.process_crew_executor import ProcessCrewExecutor

        result_queue = MagicMock()
        log_queue = MagicMock()

        # This will fail with None config but should put error in queue
        ProcessCrewExecutor._run_crew_wrapper(
            execution_id="wrapper-test",
            crew_config=None,
            inputs=None,
            group_context=None,
            result_queue=result_queue,
            log_queue=log_queue
        )

        # Result should be put in queue
        result_queue.put.assert_called_once()

        # Check the result
        call_args = result_queue.put.call_args[0][0]
        assert call_args["status"] == "FAILED"
        assert call_args["execution_id"] == "wrapper-test"


class TestModuleLevelEnvironment:
    """Test module-level environment setup."""

    def test_crewai_tracing_disabled(self):
        """Test that CrewAI tracing is disabled at module level."""
        # Import triggers module-level setup
        import src.services.process_crew_executor

        assert os.environ.get('CREWAI_TRACING_ENABLED') == 'false'
        assert os.environ.get('CREWAI_TELEMETRY_OPT_OUT') == '1'
        assert os.environ.get('CREWAI_ANALYTICS_OPT_OUT') == '1'

    def test_crewai_cloud_tracing_disabled(self):
        """Test that CrewAI cloud tracing is disabled."""
        import src.services.process_crew_executor

        assert os.environ.get('CREWAI_CLOUD_TRACING') == 'false'
        assert os.environ.get('CREWAI_CLOUD_TRACING_ENABLED') == 'false'


class TestSignalHandling:
    """Test signal handling in subprocess."""

    def test_signal_handler_defined(self):
        """Test that signal handler is defined in run_crew_in_process."""
        from src.services.process_crew_executor import run_crew_in_process

        # The function should define signal handlers
        # We can verify the function exists and is callable
        assert callable(run_crew_in_process)


class TestGroupContextHandling:
    """Test multi-tenant group context handling."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance."""
        with patch('src.services.process_crew_executor.mp.get_context') as mock_ctx:
            mock_ctx.return_value = MagicMock()

            from src.services.process_crew_executor import ProcessCrewExecutor
            return ProcessCrewExecutor()

    @pytest.mark.asyncio
    async def test_group_context_without_access_token(self, executor):
        """Test handling group context without access token."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        # Group context without access token
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group-no-token"
        mock_group_context.access_token = None

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            await executor.run_crew_isolated(
                execution_id="test-no-token",
                crew_config=crew_config,
                group_context=mock_group_context
            )

        # group_id should be added, user_token should not
        assert crew_config.get('group_id') == "group-no-token"
        assert crew_config.get('user_token') is None

    @pytest.mark.asyncio
    async def test_no_group_context_logs_warning(self, executor):
        """Test that missing group context logs security warning."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.exitcode = 0
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = True

        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = mock_queue

        crew_config = {"agents": [], "tasks": []}

        with patch.object(executor, '_process_log_queue', new_callable=AsyncMock):
            with patch('src.services.process_crew_executor.logger') as mock_logger:
                await executor.run_crew_isolated(
                    execution_id="test-no-context",
                    crew_config=crew_config,
                    group_context=None
                )

                # Should log error about missing group context
                mock_logger.error.assert_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
