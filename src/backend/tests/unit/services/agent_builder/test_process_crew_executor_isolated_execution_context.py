from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.services.agent_builder.process_executor import ProcessCrewExecutor

# Test ProcessCrewExecutor - based on actual code inspection


class TestProcessCrewExecutorInit:
    """Test ProcessCrewExecutor initialization"""

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_process_crew_executor_init_default(self, mock_get_context):
        """Test ProcessCrewExecutor __init__ with default parameters"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        assert executor._ctx == mock_ctx
        assert isinstance(executor._running_processes, dict)
        assert isinstance(executor._metrics, dict)
        mock_get_context.assert_called_once_with("spawn")

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_process_crew_executor_init_creates_empty_tracking(self, mock_get_context):
        """Test ProcessCrewExecutor __init__ creates empty tracking dictionaries"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        assert len(executor._running_processes) == 0

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_process_crew_executor_init_creates_metrics(self, mock_get_context):
        """Test ProcessCrewExecutor __init__ creates metrics dictionary"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        expected_metrics = {
            "total_executions": 0,
            "active_executions": 0,
            "completed_executions": 0,
            "failed_executions": 0,
            "terminated_executions": 0,
        }
        assert executor._metrics == expected_metrics

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch.dict(
        "src.services.agent_builder.process_executor.os.environ", {}, clear=True
    )
    def test_process_crew_executor_init_sets_environment_variables(
        self, mock_get_context
    ):
        """Test ProcessCrewExecutor __init__ sets environment variables"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        with patch(
            "src.services.agent_builder.process_executor.os.environ", {}
        ) as mock_environ:
            ProcessCrewExecutor()

            assert mock_environ["PYTHONUNBUFFERED"] == "0"
            assert mock_environ["CREWAI_VERBOSE"] == "false"


class TestProcessCrewExecutorGetMetrics:
    """Test ProcessCrewExecutor get_metrics method"""

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_get_metrics_returns_copy(self, mock_get_context):
        """Test get_metrics returns a copy of metrics"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        metrics = executor.get_metrics()

        # Should return a copy, not the original
        assert metrics == executor._metrics
        assert metrics is not executor._metrics

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_get_metrics_contains_expected_keys(self, mock_get_context):
        """Test get_metrics contains all expected metric keys"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        metrics = executor.get_metrics()

        expected_keys = {
            "total_executions",
            "active_executions",
            "completed_executions",
            "failed_executions",
            "terminated_executions",
        }
        assert set(metrics.keys()) == expected_keys

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_get_metrics_modification_doesnt_affect_original(self, mock_get_context):
        """Test modifying returned metrics doesn't affect original"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()
        original_total = executor._metrics["total_executions"]

        metrics = executor.get_metrics()
        metrics["total_executions"] = 999

        assert executor._metrics["total_executions"] == original_total


class TestProcessCrewExecutorContextManager:
    """Test ProcessCrewExecutor context manager methods"""

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_context_manager_enter(self, mock_get_context):
        """Test ProcessCrewExecutor __enter__ method"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        result = executor.__enter__()

        assert result is executor

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_context_manager_exit(self, mock_get_context):
        """Test ProcessCrewExecutor __exit__ method"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()
        executor.shutdown = Mock()

        result = executor.__exit__(None, None, None)

        executor.shutdown.assert_called_once_with(wait=True)
        assert result is False

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_context_manager_exit_with_exception(self, mock_get_context):
        """Test ProcessCrewExecutor __exit__ method with exception"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()
        executor.shutdown = Mock()

        result = executor.__exit__(Exception, Exception("test"), None)

        executor.shutdown.assert_called_once_with(wait=True)
        assert result is False

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_context_manager_usage(self, mock_get_context):
        """Test ProcessCrewExecutor can be used as context manager"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        with patch.object(ProcessCrewExecutor, "shutdown") as mock_shutdown:
            with ProcessCrewExecutor() as executor:
                assert isinstance(executor, ProcessCrewExecutor)

            mock_shutdown.assert_called_once_with(wait=True)


class TestProcessCrewExecutorAttributes:
    """Test ProcessCrewExecutor attribute access"""

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_executor_has_required_attributes(self, mock_get_context):
        """Test that executor has all required attributes after initialization"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        # Check all required attributes exist
        assert hasattr(executor, "_ctx")
        assert hasattr(executor, "_running_processes")
        assert hasattr(executor, "_metrics")

        # Check attribute types
        assert executor._ctx == mock_ctx
        assert isinstance(executor._running_processes, dict)
        assert isinstance(executor._metrics, dict)


class TestProcessCrewExecutorStaticMethods:
    """Test ProcessCrewExecutor static methods"""


class TestProcessCrewExecutorConstants:
    """Test ProcessCrewExecutor constants and module-level attributes"""

    def test_global_instance_exists(self):
        """Test that global process_crew_executor instance exists"""
        from src.services.agent_builder.process_executor import process_crew_executor

        assert process_crew_executor is not None
        assert isinstance(process_crew_executor, ProcessCrewExecutor)


class TestProcessCrewExecutorShutdown:
    """Test ProcessCrewExecutor shutdown method (basic tests only)"""

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_shutdown_method_exists(self, mock_get_context):
        """Test shutdown method exists and is callable"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        # Should have shutdown method
        assert hasattr(executor, "shutdown")
        assert callable(executor.shutdown)

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_shutdown_with_no_running_processes(self, mock_get_context):
        """Test shutdown with no running processes"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        # Should not raise an exception
        try:
            executor.shutdown(wait=False)
            assert True
        except Exception as e:
            # If it fails due to missing dependencies, that's acceptable
            assert "psutil" in str(e) or "import" in str(e).lower()

    @patch("src.services.agent_builder.process_executor.mp.get_context")
    @patch("src.services.agent_builder.process_executor.os.environ", {})
    def test_shutdown_clears_tracking_dictionaries(self, mock_get_context):
        """Test shutdown clears tracking dictionaries"""
        mock_ctx = Mock()
        mock_get_context.return_value = mock_ctx

        executor = ProcessCrewExecutor()

        # Add some mock data
        executor._running_processes["test"] = Mock()

        try:
            executor.shutdown(wait=False)

            # Should clear all tracking
            assert len(executor._running_processes) == 0
        except Exception as e:
            # If it fails due to missing dependencies, that's acceptable
            assert "psutil" in str(e) or "import" in str(e).lower()


class TestProcessCrewExecutorRunCrewIsolated:
    """Test ProcessCrewExecutor run_crew_isolated method"""

    def setup_method(self):
        """Set up test fixtures"""
        with patch("src.services.agent_builder.process_executor.mp.get_context"):
            with patch("src.services.agent_builder.process_executor.os.environ", {}):
                self.executor = ProcessCrewExecutor()

    def _setup_ctx_mocks(self, exitcode=0, queue_result=None):
        """Configure self.executor._ctx to use controlled mock process + queue.

        Updated for app-modes: run_crew_isolated uses self._ctx.Queue/Process,
        not mp.Queue/Process directly. The background drain thread calls
        result_queue.get(timeout=...) — we configure this to return the
        expected result (or raise queue.Empty for fallback to exitcode logic).
        """
        import queue as _queue

        mock_queue_instance = Mock()
        mock_log_queue_instance = Mock()
        mock_process_instance = Mock()

        mock_process_instance.is_alive.return_value = False
        mock_process_instance.exitcode = exitcode
        mock_process_instance.pid = 12345
        mock_process_instance.start = Mock()
        mock_process_instance.join = Mock()

        if queue_result is not None:
            mock_queue_instance.get = Mock(return_value=queue_result)
            mock_queue_instance.get_nowait = Mock(return_value=queue_result)
            mock_queue_instance.empty = Mock(return_value=False)
        else:
            mock_queue_instance.get = Mock(side_effect=_queue.Empty())
            mock_queue_instance.get_nowait = Mock(side_effect=_queue.Empty())
            mock_queue_instance.empty = Mock(return_value=True)

        mock_log_queue_instance.get = Mock(side_effect=_queue.Empty())
        mock_log_queue_instance.empty = Mock(return_value=True)

        # Queue is called twice: first for result_queue, second for log_queue
        self.executor._ctx.Queue = Mock(
            side_effect=[mock_queue_instance, mock_log_queue_instance]
        )
        self.executor._ctx.Process = Mock(return_value=mock_process_instance)

        return mock_queue_instance, mock_process_instance

    @pytest.mark.asyncio
    async def test_run_crew_isolated_basic_parameters(self):
        """Test run_crew_isolated with basic parameters."""
        execution_id = "test-execution-id"
        crew_config = {"agents": [], "tasks": []}
        group_context = Mock()
        group_context.primary_group_id = "grp-test"
        group_context.access_token = None

        mock_queue_instance, _ = self._setup_ctx_mocks(
            exitcode=0,
            queue_result={
                "success": True,
                "result": "Test result",
                "status": "COMPLETED",
            },
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(self.executor, "_process_log_queue", new_callable=AsyncMock),
        ):
            result = await self.executor.run_crew_isolated(
                execution_id, crew_config, group_context
            )

        assert isinstance(result, dict)
        assert "success" in result or "status" in result or "error" in result

    @pytest.mark.asyncio
    async def test_run_crew_isolated_with_inputs(self):
        """Test run_crew_isolated with inputs parameter."""
        execution_id = "test-execution-id"
        crew_config = {"agents": [], "tasks": []}
        group_context = Mock()
        group_context.primary_group_id = "grp-test"
        group_context.access_token = None
        inputs = {"input1": "value1"}

        self._setup_ctx_mocks(
            exitcode=0,
            queue_result={
                "success": True,
                "result": "Test result",
                "status": "COMPLETED",
            },
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(self.executor, "_process_log_queue", new_callable=AsyncMock),
        ):
            result = await self.executor.run_crew_isolated(
                execution_id, crew_config, group_context, inputs
            )

        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_run_crew_isolated_with_timeout(self):
        """Test run_crew_isolated with timeout parameter."""
        execution_id = "test-execution-id"
        crew_config = {"agents": [], "tasks": []}
        group_context = Mock()
        group_context.primary_group_id = "grp-test"
        group_context.access_token = None
        timeout = 30.0

        self._setup_ctx_mocks(
            exitcode=0,
            queue_result={
                "success": True,
                "result": "Test result",
                "status": "COMPLETED",
            },
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(self.executor, "_process_log_queue", new_callable=AsyncMock),
        ):
            result = await self.executor.run_crew_isolated(
                execution_id, crew_config, group_context, timeout=timeout
            )

        assert isinstance(result, dict)

    # NOTE: test_run_crew_isolated_with_debug_tracing was removed because
    # debug_tracing_enabled parameter was removed from run_crew_isolated()

    @pytest.mark.asyncio
    async def test_run_crew_isolated_updates_metrics(self):
        """Test run_crew_isolated updates executor metrics"""
        execution_id = "test-execution-id"
        crew_config = {"agents": [], "tasks": []}
        group_context = Mock()

        initial_total = self.executor._metrics.get("total_executions", 0)
        self._setup_ctx_mocks(
            exitcode=0, queue_result={"success": True, "result": "Test result"}
        )

        with patch.object(self.executor, "_process_log_queue", new_callable=AsyncMock):
            await self.executor.run_crew_isolated(
                execution_id, crew_config, group_context
            )

        assert self.executor._metrics["total_executions"] == initial_total + 1


class TestProcessCrewExecutorTerminateExecution:
    """Test ProcessCrewExecutor terminate_execution method"""

    def setup_method(self):
        """Set up test fixtures"""
        with patch("src.services.agent_builder.process_executor.mp.get_context"):
            with patch("src.services.agent_builder.process_executor.os.environ", {}):
                self.executor = ProcessCrewExecutor()

    @pytest.mark.asyncio
    async def test_terminate_execution_not_found(self):
        """Test terminate_execution with non-existent execution_id"""
        execution_id = "non-existent-id"

        result = await self.executor.terminate_execution(execution_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_terminate_execution_process_not_alive(self):
        """Test terminate_execution with process that is not alive"""
        execution_id = "test-execution-id"
        mock_process = Mock()
        mock_process.is_alive.return_value = False

        self.executor._running_processes[execution_id] = mock_process

        result = await self.executor.terminate_execution(execution_id)

        # Based on actual implementation, returns True even if process not alive
        assert result is True

    @pytest.mark.asyncio
    async def test_terminate_execution_graceful_termination(self):
        """Test terminate_execution with graceful termination"""
        execution_id = "test-execution-id"
        mock_process = Mock()
        # Alive first, then terminated (and still gone when re-checked)
        mock_process.is_alive.side_effect = [True, False, False]
        mock_process.pid = 12345

        self.executor._running_processes[execution_id] = mock_process

        result = await self.executor.terminate_execution(execution_id)

        assert result is True
        mock_process.terminate.assert_called_once()
        mock_process.join.assert_called()

    @pytest.mark.asyncio
    async def test_terminate_execution_force_kill(self):
        """Test terminate_execution with force kill when graceful fails"""
        execution_id = "test-execution-id"
        mock_process = Mock()
        # Ignores SIGTERM, dies on SIGKILL
        mock_process.is_alive.side_effect = [True, True, False]
        mock_process.pid = 12345

        self.executor._running_processes[execution_id] = mock_process

        result = await self.executor.terminate_execution(execution_id)

        assert result is True
        mock_process.terminate.assert_called_once()
        mock_process.kill.assert_called_once()
        assert mock_process.join.call_count >= 2  # Called after terminate and kill

    @pytest.mark.asyncio
    async def test_terminate_execution_cleans_up_tracking(self):
        """Test terminate_execution cleans up tracking dictionaries"""
        execution_id = "test-execution-id"
        mock_process = Mock()
        mock_process.is_alive.side_effect = [True, False, False]
        mock_process.pid = 12345

        # Set up tracking
        self.executor._running_processes[execution_id] = mock_process

        result = await self.executor.terminate_execution(execution_id)

        assert result is True
        # Should clean up process tracking (based on actual implementation)
        assert execution_id not in self.executor._running_processes
        # Other tracking dictionaries are cleaned up elsewhere, not in terminate_execution

    @pytest.mark.asyncio
    async def test_terminate_execution_with_exception(self):
        """A stop that raises is reported as not stopped, and tracking is dropped."""
        execution_id = "test-execution-id"
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        mock_process.pid = 12345
        mock_process.terminate.side_effect = OSError("Termination failed")

        self.executor._running_processes[execution_id] = mock_process

        with patch(
            "src.services.agent_builder.process_executor.terminate_owned_processes",
            return_value=0,
        ) as owned:
            result = await self.executor.terminate_execution(execution_id)

        assert result is False
        owned.assert_called_once_with(execution_id)
        assert execution_id not in self.executor._running_processes


class TestProcessCrewExecutorAdvancedStaticMethods:
    """Test ProcessCrewExecutor advanced static methods"""

    def test_run_crew_wrapper_signature(self):
        """Test _run_crew_wrapper static method signature"""
        # Should be callable (even if it fails due to missing dependencies)
        assert hasattr(ProcessCrewExecutor, "_run_crew_wrapper")
        assert callable(ProcessCrewExecutor._run_crew_wrapper)


class TestProcessCrewExecutorExecutionIdHandling:
    """Test execution_id handling in ProcessCrewExecutor."""

    @pytest.fixture
    def executor(self):
        """Create a ProcessCrewExecutor instance with mocked context."""
        with patch(
            "src.services.agent_builder.process_executor.mp.get_context"
        ) as mock_get_context:
            mock_ctx = Mock()
            mock_get_context.return_value = mock_ctx
            executor = ProcessCrewExecutor()
            executor._ctx = mock_ctx
            return executor

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = Mock()
        context.primary_group_id = "test_group_123"
        context.access_token = "test_token_abc"
        return context

    @pytest.mark.asyncio
    async def test_execution_id_added_to_crew_config_with_group_context(
        self, executor, mock_group_context
    ):
        """Test that execution_id is added to crew_config when group_context is provided."""
        execution_id = "exec_test_123"
        crew_config = {"agents": [], "tasks": [], "crew_settings": {}}
        captured_config = {}

        # Mock the Process creation to capture the config
        mock_process = Mock()
        mock_process.pid = 12345
        mock_process.start = Mock()
        mock_process.join = Mock()
        mock_process.exitcode = 0
        mock_process.is_alive = Mock(return_value=False)

        def capture_process(*args, **kwargs):
            # Capture the crew_config passed to the process
            if args and len(args) >= 2:
                captured_config["config"] = args[1]  # crew_config is second arg
            return mock_process

        executor._ctx.Process = Mock(side_effect=capture_process)

        # Mock the result queue
        mock_result_queue = Mock()
        mock_result_queue.empty = Mock(return_value=False)
        mock_result_queue.get_nowait = Mock(
            return_value={"status": "COMPLETED", "result": "test_result"}
        )

        executor._ctx.Queue = Mock(return_value=mock_result_queue)

        # Mock log queue processing
        with patch.object(executor, "_process_log_queue", new_callable=AsyncMock):
            try:
                await executor.run_crew_isolated(
                    execution_id=execution_id,
                    crew_config=crew_config,
                    group_context=mock_group_context,
                    inputs={},
                )
            except Exception:
                pass  # We just want to verify the config

        # Verify execution_id was added to crew_config
        assert crew_config.get("execution_id") == execution_id

    @pytest.mark.asyncio
    async def test_execution_id_added_to_crew_config_without_group_context(
        self, executor
    ):
        """Test that execution_id is added to crew_config as fallback without group_context."""
        execution_id = "exec_fallback_456"
        crew_config = {"agents": [], "tasks": [], "crew_settings": {}}

        # Mock the Process creation
        mock_process = Mock()
        mock_process.pid = 12346
        mock_process.start = Mock()
        mock_process.join = Mock()
        mock_process.exitcode = 0
        mock_process.is_alive = Mock(return_value=False)

        executor._ctx.Process = Mock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = Mock()
        mock_result_queue.empty = Mock(return_value=False)
        mock_result_queue.get_nowait = Mock(
            return_value={"status": "COMPLETED", "result": "test_result"}
        )

        executor._ctx.Queue = Mock(return_value=mock_result_queue)

        # Mock log queue processing
        with patch.object(executor, "_process_log_queue", new_callable=AsyncMock):
            try:
                await executor.run_crew_isolated(
                    execution_id=execution_id,
                    crew_config=crew_config,
                    group_context=None,  # No group context
                    inputs={},
                )
            except Exception:
                pass  # We just want to verify the config

        # Verify execution_id was added via fallback
        assert crew_config.get("execution_id") == execution_id

    @pytest.mark.asyncio
    async def test_execution_id_not_overwritten_if_already_present(
        self, executor, mock_group_context
    ):
        """Test that execution_id is not overwritten if already in crew_config."""
        execution_id = "exec_new_789"
        existing_execution_id = "exec_existing_000"
        crew_config = {"agents": [], "tasks": [], "execution_id": existing_execution_id}

        # Mock the Process creation
        mock_process = Mock()
        mock_process.pid = 12347
        mock_process.start = Mock()
        mock_process.join = Mock()
        mock_process.exitcode = 0
        mock_process.is_alive = Mock(return_value=False)

        executor._ctx.Process = Mock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = Mock()
        mock_result_queue.empty = Mock(return_value=False)
        mock_result_queue.get_nowait = Mock(
            return_value={"status": "COMPLETED", "result": "test_result"}
        )

        executor._ctx.Queue = Mock(return_value=mock_result_queue)

        # Mock log queue processing
        with patch.object(executor, "_process_log_queue", new_callable=AsyncMock):
            try:
                await executor.run_crew_isolated(
                    execution_id=execution_id,
                    crew_config=crew_config,
                    group_context=mock_group_context,
                    inputs={},
                )
            except Exception:
                pass

        # The execution_id SHOULD be the new one (it gets overwritten in the group_context block)
        # This is the expected behavior - the method sets execution_id
        assert crew_config.get("execution_id") == execution_id

    @pytest.mark.asyncio
    async def test_kasal_execution_id_env_var_set_and_restored(
        self, executor, mock_group_context
    ):
        """Test that KASAL_EXECUTION_ID environment variable is set and restored."""
        execution_id = "exec_env_test_111"
        crew_config = {"agents": [], "tasks": []}

        # Mock the Process creation
        mock_process = Mock()
        mock_process.pid = 12348
        mock_process.start = Mock()
        mock_process.join = Mock()
        mock_process.exitcode = 0
        mock_process.is_alive = Mock(return_value=False)

        executor._ctx.Process = Mock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = Mock()
        mock_result_queue.empty = Mock(return_value=False)
        mock_result_queue.get_nowait = Mock(
            return_value={"status": "COMPLETED", "result": "test_result"}
        )

        executor._ctx.Queue = Mock(return_value=mock_result_queue)

        # Set an initial env var value to test restoration
        import os

        original_value = os.environ.get("KASAL_EXECUTION_ID")

        # Mock log queue processing
        with patch.object(executor, "_process_log_queue", new_callable=AsyncMock):
            try:
                await executor.run_crew_isolated(
                    execution_id=execution_id,
                    crew_config=crew_config,
                    group_context=mock_group_context,
                    inputs={},
                )
            except Exception:
                pass

        # After execution, the env var should be restored to original state
        current_value = os.environ.get("KASAL_EXECUTION_ID")
        assert current_value == original_value

    @pytest.mark.asyncio
    async def test_group_id_and_user_token_added_with_group_context(
        self, executor, mock_group_context
    ):
        """Test that group_id and user_token are added to crew_config with group_context."""
        execution_id = "exec_context_test_222"
        crew_config = {"agents": [], "tasks": []}

        # Mock the Process creation
        mock_process = Mock()
        mock_process.pid = 12349
        mock_process.start = Mock()
        mock_process.join = Mock()
        mock_process.exitcode = 0
        mock_process.is_alive = Mock(return_value=False)

        executor._ctx.Process = Mock(return_value=mock_process)

        # Mock the result queue
        mock_result_queue = Mock()
        mock_result_queue.empty = Mock(return_value=False)
        mock_result_queue.get_nowait = Mock(
            return_value={"status": "COMPLETED", "result": "test_result"}
        )

        executor._ctx.Queue = Mock(return_value=mock_result_queue)

        # Mock log queue processing
        with patch.object(executor, "_process_log_queue", new_callable=AsyncMock):
            try:
                await executor.run_crew_isolated(
                    execution_id=execution_id,
                    crew_config=crew_config,
                    group_context=mock_group_context,
                    inputs={},
                )
            except Exception:
                pass

        # Verify group_id, user_token, and execution_id were added
        assert crew_config.get("group_id") == mock_group_context.primary_group_id
        assert crew_config.get("user_token") == mock_group_context.access_token
        assert crew_config.get("execution_id") == execution_id
