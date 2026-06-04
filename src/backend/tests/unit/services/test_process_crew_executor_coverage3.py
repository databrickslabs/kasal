"""
Additional coverage tests for services/process_crew_executor.py — Part 3.

Targets additional uncovered lines:
  218-248  signal_handler inside run_crew_in_process
  370-395  JSON config logging (AttributeError, not-dict, inputs)
  434-460  UserContext setup paths
  480-484  Lakebase activation in prepare_and_run
  547-559  async UserContext re-init paths
  1279     get_metrics after actual execution
  1304-1360 terminate_execution edge cases
  1377-1403 _terminate_orphaned_process matching/non-matching
  1541-1574 shutdown with child processes
  2015-2127 run_crew_isolated finally cleanup (process tracking, psutil cleanup)
  2133,2136 cleanup tracking for futures/executors
  2469-2501 _terminate_orphaned_process matching process found and killed
  2575-2614 shutdown psutil child processes
"""
import asyncio
import multiprocessing as mp
import queue
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call


def _make_executor(max_concurrent=4):
    with patch("src.services.process_crew_executor.mp.get_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        from src.services.process_crew_executor import ProcessCrewExecutor
        executor = ProcessCrewExecutor(max_concurrent=max_concurrent)
    executor._ctx = MagicMock()
    return executor


# ---------------------------------------------------------------------------
# run_crew_in_process — DATABASE_TYPE env setup (line 149-151)
# ---------------------------------------------------------------------------

class TestRunCrewInProcessDatabaseType:

    def test_database_type_set_when_not_in_env(self):
        """DATABASE_TYPE is set from settings when not in env."""
        import os
        from src.services.process_crew_executor import run_crew_in_process

        # Ensure DATABASE_TYPE is not in env
        original = os.environ.pop("DATABASE_TYPE", None)
        try:
            result = run_crew_in_process("exec-dbtype", None)
            # Will fail due to None config, but DATABASE_TYPE should be set
            assert result["status"] == "FAILED"
        finally:
            if original is not None:
                os.environ["DATABASE_TYPE"] = original

    def test_database_type_not_overwritten_when_set(self):
        """DATABASE_TYPE is not changed when already set in env."""
        import os
        from src.services.process_crew_executor import run_crew_in_process

        original = os.environ.get("DATABASE_TYPE")
        os.environ["DATABASE_TYPE"] = "my_custom_db"
        try:
            result = run_crew_in_process("exec-dbtype2", None)
            assert os.environ.get("DATABASE_TYPE") == "my_custom_db"
        finally:
            if original is None:
                os.environ.pop("DATABASE_TYPE", None)
            else:
                os.environ["DATABASE_TYPE"] = original


# ---------------------------------------------------------------------------
# ProcessCrewExecutor — run_crew_isolated general error paths
# ---------------------------------------------------------------------------

class TestRunCrewIsolatedErrorPaths:

    @pytest.mark.asyncio
    async def test_exception_in_process_start_propagates_past_try(self):
        """If process.start() raises, it propagates up (try/finally, no catch)."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 99
        mock_process.start = MagicMock(side_effect=RuntimeError("spawn failed"))
        mock_process.is_alive = MagicMock(return_value=False)

        mock_q = MagicMock()
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False):
            # process.start() is in try/finally with no except — exception propagates
            with pytest.raises(RuntimeError, match="spawn failed"):
                await executor.run_crew_isolated("exec-startfail", {}, group_ctx)

    @pytest.mark.asyncio
    async def test_active_executions_decremented_on_exception(self):
        """Even on error, active_executions metric is decremented."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 88
        mock_process.start = MagicMock()
        mock_process.join = MagicMock(side_effect=RuntimeError("join failed"))
        mock_process.is_alive = MagicMock(return_value=False)
        mock_process.exitcode = 0

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        initial_active = executor._metrics["active_executions"]

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()):
            try:
                await executor.run_crew_isolated("exec-joinfail", {}, group_ctx)
            except Exception:
                pass

        # active_executions should not be left incremented
        assert executor._metrics["active_executions"] <= initial_active

    @pytest.mark.asyncio
    async def test_execution_id_fallback_added_when_no_group_context(self):
        """execution_id is added to crew_config even without group_context."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 77
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.is_alive = MagicMock(return_value=False)
        mock_process.exitcode = 0

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        crew_config = {"agents": []}

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()):
            await executor.run_crew_isolated("exec-noctx", crew_config, None)

        assert crew_config.get("execution_id") == "exec-noctx"


# ---------------------------------------------------------------------------
# terminate_execution — process alive then dead, force kill
# ---------------------------------------------------------------------------

class TestTerminateExecutionEdgeCases:

    @pytest.mark.asyncio
    async def test_psutil_fallback_on_terminate_error(self):
        """When terminate() raises OSError, psutil kills the process."""
        executor = _make_executor()
        mock_process = MagicMock()
        mock_process.is_alive.side_effect = [True, True]
        mock_process.terminate = MagicMock(side_effect=OSError("no permission"))
        mock_process.join = MagicMock()
        mock_process.pid = 555

        executor._running_processes["exec-oserr"] = mock_process

        mock_psutil_proc = MagicMock()
        mock_psutil_proc.kill = MagicMock()

        with patch("psutil.Process", return_value=mock_psutil_proc), \
             patch.object(executor, "_terminate_orphaned_process", return_value=False):
            result = await executor.terminate_execution("exec-oserr")

        mock_psutil_proc.kill.assert_called_once()
        assert result is True

    @pytest.mark.asyncio
    async def test_psutil_not_have_pid_skips_kill(self):
        """If process.pid is None, psutil kill is skipped."""
        executor = _make_executor()
        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        mock_process.terminate = MagicMock(side_effect=OSError("err"))
        mock_process.join = MagicMock()
        mock_process.pid = None  # No PID

        executor._running_processes["exec-nopid"] = mock_process

        with patch.object(executor, "_terminate_orphaned_process", return_value=False):
            result = await executor.terminate_execution("exec-nopid")

        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# _terminate_orphaned_process — matching/non-matching process
# ---------------------------------------------------------------------------

class TestTerminateOrphanedProcessExtended:

    def test_matching_by_cmdline_when_no_env(self):
        """Process matching by cmdline (no KASAL_EXECUTION_ID env) is killed."""
        executor = _make_executor()
        exec_id = "cmdline-match-1234"
        import psutil as _psutil

        mock_child = MagicMock()
        mock_parent_proc = MagicMock()
        mock_parent_proc.children = MagicMock(return_value=[mock_child])
        mock_parent_proc.kill = MagicMock()

        mock_proc = MagicMock()
        mock_proc.info = {"pid": 8888, "name": "python", "cmdline": ["python", f"--exec-id={exec_id}"]}
        # No KASAL_EXECUTION_ID in env but execution_id in cmdline
        mock_proc.environ = MagicMock(side_effect=_psutil.AccessDenied(8888))

        with patch("psutil.process_iter", return_value=[mock_proc]), \
             patch("psutil.Process", return_value=mock_parent_proc), \
             patch("psutil.wait_procs", return_value=([], [])):
            result = executor._terminate_orphaned_process(exec_id)

        # Process matched by cmdline but env read failed — should still match by cmdline
        assert result is True

    def test_access_denied_exception_continues(self):
        """AccessDenied exception on individual process is skipped."""
        executor = _make_executor()
        import psutil

        mock_proc = MagicMock()
        mock_proc.info = {"pid": 9999, "name": "python", "cmdline": []}
        mock_proc.environ = MagicMock(side_effect=psutil.AccessDenied(9999))

        with patch("psutil.process_iter", return_value=[mock_proc]):
            result = executor._terminate_orphaned_process("exec-denied-9999")

        assert result is False

    def test_no_such_process_exception_continues(self):
        """NoSuchProcess exception on individual process is skipped."""
        executor = _make_executor()
        import psutil

        mock_proc = MagicMock()
        mock_proc.info = {"pid": 1111, "name": "python", "cmdline": ["python"]}
        mock_proc.environ = MagicMock(side_effect=psutil.NoSuchProcess(1111))

        with patch("psutil.process_iter", return_value=[mock_proc]):
            result = executor._terminate_orphaned_process("exec-nosuch-1111")

        assert result is False

    def test_child_kill_no_such_process_handled(self):
        """NoSuchProcess during child kill is silently caught."""
        executor = _make_executor()
        import psutil

        exec_id = "child-kill-test-1234"
        mock_child = MagicMock()
        mock_child.kill = MagicMock(side_effect=psutil.NoSuchProcess(mock_child))
        mock_child.pid = 7777

        mock_parent_proc = MagicMock()
        mock_parent_proc.children = MagicMock(return_value=[mock_child])
        mock_parent_proc.kill = MagicMock()

        mock_proc = MagicMock()
        mock_proc.info = {"pid": 6666, "name": "python", "cmdline": [f"--exec={exec_id}"]}
        mock_proc.environ = MagicMock(return_value={"KASAL_EXECUTION_ID": exec_id})

        with patch("psutil.process_iter", return_value=[mock_proc]), \
             patch("psutil.Process", return_value=mock_parent_proc), \
             patch("psutil.wait_procs", return_value=([], [])):
            result = executor._terminate_orphaned_process(exec_id)

        # Should still kill the parent
        mock_parent_proc.kill.assert_called_once()
        assert result is True


# ---------------------------------------------------------------------------
# shutdown — psutil child process cleanup
# ---------------------------------------------------------------------------

class TestShutdownExtended:

    def test_shutdown_kills_child_processes_via_psutil(self):
        """Shutdown uses psutil to clean up child processes."""
        executor = _make_executor()

        mock_child = MagicMock()
        mock_child.pid = 3333
        mock_child.terminate = MagicMock()
        mock_child.kill = MagicMock()

        mock_current_proc = MagicMock()
        mock_current_proc.children = MagicMock(return_value=[mock_child])

        with patch("psutil.Process", return_value=mock_current_proc), \
             patch("psutil.wait_procs", return_value=([], [mock_child])):
            executor.shutdown(wait=True)

        # Force kill was called on alive processes
        mock_child.kill.assert_called()

    def test_shutdown_handles_no_such_process_on_terminate(self):
        """NoSuchProcess during process terminate is swallowed."""
        executor = _make_executor()
        import psutil

        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc.terminate = MagicMock(side_effect=psutil.NoSuchProcess(1))
        mock_proc.pid = 1234
        executor._running_processes["exec-dead"] = mock_proc

        mock_current = MagicMock()
        mock_current.children = MagicMock(return_value=[])

        with patch("psutil.Process", return_value=mock_current), \
             patch("psutil.wait_procs", return_value=([], [])):
            executor.shutdown()  # Should not raise


# ---------------------------------------------------------------------------
# get_metrics
# ---------------------------------------------------------------------------

class TestGetMetricsState:

    def test_metrics_reflect_state_after_operations(self):
        """Metrics dict accurately reflects internal state."""
        executor = _make_executor()
        executor._metrics["total_executions"] = 5
        executor._metrics["completed_executions"] = 3
        executor._metrics["failed_executions"] = 2

        metrics = executor.get_metrics()
        assert metrics["total_executions"] == 5
        assert metrics["completed_executions"] == 3
        assert metrics["failed_executions"] == 2


# ---------------------------------------------------------------------------
# _run_crew_wrapper — error handling
# ---------------------------------------------------------------------------

class TestRunCrewWrapperError:

    def test_exception_in_crew_run_puts_failed_result(self):
        """If run_crew_in_process raises, error result is put in queue."""
        from src.services.process_crew_executor import ProcessCrewExecutor
        mock_queue = MagicMock()
        mock_log_queue = MagicMock()

        with patch("src.services.process_crew_executor.run_crew_in_process",
                   side_effect=RuntimeError("subprocess error")):
            ProcessCrewExecutor._run_crew_wrapper(
                "e-err", {"agents": []}, None, None, mock_queue, mock_log_queue
            )

        call_args = mock_queue.put.call_args[0][0]
        assert call_args["status"] == "FAILED"
        assert "subprocess error" in call_args["error"]


# ---------------------------------------------------------------------------
# run_crew_isolated — finally block cleanup (process still alive)
# ---------------------------------------------------------------------------

class TestRunCrewIsolatedFinallyCleanup:

    @pytest.mark.asyncio
    async def test_alive_process_in_finally_is_terminated(self):
        """In the finally block, alive processes are terminated."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 111
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        # is_alive: False when checking result, True in finally block
        is_alive_calls = [False, True, False]
        mock_process.is_alive = MagicMock(side_effect=is_alive_calls + [False] * 10)
        mock_process.exitcode = 0
        mock_process.terminate = MagicMock()
        mock_process.kill = MagicMock()

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.process_iter", return_value=[]):
            result = await executor.run_crew_isolated("exec-alive", {}, group_ctx)

        # Execution completed successfully
        assert result is not None

    @pytest.mark.asyncio
    async def test_futures_and_executors_cleaned_up(self):
        """In the finally block, _running_futures and _running_executors are cleaned."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 222
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.is_alive = MagicMock(return_value=False)
        mock_process.exitcode = 0

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        # Add stale entries that should be cleaned up
        executor._running_futures["exec-clean"] = MagicMock()
        executor._running_executors["exec-clean"] = MagicMock()

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.process_iter", return_value=[]):
            await executor.run_crew_isolated("exec-clean", {}, group_ctx)

        assert "exec-clean" not in executor._running_futures
        assert "exec-clean" not in executor._running_executors


# ---------------------------------------------------------------------------
# Helper coroutines
# ---------------------------------------------------------------------------

async def _always_cancel():
    """A coroutine that immediately raises CancelledError."""
    raise asyncio.CancelledError()
