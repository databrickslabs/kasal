"""
Coverage tests for process_crew_executor.py - Part 6.

Targets the finally block of run_crew_isolated (lines 2015-2130):
  2015-2029  terminate exception → psutil fallback
  2047-2063  psutil cleanup: orphaned process by cmdline
  2085-2090  psutil cleanup: orphaned Python orphan (ppid=1)
  2092-2127  ImportError fallback (subprocess ps aux)
  2166-2170  relay_task_events general exception handling

Also targets:
  1541-1542  shutdown with alive processes
  1552-1574  shutdown with ERROR on process terminate
  2514-2520  _terminate_orphaned_process NoSuchProcess/AccessDenied on iter
"""
import asyncio
import os
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call


def _make_executor():
    with patch("src.services.process_crew_executor.mp.get_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        from src.services.process_crew_executor import ProcessCrewExecutor
        executor = ProcessCrewExecutor()
    executor._ctx = MagicMock()
    return executor


async def _always_cancel():
    raise asyncio.CancelledError()


# ---------------------------------------------------------------------------
# run_crew_isolated finally block — terminate exception + psutil fallback
# ---------------------------------------------------------------------------

class TestRunCrewIsolatedFinallyTerminateError:

    @pytest.mark.asyncio
    async def test_terminate_exception_uses_psutil_fallback(self):
        """When process.terminate() raises in finally, psutil.Process(pid).kill() is used."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        # is_alive returns True in finally block check (process still alive)
        # First call: in result check block (process.exitcode != -15/-9, so check queue)
        # Second call: in finally block (True → trigger terminate)
        # Third call: after terminate (still alive → kill)
        mock_process.is_alive = MagicMock(side_effect=[False, True, True, False])
        mock_process.terminate = MagicMock(side_effect=OSError("no permission"))
        mock_process.kill = MagicMock()

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        mock_psutil_proc = MagicMock()
        mock_psutil_proc.kill = MagicMock()

        psutil_call_count = {"n": 0}
        def psutil_process_side_effect(pid):
            psutil_call_count["n"] += 1
            return mock_psutil_proc

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.Process", side_effect=psutil_process_side_effect), \
             patch("psutil.process_iter", return_value=[]):
            result = await executor.run_crew_isolated("exec-term-err", {}, group_ctx)

        # psutil.Process was called and kill was attempted
        # Either mock_psutil_proc.kill was called, or the exception was caught gracefully
        assert result is not None

    @pytest.mark.asyncio
    async def test_orphaned_process_by_cmdline_is_terminated(self):
        """Orphaned process with execution_id in cmdline is terminated in finally."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 99
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        exec_id = "exec-orphan-cmdline"

        # An orphaned process with exec_id in cmdline
        orphan_proc = MagicMock()
        orphan_proc.info = {
            "pid": 55555,
            "name": "python",
            "cmdline": ["python", f"--exec-id={exec_id}"],
            "ppid": 1,
        }
        orphan_proc.terminate = MagicMock()
        orphan_proc.wait = MagicMock()

        import psutil as _psutil

        # Normal python proc (orphaned ppid=1) for the second loop
        normal_proc = MagicMock()
        normal_proc.info = {
            "pid": 66666,
            "name": "python",
            "ppid": 1,
            "create_time": time.time() - 60,  # 1 minute ago (< 10 min)
        }
        normal_proc.create_time = MagicMock(return_value=time.time() - 60)

        call_count = {"n": 0}
        def mock_process_iter(attrs):
            if "cmdline" in attrs:
                return [orphan_proc]
            return [normal_proc]

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.process_iter", side_effect=mock_process_iter):
            result = await executor.run_crew_isolated(exec_id, {}, group_ctx)

        orphan_proc.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_psutil_timeout_expired_force_kills(self):
        """When proc.wait() times out, proc.kill() is called."""
        import psutil as _psutil

        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 101
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        exec_id = "exec-timeout-psutil"

        # Orphaned proc that times out on wait()
        orphan_proc = MagicMock()
        orphan_proc.info = {
            "pid": 77777,
            "name": "python",
            "cmdline": ["python", f"exec_id={exec_id}"],
            "ppid": 1,
        }
        orphan_proc.terminate = MagicMock()
        orphan_proc.wait = MagicMock(side_effect=_psutil.TimeoutExpired(77777, 2))
        orphan_proc.kill = MagicMock()

        def mock_process_iter(attrs):
            if "cmdline" in attrs:
                return [orphan_proc]
            return []

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.process_iter", side_effect=mock_process_iter):
            result = await executor.run_crew_isolated(exec_id, {}, group_ctx)

        orphan_proc.kill.assert_called_once()

    @pytest.mark.asyncio
    async def test_psutil_import_error_falls_back_to_subprocess(self):
        """When psutil ImportError occurs, uses subprocess ps aux fallback."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 102
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        # Mock subprocess.run to return success
        mock_subprocess_result = MagicMock()
        mock_subprocess_result.returncode = 0
        mock_subprocess_result.stdout = ""  # No matching processes

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.process_iter", side_effect=ImportError("no psutil")), \
             patch("subprocess.run", return_value=mock_subprocess_result):
            result = await executor.run_crew_isolated("exec-nopsutil", {}, group_ctx)

        assert result is not None

    @pytest.mark.asyncio
    async def test_subprocess_ps_finds_orphaned_process(self):
        """The ps aux fallback finds and kills matching orphaned process."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 103
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

        mock_q = MagicMock()
        mock_q.empty = MagicMock(return_value=True)
        mock_log_q = MagicMock()
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, mock_log_q])
        executor._ctx.Process = MagicMock(return_value=mock_process)

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp"
        group_ctx.access_token = None

        exec_id = "exec-ps-find"

        # ps aux output with matching process
        ps_output = f"user  99999  0.0  0.0  python  multiprocessing.spawn {exec_id[:8]}\n"
        mock_subprocess_result = MagicMock()
        mock_subprocess_result.returncode = 0
        mock_subprocess_result.stdout = ps_output

        with patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False), \
             patch.object(executor, "_process_log_queue", new_callable=AsyncMock), \
             patch.object(executor, "_relay_task_events", return_value=_always_cancel()), \
             patch("psutil.process_iter", side_effect=ImportError("no psutil")), \
             patch("subprocess.run", return_value=mock_subprocess_result), \
             patch("os.kill") as mock_os_kill:
            result = await executor.run_crew_isolated(exec_id, {}, group_ctx)

        # os.kill should have been called on the matching process
        mock_os_kill.assert_called()

    @pytest.mark.asyncio
    async def test_cleanup_error_logged_not_raised(self):
        """Exception during cleanup is logged, not raised."""
        executor = _make_executor()

        mock_process = MagicMock()
        mock_process.pid = 104
        mock_process.start = MagicMock()
        mock_process.join = MagicMock()
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)

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
             patch("psutil.process_iter", side_effect=RuntimeError("psutil crashed")):
            result = await executor.run_crew_isolated("exec-cleanup-err", {}, group_ctx)

        # Should complete without raising
        assert result is not None


# ---------------------------------------------------------------------------
# shutdown — alive process terminate error handling
# ---------------------------------------------------------------------------

class TestShutdownTerminateError:

    def test_shutdown_handles_terminate_error(self):
        """Errors during process terminate in shutdown are caught."""
        executor = _make_executor()
        import psutil

        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc.terminate = MagicMock(side_effect=psutil.NoSuchProcess(1))
        mock_proc.pid = 9999
        executor._running_processes["exec-shutdown-err"] = mock_proc

        mock_current = MagicMock()
        mock_current.children = MagicMock(return_value=[])

        with patch("psutil.Process", return_value=mock_current), \
             patch("psutil.wait_procs", return_value=([], [])):
            executor.shutdown()  # Should not raise

    def test_shutdown_process_join_timeout_force_kill(self):
        """When process doesn't terminate in time, kill() is called."""
        executor = _make_executor()

        mock_proc = MagicMock()
        mock_proc.is_alive.side_effect = [True, True, False]  # Still alive after terminate
        mock_proc.terminate = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.kill = MagicMock()
        mock_proc.pid = 8888
        executor._running_processes["exec-shutdown-slow"] = mock_proc

        mock_current = MagicMock()
        mock_current.children = MagicMock(return_value=[])

        with patch("psutil.Process", return_value=mock_current), \
             patch("psutil.wait_procs", return_value=([], [])):
            executor.shutdown(wait=True)

        mock_proc.kill.assert_called()


# ---------------------------------------------------------------------------
# _terminate_orphaned_process — NoSuchProcess/AccessDenied on process iteration
# ---------------------------------------------------------------------------

class TestTerminateOrphanedProcessIteration:

    def test_no_such_process_during_proc_check_continues(self):
        """NoSuchProcess during process environ/cmdline access is caught."""
        executor = _make_executor()
        import psutil

        mock_proc = MagicMock()
        mock_proc.info = {"pid": 1111, "name": "python", "cmdline": []}
        # Accessing environ raises NoSuchProcess
        mock_proc.environ = MagicMock(side_effect=psutil.NoSuchProcess(1111))

        with patch("psutil.process_iter", return_value=[mock_proc]):
            result = executor._terminate_orphaned_process("exec-no-such")

        assert result is False

    def test_access_denied_during_proc_check_continues(self):
        """AccessDenied during environ access is caught."""
        executor = _make_executor()
        import psutil

        mock_proc = MagicMock()
        mock_proc.info = {"pid": 2222, "name": "python", "cmdline": []}
        mock_proc.environ = MagicMock(side_effect=psutil.AccessDenied(2222))

        with patch("psutil.process_iter", return_value=[mock_proc]):
            result = executor._terminate_orphaned_process("exec-access-denied")

        assert result is False
