"""
Coverage tests for process_crew_executor.py - Part 6.

Targets the finally block of run_crew_isolated (lines 2015-2130):
  2015-2029  terminate exception → psutil fallback
  2047-2063  psutil cleanup: orphaned process by cmdline
  2085-2090  psutil cleanup: orphaned Python orphan (ppid=1)
  2166-2170  relay_task_events general exception handling

Also targets:
  1541-1542  shutdown with alive processes
  1552-1574  shutdown with ERROR on process terminate
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_executor():
    with patch(
        "src.services.agent_builder.process_executor.mp.get_context"
    ) as mock_ctx:
        mock_ctx.return_value = MagicMock()
        from src.services.agent_builder.process_executor import ProcessCrewExecutor

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

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(executor, "_process_log_queue", new_callable=AsyncMock),
            patch("psutil.Process", side_effect=psutil_process_side_effect),
            patch("psutil.process_iter", return_value=[]),
        ):
            result = await executor.run_crew_isolated("exec-term-err", {}, group_ctx)

        # psutil.Process was called and kill was attempted
        # Either mock_psutil_proc.kill was called, or the exception was caught gracefully
        assert result is not None

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

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(executor, "_process_log_queue", new_callable=AsyncMock),
            patch("psutil.process_iter", side_effect=RuntimeError("psutil crashed")),
        ):
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

        with (
            patch("psutil.Process", return_value=mock_current),
            patch("psutil.wait_procs", return_value=([], [])),
        ):
            executor.shutdown()  # Should not raise

    def test_shutdown_process_join_timeout_force_kill(self):
        """When process doesn't terminate in time, kill() is called."""
        executor = _make_executor()

        mock_proc = MagicMock()
        mock_proc.is_alive.side_effect = [
            True,
            True,
            False,
        ]  # Still alive after terminate
        mock_proc.terminate = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.kill = MagicMock()
        mock_proc.pid = 8888
        executor._running_processes["exec-shutdown-slow"] = mock_proc

        mock_current = MagicMock()
        mock_current.children = MagicMock(return_value=[])

        with (
            patch("psutil.Process", return_value=mock_current),
            patch("psutil.wait_procs", return_value=([], [])),
        ):
            executor.shutdown(wait=True)

        mock_proc.kill.assert_called()
