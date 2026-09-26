"""
The finally block of run_crew_isolated, and shutdown's error handling.

Cleanup stops only the tracked tree (``terminate_process_tree``); nothing here
scans the host, so there is no ``psutil.process_iter`` to patch.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import psutil
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
    """A failure while stopping the tree must not turn a finished run into an error."""

    @staticmethod
    def _executor(pid):
        executor = _make_executor()
        mock_process = MagicMock()
        mock_process.pid = pid
        mock_process.exitcode = 0
        mock_process.is_alive = MagicMock(return_value=False)
        mock_q = MagicMock()
        mock_q.get = MagicMock(return_value={"status": "COMPLETED"})
        executor._ctx.Queue = MagicMock(side_effect=[mock_q, MagicMock()])
        executor._ctx.Process = MagicMock(return_value=mock_process)
        return executor, mock_process

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error", [OSError("no permission"), psutil.AccessDenied(12345)]
    )
    async def test_stop_failure_in_finally_is_logged_not_raised(self, error):
        executor, mock_process = self._executor(12345)
        group_ctx = MagicMock(primary_group_id="grp", access_token=None)

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(executor, "_process_log_queue", new_callable=AsyncMock),
            patch(
                "src.services.agent_builder.process_executor.terminate_process_tree",
                side_effect=error,
            ) as stop_tree,
        ):
            result = await executor.run_crew_isolated("exec-term-err", {}, group_ctx)

        stop_tree.assert_called_once_with(mock_process)
        assert result["status"] == "COMPLETED"
        assert "exec-term-err" not in executor._running_processes


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
