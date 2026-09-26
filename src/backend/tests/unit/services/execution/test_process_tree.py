"""Stopping a run touches only the process tree this server started.

Regression for the cleanup heuristics that killed processes the server did not
own: every python process whose parent is PID 1 (a nohup'd ``mlflow server``),
anything whose ``ps`` line held ``execution_id[:8]`` plus
``multiprocessing.spawn`` (uvicorn's reload worker, the pytest controller).

These tests use REAL processes. The canary is built to match every one of the
old heuristics — parent PID 1, ``multiprocessing.spawn`` and the execution id
on its command line, and even ``KASAL_EXECUTION_ID`` set to the run's id — and
must survive every cleanup path, because it is not a descendant of this
process.
"""

import os
import signal
import subprocess
import sys
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import psutil
import pytest

from src.services.execution.process_tree import (
    find_owned_processes,
    terminate_owned_processes,
    terminate_process_tree,
)


def _wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _alive(pid: int) -> bool:
    try:
        proc = psutil.Process(pid)
        return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


@pytest.fixture
def execution_id():
    return str(uuid.uuid4())


@pytest.fixture
def unrelated_canary(execution_id):
    """A python process that is NOT a descendant of this test process.

    The shell backgrounds it and exits, so it is re-parented away from us —
    exactly the shape of a nohup'd server the old scan used to kill.
    """
    script = (
        f'KASAL_EXECUTION_ID={execution_id} "{sys.executable}" -c '
        f'"import time; time.sleep(120)" multiprocessing.spawn {execution_id} '
        "> /dev/null 2>&1 & echo $!"
    )
    out = subprocess.run(
        ["/bin/sh", "-c", script], capture_output=True, text=True, timeout=10
    )
    pid = int(out.stdout.strip())
    assert _wait_until(lambda: _alive(pid)), "canary did not start"
    assert _wait_until(lambda: psutil.Process(pid).ppid() != os.getpid())
    yield pid
    try:
        os.kill(pid, signal.SIGKILL)  # our own canary, by the pid we recorded
    except OSError:
        pass


@pytest.fixture
def tracked_tree():
    """A real spawn-context Process that itself has a child, as a run does."""
    import multiprocessing as mp

    process = mp.get_context("spawn").Process(
        target=subprocess.call, args=(["/bin/sh", "-c", "sleep 120 & wait"],)
    )
    process.start()
    assert _wait_until(
        lambda: len(psutil.Process(process.pid).children(recursive=True)) >= 2,
        timeout=15,
    ), "tracked process did not spawn its children"
    descendants = [p.pid for p in psutil.Process(process.pid).children(recursive=True)]
    yield process, descendants
    if process.is_alive():
        process.kill()
        process.join(timeout=2)


class TestOwnershipIsByProcessTree:
    def test_unrelated_process_is_never_matched(self, unrelated_canary, execution_id):
        assert find_owned_processes(execution_id) == []
        assert terminate_owned_processes(execution_id) == 0
        assert _alive(unrelated_canary)

    @pytest.mark.parametrize("bad_id", ["", None])
    def test_empty_execution_id_matches_nothing(self, bad_id):
        with patch("psutil.Process") as proc:
            assert find_owned_processes(bad_id) == []
        proc.assert_not_called()

    def test_foreign_pid_is_not_walked(self):
        """A handle whose pid is not our child (reused pid, test double) must
        not lead to signalling someone else's children."""
        handle = MagicMock(pid=1)
        handle.is_alive.side_effect = [True, False, False]
        with patch("psutil.Process") as proc:
            proc.return_value.ppid.return_value = 0
            terminate_process_tree(handle)
        proc.return_value.children.assert_not_called()
        handle.terminate.assert_called_once_with()

    def test_nothing_scans_the_host(self, execution_id):
        with patch("psutil.process_iter", side_effect=AssertionError("host scan")):
            terminate_owned_processes(execution_id)


class TestTrackedTreeIsStopped:
    def test_tracked_process_and_descendants_stop_canary_survives(
        self, tracked_tree, unrelated_canary
    ):
        process, descendants = tracked_tree

        assert terminate_process_tree(process, grace_timeout=2) is True

        assert not process.is_alive()
        assert _wait_until(lambda: not any(_alive(pid) for pid in descendants))
        assert _alive(unrelated_canary)


class TestExecutorsLeaveUnrelatedProcessesAlone:
    @pytest.mark.asyncio
    async def test_crew_stop_of_untracked_id(self, unrelated_canary, execution_id):
        from src.services.agent_builder.process_executor import ProcessCrewExecutor

        assert await ProcessCrewExecutor().terminate_execution(execution_id) is False
        assert _alive(unrelated_canary)

    @pytest.mark.asyncio
    async def test_flow_stop_of_untracked_id(self, unrelated_canary, execution_id):
        from src.services.flow_builder.process_executor import ProcessFlowExecutor

        stopped = await ProcessFlowExecutor().terminate_execution(execution_id)
        assert stopped is False
        assert _alive(unrelated_canary)

    @pytest.mark.asyncio
    async def test_crew_run_cleanup_does_not_scan_the_host(
        self, unrelated_canary, execution_id
    ):
        """run_crew_isolated's finally block touches only its own process."""
        from src.services.agent_builder.process_executor import ProcessCrewExecutor

        with patch("src.services.agent_builder.process_executor.mp.get_context"):
            executor = ProcessCrewExecutor()
        process = MagicMock(pid=None, exitcode=0)
        process.is_alive.return_value = False
        result_queue = MagicMock()
        result_queue.get.return_value = {"status": "COMPLETED"}
        executor._ctx = MagicMock()
        executor._ctx.Queue.side_effect = [result_queue, MagicMock()]
        executor._ctx.Process.return_value = process

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(executor, "_process_log_queue", new_callable=AsyncMock),
            patch("psutil.process_iter", side_effect=AssertionError("host scan")),
        ):
            result = await executor.run_crew_isolated(
                execution_id, {}, MagicMock(primary_group_id="g", access_token=None)
            )

        assert result["status"] == "COMPLETED"
        assert _alive(unrelated_canary)


class TestOwnedFallbackStopsTheRun:
    """The reload case: the handle is gone, the owned child must still stop."""

    def test_owned_untracked_child_is_terminated(self, execution_id, unrelated_canary):
        env = {**os.environ, "KASAL_EXECUTION_ID": execution_id}
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"], env=env
        )
        try:
            assert _wait_until(
                lambda: [p.pid for p in find_owned_processes(execution_id)]
                == [child.pid]
            )

            assert terminate_owned_processes(execution_id, grace_timeout=2) == 1

            assert child.wait(timeout=5) is not None
            assert _alive(unrelated_canary)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)

    def test_a_survivor_is_not_counted_as_stopped(self, execution_id):
        stubborn = MagicMock(pid=424242)
        stubborn.children.return_value = []
        stubborn.is_running.return_value = True
        stubborn.status.return_value = psutil.STATUS_RUNNING
        with (
            patch(
                "src.services.execution.process_tree.find_owned_processes",
                return_value=[stubborn],
            ),
            patch("psutil.wait_procs", side_effect=lambda procs, timeout: ([], procs)),
        ):
            assert terminate_owned_processes(execution_id) == 0
        stubborn.kill.assert_called()


class TestFailuresAreNotSwallowed:
    def test_root_that_outlives_sigkill_reports_false(self):
        handle = MagicMock(pid=None)
        handle.is_alive.return_value = True
        assert terminate_process_tree(handle, grace_timeout=0) is False
        handle.kill.assert_called()

    def test_access_denied_is_logged_not_raised(self, caplog):
        proc = MagicMock(pid=77)
        proc.terminate.side_effect = psutil.AccessDenied(77)
        from src.services.execution.process_tree import _signal_all

        _signal_all([proc], graceful=True)
        assert "could not signal process 77" in caplog.text

    def test_already_gone_is_quiet(self, caplog):
        proc = MagicMock(pid=78)
        proc.kill.side_effect = psutil.NoSuchProcess(78)
        from src.services.execution.process_tree import _signal_all

        _signal_all([proc], graceful=False)
        assert "could not signal" not in caplog.text

    def test_non_psutil_errors_propagate(self):
        """Only psutil failures are handled; a bug must not read as 'stopped'."""
        proc = MagicMock(pid=79)
        proc.terminate.side_effect = RuntimeError("bug")
        from src.services.execution.process_tree import _signal_all

        with pytest.raises(RuntimeError):
            _signal_all([proc], graceful=True)

    def test_unreadable_process_table_matches_nothing(self, execution_id, caplog):
        with patch("psutil.Process", side_effect=psutil.AccessDenied(os.getpid())):
            assert find_owned_processes(execution_id) == []
        assert "cannot list this server's children" in caplog.text
