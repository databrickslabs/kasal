"""Both subprocess executors enforce ONE concurrent-run limit.

Over the limit a run queues: PENDING with a message, no thread held, stoppable
before it spawns. The run timeout starts when the subprocess starts.
"""

import asyncio
import queue as queue_module
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.agent_builder.process_executor import ProcessCrewExecutor
from src.services.flow_builder.process_executor import ProcessFlowExecutor


class FakeChild:
    """A spawned run that stays alive until the test lets it finish."""

    def __init__(self, pid):
        self.pid = pid
        self.exitcode = None
        self._alive = False

    def start(self):
        self._alive = True

    def is_alive(self):
        return self._alive

    def finish(self, exitcode=0):
        self._alive = False
        self.exitcode = exitcode

    def join(self, timeout=None):
        pass

    def terminate(self):
        self.finish(-15)

    def kill(self):
        self.finish(-9)


def _queue(result=None):
    """An in-memory stand-in with the mp.Queue calls the executors make."""
    q = queue_module.Queue()
    if result is not None:
        q.put(result)
    return q


def _executor(cls, children, results):
    with patch(f"{cls.__module__}.mp.get_context"):
        executor = cls()
    executor._ctx = MagicMock()
    executor._ctx.Process.side_effect = children
    queues = []
    for result in results:
        queues += [_queue(result), _queue()]
    executor._ctx.Queue.side_effect = queues
    executor._process_log_queue = AsyncMock()
    return executor


async def _until(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


@pytest.fixture
def status_writes():
    """Capture the queued/admitted status writes instead of hitting the DB."""
    writes = []

    def callbacks(execution_id):
        async def on_queued(position, limit):
            writes.append(("PENDING", execution_id, position, limit))

        async def on_admitted():
            writes.append(("RUNNING", execution_id))

        return {"on_queued": on_queued, "on_admitted_after_queue": on_admitted}

    with (
        patch(
            "src.services.agent_builder.process_executor.queue_status_callbacks",
            side_effect=callbacks,
        ),
        patch(
            "src.services.flow_builder.process_executor.queue_status_callbacks",
            side_effect=callbacks,
        ),
        patch(
            "src.db.database_router.is_lakebase_enabled",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        yield writes


def _limit(gate, value):
    gate._limit_provider = AsyncMock(return_value=value)
    gate.invalidate_limit()


GC = MagicMock(primary_group_id="g", access_token=None)


class TestSharedLimit:
    @pytest.mark.asyncio
    async def test_flow_waits_for_a_crew_slot_then_runs(
        self, isolated_run_gate, status_writes
    ):
        _limit(isolated_run_gate, 1)
        crew_child, flow_child = FakeChild(101), FakeChild(202)
        crew = _executor(ProcessCrewExecutor, [crew_child], [{"status": "COMPLETED"}])
        flow = _executor(ProcessFlowExecutor, [flow_child], [{"status": "COMPLETED"}])

        crew_run = asyncio.create_task(crew.run_crew_isolated("crew-1", {}, GC))
        await _until(crew_child.is_alive)
        flow_run = asyncio.create_task(flow.run_flow_isolated("flow-1", {}, GC))
        await _until(lambda: isolated_run_gate.is_queued("flow-1"))

        assert status_writes == [("PENDING", "flow-1", 1, 1)]
        flow._ctx.Process.assert_not_called()  # queued runs do not spawn

        crew_child.finish(0)
        assert (await crew_run)["status"] == "COMPLETED"
        await _until(flow_child.is_alive)
        assert status_writes[-1] == ("RUNNING", "flow-1")

        flow_child.finish(0)
        assert (await flow_run)["status"] == "COMPLETED"
        assert isolated_run_gate.active_count == 0

    @pytest.mark.asyncio
    async def test_stopping_a_queued_run_never_spawns_it(
        self, isolated_run_gate, status_writes
    ):
        _limit(isolated_run_gate, 1)
        first = FakeChild(1)
        crew = _executor(
            ProcessCrewExecutor, [first, FakeChild(2)], [{"status": "COMPLETED"}] * 2
        )

        running = asyncio.create_task(crew.run_crew_isolated("run-1", {}, GC))
        await _until(first.is_alive)
        queued = asyncio.create_task(crew.run_crew_isolated("run-2", {}, GC))
        await _until(lambda: isolated_run_gate.is_queued("run-2"))

        assert await crew.terminate_execution("run-2") is True
        result = await queued
        assert result["status"] == "STOPPED"
        assert "queued" in result["message"]
        assert crew._ctx.Process.call_count == 1

        first.finish(0)
        await running
        assert isolated_run_gate.active_count == 0

    @pytest.mark.asyncio
    async def test_flow_stop_reaches_a_queued_crew_run(
        self, isolated_run_gate, status_writes
    ):
        """ExecutionService.stop_execution asks the flow executor first."""
        _limit(isolated_run_gate, 1)
        first = FakeChild(1)
        crew = _executor(
            ProcessCrewExecutor, [first, FakeChild(2)], [{"status": "COMPLETED"}] * 2
        )
        flow = _executor(ProcessFlowExecutor, [], [])

        running = asyncio.create_task(crew.run_crew_isolated("run-1", {}, GC))
        await _until(first.is_alive)
        queued = asyncio.create_task(crew.run_crew_isolated("run-2", {}, GC))
        await _until(lambda: isolated_run_gate.is_queued("run-2"))

        assert await flow.terminate_execution("run-2") is True
        assert (await queued)["status"] == "STOPPED"
        first.finish(0)
        await running


class TestTimeoutIsRunTime:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("cls", [ProcessCrewExecutor, ProcessFlowExecutor])
    async def test_time_spent_queued_does_not_count(
        self, cls, isolated_run_gate, status_writes
    ):
        _limit(isolated_run_gate, 1)
        blocker, timed = FakeChild(1), FakeChild(2)
        executor = _executor(
            cls, [blocker, timed], [{"status": "COMPLETED"}, {"status": "COMPLETED"}]
        )
        run = (
            executor.run_crew_isolated
            if cls is ProcessCrewExecutor
            else executor.run_flow_isolated
        )

        first = asyncio.create_task(run("blocker", {}, GC))
        await _until(blocker.is_alive)
        second = asyncio.create_task(run("timed", {}, GC, timeout=0.5))
        await _until(lambda: isolated_run_gate.is_queued("timed"))

        await asyncio.sleep(0.8)  # queued for longer than its whole timeout
        blocker.finish(0)
        await first
        await _until(timed.is_alive)
        await asyncio.sleep(0.1)  # well inside the 0.5 s of run time
        timed.finish(0)

        assert (await second)["status"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_run_time_over_the_timeout_still_times_out(
        self, isolated_run_gate, status_writes
    ):
        child = FakeChild(7)
        crew = _executor(ProcessCrewExecutor, [child], [None])

        result = await crew.run_crew_isolated("slow", {}, GC, timeout=0.2)

        assert result["status"] == "TIMEOUT"
        assert not child.is_alive()  # stopped through terminate_execution


class TestFinishedRunDoesNotQueueBehindOthers:
    @pytest.mark.asyncio
    async def test_completion_with_every_pool_thread_busy(
        self, isolated_run_gate, status_writes
    ):
        """A run's exit wait and reader settle need no NEW pool thread.

        Every other run-wait thread is occupied; the finished run must still
        complete promptly instead of queueing behind a whole-run wait.
        """
        import threading

        from src.services.execution.blocking_pools import (
            RUN_WAIT_EXECUTOR,
            RUN_WAIT_MAX_WORKERS,
        )

        child = FakeChild(9)
        crew = _executor(ProcessCrewExecutor, [child], [{"status": "COMPLETED"}])
        run = asyncio.create_task(crew.run_crew_isolated("done-fast", {}, GC))
        await _until(child.is_alive)

        release = threading.Event()
        # Its own reader holds one thread; occupy all the others.
        hogs = [
            RUN_WAIT_EXECUTOR.submit(release.wait, 10)
            for _ in range(RUN_WAIT_MAX_WORKERS - 1)
        ]
        try:
            await asyncio.sleep(0.05)
            child.finish(0)
            result = await asyncio.wait_for(run, timeout=3)
        finally:
            release.set()
            for hog in hogs:
                hog.result(timeout=5)
        assert result["status"] == "COMPLETED"
