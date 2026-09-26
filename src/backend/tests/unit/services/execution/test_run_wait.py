"""Collecting a run's result from its subprocess, with REAL spawned processes.

The flow child leaves through ``os._exit``. That kills the queue's feeder
thread wherever it is, so a result not yet written into the pipe is lost.
These tests spawn children that do exactly that, with a result several
times larger than the pipe buffer.
"""

import asyncio
import multiprocessing as mp
import os
import queue as queue_module
import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest

from src.services.execution.run_wait import (
    collect_result,
    drain_result_queue,
    flush_queues_before_exit,
    run_deadline,
    wait_for_exit,
)

# Far above the ~64 KB pipe buffer: put() cannot complete without a reader.
BIG = "x" * (1024 * 1024)


def _child_put_then_hard_exit(result_queue, flush):
    """What the flow child does: put the result, then os._exit."""
    result_queue.put({"status": "COMPLETED", "result": BIG})
    if flush:
        flush_queues_before_exit(result_queue, timeout=30)
    os._exit(0)


def _child_sleeps(result_queue):
    time.sleep(30)


def _child_exits_without_result(result_queue):
    os._exit(3)


@pytest.fixture
def spawn():
    ctx = mp.get_context("spawn")
    started = []

    def _start(target, *args):
        q = ctx.Queue()
        p = ctx.Process(target=target, args=(q, *args))
        p.start()
        started.append((p, q))
        return p, q

    yield _start
    for p, q in started:
        if p.is_alive():
            p.kill()
        p.join(timeout=5)
        q.cancel_join_thread()
        q.close()


class TestCollectResult:
    @pytest.mark.asyncio
    async def test_large_result_survives_os_exit_when_flushed(self, spawn):
        process, result_queue = spawn(_child_put_then_hard_exit, True)

        drained = await collect_result(process, result_queue, run_deadline(60))

        assert len(drained) == 1
        assert drained[0]["status"] == "COMPLETED"
        assert len(drained[0]["result"]) == len(BIG)
        assert process.exitcode == 0

    @pytest.mark.asyncio
    async def test_child_without_result_yields_nothing(self, spawn):
        process, result_queue = spawn(_child_exits_without_result)

        drained = await collect_result(process, result_queue, run_deadline(60))

        assert drained == []
        assert process.exitcode == 3

    @pytest.mark.asyncio
    async def test_deadline_raises_and_leaves_the_child_to_the_caller(self, spawn):
        process, result_queue = spawn(_child_sleeps)

        started = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await collect_result(process, result_queue, run_deadline(0.5))

        assert time.monotonic() - started < 5
        assert process.is_alive()  # stopping it is the executor's decision


class TestWaitForExit:
    @pytest.mark.asyncio
    async def test_polls_without_a_thread(self):
        process = MagicMock()
        process.is_alive.side_effect = [True, True, False]
        with patch(
            "src.services.execution.blocking_pools.RUN_WAIT_EXECUTOR.submit",
            side_effect=AssertionError("exit wait used a pool thread"),
        ):
            await wait_for_exit(process, deadline=None, poll_interval=0.001)
        assert process.is_alive.call_count == 3

    @pytest.mark.asyncio
    async def test_deadline_is_absolute(self):
        process = MagicMock()
        process.is_alive.return_value = True
        with pytest.raises(asyncio.TimeoutError):
            await wait_for_exit(process, time.monotonic() - 1, poll_interval=0.001)

    def test_no_timeout_means_no_deadline(self):
        assert run_deadline(None) is None
        assert run_deadline(0) is None
        assert run_deadline(10) > time.monotonic()


class TestDrainResultQueue:
    def test_returns_after_child_exit_when_nothing_arrives(self):
        q = MagicMock()
        q.get.side_effect = queue_module.Empty()
        exited = threading.Event()
        exited.set()
        sink = []
        drain_result_queue(q, sink, exited)
        assert sink == [] and q.get.call_count == 1

    def test_keeps_reading_while_the_child_runs(self):
        q = MagicMock()
        q.get.side_effect = [queue_module.Empty(), queue_module.Empty(), {"ok": 1}]
        sink = []
        drain_result_queue(q, sink, threading.Event())
        assert sink == [{"ok": 1}]

    def test_closed_queue_stops_the_reader(self):
        q = MagicMock()
        q.get.side_effect = OSError("handle is closed")
        sink = []
        drain_result_queue(q, sink, threading.Event())
        assert sink == []


class TestFlushQueuesBeforeExit:
    def test_closes_and_joins_each_queue(self):
        a, b = MagicMock(), MagicMock()
        flush_queues_before_exit(a, None, b, timeout=5)
        for q in (a, b):
            assert q.mock_calls[:2] == [call.close(), call.join_thread()]

    def test_bounded_when_nobody_reads(self):
        stuck = MagicMock()
        stuck.join_thread.side_effect = lambda: time.sleep(5)
        started = time.monotonic()
        flush_queues_before_exit(stuck, timeout=0.2)
        assert time.monotonic() - started < 2

    def test_flow_wrapper_flushes_before_os_exit(self):
        """The ordering that fixes the flow path's lost results."""
        from src.services.flow_builder.process_executor import ProcessFlowExecutor

        order = []
        result_queue, log_queue = MagicMock(), MagicMock()
        result_queue.put.side_effect = lambda r: order.append("put")
        with (
            patch(
                "src.services.flow_builder.process_executor.run_flow_in_process",
                return_value={"status": "COMPLETED"},
            ),
            patch(
                "src.services.flow_builder.process_executor.flush_queues_before_exit",
                side_effect=lambda *qs: order.append(("flush", qs)),
            ),
            patch("os._exit", side_effect=lambda code: order.append(("exit", code))),
            patch("logging.shutdown"),
        ):
            ProcessFlowExecutor._run_flow_wrapper(
                "e1", {}, None, None, result_queue, log_queue
            )

        assert order == ["put", ("flush", (result_queue, log_queue)), ("exit", 0)]
