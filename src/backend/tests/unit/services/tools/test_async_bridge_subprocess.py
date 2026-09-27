"""``async_bridge`` timeouts inside a REAL spawned interpreter.

The crew and flow subprocesses import the bridge (via ``ToolFactory`` and the
tool-approval hook, both loaded by ``run_crew_in_process`` in the child), and
changes that pass in-process can still break a spawned interpreter (see
``services/execution/CLAUDE.md``). The child here imports it the way a run does,
times out from a loopless thread and from inside a running loop, and abandons a
coroutine that cannot be cancelled; the parent checks the timings and that the
abandoned worker did not hold up the child's exit.
"""

import multiprocessing as mp
import queue as queue_module
import time

import pytest

#: Timeout used in the child, and the audit's bar for "returned promptly".
_TIMEOUT = 0.3
_SLACK = 0.3
#: How long the uncancellable coroutine blocks its worker thread.
_STUCK_SECONDS = 60


def _child_bridge_timeouts(result_queue):
    import asyncio
    import time as _time

    # The child's real import path: ToolFactory imports the bridge at module top.
    from src.services.tools import tool_factory  # noqa: F401
    from src.services.tools.async_bridge import run_async_with_context

    cancelled = []

    async def _slow():
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async def _stuck():
        _time.sleep(_STUCK_SECONDS)  # blocks its loop: no await to cancel at

    def _time_out(coro):
        start = _time.monotonic()
        try:
            run_async_with_context(coro, timeout=_TIMEOUT)
        except TimeoutError:
            return _time.monotonic() - start
        return None

    async def _from_loop():
        return _time_out(_slow())

    results = {
        "no_loop": _time_out(_slow()),
        "in_loop": asyncio.run(_from_loop()),
        "stuck": _time_out(_stuck()),
    }
    _time.sleep(0.5)  # let the cancellations land
    results["cancelled"] = len(cancelled)
    result_queue.put(results)
    # Return normally: a non-daemon worker still stuck in _stuck() would keep
    # this interpreter alive for _STUCK_SECONDS.


@pytest.fixture
def spawn():
    ctx = mp.get_context("spawn")
    started = []

    def _start(target):
        q = ctx.Queue()
        p = ctx.Process(target=target, args=(q,))
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


def test_timeouts_hold_in_a_spawned_interpreter(spawn):
    process, result_queue = spawn(_child_bridge_timeouts)

    try:
        results = result_queue.get(timeout=180)  # importing ToolFactory is slow
    except queue_module.Empty:
        pytest.fail(f"child produced no result (exitcode={process.exitcode})")
    reported = time.monotonic()

    for key in ("no_loop", "in_loop", "stuck"):
        assert results[key] is not None, f"{key}: no TimeoutError"
        assert results[key] < _TIMEOUT + _SLACK, f"{key}: took {results[key]:.2f}s"
    assert results["cancelled"] == 2

    process.join(timeout=20)
    assert process.exitcode == 0
    assert time.monotonic() - reported < _STUCK_SECONDS / 2
