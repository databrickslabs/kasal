"""Waiting for a run's subprocess and collecting its result, for both paths.

The Agent Builder and Flow Builder executors each spawn one interpreter per
run and read exactly one result from a ``multiprocessing.Queue``. This module
is the part of that they share.

**The result is read while the child runs, not after it exits.**
``Queue.put`` hands the object to a feeder thread that writes it into a pipe.
A result larger than the pipe buffer (about 64 KB on Linux) blocks that
feeder until someone reads. A parent that joins first and reads second
deadlocks with a child that cannot finish its write. So a reader is always
draining the queue while the child lives.

**Thread use per run is fixed at one.** The reader holds one
``RUN_WAIT_EXECUTOR`` thread for the run's lifetime. The exit wait holds no
thread: it polls ``is_alive()`` (a non-blocking ``waitpid``) on the event loop.
The final "has the reader finished" wait awaits the reader's own future and
submits nothing new to the pool, so a run that has finished cannot queue behind
other runs' whole-run waits. ``run_admission`` keeps the number of live runs at
or below the pool size, so a reader never waits for a thread either.

**The timeout is run time.** The deadline is computed when the child has been
started, not when a wait is submitted anywhere.

``flush_queues_before_exit`` is the child's half. A child that leaves through
``os._exit`` (the flow path does, to avoid hanging on stray non-daemon
threads) kills the feeder thread mid-write unless it is flushed first.
"""

from __future__ import annotations

import asyncio
import logging
import queue as queue_module
import threading
import time
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

# How often the exit wait checks the child. is_alive() is a WNOHANG waitpid,
# so this costs nothing measurable even with every run slot in use.
EXIT_POLL_SECONDS = 0.2

# After the child exits, the reader needs one more short read (it blocks for at
# most 0.5 s per read). This is a safety net, not an expected wait.
READER_SETTLE_SECONDS = 10.0


def drain_result_queue(
    result_queue: Any,
    sink: List[Any],
    child_exited: threading.Event,
    max_wait: Optional[float] = None,
) -> None:
    """Read the run's single result so the child's pipe never fills.

    Polls in short slices and stops shortly after ``child_exited`` is set,
    instead of blocking for the whole run when the child died without putting
    a result (stop, OOM, crash). ``max_wait`` is a safety bound; None means
    "until the child exits".
    """
    deadline = None if max_wait is None else time.monotonic() + max_wait
    while deadline is None or time.monotonic() < deadline:
        # Decide BEFORE reading: a child flushes its queue before it exits,
        # so one read that starts after the exit is conclusive.
        last_read = child_exited.is_set()
        try:
            sink.append(result_queue.get(timeout=0.5))
            return
        except queue_module.Empty:
            if last_read:
                return
        except (EOFError, OSError, ValueError) as e:
            # The queue was closed under us (a timed-out run being torn down)
            # or its pipe broke: there is nothing more to read.
            logger.debug("[run_wait] result reader stopping: %s", e)
            return


async def wait_for_exit(
    process: Any,
    deadline: Optional[float],
    poll_interval: float = EXIT_POLL_SECONDS,
) -> None:
    """Wait for ``process`` to exit without holding a thread.

    Raises:
        asyncio.TimeoutError: ``deadline`` (a ``time.monotonic()`` value)
            passed while the process was still alive. The process is left
            running; stopping it is the caller's decision.
    """
    while process.is_alive():
        if deadline is not None and time.monotonic() >= deadline:
            raise asyncio.TimeoutError()
        await asyncio.sleep(poll_interval)


def run_deadline(timeout: Optional[float]) -> Optional[float]:
    """The monotonic deadline for a run that starts now."""
    return time.monotonic() + timeout if timeout else None


async def collect_result(
    process: Any,
    result_queue: Any,
    deadline: Optional[float],
) -> List[Any]:
    """Wait for the child to exit, reading its result while it runs.

    Returns a list holding the result, or an empty list when the child exited
    without writing one (it was stopped, crashed, or ran out of memory).

    Raises:
        asyncio.TimeoutError: the deadline passed first. The reader has
            already been stopped when this propagates.
    """
    from src.services.execution.blocking_pools import RUN_WAIT_EXECUTOR, run_in_pool

    sink: List[Any] = []
    child_exited = threading.Event()
    reader = run_in_pool(
        RUN_WAIT_EXECUTOR, drain_result_queue, result_queue, sink, child_exited
    )
    try:
        await wait_for_exit(process, deadline)
    finally:
        # Also on timeout and cancellation: the reader must not outlive the
        # queue, which the caller closes next.
        child_exited.set()
        await _settle(reader)
    return sink


async def _settle(reader: "asyncio.Future[Any]") -> None:
    try:
        await asyncio.wait_for(asyncio.shield(reader), timeout=READER_SETTLE_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(
            "[run_wait] result reader still busy after %ss; leaving it",
            READER_SETTLE_SECONDS,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 — the reader's failure is not the run's
        logger.warning("[run_wait] result reader failed: %s", e)


def flush_queues_before_exit(*queues: Any, timeout: float = 30.0) -> None:
    """Child side: push everything already put into ``queues`` down the pipe.

    ``close()`` + ``join_thread()`` waits for each queue's feeder thread to
    finish writing. The wait runs in a helper thread bounded by ``timeout``,
    because it only completes while the parent is reading. A parent that has
    died must not leave the child hanging forever on a full pipe.
    """

    def _flush() -> None:
        for q in queues:
            if q is None:
                continue
            try:
                q.close()
                q.join_thread()
            except Exception as e:  # noqa: BLE001 — the child exits next either way
                logger.debug("[run_wait] queue flush skipped: %s", e)

    flusher = threading.Thread(target=_flush, name="kasal-queue-flush", daemon=True)
    flusher.start()
    flusher.join(timeout)
    if flusher.is_alive():
        logger.warning(
            "[run_wait] queues not flushed within %ss; the parent may not "
            "receive this run's result",
            timeout,
        )
