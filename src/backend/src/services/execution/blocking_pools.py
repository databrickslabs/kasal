"""Dedicated, bounded thread pools for the subprocess paths' blocking reads.

Agent Builder and Flow Builder runs each hold one thread here for their whole
lifetime: the reader that drains the run's result queue while the child runs
(``run_wait.collect_result``). Their event relay polls
``queue.get(timeout=0.5)`` in a thread for as long as the run lasts. On the
loop's DEFAULT executor (``min(32, cpu + 4)`` threads, six to eight on a small
Databricks Apps container) a handful of concurrent builds starved every Chat
turn and ``to_thread`` call queued behind them.

Two pools, not one, on purpose. A child flushes its event queue on exit, so it
cannot exit while nobody reads that queue. If result readers and relay reads
shared one bounded pool, enough runs could fill it with readers, starve their
own relays, and never exit: a deadlock.

**Nothing waits for a thread here.** ``run_admission`` caps concurrent runs at
``RUN_WAIT_MAX_WORKERS``, so every live run's reader has a thread and every
relay read is served within a round. The exit wait itself polls on the event
loop and holds no thread, so a finished run never queues behind another run's
whole-run wait. Raising the pool size raises the ceiling on that limit; the
two numbers move together.

Neither pool is used for short, latency-sensitive work such as a stop request.
"""

import asyncio
import contextvars
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

# Result readers: each running crew/flow holds one thread here. Also the
# ceiling on run_admission's concurrent-run limit (see the module docstring).
RUN_WAIT_MAX_WORKERS = 16

# Event relay reads: one short-lived blocking get() per running execution.
EVENT_RELAY_MAX_WORKERS = 16

RUN_WAIT_EXECUTOR = ThreadPoolExecutor(
    max_workers=RUN_WAIT_MAX_WORKERS, thread_name_prefix="kasal-run-wait"
)
EVENT_RELAY_EXECUTOR = ThreadPoolExecutor(
    max_workers=EVENT_RELAY_MAX_WORKERS, thread_name_prefix="kasal-event-relay"
)


def run_in_pool(
    executor: ThreadPoolExecutor, func: Callable[..., Any], /, *args: Any
) -> "asyncio.Future[Any]":
    """Schedule ``func(*args)`` on ``executor`` with the caller's contextvars.

    Returns the loop future (not a coroutine) so callers can hand it to
    ``asyncio.wait_for`` or keep it for later, as they did with
    ``run_in_executor``.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    return loop.run_in_executor(executor, functools.partial(ctx.run, func, *args))
