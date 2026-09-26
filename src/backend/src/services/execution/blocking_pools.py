"""Dedicated, bounded thread pools for the subprocess paths' blocking waits.

Agent Builder and Flow Builder runs hold a thread for their whole lifetime
(``process.join`` / ``_wait_for_result``), and their event relay polls
``queue.get(timeout=0.5)`` in a thread for as long as the run lasts. On the
loop's DEFAULT executor — ``min(32, cpu + 4)`` threads, six to eight on a small
Databricks Apps container — a handful of concurrent builds starved every Chat
turn and ``to_thread`` call queued behind them.

Two pools, not one, on purpose. A child flushes its event queue on exit, so it
cannot exit while nobody reads that queue. If run waits and relay reads shared
one bounded pool, enough runs could fill it with waits, starve their own
relays, and never exit: a deadlock. Kept apart, a full wait pool only delays
the NEXT run's wait, and relay reads return within 0.5 s, so they round-robin.

Neither pool is used for short, latency-sensitive work such as a stop request.
"""

import asyncio
import contextvars
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

# Whole-run waits. Each running crew/flow holds one thread here. Runs beyond
# this bound still start; their wait simply queues until a thread frees.
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
