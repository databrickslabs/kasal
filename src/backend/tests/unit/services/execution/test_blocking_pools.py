"""The run-wait and event-relay pools, and the helper that schedules on them."""

import asyncio
import contextvars
import threading

import pytest

from src.services.execution import blocking_pools
from src.services.execution.blocking_pools import (
    EVENT_RELAY_EXECUTOR,
    EVENT_RELAY_MAX_WORKERS,
    RUN_WAIT_EXECUTOR,
    RUN_WAIT_MAX_WORKERS,
    run_in_pool,
)
from src.services.execution.run_admission import (
    DEFAULT_MAX_CONCURRENT_RUNS,
    MAX_CONCURRENT_RUNS_CEILING,
)

_request_group = contextvars.ContextVar("_request_group", default=None)


class TestPools:
    def test_two_separate_bounded_pools(self):
        assert RUN_WAIT_EXECUTOR is not EVENT_RELAY_EXECUTOR
        assert RUN_WAIT_EXECUTOR._max_workers == RUN_WAIT_MAX_WORKERS
        assert EVENT_RELAY_EXECUTOR._max_workers == EVENT_RELAY_MAX_WORKERS

    def test_run_limit_never_exceeds_either_pool(self):
        """Every live run needs one thread in each pool; the limit keeps that true."""
        assert MAX_CONCURRENT_RUNS_CEILING <= RUN_WAIT_MAX_WORKERS
        assert MAX_CONCURRENT_RUNS_CEILING <= EVENT_RELAY_MAX_WORKERS
        assert DEFAULT_MAX_CONCURRENT_RUNS <= MAX_CONCURRENT_RUNS_CEILING

    def test_threads_are_named_for_the_pool(self):
        names = []

        def record():
            names.append(threading.current_thread().name)

        RUN_WAIT_EXECUTOR.submit(record).result(timeout=5)
        EVENT_RELAY_EXECUTOR.submit(record).result(timeout=5)
        assert names[0].startswith("kasal-run-wait")
        assert names[1].startswith("kasal-event-relay")


class TestRunInPool:
    @pytest.mark.asyncio
    async def test_returns_the_result(self):
        assert await run_in_pool(RUN_WAIT_EXECUTOR, sum, [1, 2, 3]) == 6

    @pytest.mark.asyncio
    async def test_runs_off_the_event_loop(self):
        loop_thread = threading.current_thread()
        worker = await run_in_pool(RUN_WAIT_EXECUTOR, threading.current_thread)
        assert worker is not loop_thread

    @pytest.mark.asyncio
    async def test_propagates_the_callers_contextvars(self):
        """The reason the helper exists: plain run_in_executor drops them."""
        token = _request_group.set("group-a")
        try:
            seen = await run_in_pool(EVENT_RELAY_EXECUTOR, _request_group.get)
        finally:
            _request_group.reset(token)
        assert seen == "group-a"

    @pytest.mark.asyncio
    async def test_exceptions_propagate(self):
        def boom():
            raise ValueError("from the worker")

        with pytest.raises(ValueError, match="from the worker"):
            await run_in_pool(RUN_WAIT_EXECUTOR, boom)

    @pytest.mark.asyncio
    async def test_returns_a_future_usable_with_wait_for(self):
        release = threading.Event()
        future = run_in_pool(RUN_WAIT_EXECUTOR, release.wait, 5)
        assert isinstance(future, asyncio.Future)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(future), timeout=0.05)
        release.set()
        assert await future is True

    def test_module_exposes_only_the_documented_pools(self):
        pools = {
            name
            for name, value in vars(blocking_pools).items()
            if isinstance(value, type(RUN_WAIT_EXECUTOR))
        }
        assert pools == {"RUN_WAIT_EXECUTOR", "EVENT_RELAY_EXECUTOR"}
