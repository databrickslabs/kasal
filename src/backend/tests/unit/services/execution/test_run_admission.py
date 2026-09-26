"""The one concurrent-run limit shared by Agent Builder and Flow Builder."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution import run_admission as module
from src.services.execution.run_admission import (
    CONFIG_ENGINE_NAME,
    CONFIG_KEY,
    DEFAULT_MAX_CONCURRENT_RUNS,
    MAX_CONCURRENT_RUNS_CEILING,
    RunAdmission,
    RunCancelledWhileQueued,
    clamp_limit,
    read_configured_limit,
)


def _gate(limit):
    return RunAdmission(limit_provider=AsyncMock(return_value=limit))


async def _until(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


class TestLimit:
    @pytest.mark.parametrize(
        "configured, expected",
        [
            (None, DEFAULT_MAX_CONCURRENT_RUNS),
            (0, 1),
            (-3, 1),
            (1, 1),
            (4, 4),
            (MAX_CONCURRENT_RUNS_CEILING, MAX_CONCURRENT_RUNS_CEILING),
            (MAX_CONCURRENT_RUNS_CEILING + 50, MAX_CONCURRENT_RUNS_CEILING),
        ],
    )
    def test_clamp(self, configured, expected):
        assert clamp_limit(configured) == expected

    @pytest.mark.asyncio
    async def test_provider_failure_falls_back_to_the_default(self):
        gate = RunAdmission(limit_provider=AsyncMock(side_effect=RuntimeError("db")))
        assert await gate.current_limit() == DEFAULT_MAX_CONCURRENT_RUNS

    @pytest.mark.asyncio
    async def test_a_later_failure_keeps_the_last_good_limit(self):
        provider = AsyncMock(side_effect=[3, RuntimeError("db")])
        gate = RunAdmission(limit_provider=provider)
        assert await gate.current_limit() == 3
        gate.invalidate_limit()
        assert await gate.current_limit() == 3

    @pytest.mark.asyncio
    async def test_limit_is_cached_between_reads(self):
        provider = AsyncMock(return_value=2)
        gate = RunAdmission(limit_provider=provider)
        await gate.current_limit()
        await gate.current_limit()
        assert provider.await_count == 1
        gate.invalidate_limit()
        await gate.current_limit()
        assert provider.await_count == 2


class TestReadConfiguredLimit:
    """The limit is an engine-config row, read through the settings service."""

    @staticmethod
    def _patched(row):
        service = MagicMock()
        service.find_by_engine_and_key = AsyncMock(return_value=row)

        @asynccontextmanager
        async def session():
            yield MagicMock()

        return (
            patch("src.db.session.routed_scoped_session", session),
            patch(
                "src.services.settings.engine.EngineConfigService",
                return_value=service,
            ),
            service,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "row, expected",
        [
            (None, None),
            (MagicMock(config_value="6", enabled=True), 6),
            (MagicMock(config_value=" 3 ", enabled=True), 3),
            (MagicMock(config_value="6", enabled=False), None),
            (MagicMock(config_value="lots", enabled=True), None),
        ],
    )
    async def test_row_is_parsed(self, row, expected):
        session_patch, service_patch, service = self._patched(row)
        with session_patch, service_patch:
            assert await read_configured_limit() == expected
        service.find_by_engine_and_key.assert_awaited_once_with(
            CONFIG_ENGINE_NAME, CONFIG_KEY
        )

    def test_no_environment_variable_is_consulted(self):
        import inspect

        source = inspect.getsource(module)
        assert "os.environ" not in source and "getenv" not in source


class TestAdmission:
    @pytest.mark.asyncio
    async def test_admits_up_to_the_limit_without_queueing(self):
        gate = _gate(2)
        assert await gate.acquire("a", "crew") is False
        assert await gate.acquire("b", "flow") is False
        assert gate.active_count == 2 and gate.queued_count == 0

    @pytest.mark.asyncio
    async def test_over_the_limit_queues_and_is_admitted_fifo(self):
        gate = _gate(1)
        await gate.acquire("a", "crew")
        order = []

        async def run(execution_id):
            await gate.acquire(execution_id, "crew")
            order.append(execution_id)

        second = asyncio.create_task(run("b"))
        await _until(lambda: gate.queued_count == 1)
        third = asyncio.create_task(run("c"))
        await _until(lambda: gate.queued_count == 2)
        assert order == []

        gate.release("a")
        await second
        assert order == ["b"] and gate.is_queued("c")
        gate.release("b")
        await third
        assert order == ["b", "c"]
        assert gate.snapshot()["active"] == {"c": "crew"}

    @pytest.mark.asyncio
    async def test_crew_and_flow_runs_share_one_limit(self):
        gate = _gate(1)
        await gate.acquire("crew-1", "crew")
        flow = asyncio.create_task(gate.acquire("flow-1", "flow"))
        await _until(lambda: gate.is_queued("flow-1"))
        gate.release("crew-1")
        assert await flow is True

    @pytest.mark.asyncio
    async def test_queued_run_holds_no_pool_thread(self):
        """Waiting for a slot is an asyncio future, never a pool submission."""
        gate = _gate(1)
        await gate.acquire("a", "crew")
        with patch(
            "src.services.execution.blocking_pools.RUN_WAIT_EXECUTOR.submit",
            side_effect=AssertionError("queued run used a pool thread"),
        ):
            waiter = asyncio.create_task(gate.acquire("b", "crew"))
            await _until(lambda: gate.is_queued("b"))
            gate.release("a")
            assert await waiter is True

    @pytest.mark.asyncio
    async def test_status_callbacks_show_the_queue(self):
        gate = _gate(1)
        await gate.acquire("a", "crew")
        on_queued = AsyncMock()
        on_admitted = AsyncMock()

        async def run():
            async with gate.slot(
                "b",
                "flow",
                on_queued=on_queued,
                on_admitted_after_queue=on_admitted,
            ):
                return "ran"

        task = asyncio.create_task(run())
        await _until(lambda: on_queued.await_count == 1)
        on_queued.assert_awaited_once_with(1, 1)  # position 1, limit 1
        on_admitted.assert_not_awaited()
        gate.release("a")
        assert await task == "ran"
        on_admitted.assert_awaited_once_with()
        assert gate.active_count == 0

    @pytest.mark.asyncio
    async def test_failed_status_write_does_not_lose_the_slot(self):
        gate = _gate(1)
        await gate.acquire("a", "crew")
        task = asyncio.create_task(
            gate.acquire("b", "crew", on_queued=AsyncMock(side_effect=OSError("db")))
        )
        await _until(lambda: gate.is_queued("b"))
        gate.release("a")
        assert await task is True

    @pytest.mark.asyncio
    async def test_admission_without_queueing_writes_no_status(self):
        gate = _gate(2)
        on_queued, on_admitted = AsyncMock(), AsyncMock()
        async with gate.slot(
            "a", "crew", on_queued=on_queued, on_admitted_after_queue=on_admitted
        ):
            pass
        on_queued.assert_not_awaited()
        on_admitted.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_slot_is_released_when_the_body_raises(self):
        gate = _gate(1)
        with pytest.raises(RuntimeError):
            async with gate.slot("a", "crew"):
                raise RuntimeError("run failed")
        assert gate.active_count == 0

    @pytest.mark.asyncio
    async def test_raising_the_limit_admits_more_on_the_next_release(self):
        provider = AsyncMock(return_value=1)
        gate = RunAdmission(limit_provider=provider)
        await gate.acquire("a", "crew")
        b = asyncio.create_task(gate.acquire("b", "crew"))
        c = asyncio.create_task(gate.acquire("c", "crew"))
        await _until(lambda: gate.queued_count == 2)

        provider.return_value = 3
        gate.invalidate_limit()
        await gate.current_limit()
        gate.release("a")
        assert await b is True and await c is True
        assert gate.active_count == 2


class TestStopWhileQueued:
    @pytest.mark.asyncio
    async def test_cancel_waiting_removes_the_run(self):
        gate = _gate(1)
        await gate.acquire("a", "crew")
        waiter = asyncio.create_task(gate.acquire("b", "crew"))
        await _until(lambda: gate.is_queued("b"))

        assert gate.cancel_waiting("b") is True
        with pytest.raises(RunCancelledWhileQueued):
            await waiter
        assert gate.queued_count == 0
        gate.release("a")
        assert gate.active_count == 0  # "b" was never admitted

    def test_cancel_waiting_unknown_run(self):
        assert _gate(1).cancel_waiting("nobody") is False

    @pytest.mark.asyncio
    async def test_task_cancelled_while_queued_leaves_the_queue(self):
        gate = _gate(1)
        await gate.acquire("a", "crew")
        waiter = asyncio.create_task(gate.acquire("b", "crew"))
        await _until(lambda: gate.is_queued("b"))
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert gate.queued_count == 0

    @pytest.mark.asyncio
    async def test_cancelled_in_the_tick_it_was_admitted_gives_the_slot_back(self):
        gate = _gate(1)
        await gate.acquire("a", "crew")
        waiter = asyncio.create_task(gate.acquire("b", "crew"))
        await _until(lambda: gate.is_queued("b"))
        gate.release("a")  # admits "b": its future now holds True
        waiter.cancel()  # ...but the task is cancelled before it resumes
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert gate.active_count == 0
