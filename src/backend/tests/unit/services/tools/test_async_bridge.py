"""Tests for the context-preserving sync→async bridge.

Regression coverage for the bug where CrewAI tools offloaded
``LLMManager.completion`` to a fresh thread via a bare
``ThreadPoolExecutor.submit(asyncio.run, coro)`` — new threads start with an
EMPTY contextvars Context, so the UserContext group_id (and OBO token) were
silently dropped and every completion raised
``ValueError: group_id is required``.
"""

import asyncio
import threading
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.services.tools.async_bridge import (
    WORKER_THREAD_PREFIX,
    _TaskHandle,
    run_async_with_context,
    run_sync_with_context,
)
from src.utils.user_context import GroupContext, UserContext


def _set_group(group_id: str) -> None:
    UserContext.set_group_context(
        GroupContext(
            group_ids=[group_id],
            group_email=f"{group_id}@example.com",
            email_domain="example.com",
        )
    )


async def _read_group_id():
    gc = UserContext.get_group_context()
    return gc.primary_group_id if gc else None


class TestRunAsyncWithContext:
    def test_no_running_loop_preserves_context(self):
        _set_group("grp_inline")
        try:
            assert run_async_with_context(_read_group_id()) == "grp_inline"
        finally:
            UserContext.clear_context()

    def test_running_loop_offloads_and_preserves_context(self):
        """The offloaded worker thread must see the caller's ContextVars."""

        async def _caller():
            _set_group("grp_offload")
            # We're inside a running loop → bridge offloads to a thread
            return run_async_with_context(_read_group_id())

        try:
            assert asyncio.run(_caller()) == "grp_offload"
        finally:
            UserContext.clear_context()

    def test_returns_coroutine_result(self):
        async def _compute():
            return 41 + 1

        assert run_async_with_context(_compute()) == 42

    def test_propagates_exceptions(self):
        async def _boom():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            run_async_with_context(_boom())


#: The audit's bar: a timed-out call returns within this much of its timeout.
_SLACK = 0.3
_TIMEOUT = 0.5


def _bridge_threads() -> list:
    return [t for t in threading.enumerate() if t.name.startswith(WORKER_THREAD_PREFIX)]


def _wait_for_no_bridge_threads(deadline: float = 3.0) -> list:
    end = time.monotonic() + deadline
    while _bridge_threads() and time.monotonic() < end:
        time.sleep(0.02)
    return _bridge_threads()


class _Slow:
    """A coroutine factory that records whether it was cancelled or finished."""

    def __init__(self, seconds: float = 3.0) -> None:
        self.seconds = seconds
        self.cancelled = threading.Event()
        self.finished = threading.Event()

    async def run(self) -> str:
        try:
            await asyncio.sleep(self.seconds)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        self.finished.set()
        return "done"


def _timed(fn) -> float:
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        fn()
    return time.monotonic() - start


class TestRunAsyncTimeout:
    """The timeout is real: the caller never waits for the worker thread.

    It used to be enforced by ``ThreadPoolExecutor``'s ``with``-exit, which
    joins the worker, so a 0.5 s timeout returned after the full 3 s of work
    (and with no running loop there was no timeout at all).
    """

    def test_times_out_promptly_without_a_running_loop(self):
        slow = _Slow()
        elapsed = _timed(lambda: run_async_with_context(slow.run(), timeout=_TIMEOUT))
        assert elapsed < _TIMEOUT + _SLACK

    def test_times_out_promptly_inside_a_running_loop(self):
        slow = _Slow()

        async def _caller() -> float:
            return _timed(lambda: run_async_with_context(slow.run(), timeout=_TIMEOUT))

        assert asyncio.run(_caller()) < _TIMEOUT + _SLACK

    @pytest.mark.parametrize("in_loop", [False, True])
    def test_timed_out_coroutine_is_cancelled(self, in_loop):
        slow = _Slow()

        def _call() -> None:
            with pytest.raises(TimeoutError):
                run_async_with_context(slow.run(), timeout=0.2)

        if in_loop:

            async def _caller() -> None:
                _call()

            asyncio.run(_caller())
        else:
            _call()

        assert slow.cancelled.wait(2.0), "timed-out coroutine kept running"
        assert not slow.finished.is_set()

    def test_no_thread_leak_across_many_timeouts(self):
        assert _wait_for_no_bridge_threads() == []
        for _ in range(25):
            with pytest.raises(TimeoutError):
                run_async_with_context(_Slow(30).run(), timeout=0.01)
        assert _wait_for_no_bridge_threads() == []

    def test_no_thread_left_after_success(self):
        for _ in range(10):
            assert run_async_with_context(_Slow(0).run()) == "done"
        assert _wait_for_no_bridge_threads() == []

    def test_cancel_before_start_prevents_the_run(self):
        handle = _TaskHandle()
        handle.cancel()
        assert handle.attach(object(), object()) is False  # type: ignore[arg-type]

    def test_result_and_exception_propagate_inside_a_running_loop(self):
        async def _ok():
            return 7

        async def _boom():
            raise ValueError("inner")

        async def _caller():
            assert run_async_with_context(_ok(), timeout=5) == 7
            with pytest.raises(ValueError, match="inner"):
                run_async_with_context(_boom(), timeout=5)

        asyncio.run(_caller())

    def test_disposes_thread_local_lakebase_on_the_workers_loop(self):
        async def _ok():
            return 1

        with patch(
            "src.db.lakebase_session.dispose_thread_local_lakebase_factory",
            new_callable=AsyncMock,
        ) as dispose:
            assert run_async_with_context(_ok()) == 1
        dispose.assert_awaited_once()


class TestRunSyncWithContext:
    def test_no_running_loop_runs_inline(self):
        assert run_sync_with_context(lambda: "ok") == "ok"

    def test_running_loop_offloads_and_preserves_context(self):
        def _blocking_read():
            gc = UserContext.get_group_context()
            return gc.primary_group_id if gc else None

        async def _caller():
            _set_group("grp_sync")
            return run_sync_with_context(_blocking_read)

        try:
            assert asyncio.run(_caller()) == "grp_sync"
        finally:
            UserContext.clear_context()

    def test_times_out_promptly_inside_a_running_loop(self):
        release = threading.Event()

        async def _caller() -> float:
            return _timed(
                lambda: run_sync_with_context(lambda: release.wait(5), timeout=_TIMEOUT)
            )

        try:
            assert asyncio.run(_caller()) < _TIMEOUT + _SLACK
        finally:
            release.set()

    def test_exception_propagates_inside_a_running_loop(self):
        def _boom():
            raise KeyError("sync")

        async def _caller():
            run_sync_with_context(_boom)

        with pytest.raises(KeyError, match="sync"):
            asyncio.run(_caller())


class TestToolLLMBridgeRegression:
    """The two UCMV tools must propagate group_id into LLMManager.completion."""

    @pytest.mark.parametrize(
        "tool_module,tool_class",
        [
            (
                "src.services.tools.pbi_visual_ucmv_mapper_tool",
                "PBIVisualUCMVMapperTool",
            ),
            (
                "src.services.tools.ucmv_genie_config_generator_tool",
                "UCMVGenieConfigGeneratorTool",
            ),
        ],
    )
    def test_call_llm_preserves_group_context(self, tool_module, tool_class):
        import importlib

        module = importlib.import_module(tool_module)
        tool = getattr(module, tool_class)()

        seen_group_ids = []

        async def _capturing_completion(*args, **kwargs):
            seen_group_ids.append(await _read_group_id())
            return "{}"

        _set_group("grp_tool_bridge")
        try:
            with patch(
                "src.services.llm.manager.LLMManager.completion",
                AsyncMock(side_effect=_capturing_completion),
            ):
                result = tool._call_llm("test prompt", "databricks/test-model")
            assert result == "{}"
            assert seen_group_ids == ["grp_tool_bridge"]
        finally:
            UserContext.clear_context()
