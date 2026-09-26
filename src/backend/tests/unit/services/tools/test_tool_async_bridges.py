"""The six tools' ``_run_async_in_sync_context`` delegate to the shared bridge.

Each tool used to carry its own copy, which wrapped ``future.result()`` in
``except RuntimeError``: a RuntimeError raised BY the coroutine was taken for
"no running loop", the spent coroutine was run again, and the caller got
"cannot reuse already awaited coroutine" instead of the real error. None of
the copies had a timeout.
"""

import asyncio
import contextvars
import importlib
from unittest.mock import patch

import pytest

from src.services.tools import async_bridge

TOOL_MODULES = [
    "src.services.tools.databricks_jobs_tool",
    "src.services.tools.powerbi_analysis_tool",
    "src.services.tools.powerbi_dax_executor_tool",
    "src.services.tools.powerbi_metadata_reducer_tool",
    "src.services.tools.powerbi_semantic_model_dax_tool",
    "src.services.tools.powerbi_semantic_model_fetcher_tool",
]

_marker = contextvars.ContextVar("bridge_test_marker", default=None)


@pytest.fixture(params=TOOL_MODULES, ids=lambda m: m.rsplit(".", 1)[-1])
def bridge(request):
    return importlib.import_module(request.param)._run_async_in_sync_context


def _failing(calls):
    async def coro():
        calls.append(1)
        raise RuntimeError("the coroutine's own error")

    return coro()


def test_a_runtime_error_from_the_coroutine_propagates_without_a_loop(bridge):
    calls = []
    with pytest.raises(RuntimeError, match="the coroutine's own error"):
        bridge(_failing(calls))
    assert calls == [1]


@pytest.mark.asyncio
async def test_a_runtime_error_from_the_coroutine_propagates_inside_a_loop(bridge):
    calls = []
    with pytest.raises(RuntimeError, match="the coroutine's own error"):
        bridge(_failing(calls))
    assert calls == [1]


@pytest.mark.asyncio
async def test_context_vars_reach_the_worker_thread(bridge):
    async def read():
        return _marker.get()

    token = _marker.set("group-1")
    try:
        assert bridge(read()) == "group-1"
    finally:
        _marker.reset(token)


def test_it_delegates_to_the_shared_bridge_with_a_timeout(bridge):
    async def value():
        return "v"

    coro = value()
    with patch.object(
        async_bridge, "run_async_with_context", return_value="v"
    ) as shared:
        assert bridge(coro) == "v"
    shared.assert_called_once_with(coro, timeout=async_bridge.DEFAULT_TIMEOUT)
    coro.close()


def test_a_plain_result_comes_back(bridge):
    async def value():
        await asyncio.sleep(0)
        return 42

    assert bridge(value()) == 42
