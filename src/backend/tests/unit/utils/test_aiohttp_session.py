"""Tests for the shared per-event-loop aiohttp session pool (PERF-012/020)."""

import asyncio
import pytest

from src.utils import aiohttp_session as m


@pytest.fixture(autouse=True)
def _clean_pool():
    m._SESSIONS.clear()
    yield
    # Close anything we created on this loop so aiohttp doesn't warn
    async def _cleanup():
        await m.close_shared_sessions()
    try:
        asyncio.get_event_loop_policy().new_event_loop()  # no-op guard
    except Exception:
        pass
    m._SESSIONS.clear()


@pytest.mark.asyncio
async def test_same_loop_reuses_session():
    s1 = await m.get_shared_session()
    s2 = await m.get_shared_session()
    assert s1 is s2
    await m.close_shared_sessions()


@pytest.mark.asyncio
async def test_context_manager_does_not_close_session():
    async with m.shared_client_session() as s1:
        pass
    assert not s1.closed  # exit must NOT tear down the shared pool
    async with m.shared_client_session() as s2:
        assert s2 is s1
    await m.close_shared_sessions()


@pytest.mark.asyncio
async def test_closed_session_is_replaced():
    s1 = await m.get_shared_session()
    await s1.close()
    s2 = await m.get_shared_session()
    assert s2 is not s1 and not s2.closed
    await m.close_shared_sessions()


def test_separate_loops_get_separate_sessions():
    async def grab():
        return await m.get_shared_session()

    loop1 = asyncio.new_event_loop()
    s1 = loop1.run_until_complete(grab())
    loop1.run_until_complete(m.close_shared_sessions())
    loop1.close()

    loop2 = asyncio.new_event_loop()
    s2 = loop2.run_until_complete(grab())
    assert s2 is not s1
    # Entry for the dead loop1 was pruned when loop2 created its session
    assert all(not lp.is_closed() for lp, _ in m._SESSIONS.values())
    loop2.run_until_complete(m.close_shared_sessions())
    loop2.close()


@pytest.mark.asyncio
async def test_close_shared_sessions_empties_pool():
    await m.get_shared_session()
    await m.close_shared_sessions()
    assert m._SESSIONS == {}
