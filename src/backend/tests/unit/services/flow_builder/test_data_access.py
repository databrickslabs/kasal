"""Flow data-access bundle: each entry runs every call on its OWN short-lived
session, so a long (LLM-bound, DB-idle) flow run holds no connection the Lakebase
idle timeout can drop — the durable fix for the long-run "connection closed" death.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.flow_builder import data_access as da


@pytest.mark.asyncio
async def test_proxy_runs_each_call_on_a_fresh_session():
    svc = MagicMock()
    svc.get = AsyncMock(return_value="RESULT")
    seen_sessions = []

    @asynccontextmanager
    async def fake_session():
        s = MagicMock(name="fresh-session")
        seen_sessions.append(s)
        yield s

    with patch.object(da, "_short_lived_session", fake_session):
        proxy = da._ShortLivedDataAccess(lambda s: svc)
        r1 = await proxy.get(1)
        r2 = await proxy.get(2)

    assert (r1, r2) == ("RESULT", "RESULT")
    assert svc.get.await_count == 2
    # A FRESH session per call — nothing held across calls.
    assert len(seen_sessions) == 2 and seen_sessions[0] is not seen_sessions[1]


@pytest.mark.asyncio
async def test_proxy_builds_service_with_the_short_lived_session():
    captured = {}

    def build(session):
        captured["session"] = session
        svc = MagicMock()
        svc.get = AsyncMock(return_value="X")
        return svc

    @asynccontextmanager
    async def fake_session():
        yield "SESSION_SENTINEL"

    with patch.object(da, "_short_lived_session", fake_session):
        out = await da._ShortLivedDataAccess(build).get(99)

    assert out == "X"
    assert captured["session"] == "SESSION_SENTINEL"


def test_proxy_does_not_synthesize_dunders():
    # repr/copy/pickle/awaitable checks must not get a synthesized async method.
    proxy = da._ShortLivedDataAccess(lambda s: None)
    with pytest.raises(AttributeError):
        proxy.__wrapped__  # noqa: B018


def test_build_flow_data_access_returns_short_lived_handles():
    bundle = da.build_flow_data_access()
    assert set(bundle) == {"flow", "task", "agent", "crew", "execution_history"}
    assert all(isinstance(v, da._ShortLivedDataAccess) for v in bundle.values())


def test_build_flow_data_access_ignores_passed_session():
    # The session arg is accepted for call-site compat but must NOT be held.
    held = MagicMock(name="would-be-held-session")
    bundle = da.build_flow_data_access(held)
    assert isinstance(bundle["crew"], da._ShortLivedDataAccess)
    held.assert_not_called()
