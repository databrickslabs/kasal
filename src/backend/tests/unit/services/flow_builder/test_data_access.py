"""The flow data-access bundle.

Two guarantees, tested separately:

* Each entry runs every call on its OWN short-lived session, so a long (LLM-bound,
  DB-idle) flow run holds no connection the Lakebase idle timeout can drop — the
  durable fix for the long-run "connection closed" death.
* ``task``/``agent``/``crew`` read catalog rows only in the run's workspace: they
  are bound to the run's group context and read through ``catalog/run_lookup``;
  the subprocess end of this is ``test_flow_run_lookups_workspace_scoped.py``.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.flow_builder import data_access as da
from src.services.flow_builder.data_access import build_flow_data_access
from src.utils.user_context import GroupContext

# ── Short-lived sessions ────────────────────────────────────────────────────


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
        proxy.__wrapped__


def test_the_bundle_keeps_its_keys_and_holds_no_session():
    # The session arg is accepted for call-site compat but must NOT be held.
    held = MagicMock(name="would-be-held-session")
    bundle = build_flow_data_access(held, GroupContext(group_ids=["ws-a"]))
    assert set(bundle) == {"flow", "task", "agent", "crew", "execution_history"}
    assert isinstance(bundle["flow"], da._ShortLivedDataAccess)
    assert isinstance(bundle["execution_history"], da._ShortLivedDataAccess)
    for kind in ("task", "agent", "crew"):
        assert isinstance(bundle[kind]._service, da._ShortLivedDataAccess)
    held.assert_not_called()


# ── Workspace scoping ───────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind, read",
    [("agent", "agent_for_run"), ("task", "task_for_run"), ("crew", "crew_for_run")],
)
async def test_get_reads_through_the_run_scoped_lookup(kind, read):
    context = GroupContext(group_ids=["ws-a"])
    scoped = AsyncMock(return_value="row")
    with patch(f"src.services.catalog.run_lookup.{read}", scoped):
        bundle = build_flow_data_access(MagicMock(), context)
        assert await bundle[kind].get("some-id") == "row"
    service, entity_id, group_context, _log = scoped.await_args.args
    assert (entity_id, group_context) == ("some-id", context)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["agent", "task", "crew"])
@pytest.mark.parametrize("context", [None, GroupContext(group_ids=[])])
async def test_a_run_with_no_group_ids_reads_nothing(kind, context):
    # No group ids → refused before any service call, so no session is even opened.
    opened = []

    @asynccontextmanager
    async def fake_session():
        opened.append(True)
        yield MagicMock()

    with patch.object(da, "_short_lived_session", fake_session):
        bundle = build_flow_data_access(None, context)
        assert await bundle[kind].get("aaaaaaaa-0000-4000-8000-00000000000a") is None
    assert opened == []
