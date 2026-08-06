"""In-process background work must ROUTE its DB reads, not snapshot them.

``request_scoped_session`` reuses the request's session when one is set and
otherwise falls back to the raw ``async_session_factory``. That factory is a
per-process SNAPSHOT, and ONLY a subprocess ever swaps it to Lakebase
(``activate_lakebase_in_subprocess``) — the main process never does.

Chat is the only path that runs IN-PROCESS, in a FastAPI ``BackgroundTask``, where
the request's session is already closed. So its reads landed on the snapshot —
local SQLite — while the crew and flow subprocesses read Lakebase. Identical
config, opposite answers, and no error either way:

    23:02:33  [CREW][e78bb47e]  Added 1 explicit MCP servers   <- Agent Builder
    23:03:11  [CREW]            Added 0 explicit MCP servers   <- Chat

That is the user-visible bug: "MCP works in the agent builder but not in chat
mode". The MCP rows were correct in Lakebase the whole time (verified: both the
global and the workspace override were ``enabled=True``).

``routed_scoped_session`` keeps the first branch and routes the fallback.

It is deliberately NOT a drop-in replacement everywhere. The router needs a
credential to reach Lakebase and, in local dev, resolves one through
``get_auth_context`` → ``ApiKeysService`` — itself a ``request_scoped_session``
caller. Routing that would close the loop that produced 1,287 "maximum recursion
depth exceeded" in production. So credential lookups keep the old helper.
"""

import asyncio
import contextlib
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.db.database_router as router
from src.db import session as session_module


def _raw_factory(name="LOCAL_SQLITE"):
    """Stand-in for the snapshot factory's INNER callable.

    Patched onto ``async_session_factory._factory`` rather than over the module
    attribute: ``TestSwappableSessionFactory`` reads
    ``src.db.session.async_session_factory`` directly, so rebinding the name
    leaked a MagicMock into that test on the same xdist worker.
    """
    sess = MagicMock(name=name)
    cm = MagicMock()

    async def _aenter(*_a):
        return sess

    async def _aexit(*_a):
        return False

    cm.__aenter__ = _aenter
    cm.__aexit__ = _aexit
    return MagicMock(return_value=cm), sess


@contextlib.contextmanager
def _snapshot_factory(factory):
    """Swap the factory's inner callable, restoring it afterwards."""
    real = session_module.async_session_factory._factory
    session_module.async_session_factory._factory = factory
    try:
        yield
    finally:
        session_module.async_session_factory._factory = real


@asynccontextmanager
async def _fake_lakebase(*_args, **_kwargs):
    yield MagicMock(name="LAKEBASE")


def _on_lakebase(stack):
    """Put the router on its Lakebase branch, as a deployed app (SPN set)."""
    import os

    stack.enter_context(
        patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-client-id"})
    )
    stack.enter_context(
        patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=True))
    )
    stack.enter_context(
        patch.object(
            router,
            "get_lakebase_config_from_db",
            AsyncMock(return_value={"instance_name": "kasalnt", "endpoint": "h"}),
        )
    )
    stack.enter_context(patch.object(router, "get_lakebase_session", _fake_lakebase))


@pytest.mark.asyncio
class TestTheFallbackRoutes:
    async def test_outside_a_request_it_reaches_lakebase(self):
        """THE fix. request_scoped_session would hand back the local snapshot."""
        from contextlib import ExitStack

        factory, _ = _raw_factory()
        with ExitStack() as stack:
            _on_lakebase(stack)
            stack.enter_context(_snapshot_factory(factory))
            async with session_module.routed_scoped_session() as session:
                assert session._mock_name == "LAKEBASE"
        # The snapshot must not even be consulted.
        factory.assert_not_called()

    async def test_the_old_helper_still_snapshots(self):
        """Contrast, so the difference between the two is pinned, not implied."""
        factory, _ = _raw_factory()
        with _snapshot_factory(factory):
            async with session_module.request_scoped_session() as session:
                assert session._mock_name == "LOCAL_SQLITE"


@pytest.mark.asyncio
class TestInsideARequestNothingChanges:
    async def test_it_reuses_the_request_session(self):
        """Both helpers must join the single request transaction."""
        existing = MagicMock(name="REQUEST_session")
        token = session_module._request_session.set(existing)
        try:
            async with session_module.routed_scoped_session() as session:
                assert session is existing
        finally:
            session_module._request_session.reset(token)

    async def test_a_nested_reader_inherits_the_routed_session(self):
        """Why converting only the OUTER opens is sufficient.

        The router sets ``_request_session``, so every nested
        ``request_scoped_session`` — tool_factory, LLMManager, the MCP lookup —
        rides the same routed session. ~45 call sites therefore need no change,
        which matters because most of them are shared with the subprocess paths
        where the snapshot is already correct.
        """
        from contextlib import ExitStack

        factory, _ = _raw_factory()
        with ExitStack() as stack:
            _on_lakebase(stack)
            stack.enter_context(_snapshot_factory(factory))
            async with session_module.routed_scoped_session() as outer:
                async with session_module.request_scoped_session() as inner:
                    assert inner is outer
                    assert inner._mock_name == "LAKEBASE"


@pytest.mark.asyncio
class TestNoRecursionIntoTheRouter:
    async def test_the_router_is_entered_exactly_once(self):
        """Local dev is the risky case: there the router DOES consult auth.

        Before the router set ``_RESOLVING_AUTH`` around that call, this measured
        TWO simultaneous ``get_smart_db_session`` frames — auth's guard only fires
        on ITS outermost entry, and here the router is the outer caller. One level
        is survivable, but it is the same loop that took the app down.
        """
        import os
        import traceback
        from contextlib import ExitStack

        frames_seen = []

        @asynccontextmanager
        async def counting_lakebase(*_a, **_k):
            frames_seen.append(
                len(
                    [
                        f
                        for f in traceback.extract_stack()
                        if f.name == "get_smart_db_session"
                    ]
                )
            )
            yield MagicMock(name="LAKEBASE")

        env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_CLIENT_ID"}
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, env, clear=True))
            stack.enter_context(
                patch.object(
                    router, "is_lakebase_enabled", AsyncMock(return_value=True)
                )
            )
            stack.enter_context(
                patch.object(
                    router,
                    "get_lakebase_config_from_db",
                    AsyncMock(return_value={"instance_name": "i", "endpoint": "h"}),
                )
            )
            stack.enter_context(
                patch.object(router, "get_lakebase_session", counting_lakebase)
            )
            async with session_module.routed_scoped_session():
                pass

        assert frames_seen, "the router never ran"
        assert max(frames_seen) == 1, (
            f"router re-entered itself ({max(frames_seen)} frames on the stack) — "
            "the auth↔router cycle is open again"
        )

    async def test_concurrent_routed_reads_do_not_interfere(self):
        """The guard is a ContextVar, so tasks must not demote each other."""
        from contextlib import ExitStack

        results = []

        async def one():
            factory, _ = _raw_factory()
            with ExitStack() as stack:
                _on_lakebase(stack)
                stack.enter_context(_snapshot_factory(factory))
                async with session_module.routed_scoped_session() as s:
                    await asyncio.sleep(0)
                    results.append(s._mock_name)

        await asyncio.gather(*(asyncio.create_task(one()) for _ in range(4)))
        assert results == ["LAKEBASE"] * 4


class TestTheChatPathIsConverted:
    """Assert the call sites, so a revert is caught rather than re-debugged."""

    def _source(self, relative: str) -> str:
        import pathlib

        backend = pathlib.Path(__file__).resolve().parents[3]
        return (backend / "src" / relative).read_text()

    @pytest.mark.parametrize(
        "relative",
        [
            # Chat runs in-process: agent spec, tool factory, chat history.
            "services/chat/service.py",
            # Chat's summary folding, same background task.
            "services/chat/context_compaction.py",
            # The MCP lookup itself — where the user saw "Added 0".
            "services/execution/kernel/agent_tools.py",
            # Run rename + LLM logging, fired via create_task in the MAIN process.
            "services/execution/naming.py",
        ],
    )
    def test_it_uses_the_routed_helper(self, relative):
        source = self._source(relative)
        assert "routed_scoped_session" in source, (
            f"{relative} is in-process background work reading tenant data; on the "
            "raw snapshot it silently reads the local database"
        )

    def test_credential_lookups_are_left_alone(self):
        """api_keys must NOT be routed — that is the recursion path."""
        source = self._source("services/settings/api_keys.py")
        assert "request_scoped_session" in source
        assert "routed_scoped_session" not in source, (
            "routing the API-key read reopens the auth↔router cycle: the router "
            "calls get_auth_context, which reads api keys"
        )
