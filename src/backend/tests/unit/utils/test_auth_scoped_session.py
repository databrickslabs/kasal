"""Auth's own database reads must not go back through the database router.

The router needs a credential to reach Lakebase, so
``get_smart_db_session()`` calls ``get_auth_context()``. Routing auth's OWN reads
through it therefore closes a loop:

    get_auth_context → get_smart_db_session → get_auth_context → …

which the deployed app hit as, thousands of times over::

    [AUTH PAT] Error during PAT lookup: maximum recursion depth exceeded
    [AUTH PAT] Error loading DATABRICKS_TOKEN: maximum recursion depth exceeded

Every LLM call resolves auth, so nothing worked while this was live.

The router still has to be used on the OUTERMOST entry — that is the whole point
of routing, and skipping it is what made a configured Perplexity key read as
absent after a runtime ``/lakebase/enable`` (routed reads went to Lakebase while
the raw factory stayed local). So the rule is not "never route", it is "route
once": use the router unless we are ALREADY resolving auth, and fall back to the
raw factory for the nested read. The bootstrap read wants the local database
anyway — the Lakebase config row itself lives there.

The guard is a ``ContextVar`` rather than a module-level bool because concurrent
requests each resolve auth on their own task; a shared flag would let one
request's in-flight lookup silently disable routing for another's.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.db.database_router as db_router
from src.db import session as session_module
from src.utils import databricks_auth as da


def _raw_factory_mock(session=None):
    """A stand-in for ``async_session_factory()`` (an async context manager)."""
    session = session or MagicMock(name="raw_session")
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm), session


@pytest.mark.asyncio
class TestTheOutermostReadIsRouted:
    async def test_it_uses_the_router(self):
        routed = MagicMock(name="routed_session")

        async def fake_router():
            yield routed

        with patch("src.db.database_router.get_smart_db_session", fake_router):
            async with da._auth_scoped_session() as session:
                assert session is routed

    async def test_the_flag_is_reset_afterwards(self):
        """A leaked flag would send every LATER read to the raw factory."""

        async def fake_router():
            yield MagicMock()

        with patch("src.db.database_router.get_smart_db_session", fake_router):
            async with da._auth_scoped_session():
                assert da._RESOLVING_AUTH.get() is True
        assert da._RESOLVING_AUTH.get() is False

    async def test_the_flag_is_reset_even_when_the_body_raises(self):
        async def fake_router():
            yield MagicMock()

        with patch("src.db.database_router.get_smart_db_session", fake_router):
            with pytest.raises(RuntimeError):
                async with da._auth_scoped_session():
                    raise RuntimeError("boom")
        assert da._RESOLVING_AUTH.get() is False


@pytest.mark.asyncio
class TestANestedReadBypassesTheRouter:
    async def test_it_uses_the_raw_factory(self):
        factory, raw = _raw_factory_mock()
        token = da._RESOLVING_AUTH.set(True)
        try:
            with patch("src.db.session.async_session_factory", factory):
                async with da._auth_scoped_session() as session:
                    assert session is raw
            factory.assert_called_once()
        finally:
            da._RESOLVING_AUTH.reset(token)

    async def test_the_router_is_never_called(self):
        factory, _ = _raw_factory_mock()
        router = MagicMock(name="router")
        token = da._RESOLVING_AUTH.set(True)
        try:
            with (
                patch("src.db.session.async_session_factory", factory),
                patch("src.db.database_router.get_smart_db_session", router),
            ):
                async with da._auth_scoped_session():
                    pass
            router.assert_not_called()
        finally:
            da._RESOLVING_AUTH.reset(token)

    async def test_the_router_auth_cycle_terminates(self):
        """THE regression, with the router wired to call back into auth.

        Unbounded recursion is what shipped; one level of nesting is correct.
        """
        depth = {"current": 0, "max": 0}

        async def router_that_needs_auth():
            depth["current"] += 1
            depth["max"] = max(depth["max"], depth["current"])
            if depth["current"] > 20:
                raise RecursionError("router <-> auth cycle")
            # database_router does exactly this to mint a Lakebase credential.
            async with da._auth_scoped_session() as nested:
                yield nested
            depth["current"] -= 1

        factory, _ = _raw_factory_mock()
        with (
            patch(
                "src.db.database_router.get_smart_db_session", router_that_needs_auth
            ),
            patch("src.db.session.async_session_factory", factory),
        ):
            async with da._auth_scoped_session():
                pass

        assert depth["max"] == 1, f"router re-entered {depth['max']} times"


@pytest.mark.asyncio
class TestConcurrentTasksDoNotShareTheFlag:
    async def test_one_tasks_lookup_does_not_derail_another(self):
        """A module-level bool would fail this; a ContextVar does not.

        Under load, request B's read would land on the raw factory just because
        request A happened to be resolving auth — the exact silent split this
        conversion was meant to remove.
        """
        used: list[str] = []

        async def fake_router():
            used.append("router")
            yield MagicMock()

        factory, _ = _raw_factory_mock()

        async def one():
            async with da._auth_scoped_session():
                await asyncio.sleep(0)  # let the sibling run mid-flight

        # Patch ONCE, outside the tasks. Entering/exiting ``patch`` inside each
        # interleaved task restores in the wrong order: the last task to exit
        # "restores" a sibling's mock, which then leaks into every later test
        # on the worker (tests/unit/db/test_isolated_db_session.py saw a
        # MagicMock async_session_factory).
        with (
            patch("src.db.database_router.get_smart_db_session", fake_router),
            patch("src.db.session.async_session_factory", factory),
        ):
            await asyncio.gather(*(asyncio.create_task(one()) for _ in range(4)))

        # Each task routed independently; none was demoted by a sibling.
        assert used == ["router"] * 4
        assert da._RESOLVING_AUTH.get() is False


@pytest.fixture
async def routed_db(tmp_path):
    """The real router over a throwaway SQLite file, on its local branch."""
    path = tmp_path / "auth.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE item (name TEXT NOT NULL)"))
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    with (
        patch.object(db_router, "async_session_factory", factory),
        patch.object(db_router, "is_lakebase_enabled", AsyncMock(return_value=False)),
    ):
        yield {"path": path, "factory": factory}
    await engine.dispose()


async def _committed_names(path):
    async with aiosqlite.connect(path) as conn:
        cursor = await conn.execute("SELECT name FROM item ORDER BY name")
        return [row[0] for row in await cursor.fetchall()]


class TestTheRoutedReadIsFinishedWhereItIsMade:
    """``async for ...: yield; break`` left the router suspended, so its commit,
    close and ``_request_session`` reset never ran in the caller's task.
    ``get_auth_context`` reaches this on the first config load in every process."""

    async def test_outside_a_request_the_next_routed_block_gets_its_own_session(
        self, routed_db
    ):
        async with da._auth_scoped_session() as auth_session:
            await auth_session.execute(text("SELECT 1"))
        assert session_module._request_session.get() is None
        assert not auth_session.in_transaction()

        async with session_module.routed_scoped_session() as later:
            assert later is not auth_session
            await later.execute(text("INSERT INTO item (name) VALUES ('kept')"))
        assert await _committed_names(routed_db["path"]) == ["kept"]

    async def test_inside_a_request_the_request_session_is_restored(self, routed_db):
        request_session = routed_db["factory"]()
        tokens = session_module._enter_request_session(request_session)
        try:
            async with da._auth_scoped_session() as auth_session:
                assert auth_session is not request_session
            # Left on the auth session, later nested blocks would join IT, and
            # the request's commit would not cover their writes.
            assert session_module._request_session.get() is request_session
            async with session_module.routed_scoped_session() as nested:
                assert nested is request_session
        finally:
            session_module._exit_request_session(tokens)
            await request_session.close()
