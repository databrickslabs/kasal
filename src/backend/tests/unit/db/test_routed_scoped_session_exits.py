"""Every exit from ``routed_scoped_session`` commits or rolls back, in the caller.

The router branch used to read::

    async for session in get_smart_db_session():
        yield session
        break

The ``break`` leaves the router generator suspended at its ``yield``, so the
code after it — the router's ``commit()``, its rollback, ``close()`` and the
reset of ``_request_session`` — never ran in the caller's task. It ran later, if
at all, when the garbage-collected generator was finalised in a fresh task. Three
consequences, one test class each:

- a write made inside the block was never committed;
- an exception left the transaction open (and, on SQLite, the write lock held)
  after the block had already returned;
- ``_request_session`` stayed pointed at the finished session with the current
  task as its owner, so the NEXT block in the same task reused it instead of
  getting its own.

These tests run the real router against a throwaway SQLite file and read the
result through a separate connection, which is what another request would see.
"""

import sqlite3
from contextlib import asynccontextmanager
from typing import AsyncIterator, List
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.db.database_router as router
from src.db import session as session_module


def _wal(dbapi_connection, _record):
    dbapi_connection.execute("PRAGMA journal_mode=WAL")


@pytest.fixture
async def db(tmp_path):
    """A StaticPool SQLite engine like the app's, routed to by the real router."""
    path = tmp_path / "routed.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    event.listen(engine.sync_engine, "connect", _wal)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE item (name TEXT NOT NULL)"))
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    with (
        patch.object(router, "async_session_factory", factory),
        patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=False)),
    ):
        yield {"path": path, "factory": factory}
    await engine.dispose()


async def _committed_names(path) -> List[str]:
    """What a separate connection sees — only committed rows."""
    async with aiosqlite.connect(path) as conn:
        cursor = await conn.execute("SELECT name FROM item ORDER BY name")
        return [row[0] for row in await cursor.fetchall()]


async def _write_lock_is_free(path) -> bool:
    """Whether another connection could start writing right now."""
    async with aiosqlite.connect(path, timeout=0.2, isolation_level=None) as conn:
        try:
            await conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            return False
        await conn.execute("ROLLBACK")
        return True


async def _insert(session: AsyncSession, name: str) -> None:
    await session.execute(text("INSERT INTO item (name) VALUES (:n)"), {"n": name})


class TestTheRouterBranch:
    async def test_a_clean_exit_commits(self, db):
        """THE bug: the router's commit sat after the ``yield`` the ``break`` skipped."""
        async with session_module.routed_scoped_session() as session:
            await _insert(session, "kept")

        assert await _committed_names(db["path"]) == ["kept"]

    async def test_an_exception_rolls_back_before_the_block_returns(self, db):
        """The rollback must happen here, not in a finaliser some time later."""
        with pytest.raises(RuntimeError, match="boom"):
            async with session_module.routed_scoped_session() as session:
                await _insert(session, "dropped")
                raise RuntimeError("boom")

        # Checked with no await in between: a deferred close would not have run.
        assert not session.in_transaction()
        assert await _write_lock_is_free(db["path"])
        assert await _committed_names(db["path"]) == []

    async def test_consecutive_blocks_in_one_task_get_their_own_session(self, db):
        """The leaked ContextVar made the second block reuse the first session."""
        async with session_module.routed_scoped_session() as first:
            await _insert(session=first, name="one")
        assert session_module._request_session.get() is None

        async with session_module.routed_scoped_session() as second:
            await _insert(session=second, name="two")
        assert session_module._request_session.get() is None

        assert second is not first
        assert await _committed_names(db["path"]) == ["one", "two"]

    async def test_a_nested_block_joins_the_outer_transaction(self, db):
        """Unchanged: a nested reader rides the routed session, and only the
        outer block commits."""
        async with session_module.routed_scoped_session() as outer:
            async with session_module.routed_scoped_session() as inner:
                assert inner is outer
                await _insert(inner, "nested")
            # The inner exit must not have committed the outer transaction.
            assert await _committed_names(db["path"]) == []
        assert await _committed_names(db["path"]) == ["nested"]


def _recording_lakebase(events: List[str]):
    """Stand-in for ``get_lakebase_session`` with its commit/rollback shape."""

    @asynccontextmanager
    async def fake(*_args, **_kwargs) -> AsyncIterator[MagicMock]:
        session = AsyncMock(name="LAKEBASE")
        try:
            yield session
            events.append("commit")
        except Exception:
            events.append("rollback")
            raise
        finally:
            events.append("close")

    return fake


class TestTheLakebaseRoute:
    @pytest.fixture
    def on_lakebase(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "spn-client-id")
        events: List[str] = []
        with (
            patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=True)),
            patch.object(
                router,
                "get_lakebase_config_from_db",
                AsyncMock(return_value={"instance_name": "i", "endpoint": "h"}),
            ),
            patch.object(router, "get_lakebase_session", _recording_lakebase(events)),
        ):
            yield events

    async def test_a_clean_exit_commits_and_closes_in_the_caller(self, on_lakebase):
        async with session_module.routed_scoped_session():
            pass
        assert on_lakebase == ["commit", "close"]

    async def test_an_exception_rolls_back_and_closes_in_the_caller(self, on_lakebase):
        with pytest.raises(ValueError):
            async with session_module.routed_scoped_session():
                raise ValueError("boom")
        assert on_lakebase == ["rollback", "close"]


class TestTheAuthBranch:
    """The raw-factory branch, taken while auth is resolving, follows the same
    contract: commit on a clean exit, roll back on an exception."""

    @pytest.fixture
    def resolving_auth(self, db):
        from src.utils.databricks_auth import _RESOLVING_AUTH

        real = session_module.async_session_factory._factory
        session_module.async_session_factory._factory = db["factory"]
        token = _RESOLVING_AUTH.set(True)
        try:
            yield db
        finally:
            _RESOLVING_AUTH.reset(token)
            session_module.async_session_factory._factory = real

    async def test_a_clean_exit_commits(self, resolving_auth):
        async with session_module.routed_scoped_session() as session:
            await _insert(session, "auth-read")
        assert await _committed_names(resolving_auth["path"]) == ["auth-read"]

    async def test_an_exception_rolls_back(self, resolving_auth):
        with pytest.raises(RuntimeError):
            async with session_module.routed_scoped_session() as session:
                await _insert(session, "dropped")
                raise RuntimeError("boom")
        assert not session.in_transaction()
        assert await _committed_names(resolving_auth["path"]) == []


class TestTheRequestBranch:
    async def test_reusing_the_request_session_never_commits_it(self):
        """The request owns that transaction; a nested block must not end it."""
        request_session = AsyncMock(name="REQUEST_session")
        tokens = session_module._enter_request_session(request_session)
        try:
            async with session_module.routed_scoped_session() as got:
                assert got is request_session
        finally:
            session_module._exit_request_session(tokens)
        request_session.commit.assert_not_awaited()
        request_session.rollback.assert_not_awaited()
        request_session.close.assert_not_awaited()
