"""The converter, flow-execution and Lakebase admin routes commit before replying.

These routes took their session from ``get_db`` (converters, flow executions) or
``LocalSessionDep`` (the Lakebase admin routes). Both were request-scoped yield
dependencies, and FastAPI tears those down only after the response has been
sent, so their commit came after the reply: a 2xx could go out for a write that
was not saved yet, and a commit that then failed was never reported.

The pattern is the one ``tests/unit/dependencies/test_session_dep_commits_
before_response.py`` uses for ``SessionDep``: the database is observed through a
SEPARATE connection at the instant ``http.response.start`` is sent, the earliest
moment a client could act on the reply.
"""

import sqlite3
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.db.database_router as router
import src.db.session as session_module
from src.api.converter_router import router as converter_router
from src.api.database_management_router import router as database_management_router
from src.api.flow_execution_router import router as flow_execution_router
from src.dependencies.admin_auth import require_system_admin
from src.dependencies.providers import get_group_context
from src.services.databricks.lakebase.service import LakebaseService
from src.services.flow_builder.kasal_flow_service import KasalFlowService
from src.services.powerbi.conversions import ConverterService
from src.utils.user_context import GroupContext

#: Accepted by the INSERT, refused by the deferred foreign key at COMMIT.
ORPHAN_PARENT = 999


def _sqlite_pragmas(dbapi_connection, _record):
    dbapi_connection.execute("PRAGMA journal_mode=WAL")
    dbapi_connection.execute("PRAGMA foreign_keys=ON")


@asynccontextmanager
async def _database(path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A throwaway SQLite file shaped like the app's, and a sessionmaker on it."""
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    event.listen(engine.sync_engine, "connect", _sqlite_pragmas)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
        await conn.execute(
            text(
                "CREATE TABLE item (name TEXT NOT NULL, parent_id INTEGER "
                "REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
        )
    try:
        yield async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    finally:
        await engine.dispose()


@pytest.fixture
async def factory(tmp_path):
    path = tmp_path / "commit.db"
    async with _database(path) as sessions:
        yield path, sessions


@pytest.fixture
def routed_db(factory):
    """The database the routed session (and the old ``get_db``) opens on."""
    path, sessions = factory
    with (
        patch.object(router, "async_session_factory", sessions),
        patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=False)),
        # Where get_db took its session from, so the test also runs before the fix.
        patch.object(session_module, "async_session_factory", sessions),
    ):
        yield path


@pytest.fixture
async def lakebase_db(tmp_path, routed_db):
    """Lakebase enabled: the router hands out sessions on a SECOND file.

    ``routed_db`` stays in place, so a route that does not go through the
    router (as the old ``get_db`` did not) writes to the first file instead.
    """
    path = tmp_path / "lakebase.db"
    async with _database(path) as sessions:

        @asynccontextmanager
        async def lakebase_session(*_args, **_kwargs):
            async with sessions() as session:
                try:
                    yield session
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        config = {"instance_name": "test-instance", "endpoint": "example.com"}
        with (
            patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=True)),
            patch.object(
                router, "get_lakebase_config_from_db", AsyncMock(return_value=config)
            ),
            patch.object(router, "get_lakebase_session", lakebase_session),
            patch.object(router, "record_successful_connection", MagicMock()),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                AsyncMock(return_value=None),
            ),
        ):
            yield path


@pytest.fixture
def local_db(factory):
    """The LOCAL database; the routed factory must not be touched at all."""
    path, sessions = factory
    routed = MagicMock(side_effect=AssertionError("routed instead of local"))
    with (
        patch.object(session_module, "_local_session_factory", sessions),
        patch.object(router, "async_session_factory", routed),
        patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=False)),
    ):
        yield path


async def _observe(path) -> Dict[str, Any]:
    """What another connection sees: committed rows, and whether it may write."""
    async with aiosqlite.connect(path, timeout=0.2, isolation_level=None) as conn:
        cursor = await conn.execute("SELECT name FROM item ORDER BY name")
        names = [row[0] for row in await cursor.fetchall()]
        try:
            await conn.execute("BEGIN IMMEDIATE")
            await conn.execute("ROLLBACK")
            write_lock_free = True
        except sqlite3.OperationalError:
            write_lock_free = False
    return {"names": names, "write_lock_free": write_lock_free}


async def _insert(session: AsyncSession, name: str, parent_id=None) -> None:
    await session.execute(
        text("INSERT INTO item (name, parent_id) VALUES (:n, :p)"),
        {"n": name, "p": parent_id},
    )


def _writer(name: str, parent_id, result: Dict[str, Any]):
    """A service method that writes on the session the route handed it."""

    async def write(self, *_args, **_kwargs):
        await _insert(self.session, name, parent_id)
        return result

    return write


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(converter_router)
    app.include_router(flow_execution_router)
    app.include_router(database_management_router)
    app.dependency_overrides[get_group_context] = lambda: GroupContext(
        group_ids=["ws-a"], group_email="a@example.com", email_domain="example.com"
    )
    app.dependency_overrides[require_system_admin] = lambda: None
    return app


async def _call(path, method: str, url: str, json=None):
    """Send one request; record the database as seen when the reply starts."""
    app = _app()
    seen: List[Dict[str, Any]] = []

    async def observed_app(scope, receive, send):
        async def send_after_looking(message):
            if message["type"] == "http.response.start":
                seen.append(await _observe(path))
            await send(message)

        await app(scope, receive, send_after_looking)

    transport = httpx.ASGITransport(app=observed_app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        response = await c.request(method, url, json=json)
    assert len(seen) == 1
    return response, seen[0]


#: (service class, method, HTTP method, URL, body, handler result), one row per
#: route group that used get_db. The converter provider serves 17 routes.
GET_DB_ROUTES = [
    pytest.param(
        ConverterService,
        "delete_saved_config",
        "DELETE",
        "/api/converters/configs/1",
        None,
        {"message": "deleted"},
        id="converters",
    ),
    pytest.param(
        KasalFlowService,
        "run_flow",
        "POST",
        "/flow-executions",
        {"flow_id": "11111111-1111-1111-1111-111111111111", "job_id": "job-1"},
        {"success": True, "execution_id": "run-1"},
        id="flow-executions",
    ),
]


class TestGetDbRoutes:
    @pytest.mark.parametrize("cls,method,verb,url,body,result", GET_DB_ROUTES)
    async def test_the_write_is_committed_when_the_reply_starts(
        self, routed_db, cls, method, verb, url, body, result
    ):
        with patch.object(cls, method, _writer("saved", None, result)):
            response, at_reply = await _call(routed_db, verb, url, json=body)

        assert response.status_code in (200, 202)
        assert at_reply == {"names": ["saved"], "write_lock_free": True}

    @pytest.mark.parametrize("cls,method,verb,url,body,result", GET_DB_ROUTES)
    async def test_a_commit_failure_is_an_error_not_a_success(
        self, routed_db, cls, method, verb, url, body, result
    ):
        with patch.object(cls, method, _writer("orphan", ORPHAN_PARENT, result)):
            response, at_reply = await _call(routed_db, verb, url, json=body)

        assert response.status_code == 500
        assert at_reply["names"] == []
        assert (await _observe(routed_db))["names"] == []

    @pytest.mark.parametrize("cls,method,verb,url,body,result", GET_DB_ROUTES)
    async def test_with_lakebase_enabled_the_write_lands_in_lakebase(
        self, routed_db, lakebase_db, cls, method, verb, url, body, result
    ):
        """The session comes from the ROUTER: get_db used the local snapshot
        factory, so with Lakebase on these routes wrote to the wrong database."""
        with patch.object(cls, method, _writer("saved", None, result)):
            response, at_reply = await _call(lakebase_db, verb, url, json=body)

        assert response.status_code in (200, 202)
        assert at_reply == {"names": ["saved"], "write_lock_free": True}
        assert (await _observe(routed_db))["names"] == []


class TestLocalSessionRoutes:
    """The Lakebase admin routes: committed first, and still on the LOCAL db."""

    async def test_the_write_is_committed_when_the_reply_starts(self, local_db):
        write = _writer("saved", None, {"enabled": False})
        with patch.object(LakebaseService, "save_config", write):
            response, at_reply = await _call(
                local_db, "POST", "/database-management/lakebase/config", {}
            )

        assert response.status_code == 200
        assert at_reply == {"names": ["saved"], "write_lock_free": True}

    async def test_a_commit_failure_is_an_error_not_a_success(self, local_db):
        write = _writer("orphan", ORPHAN_PARENT, {"enabled": False})
        with patch.object(LakebaseService, "save_config", write):
            response, at_reply = await _call(
                local_db, "POST", "/database-management/lakebase/config", {}
            )

        assert response.status_code == 500
        assert at_reply["names"] == []
        assert (await _observe(local_db))["names"] == []
