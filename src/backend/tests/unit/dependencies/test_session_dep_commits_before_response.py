"""``SessionDep`` commits before the response leaves, not after.

FastAPI runs the teardown of a yield dependency with the default ("request")
scope only after ``await response(...)`` — after the body has been sent and
after BackgroundTasks have run (``fastapi/routing.py``: the endpoint runs inside
``function_stack``, the response is sent inside ``request_stack`` alone). The
router's ``commit()`` sat in that teardown, so a client could receive a 2xx for a
write that was not yet saved, and a commit that then failed was never reported.

Each test observes the database through a SEPARATE connection at the instant the
``http.response.start`` message is sent — the earliest moment a client could act
on the reply.
"""

import sqlite3
from typing import Annotated, Any, Dict, List
from unittest.mock import AsyncMock, patch

import aiosqlite
import httpx
import pytest
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.db.database_router as router
from src.db.database_router import get_smart_db_session
from src.dependencies.providers import SessionDep


def _sqlite_pragmas(dbapi_connection, _record):
    dbapi_connection.execute("PRAGMA journal_mode=WAL")
    dbapi_connection.execute("PRAGMA foreign_keys=ON")


@pytest.fixture
async def db(tmp_path):
    """The real router over a throwaway SQLite file, shaped like the app's."""
    path = tmp_path / "commit.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    event.listen(engine.sync_engine, "connect", _sqlite_pragmas)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
        # Deferred, so a violation surfaces at COMMIT rather than at the INSERT.
        await conn.execute(
            text(
                "CREATE TABLE item (name TEXT NOT NULL, parent_id INTEGER "
                "REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    with (
        patch.object(router, "async_session_factory", factory),
        patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=False)),
    ):
        yield path
    await engine.dispose()


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


def _app(path) -> FastAPI:
    app = FastAPI()

    @app.post("/items")
    async def create(session: SessionDep) -> Dict[str, bool]:
        await _insert(session, "saved")
        return {"ok": True}

    @app.post("/items/handler-fails")
    async def create_then_fail(session: SessionDep) -> Dict[str, bool]:
        await _insert(session, "discarded")
        raise HTTPException(status_code=409, detail="conflict")

    @app.post("/items/commit-fails")
    async def create_orphan(session: SessionDep) -> Dict[str, bool]:
        # Accepted by the INSERT; refused by the deferred FK at COMMIT.
        await _insert(session, "orphan", parent_id=999)
        return {"ok": True}

    @app.post("/items/stream")
    async def create_and_stream(session: SessionDep) -> StreamingResponse:
        await _insert(session, "streamed")

        async def body():
            yield b"started\n"
            # The stream may still READ on the request session...
            count = (await session.execute(text("SELECT count(*) FROM item"))).scalar()
            observed = await _observe(path)
            yield f"count={count} lock_free={observed['write_lock_free']}\n".encode()

        return StreamingResponse(body(), media_type="text/plain")

    def _same_session(session: SessionDep) -> AsyncSession:
        return session

    def _raw_router_session(
        session: Annotated[AsyncSession, Depends(get_smart_db_session)],
    ) -> AsyncSession:
        return session

    @app.get("/sessions")
    async def sessions(
        session: SessionDep,
        via_dep: Annotated[AsyncSession, Depends(_same_session)],
        via_router: Annotated[AsyncSession, Depends(_raw_router_session)],
    ) -> Dict[str, bool]:
        return {"dep": via_dep is session, "router": via_router is session}

    @app.post("/items/background")
    async def write_in_background(
        session: SessionDep, background_tasks: BackgroundTasks
    ) -> Dict[str, bool]:
        async def routed_write() -> None:
            from src.db.session import routed_scoped_session

            async with routed_scoped_session() as routed:
                # Same task, request session still open: it is reused.
                background_seen["reused"] = routed is session
                await _insert(routed, "from-background")

        background_tasks.add_task(routed_write)
        return {"ok": True}

    return app


#: What the BackgroundTask route's task observed.
background_seen: Dict[str, bool] = {}


async def _call(path, method: str, url: str):
    """Send one request; record the database as seen when the reply starts."""
    app = _app(path)
    seen: List[Dict[str, Any]] = []

    async def observed_app(scope, receive, send):
        async def send_after_looking(message):
            if message["type"] == "http.response.start":
                seen.append(await _observe(path))
            await send(message)

        await app(scope, receive, send_after_looking)

    transport = httpx.ASGITransport(app=observed_app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        response = await c.request(method, url)
    assert len(seen) == 1
    return response, seen[0]


class TestCommitBeforeResponse:
    async def test_a_write_is_committed_when_the_reply_starts(self, db):
        """THE bug: the 2xx went out while the INSERT was still uncommitted."""
        response, at_reply = await _call(db, "POST", "/items")

        assert response.status_code == 200
        assert at_reply == {"names": ["saved"], "write_lock_free": True}

    async def test_a_commit_failure_is_an_error_not_a_success(self, db):
        """A commit that fails after the reply is a write the client was told
        succeeded. Committing first turns it into the error it is."""
        response, at_reply = await _call(db, "POST", "/items/commit-fails")

        assert response.status_code == 500
        assert at_reply["names"] == []
        assert (await _observe(db))["names"] == []

    async def test_a_handler_exception_rolls_back_and_returns_the_error(self, db):
        response, at_reply = await _call(db, "POST", "/items/handler-fails")

        assert response.status_code == 409
        assert at_reply == {"names": [], "write_lock_free": True}
        assert (await _observe(db))["names"] == []


class TestStreamingResponses:
    async def test_the_handler_write_is_committed_before_the_stream_starts(self, db):
        """A stream can run for an hour; its handler's write must not wait for
        it, and the stream must not hold SQLite's write lock while it runs."""
        response, at_reply = await _call(db, "POST", "/items/stream")

        assert response.status_code == 200
        assert at_reply == {"names": ["streamed"], "write_lock_free": True}
        # ...and the stream itself still works on the request session.
        assert response.text == "started\ncount=1 lock_free=True\n"


class TestBackgroundTasks:
    async def test_a_routed_write_in_a_background_task_is_committed(self, db):
        """A BackgroundTask runs in the request's task while the request session
        is still open, so ``routed_scoped_session`` hands it that session. Its
        write lands after SessionDep's commit, and the router's own commit at the
        end of the request is what saves it."""
        background_seen.clear()
        response, at_reply = await _call(db, "POST", "/items/background")

        assert response.status_code == 200
        assert at_reply["names"] == []  # the task has not run yet
        assert background_seen == {"reused": True}
        assert (await _observe(db))["names"] == ["from-background"]


class TestOneSessionPerRequest:
    async def test_every_consumer_in_a_request_gets_the_same_session(self, db):
        """A sub-dependency taking SessionDep, or the router directly, must share
        the handler's session — two sessions would mean two transactions."""
        response, _ = await _call(db, "GET", "/sessions")

        assert response.status_code == 200
        assert response.json() == {"dep": True, "router": True}
