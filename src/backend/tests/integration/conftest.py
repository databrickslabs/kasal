"""Isolated persistence and allocated identities for HTTP integration tests."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.db.base import Base
from src.db.database_router import get_smart_db_session
from src.main import app
from src.utils.user_context import GroupContext


@pytest.fixture
def allocated_test_identities(monkeypatch):
    """Stub identity provisioning, retaining real context/authorization checks."""

    async def lookup(email):
        return (
            SimpleNamespace(
                id=email,
                email=email,
                personal_group_id=GroupContext.generate_individual_group_id(email),
                is_system_admin=False,
                is_personal_workspace_manager=False,
            ),
            [],
        )

    monkeypatch.setattr(
        GroupContext,
        "_get_user_group_memberships_with_roles",
        AsyncMock(side_effect=lookup),
    )
    from src.services.groups.users import UserService

    async def get_user(email, *args, **kwargs):
        return (await lookup(email))[0]

    monkeypatch.setattr(
        UserService, "get_or_create_user_by_email", AsyncMock(side_effect=get_user)
    )


@pytest.fixture
def chat_db(event_loop):
    """Provision an isolated, schema-complete database for the workflow tests.

    The application engine is bound at import time (PostgreSQL by default), and
    nothing in the global test setup creates the chat tables or shares a single
    SQLite connection across requests. These end-to-end workflow tests need
    write-then-read persistence within one test, so we stand up a dedicated
    in-memory SQLite engine (StaticPool => one shared connection so writes are
    visible to subsequent reads) with only the chat tables, and route the API's
    smart-session dependency to it for the duration of the test.

    Schema creation and disposal run on the session ``event_loop`` fixture — the
    SAME loop pytest-asyncio uses for the async tests — rather than
    ``asyncio.get_event_loop()``. The latter raises "no current event loop" once
    an earlier test in a combined run has left the current loop unset, which is
    why this fixture errored at setup only after the full unit suite ran first.
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _create_tables():
        async with engine.begin() as conn:
            await conn.run_sync(
                lambda c: Base.metadata.create_all(
                    c,
                    tables=[
                        Base.metadata.tables["chat_history"],
                        Base.metadata.tables["chat_sessions"],
                    ],
                )
            )

    event_loop.run_until_complete(_create_tables())

    async def _override_session():
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_smart_db_session] = _override_session
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_smart_db_session, None)
        event_loop.run_until_complete(engine.dispose())
