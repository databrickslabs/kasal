"""Regression tests for deleting agents that are still referenced by tasks.

Covers two bugs:

1. Deleting an agent raised ``FOREIGN KEY constraint failed`` on
   ``DELETE FROM agents`` because ``tasks.agent_id`` references ``agents.id``.
   Deleting an agent now also deletes the tasks assigned to it.

2. On SQLite the request session rides the shared StaticPool connection, so a
   concurrent request's commit/rollback could silently discard a just-committed
   delete — the agents "came back". The delete now runs + commits on a private
   connection via ``get_isolated_db_session``.

These use a real in-memory SQLite engine with ``PRAGMA foreign_keys=ON`` (StaticPool
so every session shares one connection and committed writes are visible), because
the FK constraint and the commit semantics are only exercised against a real driver.
``get_isolated_db_session`` is patched to hand back a session on the same engine.
"""
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.db.base import Base
import src.models  # noqa: F401  registers all ORM models on Base.metadata
from src.models.agent import Agent
from src.models.task import Task
from src.services.agent_service import AgentService


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # SQLite only enforces foreign keys when explicitly enabled per connection.
    @event.listens_for(engine.sync_engine, "connect")
    def _fk_pragma(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # The delete methods run their write+commit on get_isolated_db_session().
    # Redirect it to a session on the same (test) engine so the committed deletes
    # are visible to the assertions below.
    @asynccontextmanager
    async def _fake_isolated():
        async with factory() as s:
            yield s

    with patch("src.db.session.get_isolated_db_session", _fake_isolated):
        yield factory

    await engine.dispose()


def _make_agent(agent_id, group_id="g1"):
    return Agent(id=agent_id, name=f"a-{agent_id}", role="r", goal="g",
                 group_id=group_id, tools=[])


def _make_task(task_id, agent_id, group_id="g1"):
    return Task(id=task_id, name=f"t-{task_id}", description="d",
                expected_output="o", agent_id=agent_id, group_id=group_id, tools=[])


class _Ctx:
    def __init__(self, group_ids, primary=None, email="u@example.com"):
        self.group_ids = group_ids
        self.primary_group_id = primary or (group_ids[0] if group_ids else None)
        self.group_email = email


@pytest.mark.asyncio
async def test_delete_agent_deletes_assigned_tasks(session_factory):
    async with session_factory() as s:
        s.add(_make_agent("agent-1"))
        s.add(_make_task("task-1", "agent-1"))
        s.add(_make_task("task-2", "agent-1"))
        await s.commit()

    async with session_factory() as s:
        svc = AgentService(session=s)
        assert await svc.delete("agent-1") is True

    async with session_factory() as s:
        agents = (await s.execute(text("SELECT COUNT(*) FROM agents"))).scalar()
        tasks = (await s.execute(text("SELECT COUNT(*) FROM tasks"))).scalar()
        assert agents == 0
        assert tasks == 0


@pytest.mark.asyncio
async def test_delete_missing_agent_returns_false(session_factory):
    async with session_factory() as s:
        svc = AgentService(session=s)
        assert await svc.delete("does-not-exist") is False


@pytest.mark.asyncio
async def test_delete_with_group_check_deletes_assigned_tasks(session_factory):
    async with session_factory() as s:
        s.add(_make_agent("agent-1"))
        s.add(_make_task("task-1", "agent-1"))
        await s.commit()

    async with session_factory() as s:
        svc = AgentService(session=s)
        assert await svc.delete_with_group_check("agent-1", _Ctx(["g1"])) is True

    async with session_factory() as s:
        agents = (await s.execute(text("SELECT COUNT(*) FROM agents"))).scalar()
        tasks = (await s.execute(text("SELECT COUNT(*) FROM tasks"))).scalar()
        assert agents == 0
        assert tasks == 0


@pytest.mark.asyncio
async def test_delete_all_for_group_deletes_assigned_tasks(session_factory):
    async with session_factory() as s:
        s.add(_make_agent("agent-1", group_id="g1"))
        s.add(_make_agent("agent-2", group_id="g1"))
        s.add(_make_task("task-1", "agent-1", group_id="g1"))
        s.add(_make_task("task-2", "agent-2", group_id="g1"))
        # An agent + task in a different group must be left untouched
        s.add(_make_agent("agent-3", group_id="g2"))
        s.add(_make_task("task-3", "agent-3", group_id="g2"))
        await s.commit()

    async with session_factory() as s:
        svc = AgentService(session=s)
        await svc.delete_all_for_group(_Ctx(["g1"]))

    async with session_factory() as s:
        agents = (await s.execute(
            text("SELECT id FROM agents ORDER BY id"))).scalars().all()
        tasks = (await s.execute(
            text("SELECT id FROM tasks ORDER BY id"))).scalars().all()
        assert agents == ["agent-3"]
        assert tasks == ["task-3"]


@pytest.mark.asyncio
async def test_delete_all_deletes_all_agents_and_assigned_tasks(session_factory):
    async with session_factory() as s:
        s.add(_make_agent("agent-1"))
        s.add(_make_task("task-1", "agent-1"))
        # An unassigned task should remain
        s.add(_make_task("task-2", None))
        await s.commit()

    async with session_factory() as s:
        svc = AgentService(session=s)
        await svc.delete_all()

    async with session_factory() as s:
        agents = (await s.execute(text("SELECT COUNT(*) FROM agents"))).scalar()
        tasks = (await s.execute(
            text("SELECT id FROM tasks ORDER BY id"))).scalars().all()
        assert agents == 0
        assert tasks == ["task-2"]
