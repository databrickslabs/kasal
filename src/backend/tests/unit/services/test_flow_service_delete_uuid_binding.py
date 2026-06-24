"""Regression tests for force-deleting flows against a real SQLite database.

Reproduces the bug where ``force_delete_flow_with_executions`` passed a
``uuid.UUID`` object straight into a raw ``text()`` query:

    sqlite3.ProgrammingError: Error binding parameter 1: type 'UUID' is not supported

and the follow-on bug where ``str(uuid)`` (dashed form) failed to match the
dashless hex that SQLAlchemy's ``postgresql.UUID`` type stores on SQLite,
yielding a spurious 404 "Flow not found".

These use a real in-memory SQLite engine (StaticPool so every session shares
one connection) rather than mocks, because the bug only manifests when SQLAlchemy
actually binds the value to the SQLite driver — a mocked session cannot catch it.
"""
import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy import text

from src.db.base import Base
import src.models  # noqa: F401  registers all ORM models on Base.metadata
from src.models.flow import Flow
from src.models.execution_history import ExecutionHistory
from src.services.flow_service import FlowService


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield factory
    await engine.dispose()


class _Ctx:
    def __init__(self, group_ids):
        self.group_ids = group_ids


@pytest.mark.asyncio
async def test_force_delete_flow_with_uuid_object(session_factory):
    """A UUID object (as the router passes it) deletes the flow and its executions."""
    flow_id = uuid.uuid4()
    async with session_factory() as s:
        s.add(Flow(id=flow_id, name="todelete", group_id="g1"))
        s.add(ExecutionHistory(
            job_id="job-1", flow_id=flow_id, execution_type="flow",
            status="COMPLETED", run_name="r1",
        ))
        await s.commit()

    async with session_factory() as s:
        svc = FlowService(session=s)
        assert await svc.force_delete_flow_with_executions(flow_id) is True
        await s.commit()

    async with session_factory() as s:
        flows = (await s.execute(text("SELECT COUNT(*) FROM flows"))).scalar()
        execs = (await s.execute(text("SELECT COUNT(*) FROM executionhistory"))).scalar()
        assert flows == 0
        assert execs == 0


@pytest.mark.asyncio
async def test_force_delete_flow_with_group_check_uuid_object(session_factory):
    """The group-check variant also binds the UUID correctly and deletes the flow."""
    flow_id = uuid.uuid4()
    async with session_factory() as s:
        s.add(Flow(id=flow_id, name="todelete2", group_id="g1"))
        await s.commit()

    async with session_factory() as s:
        svc = FlowService(session=s)
        assert await svc.force_delete_flow_with_executions_with_group_check(
            flow_id, _Ctx(group_ids=["g1"])
        ) is True
        await s.commit()

    async with session_factory() as s:
        flows = (await s.execute(text("SELECT COUNT(*) FROM flows"))).scalar()
        assert flows == 0


@pytest.mark.asyncio
async def test_force_delete_accepts_string_flow_id(session_factory):
    """A string flow_id is coerced and matched against the stored hex form."""
    flow_id = uuid.uuid4()
    async with session_factory() as s:
        s.add(Flow(id=flow_id, name="todelete3", group_id="g1"))
        await s.commit()

    async with session_factory() as s:
        svc = FlowService(session=s)
        assert await svc.force_delete_flow_with_executions(str(flow_id)) is True
        await s.commit()

    async with session_factory() as s:
        flows = (await s.execute(text("SELECT COUNT(*) FROM flows"))).scalar()
        assert flows == 0
