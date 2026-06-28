"""
Unit tests for FlowStateRepository.

Covers add_state (append snapshot) and get_latest_state_json (latest-wins / None)
against a real in-memory SQLite session.
"""
import json

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.models.flow_state import FlowState
from src.repositories.flow_state_repository import FlowStateRepository


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(FlowState.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


class TestFlowStateRepository:
    @pytest.mark.asyncio
    async def test_add_state_returns_persisted_row(self, session):
        repo = FlowStateRepository(session)
        obj = await repo.add_state("uuid-1", "start", json.dumps({"v": 1}))
        await session.commit()

        assert obj.id is not None
        assert obj.flow_uuid == "uuid-1"
        assert obj.method_name == "start"

    @pytest.mark.asyncio
    async def test_get_latest_state_json_returns_most_recent(self, session):
        repo = FlowStateRepository(session)
        await repo.add_state("uuid-1", "m0", json.dumps({"v": 1}))
        await repo.add_state("uuid-1", "m1", json.dumps({"v": 2}))
        await session.commit()

        latest = await repo.get_latest_state_json("uuid-1")
        assert json.loads(latest) == {"v": 2}

    @pytest.mark.asyncio
    async def test_get_latest_state_json_scoped_by_flow_uuid(self, session):
        repo = FlowStateRepository(session)
        await repo.add_state("uuid-1", "m0", json.dumps({"who": "one"}))
        await repo.add_state("uuid-2", "m0", json.dumps({"who": "two"}))
        await session.commit()

        assert json.loads(await repo.get_latest_state_json("uuid-1")) == {"who": "one"}
        assert json.loads(await repo.get_latest_state_json("uuid-2")) == {"who": "two"}

    @pytest.mark.asyncio
    async def test_get_latest_state_json_missing_returns_none(self, session):
        repo = FlowStateRepository(session)
        assert await repo.get_latest_state_json("does-not-exist") is None
