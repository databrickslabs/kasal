"""
Unit tests for FlowStateRepository.

Covers add_state (append snapshot) and get_latest_state_json (latest-wins / None)
against a real in-memory SQLite session.
"""

import json

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
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


class TestTenantScope:
    """`flow_states` was the one table in the checkpoint path with no group.

    That was defensible while a lineage id was a random UUID minted per run and
    read only by a resume. It stopped being defensible once a lineage is derived
    from a chat session and holds a conversation: the rows became user-facing
    data, and no listing over them can be written safely without this.
    """

    @pytest.mark.asyncio
    async def test_a_row_carries_the_group_that_wrote_it(self, session):
        repo = FlowStateRepository(session)

        obj = await repo.add_state("uuid-1", "start", "{}", group_id="group-a")
        await session.commit()

        assert obj.group_id == "group-a"

    @pytest.mark.asyncio
    async def test_another_group_cannot_read_a_lineage(self, session):
        repo = FlowStateRepository(session)
        await repo.add_state("uuid-1", "m0", json.dumps({"v": 1}), group_id="group-a")
        await session.commit()

        assert await repo.get_latest_state_json("uuid-1", group_id="group-b") is None
        assert (
            await repo.get_latest_state_json("uuid-1", group_id="group-a") is not None
        )

    @pytest.mark.asyncio
    async def test_an_untenanted_row_is_not_readable_from_a_teamspace(self, session):
        # The hole this closes: letting NULL rows match every group kept old
        # checkpoints resumable, and made one untenanted row readable from every
        # teamspace. A run that fails to resume beats a run that resumes on
        # somebody else's state.
        repo = FlowStateRepository(session)
        await repo.add_state("uuid-legacy", "m0", json.dumps({"v": 1}))
        await session.commit()

        assert (
            await repo.get_latest_state_json("uuid-legacy", group_id="group-a") is None
        )

    @pytest.mark.asyncio
    async def test_an_untenanted_run_sees_only_untenanted_rows(self, session):
        # The other half of the symmetry: no group asked for is not "no filter",
        # it is "the rows that also carry no group".
        repo = FlowStateRepository(session)
        await repo.add_state("uuid-1", "m0", json.dumps({"v": 1}), group_id="group-a")
        await repo.add_state("uuid-2", "m0", json.dumps({"v": 2}))
        await session.commit()

        assert await repo.get_latest_state_json("uuid-1") is None
        assert await repo.get_latest_state_json("uuid-2") is not None

    @pytest.mark.asyncio
    async def test_history_and_fork_reads_are_scoped_too(self, session):
        # Every read path, not just the one a resume happens to use.
        repo = FlowStateRepository(session)
        row = await repo.add_state("uuid-1", "m0", "{}", group_id="group-a")
        await session.commit()

        assert await repo.get_history("uuid-1", group_id="group-b") == []
        assert await repo.get_history("uuid-1", group_id="group-a") != []


class TestHistory:
    """The table is append-only, so the whole history was always there.

    Only the newest row was ever read, which is why a thread had no timeline and
    a fork had nothing to fork from.
    """

    @pytest.mark.asyncio
    async def test_history_is_oldest_first(self, session):
        # Read as a timeline: the order the methods completed in, and for a
        # conversation the order the turns happened in.
        repo = FlowStateRepository(session)
        for i in range(3):
            await repo.add_state("uuid-1", f"m{i}", json.dumps({"v": i}))
        await session.commit()

        rows = await repo.get_history("uuid-1")

        assert [r.method_name for r in rows] == ["m0", "m1", "m2"]

    @pytest.mark.asyncio
    async def test_history_is_bounded(self, session):
        repo = FlowStateRepository(session)
        for i in range(10):
            await repo.add_state("uuid-1", f"m{i}", "{}")
        await session.commit()

        assert len(await repo.get_history("uuid-1", limit=4)) == 4

    @pytest.mark.asyncio
    async def test_history_is_scoped_to_one_lineage(self, session):
        repo = FlowStateRepository(session)
        await repo.add_state("uuid-1", "mine", "{}")
        await repo.add_state("uuid-2", "theirs", "{}")
        await session.commit()

        assert [r.method_name for r in await repo.get_history("uuid-1")] == ["mine"]
