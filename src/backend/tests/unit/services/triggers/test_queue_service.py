"""TriggerQueueService — the /triggers CRUD, against real in-memory SQLite.

Exercises enqueue + group-scoped list/get/delete so tenant isolation (a caller
only ever sees its own group's events) is actually verified, not mocked.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.core.exceptions import NotFoundError
from src.models.trigger_queue import STATUS_DISPATCHED, STATUS_PENDING, TriggerQueue
from src.schemas.triggers import EnqueueTrigger, TriggerTarget
from src.services.triggers.queue_service import TriggerQueueService
from src.utils.user_context import GroupContext


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(TriggerQueue.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def service(session):
    return TriggerQueueService(session)


def _ctx(group_id: str) -> GroupContext:
    return GroupContext(group_ids=[group_id], group_email=f"{group_id}@example.com")


def _flow_event(flow_id: str = "flow-1", **kw) -> EnqueueTrigger:
    return EnqueueTrigger(target=TriggerTarget(kind="flow", id=flow_id), **kw)


class TestEnqueue:
    @pytest.mark.asyncio
    async def test_enqueue_stamps_group_and_defaults_to_pending(self, service, session):
        row = await service.enqueue(
            _flow_event(payload={"inputs": {"topic": "x"}}), _ctx("g1")
        )
        await session.commit()
        assert row.id is not None
        assert row.group_id == "g1"
        assert row.status == STATUS_PENDING
        assert row.target == {
            "kind": "flow",
            "id": "flow-1",
            "config": None,
            "harness": None,
            "url": None,
        }


class TestListScoping:
    @pytest.mark.asyncio
    async def test_list_returns_only_callers_group(self, service, session):
        await service.enqueue(_flow_event(), _ctx("g1"))
        await service.enqueue(_flow_event(), _ctx("g1"))
        await service.enqueue(_flow_event(), _ctx("g2"))
        await session.commit()

        g1 = await service.list_events(_ctx("g1"))
        assert len(g1) == 2
        assert all(e.group_id == "g1" for e in g1)

        g2 = await service.list_events(_ctx("g2"))
        assert len(g2) == 1

    @pytest.mark.asyncio
    async def test_list_status_filter(self, service, session):
        row = await service.enqueue(_flow_event(), _ctx("g1"))
        other = await service.enqueue(_flow_event(), _ctx("g1"))
        other.status = STATUS_DISPATCHED
        await session.commit()

        pending = await service.list_events(_ctx("g1"), status=STATUS_PENDING)
        assert [e.id for e in pending] == [row.id]


class TestGetDeleteScoping:
    @pytest.mark.asyncio
    async def test_get_event_cross_group_is_not_found(self, service, session):
        row = await service.enqueue(_flow_event(), _ctx("g1"))
        await session.commit()

        got = await service.get_event(row.id, _ctx("g1"))
        assert got.id == row.id

        with pytest.raises(NotFoundError):
            await service.get_event(row.id, _ctx("g2"))

    @pytest.mark.asyncio
    async def test_delete_event_removes_only_own(self, service, session):
        row = await service.enqueue(_flow_event(), _ctx("g1"))
        await session.commit()

        # Another tenant cannot delete it.
        with pytest.raises(NotFoundError):
            await service.delete_event(row.id, _ctx("g2"))

        await service.delete_event(row.id, _ctx("g1"))
        await session.commit()
        assert await service.list_events(_ctx("g1")) == []
