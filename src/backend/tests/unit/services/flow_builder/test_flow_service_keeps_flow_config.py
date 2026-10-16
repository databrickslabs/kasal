"""Creating a flow stores the whole flow_config the builder sent.

Runs that start from the stored flow (event triggers, MCP and A2A) build the
flow from this config, so a key dropped at creation is routing, state or an
outcome the flow silently loses. Against real SQLite: the property is what the
row holds after a round trip, not what was handed to a mocked repository.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.models  # noqa: F401  registers every ORM model on Base.metadata
from src.db.base import Base
from src.schemas.flow import FlowCreate
from src.services.flow_builder.flow_service import FlowService
from src.utils.user_context import GroupContext

ROUTERS = [{"name": "route_by_quality", "routes": {"good": ["t1"], "bad": ["t2"]}}]
STATE = {"enabled": True, "conversational": True, "channels": {"topic": "str"}}
PERSISTENCE = {"enabled": True, "level": "flow"}
OUTCOMES = [{"name": "brief", "produces": ["t2"]}]


@pytest_asyncio.fixture
async def maker():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    await engine.dispose()


def _flow_in(name: str = "Routed flow") -> FlowCreate:
    return FlowCreate(
        name=name,
        nodes=[],
        edges=[],
        flow_config={
            "id": "flow-1700000000000",
            "name": name,
            "type": "default",
            "listeners": [{"name": "l1", "crewId": "c1"}],
            "actions": [],
            "startingPoints": [{"crewId": "c0", "taskId": "t0"}],
            "routers": ROUTERS,
            "state": STATE,
            "persistence": PERSISTENCE,
            "outcomes": OUTCOMES,
        },
    )


class TestCreateKeepsTheFlowConfig:
    @pytest.mark.asyncio
    async def test_routers_state_persistence_and_outcomes_survive(self, maker):
        ctx = GroupContext(group_ids=["team_reports"], group_email="a@example.com")
        async with maker() as s:
            created = await FlowService(s).create_flow_with_group(_flow_in(), ctx)
            await s.commit()
            flow_id = created.id

        async with maker() as s:
            stored = (await FlowService(s).get_flow(flow_id)).flow_config

        assert stored["routers"] == ROUTERS
        assert stored["state"] == STATE
        assert stored["persistence"] == PERSISTENCE
        assert stored["outcomes"] == OUTCOMES
        # The normalised keys are still there, and the id is the server's.
        assert stored["listeners"] == [{"name": "l1", "crewId": "c1"}]
        assert stored["startingPoints"] == [{"crewId": "c0", "taskId": "t0"}]
        assert stored["id"] != "flow-1700000000000"
