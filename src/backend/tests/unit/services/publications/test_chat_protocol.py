"""Chat is a protocol on the same registry, and it must not leak either way.

Publishing something so it can be picked from your own chat box must not expose
it to MCP or A2A callers, and publishing to MCP must not make it chat-routable.
That is the whole reason ``chat`` is a value in ``protocols`` rather than a
second table: one row, one group filter, one resolve.

Real in-memory SQLite rather than a mocked session, for the same reason the
external isolation suite uses one — the property under test is "the query
filters", and a mock passes just as happily with the WHERE clause missing.
"""

import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.db.base import Base
from src.models.crew_publication import CrewPublication
from src.models.flow import Flow
from src.schemas.crew_publication import CrewPublicationCreate
from src.services.publications.publication import PublicationService

ACME = "acme_corp"
GLOBEX = "globex_inc"


class _Ctx:
    def __init__(self, group_ids, email="user@example.com"):
        self.group_ids = list(group_ids)
        self.group_email = email

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all,
            # Flows too: the catalogue now checks that a published flow still
            # exists before offering it, so the table has to be there.
            tables=[CrewPublication.__table__, Flow.__table__],
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _publish(service, entity_id, name, protocols, group=ACME, entity_type="crew"):
    row = await service.publish(
        entity_id=entity_id,
        data=CrewPublicationCreate(
            external_name=name,
            description=f"Does {name}.",
            protocols=protocols,
            input_schema={
                "type": "object",
                "properties": {"region": {"type": "string"}},
                "required": ["region"],
            },
        ),
        group_context=_Ctx([group]),
        entity_type=entity_type,
    )
    await service.session.commit()
    return row


class TestProtocolIsolation:
    @pytest.mark.asyncio
    async def test_chat_only_is_invisible_to_mcp_and_a2a(self, session):
        service = PublicationService(session)
        await _publish(service, "c1", "chat_only", ["chat"])

        assert [
            c.name for c in await service.list_capabilities_for_group([ACME], "chat")
        ] == ["chat_only"]
        assert await service.list_capabilities_for_group([ACME], "mcp") == []
        assert await service.list_capabilities_for_group([ACME], "a2a") == []

    @pytest.mark.asyncio
    async def test_mcp_only_is_not_chat_routable(self, session):
        service = PublicationService(session)
        await _publish(service, "c2", "mcp_only", ["mcp"])

        assert await service.list_capabilities_for_group([ACME], "chat") == []
        assert (
            await service.resolve_capability_for_group([ACME], "chat", "mcp_only")
            is None
        )

    @pytest.mark.asyncio
    async def test_resolve_refuses_a_protocol_the_row_does_not_carry(self, session):
        service = PublicationService(session)
        await _publish(service, "c3", "both", ["chat", "mcp"])

        assert await service.resolve_capability_for_group([ACME], "chat", "both")
        assert await service.resolve_capability_for_group([ACME], "mcp", "both")
        assert await service.resolve_capability_for_group([ACME], "a2a", "both") is None

    @pytest.mark.asyncio
    async def test_flows_are_chat_routable_on_equal_terms(self, session):
        service = PublicationService(session)
        flow_id = uuid.uuid4()
        session.add(Flow(id=flow_id, name="A Flow", nodes=[], edges=[], flow_config={}))
        await session.commit()
        await _publish(service, str(flow_id), "a_flow", ["chat"], entity_type="flow")

        [capability] = await service.list_capabilities_for_group([ACME], "chat")
        assert capability.entity_type == "flow"


class TestChatIsGroupScopedToo:
    @pytest.mark.asyncio
    async def test_another_tenant_cannot_see_or_resolve_it(self, session):
        service = PublicationService(session)
        await _publish(service, "c4", "acme_thing", ["chat"], group=ACME)

        assert await service.list_capabilities_for_group([GLOBEX], "chat") == []
        # None for "another tenant's" exactly as for "does not exist", so a name
        # cannot be used as an oracle for other workspaces' capabilities.
        assert (
            await service.resolve_capability_for_group([GLOBEX], "chat", "acme_thing")
            is None
        )
        assert (
            await service.resolve_capability_for_group([GLOBEX], "chat", "no_such_name")
            is None
        )

    @pytest.mark.asyncio
    async def test_no_group_sees_nothing_rather_than_everything(self, session):
        service = PublicationService(session)
        await _publish(service, "c5", "acme_thing", ["chat"])

        assert await service.list_capabilities_for_group([], "chat") == []
        assert (
            await service.resolve_capability_for_group([], "chat", "acme_thing") is None
        )


class TestInputSchemaSurvives:
    @pytest.mark.asyncio
    async def test_the_schema_reaches_the_router(self, session):
        # Without it the consumer falls back to treating every detected
        # placeholder as required and interrogates the user for each one.
        service = PublicationService(session)
        await _publish(service, "c6", "with_schema", ["chat"])

        [capability] = await service.list_capabilities_for_group([ACME], "chat")
        assert capability.input_schema["required"] == ["region"]
