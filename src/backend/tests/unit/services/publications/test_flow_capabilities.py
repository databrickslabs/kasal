"""What the catalogue says about a published FLOW.

Two properties, both learned from a real workspace:

* **A publication whose flow is gone must not be offered.** After a database
  restore, four flow publications pointed at flows that no longer existed. The
  router can only match on what it is shown, so a dangling capability can WIN a
  turn — it resolves to nothing, and the user is told nothing matches for a
  capability they can see in the list.

* **The conversational flag has to survive the id formats.** ``flows.id`` is a
  UUID column, which SQLite stores as 32 hex characters with no dashes, while
  ``publications.entity_id`` is a string holding the dashed form. Comparing them
  raw never matches — a conversational flow would look one-shot forever, with
  nothing logged and nothing failing.
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

GROUP = "acme_corp"
CONVERSATIONAL = {"state": {"enabled": True, "conversational": True}}


class _Ctx:
    def __init__(self, group_ids):
        self.group_ids = list(group_ids)
        self.group_email = "user@example.com"

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all,
            tables=[CrewPublication.__table__, Flow.__table__],
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _add_flow(session, flow_id, name, flow_config=None):
    session.add(
        Flow(
            id=flow_id,
            name=name,
            nodes=[],
            edges=[],
            flow_config=flow_config or {},
            group_id=GROUP,
        )
    )
    await session.commit()


async def _publish_flow(service, entity_id, name):
    await service.publish(
        entity_id=str(entity_id),
        data=CrewPublicationCreate(
            external_name=name,
            description=f"Does {name}.",
            protocols=["chat"],
        ),
        group_context=_Ctx([GROUP]),
        entity_type="flow",
    )
    await service.session.commit()


class TestDanglingCapabilities:
    @pytest.mark.asyncio
    async def test_a_publication_whose_flow_is_gone_is_not_offered(self, session):
        service = PublicationService(session)
        await _publish_flow(service, uuid.uuid4(), "ghost")

        names = [
            c.name for c in await service.list_capabilities_for_group([GROUP], "chat")
        ]

        assert names == []

    @pytest.mark.asyncio
    async def test_a_publication_whose_flow_exists_is_offered(self, session):
        service = PublicationService(session)
        flow_id = uuid.uuid4()
        await _add_flow(session, flow_id, "Swiss News")
        await _publish_flow(service, flow_id, "swiss_news")

        names = [
            c.name for c in await service.list_capabilities_for_group([GROUP], "chat")
        ]

        assert names == ["swiss_news"]

    @pytest.mark.asyncio
    async def test_only_the_dangling_one_is_dropped(self, session):
        service = PublicationService(session)
        live = uuid.uuid4()
        await _add_flow(session, live, "Swiss News")
        await _publish_flow(service, live, "swiss_news")
        await _publish_flow(service, uuid.uuid4(), "ghost")

        names = sorted(
            c.name for c in await service.list_capabilities_for_group([GROUP], "chat")
        )

        assert names == ["swiss_news"]

    @pytest.mark.asyncio
    async def test_a_crew_publication_survives_a_failed_crew_lookup(self, session):
        # Crews are existence-checked too (see test_crew_capabilities.py), but a
        # lookup that FAILS is not the same as "it does not exist" — here the
        # crews table is absent from the fixture entirely. A read error must
        # leave the catalogue alone rather than empty it.
        service = PublicationService(session)
        await service.publish(
            entity_id=str(uuid.uuid4()),
            data=CrewPublicationCreate(
                external_name="risk_review",
                description="Runs the review.",
                protocols=["chat"],
            ),
            group_context=_Ctx([GROUP]),
            entity_type="crew",
        )
        await session.commit()

        names = [
            c.name for c in await service.list_capabilities_for_group([GROUP], "chat")
        ]

        assert names == ["risk_review"]


class TestConversationalFlag:
    @pytest.mark.asyncio
    async def test_a_flow_declaring_a_conversation_is_marked(self, session):
        # The regression this guards: the ids are stored in two formats, and a
        # raw comparison silently never matches.
        service = PublicationService(session)
        flow_id = uuid.uuid4()
        await _add_flow(session, flow_id, "Swiss News", CONVERSATIONAL)
        await _publish_flow(service, flow_id, "swiss_news")

        capabilities = await service.list_capabilities_for_group([GROUP], "chat")

        assert capabilities[0].conversational is True

    @pytest.mark.asyncio
    async def test_a_flow_without_the_declaration_is_not_marked(self, session):
        service = PublicationService(session)
        flow_id = uuid.uuid4()
        await _add_flow(session, flow_id, "Swiss News", {"state": {"enabled": True}})
        await _publish_flow(service, flow_id, "swiss_news")

        capabilities = await service.list_capabilities_for_group([GROUP], "chat")

        assert capabilities[0].conversational is False

    @pytest.mark.asyncio
    async def test_a_flow_with_no_config_at_all_is_not_marked(self, session):
        service = PublicationService(session)
        flow_id = uuid.uuid4()
        await _add_flow(session, flow_id, "Swiss News", None)
        await _publish_flow(service, flow_id, "swiss_news")

        capabilities = await service.list_capabilities_for_group([GROUP], "chat")

        assert capabilities[0].conversational is False
