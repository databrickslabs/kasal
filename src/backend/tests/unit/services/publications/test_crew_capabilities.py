"""What the catalogue says about a published CREW.

The property this file exists for: **a publication whose crew is gone must not
be offered.** Nothing removes a publication when its crew is deleted, and the
catalogue is read by every external surface — one workspace was advertising
nine MCP tools for crews that had been deleted, each able to answer only
"Published crew <uuid> no longer exists".

Flows have been checked this way since publications learned about them; crews
were not, which is the asymmetry these tests pin. The symmetric case (a
publication whose FLOW is gone) lives in ``test_flow_capabilities.py``.
"""

import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.db.base import Base
from src.models.crew import Crew
from src.models.crew_publication import CrewPublication
from src.models.flow import Flow
from src.schemas.crew_publication import CrewPublicationCreate
from src.services.publications.publication import PublicationService

GROUP = "acme_corp"


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
            tables=[CrewPublication.__table__, Crew.__table__, Flow.__table__],
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _add_crew(session, crew_id, name):
    session.add(
        Crew(
            id=crew_id,
            name=name,
            agent_ids=[],
            task_ids=[],
            nodes=[],
            edges=[],
            group_id=GROUP,
        )
    )
    await session.commit()


async def _publish(service, entity_id, name, entity_type="crew"):
    await service.publish(
        entity_id=str(entity_id),
        data=CrewPublicationCreate(
            external_name=name,
            description=f"Does {name}.",
            protocols=["mcp", "chat"],
        ),
        group_context=_Ctx([GROUP]),
        entity_type=entity_type,
    )
    await service.session.commit()


async def _names(service, protocol="mcp"):
    return sorted(
        c.name for c in await service.list_capabilities_for_group([GROUP], protocol)
    )


class TestDanglingCrewPublications:
    @pytest.mark.asyncio
    async def test_a_publication_whose_crew_is_gone_is_not_offered(self, session):
        service = PublicationService(session)
        await _publish(service, uuid.uuid4(), "ghost_crew")

        assert await _names(service) == []

    @pytest.mark.asyncio
    async def test_a_publication_whose_crew_exists_is_offered(self, session):
        service = PublicationService(session)
        crew_id = uuid.uuid4()
        await _add_crew(session, crew_id, "Risk Review")
        await _publish(service, crew_id, "risk_review")

        assert await _names(service) == ["risk_review"]

    @pytest.mark.asyncio
    async def test_only_the_dangling_one_is_dropped(self, session):
        service = PublicationService(session)
        live = uuid.uuid4()
        await _add_crew(session, live, "Risk Review")
        await _publish(service, live, "risk_review")
        await _publish(service, uuid.uuid4(), "ghost_crew")

        assert await _names(service) == ["risk_review"]

    @pytest.mark.asyncio
    async def test_a_flow_publication_is_not_dropped_by_the_crew_check(self, session):
        # Each kind is checked against its OWN table. A flow must not be
        # excluded for being absent from one it was never in.
        service = PublicationService(session)
        flow_id = uuid.uuid4()
        session.add(
            Flow(id=flow_id, name="Swiss News", nodes=[], edges=[], group_id=GROUP)
        )
        await session.commit()
        await _publish(service, flow_id, "swiss_news", entity_type="flow")

        assert await _names(service) == ["swiss_news"]

    @pytest.mark.asyncio
    async def test_a_dangling_publication_refuses_to_resolve(self, session):
        """List and resolve have to agree. An MCP client caches the tool list
        from when it connected, so a name dropped from the catalogue is still
        callable from that cache — resolving it produced a run that failed deep
        in the engine instead of a plain 'unknown tool'."""
        service = PublicationService(session)
        await _publish(service, uuid.uuid4(), "ghost_crew")

        assert (
            await service.resolve_capability_for_group([GROUP], "mcp", "ghost_crew")
            is None
        )

    @pytest.mark.asyncio
    async def test_a_live_publication_still_resolves(self, session):
        service = PublicationService(session)
        crew_id = uuid.uuid4()
        await _add_crew(session, crew_id, "Risk Review")
        await _publish(service, crew_id, "risk_review")

        row = await service.resolve_capability_for_group([GROUP], "mcp", "risk_review")

        assert row is not None and row.external_name == "risk_review"

    @pytest.mark.asyncio
    async def test_an_id_that_is_not_a_uuid_is_kept(self, session):
        # Both repositories resolve ids as UUIDs, so a non-UUID id can never be
        # found. Dropping it would be filtering on a question that was never
        # answerable — it is kept, and fails loudly at invocation as before.
        service = PublicationService(session)
        await _publish(service, "legacy-crew-1", "legacy")

        assert await _names(service) == ["legacy"]


class TestDeletionWithdrawsThePublication:
    """The other half: rows stop being orphaned in the first place.

    Against a real session rather than a mocked repository, because the point is
    that the row is actually gone from the table.
    """

    @pytest.mark.asyncio
    async def test_deleting_a_crew_removes_its_publication_row(self, session):
        from src.services.catalog.crews import CrewService

        service = PublicationService(session)
        crew_id = uuid.uuid4()
        await _add_crew(session, crew_id, "Risk Review")
        await _publish(service, crew_id, "risk_review")

        assert (
            await CrewService(session).delete_by_group(crew_id, _Ctx([GROUP])) is True
        )
        await session.commit()

        assert await _names(service) == []
        assert (
            await service.resolve_capability_for_group([GROUP], "mcp", "risk_review")
            is None
        )

    @pytest.mark.asyncio
    async def test_deleting_every_crew_in_a_workspace_clears_them(self, session):
        from src.services.catalog.crews import CrewService

        service = PublicationService(session)
        for name in ("alpha", "beta"):
            crew_id = uuid.uuid4()
            await _add_crew(session, crew_id, name.title())
            await _publish(service, crew_id, name)

        await CrewService(session).delete_all_by_group(_Ctx([GROUP]))
        await session.commit()

        assert await _names(service) == []

    @pytest.mark.asyncio
    async def test_another_workspaces_publication_is_untouched(self, session):
        """The bulk withdrawal is group-scoped, like the deletion it follows."""
        from src.services.catalog.crews import CrewService

        service = PublicationService(session)
        other_id = uuid.uuid4()
        session.add(Crew(id=other_id, name="Theirs", group_id="globex_inc"))
        await session.commit()
        await service.publish(
            entity_id=str(other_id),
            data=CrewPublicationCreate(
                external_name="theirs", description="Theirs.", protocols=["mcp"]
            ),
            group_context=_Ctx(["globex_inc"]),
            entity_type="crew",
        )
        await session.commit()

        await CrewService(session).delete_all_by_group(_Ctx([GROUP]))
        await session.commit()

        survivors = await service.list_capabilities_for_group(["globex_inc"], "mcp")
        assert [c.name for c in survivors] == ["theirs"]


class TestNameClaims:
    """``(external_name, group_id)`` is unique, so publishing under a taken name
    hits the constraint. It surfaced as a raw IntegrityError in the log and a
    404 "Crew is not published" in the UI — and the name was being held by a
    publication whose flow had been deleted, so nothing could ever free it.
    """

    @pytest.mark.asyncio
    async def test_a_name_held_by_a_deleted_entity_is_reclaimed(self, session):
        service = PublicationService(session)
        await _publish(service, uuid.uuid4(), "agentic_ai_frameworks", "flow")

        crew_id = uuid.uuid4()
        await _add_crew(session, crew_id, "Frameworks")
        await _publish(service, crew_id, "agentic_ai_frameworks")

        assert await _names(service) == ["agentic_ai_frameworks"]
        row = await service.resolve_capability_for_group(
            [GROUP], "mcp", "agentic_ai_frameworks"
        )
        assert row is not None and row.entity_type == "crew"

    @pytest.mark.asyncio
    async def test_a_name_held_by_a_live_entity_is_refused(self, session):
        """Retargeting a name external callers already use, silently, would be
        worse than the error."""
        from src.core.exceptions import ConflictError

        service = PublicationService(session)
        held = uuid.uuid4()
        await _add_crew(session, held, "Holder")
        await _publish(service, held, "taken")

        newcomer = uuid.uuid4()
        await _add_crew(session, newcomer, "Newcomer")
        with pytest.raises(ConflictError):
            await _publish(service, newcomer, "taken")

    @pytest.mark.asyncio
    async def test_republishing_the_same_entity_under_its_own_name_is_fine(
        self, session
    ):
        """Idempotent by entity: the holder it collides with is itself."""
        service = PublicationService(session)
        crew_id = uuid.uuid4()
        await _add_crew(session, crew_id, "Risk Review")
        await _publish(service, crew_id, "risk_review")
        await _publish(service, crew_id, "risk_review")

        assert await _names(service) == ["risk_review"]


class TestSeveralTeamspaces:
    """A caller identified only by email sees every teamspace they belong to.

    That is the model: no `X-Group-Id` header, membership decides. It creates
    one problem the single-teamspace case never had — the name uniqueness
    constraint is per teamspace, so two of them can publish `quiz` and the
    merged list has two tools with one name. A client cannot tell them apart and
    resolution would take whichever row came back first.
    """

    @staticmethod
    async def _publish_in(service, group, entity_id, name):
        await service.publish(
            entity_id=str(entity_id),
            data=CrewPublicationCreate(
                external_name=name,
                description=f"Does {name}.",
                protocols=["mcp"],
            ),
            group_context=_Ctx([group]),
            entity_type="crew",
        )
        await service.session.commit()

    @staticmethod
    async def _crew_in(session, group, crew_id, name):
        session.add(
            Crew(
                id=crew_id,
                name=name,
                agent_ids=[],
                task_ids=[],
                nodes=[],
                edges=[],
                group_id=group,
            )
        )
        await session.commit()

    @pytest.mark.asyncio
    async def test_capabilities_from_every_teamspace_are_listed(self, session):
        service = PublicationService(session)
        for group, name in (("acme_corp", "alpha"), ("globex_inc", "beta")):
            crew_id = uuid.uuid4()
            await self._crew_in(session, group, crew_id, name)
            await self._publish_in(service, group, crew_id, name)

        names = sorted(
            c.name
            for c in await service.list_capabilities_for_group(
                ["acme_corp", "globex_inc"], "mcp"
            )
        )

        assert names == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_a_teamspace_the_caller_is_not_in_is_never_listed(self, session):
        """Membership is the whole boundary now that no group header is needed."""
        service = PublicationService(session)
        crew_id = uuid.uuid4()
        await self._crew_in(session, "globex_inc", crew_id, "theirs")
        await self._publish_in(service, "globex_inc", crew_id, "theirs")

        assert await service.list_capabilities_for_group(["acme_corp"], "mcp") == []

    @pytest.mark.asyncio
    async def test_a_clashing_name_is_qualified_by_teamspace(self, session):
        service = PublicationService(session)
        first, second = uuid.uuid4(), uuid.uuid4()
        await self._crew_in(session, "acme_corp", first, "Quiz")
        await self._publish_in(service, "acme_corp", first, "quiz")
        await self._crew_in(session, "bi-specialist", second, "Quiz")
        await self._publish_in(service, "bi-specialist", second, "quiz")

        names = sorted(
            c.name
            for c in await service.list_capabilities_for_group(
                ["acme_corp", "bi-specialist"], "mcp"
            )
        )

        # The first keeps the plain name; the other carries its teamspace, so
        # both stay callable.
        assert names == ["quiz", "quiz__bi_specialist"]

    @pytest.mark.asyncio
    async def test_each_qualified_name_resolves_to_its_own_publication(self, session):
        """The failure this prevents: calling `quiz` and getting the other
        teamspace's crew, which no error would ever report."""
        service = PublicationService(session)
        first, second = uuid.uuid4(), uuid.uuid4()
        await self._crew_in(session, "acme_corp", first, "Quiz")
        await self._publish_in(service, "acme_corp", first, "quiz")
        await self._crew_in(session, "bi-specialist", second, "Quiz")
        await self._publish_in(service, "bi-specialist", second, "quiz")

        groups = ["acme_corp", "bi-specialist"]
        plain = await service.resolve_capability_for_group(groups, "mcp", "quiz")
        qualified = await service.resolve_capability_for_group(
            groups, "mcp", "quiz__bi_specialist"
        )

        assert str(plain.entity_id) == str(first)
        assert str(qualified.entity_id) == str(second)

    @pytest.mark.asyncio
    async def test_the_listing_and_resolution_cannot_disagree(self, session):
        """Every advertised name resolves, and to the row it was advertised for."""
        service = PublicationService(session)
        for group in ("acme_corp", "bi-specialist"):
            crew_id = uuid.uuid4()
            await self._crew_in(session, group, crew_id, "Quiz")
            await self._publish_in(service, group, crew_id, "quiz")

        groups = ["acme_corp", "bi-specialist"]
        capabilities = await service.list_capabilities_for_group(groups, "mcp")

        for capability in capabilities:
            row = await service.resolve_capability_for_group(
                groups, "mcp", capability.name
            )
            assert row is not None, capability.name
            assert str(row.entity_id) == str(capability.entity_id)

    @pytest.mark.asyncio
    async def test_a_capability_carries_the_teamspace_that_published_it(self, session):
        service = PublicationService(session)
        crew_id = uuid.uuid4()
        await self._crew_in(session, "bi-specialist", crew_id, "Quiz")
        await self._publish_in(service, "bi-specialist", crew_id, "quiz")

        capability = (
            await service.list_capabilities_for_group(["bi-specialist"], "mcp")
        )[0]

        assert capability.teamspace == "bi-specialist"
