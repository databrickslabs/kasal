"""Cross-tenant isolation for the External Invocation Layer.

Written ONCE, against the EIL, because both the MCP and A2A adapters read
capabilities through it. That is the main practical payoff of the shared layer:
cross-tenant leakage is the top risk on both surfaces, and this suite covers
both.

These tests run against a REAL in-memory SQLite database rather than a mocked
session, deliberately. The property under test is "the SQL filters by group" —
a mock records whatever call it is given and would pass just as happily if the
WHERE clause were missing, which is exactly the bug that matters here.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.db.base import Base
from src.models.crew_publication import CrewPublication
from src.repositories.crew_publication_repository import CrewPublicationRepository
from src.schemas.crew_publication import CrewPublicationCreate
from src.services.external.identity import ExternalCaller
from src.services.external.publication import PublicationService

ACME = "acme_corp"
GLOBEX = "globex_inc"


class _Ctx:
    """Minimal GroupContext stand-in for the write path."""

    def __init__(self, group_ids, email="user@example.com"):
        self.group_ids = list(group_ids)
        self.group_email = email
        self.user_role = "admin"
        self.highest_role = "admin"
        self.current_user = None

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


def _caller(group_ids, protocol="mcp"):
    return ExternalCaller(
        group_context=_Ctx(group_ids),
        protocol=protocol,
        identifier="caller@example.com",
    )


@pytest_asyncio.fixture
async def session():
    """A real async session over an in-memory SQLite database."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all, tables=[CrewPublication.__table__]
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def seeded(session):
    """One publication per tenant, both exposed over both protocols."""
    svc = PublicationService(session)
    await svc.publish(
        "crew-acme",
        CrewPublicationCreate(
            external_name="acme_report",
            description="Acme's quarterly report crew.",
            protocols=["mcp", "a2a"],
        ),
        _Ctx([ACME]),
    )
    await svc.publish(
        "crew-globex",
        CrewPublicationCreate(
            external_name="globex_report",
            description="Globex's quarterly report crew.",
            protocols=["mcp", "a2a"],
        ),
        _Ctx([GLOBEX]),
    )
    await session.commit()
    return svc


class TestCapabilityListingIsGroupScoped:
    @pytest.mark.asyncio
    async def test_caller_sees_only_its_own_group(self, seeded):
        names = [c.name for c in await seeded.list_capabilities(_caller([ACME]))]
        assert names == ["acme_report"]

    @pytest.mark.asyncio
    async def test_other_tenant_sees_only_its_own(self, seeded):
        names = [c.name for c in await seeded.list_capabilities(_caller([GLOBEX]))]
        assert names == ["globex_report"]

    @pytest.mark.asyncio
    async def test_multi_group_caller_sees_the_union(self, seeded):
        names = sorted(
            c.name for c in await seeded.list_capabilities(_caller([ACME, GLOBEX]))
        )
        assert names == ["acme_report", "globex_report"]

    @pytest.mark.asyncio
    async def test_no_groups_returns_nothing_not_everything(self, seeded):
        """The failure mode that matters: an unresolved caller must not get the
        workspace catalogue. An empty `group_ids` reaching `.in_([])` is the
        classic way that happens."""
        assert await seeded.list_capabilities(_caller([])) == []

    @pytest.mark.asyncio
    async def test_unknown_group_returns_nothing(self, seeded):
        assert await seeded.list_capabilities(_caller(["not_a_group"])) == []


class TestNameResolutionIsGroupScoped:
    @pytest.mark.asyncio
    async def test_resolves_own_capability(self, seeded):
        row = await seeded.resolve_capability(_caller([ACME]), "acme_report")
        assert row is not None and row.entity_id == "crew-acme"

    @pytest.mark.asyncio
    async def test_cannot_resolve_another_tenants_name(self, seeded):
        """Knowing the name must not be enough. This is the whole attack: the
        caller guesses or learns a capability name from another workspace."""
        assert await seeded.resolve_capability(_caller([ACME]), "globex_report") is None

    @pytest.mark.asyncio
    async def test_missing_and_forbidden_are_indistinguishable(self, seeded):
        """Both return None, so the surface cannot be used to enumerate which
        capability names exist in other tenants."""
        forbidden = await seeded.resolve_capability(_caller([ACME]), "globex_report")
        missing = await seeded.resolve_capability(_caller([ACME]), "no_such_thing")
        assert forbidden is None and missing is None

    @pytest.mark.asyncio
    async def test_no_groups_resolves_nothing(self, seeded):
        assert await seeded.resolve_capability(_caller([]), "acme_report") is None


class TestProtocolScoping:
    @pytest.mark.asyncio
    async def test_capability_not_published_to_a_protocol_is_invisible(self, session):
        svc = PublicationService(session)
        await svc.publish(
            "crew-a2a-only",
            CrewPublicationCreate(
                external_name="a2a_only",
                description="Exposed over A2A only.",
                protocols=["a2a"],
            ),
            _Ctx([ACME]),
        )
        await session.commit()

        assert await svc.list_capabilities(_caller([ACME], protocol="mcp")) == []
        a2a = await svc.list_capabilities(_caller([ACME], protocol="a2a"))
        assert [c.name for c in a2a] == ["a2a_only"]

    @pytest.mark.asyncio
    async def test_a2a_publication_is_not_invocable_over_mcp(self, session):
        """Being on the A2A card must not make it callable as an MCP tool."""
        svc = PublicationService(session)
        await svc.publish(
            "crew-a2a-only",
            CrewPublicationCreate(
                external_name="a2a_only",
                description="Exposed over A2A only.",
                protocols=["a2a"],
            ),
            _Ctx([ACME]),
        )
        await session.commit()

        assert (
            await svc.resolve_capability(_caller([ACME], protocol="mcp"), "a2a_only")
            is None
        )
        assert (
            await svc.resolve_capability(_caller([ACME], protocol="a2a"), "a2a_only")
            is not None
        )


class TestBothSurfacesSeeOneList:
    @pytest.mark.asyncio
    async def test_mcp_and_a2a_render_the_same_capabilities(self, seeded):
        """The invariant the whole design rests on: the MCP tool list and the
        A2A card's skills[] are two projections of ONE query, so they cannot
        advertise different capabilities.
        """
        mcp = await seeded.list_capabilities(_caller([ACME], protocol="mcp"))
        a2a = await seeded.list_capabilities(_caller([ACME], protocol="a2a"))
        assert [c.model_dump() for c in mcp] == [c.model_dump() for c in a2a]


class TestWritesAreGroupScoped:
    @pytest.mark.asyncio
    async def test_cannot_unpublish_another_tenants_crew(self, seeded, session):
        assert await seeded.unpublish("crew-globex", _Ctx([ACME])) is False
        # ...and it is still there for its owner.
        still = await seeded.resolve_capability(_caller([GLOBEX]), "globex_report")
        assert still is not None

    @pytest.mark.asyncio
    async def test_cannot_update_another_tenants_publication(self, seeded):
        from src.schemas.crew_publication import CrewPublicationUpdate

        result = await seeded.update(
            "crew-globex",
            CrewPublicationUpdate(description="hijacked"),
            _Ctx([ACME]),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_publishing_twice_updates_rather_than_duplicates(
        self, seeded, session
    ):
        await seeded.publish(
            "crew-acme",
            CrewPublicationCreate(
                external_name="acme_report",
                description="Now with a better description.",
                protocols=["mcp"],
            ),
            _Ctx([ACME]),
        )
        await session.commit()

        repo = CrewPublicationRepository(session)
        rows = await repo.list_published_for_group([ACME])
        assert len(rows) == 1
        assert rows[0].description == "Now with a better description."
        assert rows[0].protocols == ["mcp"]

    @pytest.mark.asyncio
    async def test_two_tenants_may_use_the_same_external_name(self, session):
        """Names are unique WITHIN a group, not globally — otherwise one tenant
        publishing `analyse_powerbi_model` would block every other tenant."""
        svc = PublicationService(session)
        for group in (ACME, GLOBEX):
            await svc.publish(
                f"crew-{group}",
                CrewPublicationCreate(
                    external_name="shared_name",
                    description=f"{group} version.",
                    protocols=["mcp"],
                ),
                _Ctx([group]),
            )
        await session.commit()

        acme = await svc.resolve_capability(_caller([ACME]), "shared_name")
        globex = await svc.resolve_capability(_caller([GLOBEX]), "shared_name")
        assert acme.entity_id == f"crew-{ACME}"
        assert globex.entity_id == f"crew-{GLOBEX}"
