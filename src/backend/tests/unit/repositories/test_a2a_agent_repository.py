"""The global/workspace override queries, against a real database.

These are the risky part of the two-tier model — a mocked session proves the
method was called, not that the SQL selects the right rows. The visibility rules
here are what stop one workspace's opt-out from reaching another's, and what
makes a Kasal admin's withdrawal cascade.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.models.a2a_agent import A2AAgent
from src.repositories.a2a_agent_repository import A2AAgentRepository


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(A2AAgent.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _add(session, name, *, group_id=None, enabled=True):
    row = A2AAgent(
        name=name,
        card_url="https://remote.example.com",
        group_id=group_id,
        enabled=enabled,
        auth_type="obo",
    )
    session.add(row)
    await session.flush()
    return row


class TestVisibility:
    @pytest.mark.asyncio
    async def test_a_workspace_sees_only_agents_a_kasal_admin_made_available(
        self, session
    ):
        await _add(session, "Offered", enabled=True)
        await _add(session, "Withheld", enabled=False)

        rows = await A2AAgentRepository(session).list_for_group_scope("acme")
        assert [r.name for r in rows] == ["Offered"]

    @pytest.mark.asyncio
    async def test_a_workspaces_own_row_shadows_the_base(self, session):
        """One row per name, carrying THIS workspace's state — not the base and
        the override both, which would render as a duplicate."""
        await _add(session, "Shared", enabled=True)
        await _add(session, "Shared", group_id="acme", enabled=False)

        rows = await A2AAgentRepository(session).list_for_group_scope("acme")
        assert len(rows) == 1
        assert rows[0].group_id == "acme"
        assert rows[0].enabled is False

    @pytest.mark.asyncio
    async def test_one_workspaces_opt_out_does_not_reach_another(self, session):
        await _add(session, "Shared", enabled=True)
        await _add(session, "Shared", group_id="acme", enabled=False)

        rows = await A2AAgentRepository(session).list_for_group_scope("other")
        assert len(rows) == 1
        assert rows[0].group_id is None
        assert rows[0].enabled is True

    @pytest.mark.asyncio
    async def test_withdrawing_globally_hides_it_from_a_workspace_that_had_it_on(
        self, session
    ):
        """The cascade. Without it, a Kasal admin turning an agent off would
        leave every workspace that opted in still calling it."""
        await _add(session, "Shared", enabled=False)
        await _add(session, "Shared", group_id="acme", enabled=True)

        rows = await A2AAgentRepository(session).list_for_group_scope("acme")
        assert rows == []


class TestUsability:
    @pytest.mark.asyncio
    async def test_availability_alone_does_not_make_an_agent_callable(self, session):
        """Opt-in, not opt-out: a globally-available agent does nothing until a
        workspace admin turns it on."""
        await _add(session, "Offered", enabled=True)

        rows = await A2AAgentRepository(session).list_enabled_for_group(["acme"])
        assert rows == []

    @pytest.mark.asyncio
    async def test_an_opted_in_agent_is_callable(self, session):
        await _add(session, "Offered", enabled=True)
        await _add(session, "Offered", group_id="acme", enabled=True)

        rows = await A2AAgentRepository(session).list_enabled_for_group(["acme"])
        assert [r.group_id for r in rows] == ["acme"]

    @pytest.mark.asyncio
    async def test_a_withdrawn_base_makes_an_opted_in_agent_uncallable(self, session):
        await _add(session, "Offered", enabled=False)
        await _add(session, "Offered", group_id="acme", enabled=True)

        rows = await A2AAgentRepository(session).list_enabled_for_group(["acme"])
        assert rows == []

    @pytest.mark.asyncio
    async def test_a_workspace_that_turned_it_off_cannot_call_it(self, session):
        await _add(session, "Offered", enabled=True)
        await _add(session, "Offered", group_id="acme", enabled=False)

        rows = await A2AAgentRepository(session).list_enabled_for_group(["acme"])
        assert rows == []


class TestCleanup:
    @pytest.mark.asyncio
    async def test_deleting_overrides_leaves_other_names_alone(self, session):
        await _add(session, "Gone", group_id="acme", enabled=True)
        await _add(session, "Gone", group_id="other", enabled=True)
        await _add(session, "Kept", group_id="acme", enabled=True)

        removed = await A2AAgentRepository(session).delete_overrides_by_name("Gone")
        await session.flush()

        assert removed == 2
        remaining = await A2AAgentRepository(session).list_enabled_for_group(
            ["acme", "other"]
        )
        assert [r.name for r in remaining] == ["Kept"]

    @pytest.mark.asyncio
    async def test_deleting_overrides_never_touches_the_base(self, session):
        """It is called during base deletion, but a bug that removed the base
        here would silently orphan the very row being cleaned up around."""
        await _add(session, "Shared", enabled=True)
        await _add(session, "Shared", group_id="acme", enabled=True)

        await A2AAgentRepository(session).delete_overrides_by_name("Shared")
        await session.flush()

        base = await A2AAgentRepository(session).find_base_by_name("Shared")
        assert base is not None


class TestScopingGuards:
    @pytest.mark.asyncio
    async def test_no_groups_means_no_rows_rather_than_all_rows(self, session):
        """An empty IN () is the classic way a group filter turns into no filter
        at all."""
        await _add(session, "Offered", group_id="acme", enabled=True)
        repository = A2AAgentRepository(session)

        assert await repository.list_enabled_for_group([]) == []
        assert await repository.find_by_name("Offered", []) is None
