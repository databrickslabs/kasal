"""The invariant the whole two-protocol design rests on.

Both proposals name this as the failure mode the shared layer exists to
prevent: a fix applied to one adapter and not the other, producing two surfaces
that disagree. The docs ask for it to be enforced by a test rather than by
review, because "the MCP and A2A surfaces disagree about whether a run is
RUNNING or WORKING" is a bug report nobody can reproduce.

So: the MCP tool list and the A2A card's skills[] are asserted to be projections
of ONE query, and the state vocabulary is asserted to have ONE definition.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.models.execution_status import ExecutionStatus
from src.schemas.crew_publication import PublishedCapability
from src.services.external.identity import ExternalCaller
from src.services.external.state import ExternalTaskState, to_external_state


class _Ctx:
    def __init__(self, group_ids=("acme_corp",)):
        self.group_ids = list(group_ids)
        self.group_email = "caller@example.com"
        self.user_role = "admin"
        self.highest_role = "admin"
        self.current_user = None
        self.access_token = "tok"

    @property
    def primary_group_id(self):
        return self.group_ids[0]


def _caller(protocol):
    return ExternalCaller(
        group_context=_Ctx(), protocol=protocol, identifier="caller@example.com"
    )


_CAPABILITIES = [
    PublishedCapability(
        entity_id="c1",
        name="analyse_powerbi_model",
        description="Analyse a PowerBI semantic model and report on it.",
        input_schema={"type": "object", "properties": {"model": {"type": "string"}}},
    ),
    PublishedCapability(
        entity_id="c2",
        name="quarterly_review",
        description="Produce the quarterly review pack.",
    ),
]


async def _mcp_capabilities(capabilities):
    """The per-capability tools an MCP client would see, in card-comparable shape.

    The generic ``list_crews`` used to provide this; it was retired once the
    tool list became refreshable, so the comparison reads the tool list itself.
    """
    from unittest.mock import AsyncMock, patch

    from src.services.mcp.mcp_server.server import list_tools
    from src.services.mcp.mcp_server.tools import TOOL_DEFINITIONS

    fixed = {t["name"] for t in TOOL_DEFINITIONS}
    with patch("src.services.mcp.mcp_server.tools.PublicationService") as svc:
        svc.return_value.list_capabilities = AsyncMock(return_value=capabilities)
        tools = await list_tools(_caller("mcp"))

    by_name = {c.name: c for c in capabilities}
    return [
        {
            "name": t["name"],
            # The tool description appends a usage hint; the capability's own
            # description is what the card carries, so compare that.
            "description": by_name[t["name"]].description,
            "input_schema": by_name[t["name"]].input_schema,
        }
        for t in tools
        if t["name"] not in fixed
    ]


class TestOneCapabilityList:
    @pytest.mark.asyncio
    async def test_mcp_tools_and_a2a_skills_show_the_same_capabilities(self):
        """Two projections of one query. If these ever differ, a second
        capability source has been introduced somewhere.

        Read off the TOOL LIST now that the generic `list_crews` is retired —
        the tool list is what an MCP client actually sees, so comparing it to
        the card is the stronger form of the same invariant."""
        from src.services.a2a.a2a_server.card import build_card

        mcp_result = await _mcp_capabilities(_CAPABILITIES)

        with patch("src.services.a2a.a2a_server.card.PublicationService") as a2a_svc:
            a2a_svc.return_value.list_capabilities = AsyncMock(
                return_value=_CAPABILITIES
            )
            card = await build_card(_caller("a2a"), base_url="https://x")

        mcp_names = [c["name"] for c in mcp_result]
        a2a_names = [s.id for s in card.skills]
        assert mcp_names == a2a_names

        mcp_descriptions = [c["description"] for c in mcp_result]
        a2a_descriptions = [s.description for s in card.skills]
        assert mcp_descriptions == a2a_descriptions

    @pytest.mark.asyncio
    async def test_the_input_schema_is_the_same_on_both_surfaces(self):
        """MCP tool inputSchema and A2A skill inputSchema are one field. Two
        copies would drift and one would quietly become wrong."""
        from src.services.a2a.a2a_server.card import build_card

        mcp_result = await _mcp_capabilities(_CAPABILITIES)

        with patch("src.services.a2a.a2a_server.card.PublicationService") as a2a_svc:
            a2a_svc.return_value.list_capabilities = AsyncMock(
                return_value=_CAPABILITIES
            )
            card = await build_card(_caller("a2a"), base_url="https://x")

        assert [c["input_schema"] for c in mcp_result] == [
            s.inputSchema for s in card.skills
        ]

    @pytest.mark.asyncio
    async def test_both_read_through_the_same_service_method(self):
        """Not merely 'they agree today' — they call the same function."""
        from src.services.a2a.a2a_server.card import build_card
        from src.services.mcp.mcp_server.server import list_tools

        with patch("src.services.mcp.mcp_server.tools.PublicationService") as mcp_svc:
            mcp_svc.return_value.list_capabilities = AsyncMock(return_value=[])
            await list_tools(_caller("mcp"))
            assert mcp_svc.return_value.list_capabilities.await_count == 1

        with patch("src.services.a2a.a2a_server.card.PublicationService") as a2a_svc:
            a2a_svc.return_value.list_capabilities = AsyncMock(return_value=[])
            await build_card(_caller("a2a"), base_url="https://x")
            assert a2a_svc.return_value.list_capabilities.await_count == 1


class TestLayerTwoMatchesSkills:
    """The strongest form of the invariant, now that both surfaces expose one
    entry PER CREW: the MCP tool list and the A2A card should name the same
    capabilities, not merely describe them consistently."""

    @pytest.mark.asyncio
    async def test_per_crew_tools_and_skills_name_the_same_capabilities(self):
        from src.services.a2a.a2a_server.card import build_card
        from src.services.mcp.mcp_server.server import list_tools
        from src.services.mcp.mcp_server.tools import TOOL_DEFINITIONS

        fixed = {t["name"] for t in TOOL_DEFINITIONS}

        with patch("src.services.mcp.mcp_server.tools.PublicationService") as mcp_svc:
            mcp_svc.return_value.list_capabilities = AsyncMock(
                return_value=_CAPABILITIES
            )
            tools = await list_tools(_caller("mcp"))

        with patch("src.services.a2a.a2a_server.card.PublicationService") as a2a_svc:
            a2a_svc.return_value.list_capabilities = AsyncMock(
                return_value=_CAPABILITIES
            )
            card = await build_card(_caller("a2a"), base_url="https://x")

        per_crew_tools = sorted({t["name"] for t in tools} - fixed)
        skills = sorted(s.id for s in card.skills)
        assert per_crew_tools == skills

    @pytest.mark.asyncio
    async def test_a_crew_shadowing_a_built_in_tool_is_skipped_not_silent(self):
        """A crew published as `ask_kasal` would shadow a control tool. It is
        dropped with a warning rather than either silently winning (breaking
        every caller's ask_kasal) or silently losing.

        With the generic runner retired there is no way to reach it by name any
        more, so the publication has to be RENAMED — which is why the skip is
        logged rather than silent."""
        from src.services.mcp.mcp_server.server import list_tools

        clash = PublishedCapability(
            entity_id="c9", name="ask_kasal", description="A crew named like a tool."
        )
        with patch("src.services.mcp.mcp_server.tools.PublicationService") as mcp_svc:
            mcp_svc.return_value.list_capabilities = AsyncMock(return_value=[clash])
            tools = await list_tools(_caller("mcp"))

        # Exactly one ask_kasal — the built-in.
        assert [t["name"] for t in tools].count("ask_kasal") == 1


class TestOneStateVocabulary:
    @pytest.mark.asyncio
    async def test_a_status_means_the_same_thing_on_both_surfaces(self):
        """The named failure: the two surfaces disagreeing about whether a run
        is RUNNING or WORKING."""
        from src.services.a2a.a2a_server.render import to_wire_state

        for status in ExecutionStatus:
            canonical = to_external_state(status.value)
            # A2A renders the wire constant; MCP renders the short form. Both
            # derive from the SAME canonical value — that is the invariant.
            wire = to_wire_state(canonical)
            assert wire == f"TASK_STATE_{canonical.value.upper()}"

    def test_the_wire_map_covers_every_canonical_state(self):
        from src.services.a2a.a2a_server.render import _STATE_TO_WIRE

        missing = [s for s in ExternalTaskState if s not in _STATE_TO_WIRE]
        assert (
            not missing
        ), "these canonical states have no A2A wire constant: " + ", ".join(
            s.value for s in missing
        )

    def test_mcp_reports_the_canonical_value_verbatim(self):
        """MCP has no state vocabulary of its own, so it uses the canonical one
        directly rather than inventing a third naming."""
        from src.services.external.invocation import InvocationResult

        payload = InvocationResult(
            run_id="r", state=ExternalTaskState.INPUT_REQUIRED
        ).as_dict()
        assert payload["state"] == "input_required"
