"""The MCP server surface: what it advertises, and what it refuses.

The adapter is meant to be thin. These tests pin that: the tool list and the
dispatch table cannot drift apart, every tool takes a resolved caller, and
capability listing goes through the SAME shared call the A2A card will use.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.external.identity import ExternalCaller
from src.services.external.state import ExternalTaskState
from src.services.mcp.mcp_server import server as mcp_server
from src.services.mcp.mcp_server.tools import TOOL_DEFINITIONS, TOOL_HANDLERS


class _Ctx:
    def __init__(self, group_ids):
        self.group_ids = list(group_ids)
        self.group_email = "caller@example.com"
        self.user_role = "admin"
        self.highest_role = "admin"
        self.current_user = None

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


def _caller(group_ids=("acme_corp",)):
    return ExternalCaller(
        group_context=_Ctx(group_ids), protocol="mcp", identifier="caller@example.com"
    )


class TestAdvertisedSurface:
    def test_every_advertised_tool_is_callable(self):
        """The list and the dispatch table are two halves of one contract.
        A tool advertised but not handled is a client-visible 404 on something
        Kasal said it could do."""
        advertised = {t["name"] for t in TOOL_DEFINITIONS}
        assert advertised == set(TOOL_HANDLERS)

    def test_no_blocking_crew_tool_is_advertised(self):
        """Crew runs take minutes. A BLOCKING run_crew passes testing against a
        small crew and times out in production, so it must never exist.

        A capability's own tool is the sanctioned shape: it returns a handle and
        does not wait, and `get_run_status` is how the caller follows it. The
        distinction is the whole point, so it is pinned rather than left to
        reviewer memory."""
        names = {t["name"] for t in TOOL_DEFINITIONS}
        assert "run_crew" not in names
        assert {"get_run_status", "get_run_result"} <= names

    def test_every_tool_declares_an_input_schema(self):
        for tool in TOOL_DEFINITIONS:
            assert tool["inputSchema"]["type"] == "object", tool["name"]

    def test_every_tool_has_a_description_saying_when_to_use_it(self):
        """The description is the only thing a calling agent matches on. A vague
        one means the tool is never selected."""
        for tool in TOOL_DEFINITIONS:
            assert len(tool["description"]) > 40, tool["name"]


class TestPerCapabilityTools:
    """Layer 2: one tool per published capability — crews AND flows.

    A calling agent selects on the description, so the description has to be
    true of the thing behind it. Calling every capability a "crew" told a client
    the opposite of what a flow does: that it will not pause for a human, and
    that a follow-up needs a fresh run.
    """

    @staticmethod
    def _capabilities(*caps):
        return patch(
            "src.services.mcp.mcp_server.tools.PublicationService",
            **{"return_value.list_capabilities": AsyncMock(return_value=list(caps))},
        )

    @staticmethod
    def _cap(**kwargs):
        from src.schemas.crew_publication import PublishedCapability

        return PublishedCapability(
            **{
                "entity_id": "e1",
                "name": "thing",
                "description": "Does the thing.",
                **kwargs,
            }
        )

    @pytest.mark.asyncio
    async def test_a_published_flow_becomes_a_tool(self):
        cap = self._cap(entity_type="flow", name="swiss_news")
        with self._capabilities(cap):
            tools = await mcp_server.list_tools(_caller())

        assert "swiss_news" in {t["name"] for t in tools}

    @pytest.mark.asyncio
    async def test_a_flow_is_described_as_a_flow(self):
        cap = self._cap(entity_type="flow", name="swiss_news")
        with self._capabilities(cap):
            tools = await mcp_server.list_tools(_caller())

        description = next(t for t in tools if t["name"] == "swiss_news")["description"]
        assert "flow" in description
        assert "Starts a crew" not in description

    @pytest.mark.asyncio
    async def test_a_conversational_flow_says_follow_ups_continue_it(self):
        cap = self._cap(entity_type="flow", name="swiss_news", conversational=True)
        with self._capabilities(cap):
            tools = await mcp_server.list_tools(_caller())

        description = next(t for t in tools if t["name"] == "swiss_news")["description"]
        assert "session_id" in description

    @pytest.mark.asyncio
    async def test_a_crew_is_still_described_as_a_crew(self):
        cap = self._cap(name="acme_report")
        with self._capabilities(cap):
            tools = await mcp_server.list_tools(_caller())

        description = next(t for t in tools if t["name"] == "acme_report")[
            "description"
        ]
        assert "Starts a crew" in description


class TestDispatch:
    @pytest.mark.asyncio
    async def test_unknown_tool_is_refused(self):
        """Unknown means: not a fixed tool AND not a crew published to this
        caller. Layer-2 tool names ARE publication names, so the publication
        lookup has to miss too before a name is genuinely unknown."""
        with patch(
            "src.services.mcp.mcp_server.tools.PublicationService"
        ) as publications:
            publications.return_value.resolve_capability = AsyncMock(return_value=None)
            with pytest.raises(mcp_server.UnknownToolError):
                await mcp_server.call_tool(_caller(), "not_a_tool", {})

    @pytest.mark.asyncio
    async def test_a_published_crew_name_is_callable_as_a_tool(self):
        """Layer-2 dispatch: calling the tool starts the crew behind it."""
        from src.services.external.invocation import InvocationResult
        from src.services.external.state import ExternalTaskState

        publication = MagicMock(
            external_name="acme_report", entity_type="crew", entity_id="c1"
        )
        with (
            patch(
                "src.services.mcp.mcp_server.tools.PublicationService"
            ) as publications,
            patch(
                "src.services.mcp.mcp_server.tools.start_run",
                new=AsyncMock(
                    return_value=InvocationResult(
                        run_id="run-9", state=ExternalTaskState.SUBMITTED
                    )
                ),
            ) as start,
        ):
            publications.return_value.resolve_capability = AsyncMock(
                return_value=publication
            )
            result = await mcp_server.call_tool(
                _caller(), "acme_report", {"request": "do it"}
            )

        assert result == {"run_id": "run-9", "state": "submitted"}
        assert start.await_args.kwargs["publication"] is publication

    @pytest.mark.asyncio
    async def test_a_crew_published_to_another_tenant_is_simply_unknown(self):
        """resolve_capability is group-scoped, so a name from another workspace
        misses and becomes the same UnknownToolError as a typo."""
        with patch(
            "src.services.mcp.mcp_server.tools.PublicationService"
        ) as publications:
            publications.return_value.resolve_capability = AsyncMock(return_value=None)
            with pytest.raises(mcp_server.UnknownToolError):
                await mcp_server.call_tool(_caller(), "someone_elses_crew", {})

    @pytest.mark.asyncio
    async def test_dispatch_passes_the_resolved_caller_through(self):
        """Nothing downstream may invent its own group context."""
        seen = {}

        async def _spy(caller, session=None, **kwargs):
            seen["caller"] = caller
            return {"ok": True}

        with patch.dict(mcp_server.TOOL_HANDLERS, {"spy": _spy}, clear=False):
            await mcp_server.call_tool(_caller(["globex_inc"]), "spy", {})

        assert seen["caller"].group_ids == ["globex_inc"]


class TestRetiredTools:
    """`list_crews` and `start_crew` are gone, and must not come back by habit.

    They named the published set at runtime and ran something out of it — a
    second way to do what calling the capability's own tool does, costing an
    extra round trip and an extra decision for the calling agent. Their real job
    was working around a tool list that could not refresh; the server now
    declares `tools.listChanged` and pushes the notification, so the list a
    client holds is current and the pair had nothing left to do.
    """

    def test_neither_is_advertised(self):
        names = {t["name"] for t in TOOL_DEFINITIONS}
        assert "list_crews" not in names
        assert "start_crew" not in names

    def test_neither_is_dispatchable(self):
        assert "list_crews" not in TOOL_HANDLERS
        assert "start_crew" not in TOOL_HANDLERS

    @pytest.mark.asyncio
    async def test_calling_one_is_an_unknown_tool(self):
        """It falls through to capability dispatch and misses, which is the same
        answer a typo gets — not a special deprecation path."""
        with patch(
            "src.services.mcp.mcp_server.tools.PublicationService"
        ) as publications:
            publications.return_value.resolve_capability = AsyncMock(return_value=None)
            with pytest.raises(mcp_server.UnknownToolError):
                await mcp_server.call_tool(_caller(), "start_crew", {})

    def test_the_names_are_free_for_a_capability_to_use(self):
        """The reserved set is derived from the advertised tools, so retiring
        them releases the names rather than leaving a phantom reservation."""
        from src.services.mcp.mcp_server.tools import _RESERVED_NAMES

        assert "list_crews" not in _RESERVED_NAMES
        assert "start_crew" not in _RESERVED_NAMES


class TestAskKasal:
    @pytest.mark.asyncio
    async def test_returns_the_run_id_and_state(self):
        from src.services.external.invocation import InvocationResult

        with patch(
            "src.services.mcp.mcp_server.tools.ask",
            new=AsyncMock(
                return_value=InvocationResult(
                    run_id="run-1", state=ExternalTaskState.COMPLETED, output="42"
                )
            ),
        ):
            result = await mcp_server.call_tool(
                _caller(), "ask_kasal", {"question": "what is 6*7?"}
            )

        assert result == {"run_id": "run-1", "state": "completed", "output": "42"}

    @pytest.mark.asyncio
    async def test_state_is_the_canonical_vocabulary_not_kasal_status(self):
        """An external caller sees A2A's vocabulary, never RUNNING/COMPLETED."""
        from src.services.external.invocation import InvocationResult

        with patch(
            "src.services.mcp.mcp_server.tools.ask",
            new=AsyncMock(
                return_value=InvocationResult(
                    run_id="run-2", state=ExternalTaskState.FAILED, error="boom"
                )
            ),
        ):
            result = await mcp_server.call_tool(
                _caller(), "ask_kasal", {"question": "hi"}
            )

        assert result["state"] == "failed"
        assert result["error"] == "boom"
