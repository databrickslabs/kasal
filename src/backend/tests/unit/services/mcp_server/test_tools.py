"""The MCP server surface: what it advertises, and what it refuses.

The adapter is meant to be thin. These tests pin that: the tool list and the
dispatch table cannot drift apart, every tool takes a resolved caller, and
capability listing goes through the SAME shared call the A2A card will use.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.external.identity import ExternalCaller
from src.services.external.state import ExternalTaskState
from src.services.mcp_server import server as mcp_server
from src.services.mcp_server.tools import TOOL_DEFINITIONS, TOOL_HANDLERS


class _Ctx:
    def __init__(self, group_ids):
        self.group_ids = list(group_ids)
        self.group_email = "caller@example.com"

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
        """Crew runs take minutes. A blocking run_crew passes testing against a
        small crew and times out in production, so it must not exist until async
        handles do."""
        names = {t["name"] for t in TOOL_DEFINITIONS}
        assert "run_crew" not in names
        assert "start_crew" not in names  # phase 3

    def test_every_tool_declares_an_input_schema(self):
        for tool in TOOL_DEFINITIONS:
            assert tool["inputSchema"]["type"] == "object", tool["name"]

    def test_every_tool_has_a_description_saying_when_to_use_it(self):
        """The description is the only thing a calling agent matches on. A vague
        one means the tool is never selected."""
        for tool in TOOL_DEFINITIONS:
            assert len(tool["description"]) > 40, tool["name"]


class TestDispatch:
    @pytest.mark.asyncio
    async def test_unknown_tool_is_refused(self):
        with pytest.raises(mcp_server.UnknownToolError):
            await mcp_server.call_tool(_caller(), "not_a_tool", {})

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


class TestListCrews:
    @pytest.mark.asyncio
    async def test_lists_through_the_shared_publication_service(self):
        """Not its own query. The A2A card's skills[] reads the same call, which
        is what stops the two surfaces advertising different capabilities."""
        from src.schemas.crew_publication import PublishedCapability

        fake = [
            PublishedCapability(
                crew_id="c1", name="acme_report", description="Quarterly report."
            )
        ]
        with patch("src.services.mcp_server.tools.PublicationService") as service_cls:
            service_cls.return_value.list_capabilities = AsyncMock(return_value=fake)
            result = await mcp_server.call_tool(_caller(), "list_crews", {})

        assert result["crews"] == [
            {
                "name": "acme_report",
                "description": "Quarterly report.",
                "input_schema": None,
            }
        ]
        service_cls.return_value.list_capabilities.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_caller_is_what_scopes_the_listing(self):
        with patch("src.services.mcp_server.tools.PublicationService") as service_cls:
            service_cls.return_value.list_capabilities = AsyncMock(return_value=[])
            caller = _caller(["acme_corp"])
            await mcp_server.call_tool(caller, "list_crews", {})

        passed = service_cls.return_value.list_capabilities.await_args.args[0]
        assert passed is caller


class TestAskKasal:
    @pytest.mark.asyncio
    async def test_returns_the_run_id_and_state(self):
        from src.services.external.invocation import InvocationResult

        with patch(
            "src.services.mcp_server.tools.ask",
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
            "src.services.mcp_server.tools.ask",
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
