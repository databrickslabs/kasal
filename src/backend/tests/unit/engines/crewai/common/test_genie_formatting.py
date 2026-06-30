"""Genie MCP space-id bridging. Output FORMATTING is no longer done here — Genie
answers flow through the shared A2UI composer like every other deliverable — so
these tests cover only the functional space-id bridge."""

from types import SimpleNamespace

from src.engines.crewai.kernel.genie_formatting import (
    _genie_space_id_from_url,
    apply_genie_mcp_space_id,
    genie_mcp_space_id,
)

GENIE_MCP_URL = (
    "https://ws.databricks.com/api/2.0/mcp/genie/01f16bcd318214ec8ef983b7627e0221"
)
SPACE_ID = "01f16bcd318214ec8ef983b7627e0221"


def _genie_mcp_tool(url=GENIE_MCP_URL):
    """A managed-Genie MCP tool: MCPCrewAITool -> _mcp_tool_wrapper.adapter.server_url."""
    adapter = SimpleNamespace(server_url=url)
    return SimpleNamespace(
        name="genie_query_space",
        _mcp_tool_wrapper=SimpleNamespace(adapter=adapter),
    )


class _FakeGenieTool:
    """Stand-in for the custom GenieTool (matched by class name + _space_id attr)."""

    def __init__(self, space_id=None):
        self.name = "GenieTool"
        self._space_id = space_id


_FakeGenieTool.__name__ = "GenieTool"


class TestGenieSpaceIdFromUrl:
    def test_extracts_space_id(self):
        assert _genie_space_id_from_url(GENIE_MCP_URL) == SPACE_ID

    def test_trailing_slash_and_query(self):
        assert _genie_space_id_from_url(GENIE_MCP_URL + "/") == SPACE_ID
        assert _genie_space_id_from_url(GENIE_MCP_URL + "?x=1") == SPACE_ID

    def test_non_genie_url_is_none(self):
        assert _genie_space_id_from_url("https://ws/api/2.0/mcp/sql") is None
        assert _genie_space_id_from_url("") is None
        assert _genie_space_id_from_url(None) is None


class TestGenieMcpSpaceId:
    def test_returns_space_id_even_without_genie_tool(self):
        # apply_* returns None here (no GenieTool to fill); this returns the id.
        assert genie_mcp_space_id([_genie_mcp_tool()]) == SPACE_ID
        assert apply_genie_mcp_space_id([_genie_mcp_tool()]) is None

    def test_scans_agent_tools(self):
        agent = SimpleNamespace(tools=[_genie_mcp_tool()])
        assert genie_mcp_space_id([], agent) == SPACE_ID

    def test_none_without_genie_mcp(self):
        assert genie_mcp_space_id([_FakeGenieTool()]) is None
        assert genie_mcp_space_id([]) is None
        assert genie_mcp_space_id(None, None) is None


class TestApplyGenieMcpSpaceId:
    def test_crew_path_fills_genie_tool_from_mcp(self):
        # Crew path: both tools live in the task `tools` list.
        genie_tool = _FakeGenieTool()
        applied = apply_genie_mcp_space_id([_genie_mcp_tool(), genie_tool])
        assert applied == SPACE_ID
        assert genie_tool._space_id == SPACE_ID

    def test_flow_path_fills_from_agent_tools(self):
        # Flow path: tools live on agent.tools, task `tools` is empty.
        genie_tool = _FakeGenieTool()
        agent = SimpleNamespace(tools=[_genie_mcp_tool(), genie_tool])
        applied = apply_genie_mcp_space_id([], agent)
        assert applied == SPACE_ID
        assert genie_tool._space_id == SPACE_ID

    def test_does_not_overwrite_existing_space_id(self):
        genie_tool = _FakeGenieTool(space_id="already-set")
        applied = apply_genie_mcp_space_id([_genie_mcp_tool(), genie_tool])
        assert applied is None
        assert genie_tool._space_id == "already-set"

    def test_noop_without_genie_mcp_tool(self):
        genie_tool = _FakeGenieTool()
        applied = apply_genie_mcp_space_id([genie_tool])  # no MCP tool present
        assert applied is None
        assert genie_tool._space_id is None

    def test_noop_without_genie_tool(self):
        # MCP genie tool present but no GenieTool to configure.
        assert apply_genie_mcp_space_id([_genie_mcp_tool()]) is None

    def test_safe_with_empty_and_none(self):
        assert apply_genie_mcp_space_id([]) is None
        assert apply_genie_mcp_space_id(None) is None
        assert apply_genie_mcp_space_id(None, None) is None
