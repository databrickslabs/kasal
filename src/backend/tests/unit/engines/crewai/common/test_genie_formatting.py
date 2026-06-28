"""Single source of truth for Genie MCP output formatting, shared by the crew
and flow paths. These tests pin the canonical behavior and prove both paths'
call patterns produce identical results."""

from types import SimpleNamespace

from src.engines.crewai.kernel.genie_formatting import (
    _genie_space_id_from_url,
    append_genie_mcp_formatting,
    apply_genie_mcp_space_id,
    uses_genie_mcp,
)

GENIE = {"MCP_SERVERS": {"servers": ["Databricks Genie: my-space"]}}
NOT_GENIE = {"MCP_SERVERS": {"servers": ["other:x"]}}


class TestUsesGenieMcp:
    def test_detects_genie_server(self):
        assert uses_genie_mcp(GENIE) is True

    def test_false_without_genie(self):
        assert uses_genie_mcp(NOT_GENIE) is False
        assert uses_genie_mcp({}) is False

    def test_malformed_is_safe(self):
        assert uses_genie_mcp({"MCP_SERVERS": None}) is False
        assert uses_genie_mcp({"MCP_SERVERS": {"servers": [123, None]}}) is False


class TestAppendGenieMcpFormatting:
    def test_appended_when_genie_present(self):
        out = append_genie_mcp_formatting("base output", GENIE)
        assert out.startswith("base output")
        assert "Genie Tool output structure" in out
        assert "SQL Query" in out
        assert "Suggested Follow-up Questions" in out

    def test_not_appended_without_genie(self):
        assert append_genie_mcp_formatting("base", NOT_GENIE) == "base"

    def test_no_mcp_servers_is_noop(self):
        assert append_genie_mcp_formatting("base", {}) == "base"

    def test_empty_output_has_no_leading_whitespace(self):
        # Crew-path parity: an empty expected_output yields stripped instructions.
        out = append_genie_mcp_formatting("", GENIE)
        assert out == out.strip()
        assert out.startswith("IMPORTANT:")

    def test_idempotent_does_not_double_append(self):
        once = append_genie_mcp_formatting("base", GENIE)
        twice = append_genie_mcp_formatting(once, GENIE)
        assert once == twice

    def test_malformed_tool_configs_is_safe(self):
        assert append_genie_mcp_formatting("base", {"MCP_SERVERS": None}) == "base"
        assert (
            append_genie_mcp_formatting(
                "base", {"MCP_SERVERS": {"servers": [123, None]}}
            )
            == "base"
        )


class TestCrewFlowParity:
    """The crew path mutates a dict; the flow path threads a string. Both must
    end up with byte-identical expected_output for the same inputs."""

    def test_paths_produce_identical_output(self):
        for base in ("", "Some expected output."):
            # Flow-path call pattern: string in, string out.
            flow_out = append_genie_mcp_formatting(base, GENIE)
            # Crew-path call pattern: assign back into the task_config dict.
            task_config = {"expected_output": base, "tool_configs": GENIE}
            task_config["expected_output"] = append_genie_mcp_formatting(
                task_config.get("expected_output", ""),
                task_config.get("tool_configs", {}),
            )
            assert task_config["expected_output"] == flow_out


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
