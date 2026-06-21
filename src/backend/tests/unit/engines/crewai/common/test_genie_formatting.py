"""Single source of truth for Genie MCP output formatting, shared by the crew
and flow paths. These tests pin the canonical behavior and prove both paths'
call patterns produce identical results."""
from src.engines.crewai.common.genie_formatting import (
    append_genie_mcp_formatting,
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
        assert append_genie_mcp_formatting("base", {"MCP_SERVERS": {"servers": [123, None]}}) == "base"


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
                task_config.get("expected_output", ""), task_config.get("tool_configs", {})
            )
            assert task_config["expected_output"] == flow_out
