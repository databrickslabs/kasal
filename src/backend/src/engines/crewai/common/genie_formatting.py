"""Genie MCP output formatting — shared by the crew and flow paths.

When a task uses a Databricks Genie MCP server, its output should be formatted
like the native Genie tool. Both the crew path (``task_adapter``/
``crew_preparation``) and the flow path (``flow.modules.task_adapter``) need the
exact same instruction block, so it lives here as the single source of truth.
"""
from typing import Any, Dict

# Canonical Genie-tool output-formatting instruction block. Byte-for-byte the
# text both paths previously appended independently.
_GENIE_MCP_FORMAT_INSTRUCTIONS = (
    "\n\nIMPORTANT: Format your response to match the Genie Tool output structure:\n"
    "1. **Natural Language Summary**: Start with a clear, concise summary of what the query results show.\n"
    "2. **Query Description**: Explain in plain English what the query does.\n"
    "3. **SQL Query**: Include the generated SQL query (if available).\n"
    "4. **Query Results**: Format any returned data as a structured table with column headers and rows.\n"
    "5. **Suggested Follow-up Questions**: List 3-5 relevant follow-up questions the user might ask.\n"
    "6. **Link**: Include a link to open the results in Genie if available.\n"
    "\nThis ensures Genie MCP output is consistently formatted like the Genie Tool."
)


def uses_genie_mcp(tool_configs: Dict[str, Any]) -> bool:
    """True if ``tool_configs`` wires up at least one Databricks Genie MCP server."""
    try:
        mcp_servers = (tool_configs.get("MCP_SERVERS", {}) or {}).get("servers", []) or []
        return any(
            isinstance(server, str) and "databricks genie:" in server.lower()
            for server in mcp_servers
        )
    except Exception:  # pragma: no cover - defensive
        return False


def append_genie_mcp_formatting(expected_output: str, tool_configs: Dict[str, Any]) -> str:
    """Return ``expected_output`` with Genie-tool formatting instructions appended
    when the task uses a Genie MCP server, else unchanged.

    Pure (string in, string out) and idempotent — the block is never appended
    twice. Behavior preserves the crew path exactly: an empty ``expected_output``
    yields the instructions with no leading whitespace; a non-empty one gets the
    block appended.
    """
    try:
        if not uses_genie_mcp(tool_configs):
            return expected_output
        expected_output = expected_output or ""
        # Idempotent: never append the block twice.
        if _GENIE_MCP_FORMAT_INSTRUCTIONS.strip() in expected_output:
            return expected_output
        if not expected_output:
            return _GENIE_MCP_FORMAT_INSTRUCTIONS.strip()
        return expected_output + _GENIE_MCP_FORMAT_INSTRUCTIONS
    except Exception:  # pragma: no cover - defensive
        return expected_output
