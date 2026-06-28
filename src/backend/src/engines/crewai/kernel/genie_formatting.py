"""Genie MCP output formatting — shared by the crew and flow paths.

When a task uses a Databricks Genie MCP server, its output should be formatted
like the native Genie tool. Both the crew path (``task_adapter``/
``crew_preparation``) and the flow path (``flow.modules.task_adapter``) need the
exact same instruction block, so it lives here as the single source of truth.
"""
from typing import Any, Dict, Optional

# Managed-Genie MCP servers are registered as ``.../api/2.0/mcp/genie/<space_id>``.
_GENIE_MCP_URL_MARKER = "/api/2.0/mcp/genie/"

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


def _genie_space_id_from_url(server_url: Any) -> Optional[str]:
    """The Genie space id embedded in a managed-Genie MCP server URL
    (``.../api/2.0/mcp/genie/<space_id>``), or None for non-Genie URLs."""
    url = str(server_url or "")
    if _GENIE_MCP_URL_MARKER not in url:
        return None
    tail = url.split(_GENIE_MCP_URL_MARKER, 1)[1].strip("/")
    space_id = tail.split("/")[0].split("?")[0] if tail else ""
    return space_id or None


def apply_genie_mcp_space_id(tools: Any, agent: Any = None) -> Optional[str]:
    """Configure the custom GenieTool from a managed-Genie MCP server the task
    already selected, and return the space id applied (or None).

    The crew generator commonly assigns BOTH the custom ``GenieTool`` (for its
    data-tool prompt routing) and a managed-Genie MCP server. The MCP server
    carries the space id in its URL; the GenieTool starts with none and would
    otherwise error "Genie space ID is not configured". This copies the space
    id from the in-memory MCP tool into any GenieTool instance that lacks one,
    so picking the Genie MCP server in the picker is enough — no separate
    spaceId step.

    Scans both the task ``tools`` and ``agent.tools`` (the crew path puts tools
    on the task, the flow path puts them on the agent), mutating GenieTool
    instances in place. Shared by both paths via ``build_task_args``.
    """
    try:
        agent_tools = getattr(agent, "tools", None) if agent is not None else None
        all_tools = list(tools or []) + list(agent_tools or [])

        # Space id advertised by a managed-Genie MCP tool's adapter URL.
        space_id = None
        for tool in all_tools:
            wrapper = getattr(tool, "_mcp_tool_wrapper", None)
            adapter = getattr(wrapper, "adapter", None)
            space_id = _genie_space_id_from_url(getattr(adapter, "server_url", ""))
            if space_id:
                break
        if not space_id:
            return None

        # Populate any GenieTool instance that doesn't already have a space id.
        applied = None
        for tool in all_tools:
            is_genie_tool = (
                type(tool).__name__ == "GenieTool"
                or getattr(tool, "name", None) == "GenieTool"
            )
            if is_genie_tool and not getattr(tool, "_space_id", None):
                tool._space_id = space_id
                applied = space_id
        return applied
    except Exception:  # pragma: no cover - defensive
        return None
