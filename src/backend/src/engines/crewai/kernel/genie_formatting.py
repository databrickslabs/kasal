"""Genie MCP space-id bridging — shared by the crew and flow paths.

When a task/agent uses a Databricks Genie MCP server, the server carries the
Genie space id in its URL (``.../api/2.0/mcp/genie/<space_id>``). The crew
generator commonly co-assigns the custom ``GenieTool``, which starts with no
space id; this module copies the id across so the picker is enough.

Output FORMATTING is deliberately NOT done here: Genie answers flow through the
shared A2UI composer (``a2ui_runner.compose_surface``) exactly like every other
deliverable (dashboards, presentations, …), so there is no Genie-specific prompt
injection or hand-built surface to keep in sync.
"""
from typing import Any, Optional

# Managed-Genie MCP servers are registered as ``.../api/2.0/mcp/genie/<space_id>``.
_GENIE_MCP_URL_MARKER = "/api/2.0/mcp/genie/"


def _genie_space_id_from_url(server_url: Any) -> Optional[str]:
    """The Genie space id embedded in a managed-Genie MCP server URL
    (``.../api/2.0/mcp/genie/<space_id>``), or None for non-Genie URLs."""
    url = str(server_url or "")
    if _GENIE_MCP_URL_MARKER not in url:
        return None
    tail = url.split(_GENIE_MCP_URL_MARKER, 1)[1].strip("/")
    space_id = tail.split("/")[0].split("?")[0] if tail else ""
    return space_id or None


def genie_mcp_space_id(tools: Any = None, agent: Any = None) -> Optional[str]:
    """The Genie space id advertised by a co-assigned managed-Genie MCP tool's
    adapter URL, scanning both the task ``tools`` and ``agent.tools`` — or None."""
    try:
        agent_tools = getattr(agent, "tools", None) if agent is not None else None
        all_tools = list(tools or []) + list(agent_tools or [])
        for tool in all_tools:
            wrapper = getattr(tool, "_mcp_tool_wrapper", None)
            adapter = getattr(wrapper, "adapter", None)
            space_id = _genie_space_id_from_url(getattr(adapter, "server_url", ""))
            if space_id:
                return space_id
    except Exception:  # pragma: no cover - defensive
        pass
    return None


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
        space_id = genie_mcp_space_id(tools, agent)
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
