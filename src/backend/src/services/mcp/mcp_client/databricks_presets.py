"""Preset ``additional_config`` for Databricks-managed MCP servers.

The MCP layer itself is server-agnostic: ``services/tools/mcp_follow`` follows
ANY server whose configuration declares a start-tool + poll-tool pair, and
names none in code. What Kasal knows about the managed Databricks Genie
endpoints — their tool names and id parameters — is catalog PRESET DATA, and
this module is its single source: the Connect-a-tool options
(``api/mcp_router``) ship it on registration, and ``MCPService`` heals
already-registered rows that predate it (see ``follow_healed_config``).

Why healing exists: a Genie server registered before the follow declaration
was introduced has ``additional_config == {}``, so the follow loop never
engages — the LLM agent then polls ``genie_poll_response`` itself, one poll
per tool round, and the round budget cuts it off mid-flight with no result.

Each preset carries TWO follow declarations: the ask→poll pair, and a
poll→poll self-follow so that even a poll call the model makes directly
blocks until the work is finished instead of returning an in-progress
envelope.
"""

import re
from typing import Any, Dict, List, Optional

#: Workspace-wide managed Genie MCP: {workspace}/api/2.0/mcp/genie
GENIE_ONE_FOLLOW: List[Dict[str, Any]] = [
    {
        "name": "Genie",
        "start_tool": "genie_ask",
        "poll_tool": "genie_poll_response",
        "id_params": ["conversation_id", "response_id"],
        "cancel_tool": "genie_cancel_response",
    },
    {
        "name": "Genie",
        "start_tool": "genie_poll_response",
        "poll_tool": "genie_poll_response",
        "id_params": ["conversation_id", "response_id"],
        "cancel_tool": "genie_cancel_response",
    },
]

#: Per-space managed Genie MCP: {workspace}/api/2.0/mcp/genie/{space_id}
GENIE_SPACE_FOLLOW: List[Dict[str, Any]] = [
    {
        "name": "Genie",
        "start_tool": "query_space",
        "poll_tool": "poll_response",
        "id_params": ["conversation_id", "message_id"],
    },
    {
        "name": "Genie",
        "start_tool": "poll_response",
        "poll_tool": "poll_response",
        "id_params": ["conversation_id", "message_id"],
    },
]

_GENIE_URL = re.compile(r"/api/2\.0/mcp/genie(/[^/?#]+)?/?$")


def follow_preset_for(server_url: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    """The follow declarations for a managed Databricks MCP URL, or None."""
    match = _GENIE_URL.search((server_url or "").strip())
    if not match:
        return None
    return GENIE_SPACE_FOLLOW if match.group(1) else GENIE_ONE_FOLLOW


def follow_healed_config(
    server_url: Optional[str], additional_config: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """The healed ``additional_config`` for a stale registration, or None.

    Returns a NEW config (existing keys preserved, ``follow`` added) only when
    the URL matches a managed preset AND the stored config has no ``follow``
    yet — a row that already declares one is the operator's to own and is
    never touched.
    """
    if isinstance(additional_config, dict) and additional_config.get("follow"):
        return None
    preset = follow_preset_for(server_url)
    if preset is None:
        return None
    healed = dict(additional_config or {})
    healed["follow"] = preset
    return healed
