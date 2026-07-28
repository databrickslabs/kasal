"""Kasal as an MCP SERVER — what external agents may call.

The inbound half. ``mcp_client`` beside it is the outbound one — the registry
of remote servers this workspace may use. Both exist and both are "MCP", so the
package a file sits in is what says which direction it faces.

A thin adapter over ``services/external/`` (the External Invocation Layer),
holding transport and wire format only. Identity, publication, run state and —
from phase 3 — HITL are shared with the A2A adapter and live there.
"""

from src.services.mcp.mcp_server.server import (
    PROTOCOL_VERSION,
    UnknownToolError,
    call_tool,
    list_tools,
)

__all__ = ["PROTOCOL_VERSION", "UnknownToolError", "call_tool", "list_tools"]
