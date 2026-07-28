"""Kasal as an MCP SERVER — what external agents may call.

The mirror image of ``services/mcp/``, which is Kasal as an MCP *client* (the
registry of remote servers this workspace may use). Both exist; the names are
one word apart; state the direction before reading either.

A thin adapter over ``services/external/`` (the External Invocation Layer),
holding transport and wire format only. Identity, publication, run state and —
from phase 3 — HITL are shared with the A2A adapter and live there.
"""

from src.services.mcp_server.server import (
    PROTOCOL_VERSION,
    UnknownToolError,
    call_tool,
    list_tools,
)

__all__ = ["PROTOCOL_VERSION", "UnknownToolError", "call_tool", "list_tools"]
