"""Kasal's MCP SERVER surface.

Note the direction, because this codebase has both:

* ``services/mcp/`` + ``api/mcp_router.py`` — Kasal as an MCP **client**: the
  registry of remote servers this workspace may call.
* ``services/mcp_server/`` + ``api/mcp_server_router.py`` — this. Kasal as an
  MCP **server**: what external agents may call.

They will be confused by every future reader unless the split is stated, so it
is stated here and in both router docstrings.

This module owns dispatch and nothing else. Identity resolution, group scoping,
publication and run state live in ``services/external/`` and are shared with the
A2A adapter; if a decision is being made in this file that an A2A caller would
also need, it is in the wrong place.
"""

import logging
from typing import Any, Dict, List

from src.services.external.identity import ExternalCaller
from src.services.mcp_server.tools import TOOL_DEFINITIONS, TOOL_HANDLERS

logger = logging.getLogger(__name__)

#: Bumped when the advertised tool surface changes in a way clients must notice.
#: External clients pin behaviour, so the endpoint is versioned (`/mcp/v1`) and
#: published tool names are a stable contract.
PROTOCOL_VERSION = "1.0"


def list_tools() -> List[Dict[str, Any]]:
    """The advertised tool list.

    Static in phase 2. Layer-2 (one tool per published crew) will make this
    caller-dependent, at which point it becomes a projection of
    ``PublicationService.list_capabilities`` — the same query the A2A card reads.
    """
    return list(TOOL_DEFINITIONS)


class UnknownToolError(Exception):
    """The caller asked for a tool that is not advertised."""


async def call_tool(
    caller: ExternalCaller,
    name: str,
    arguments: Dict[str, Any],
    session: Any = None,
) -> Dict[str, Any]:
    """Dispatch a tool call for an already-resolved caller.

    ``caller`` is resolved by the router BEFORE this is reached — no tool here
    can be invoked without a group context, because there is no code path that
    constructs one internally.
    """
    handler = TOOL_HANDLERS.get(name)
    if handler is None:
        raise UnknownToolError(f"Unknown tool: {name}")

    logger.info(
        "[mcp-server] %s called %s (groups=%s)", caller.origin, name, caller.group_ids
    )
    return await handler(caller, session=session, **(arguments or {}))
