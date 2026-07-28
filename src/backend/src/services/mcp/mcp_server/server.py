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
from src.services.mcp.mcp_server.tools import (
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
    UnknownToolError,
)

logger = logging.getLogger(__name__)

#: Bumped when the advertised tool surface changes in a way clients must notice.
#: External clients pin behaviour, so the endpoint is versioned (`/mcp/v1`) and
#: published tool names are a stable contract.
PROTOCOL_VERSION = "1.0"


async def list_tools(
    caller: ExternalCaller, session: Any = None
) -> List[Dict[str, Any]]:
    """The tools this caller may use: the fixed set, plus one per published crew.

    The per-crew tools are Layer-2, and they are the reason discovery works at
    all. With only the generic set a calling agent sees ``start_crew`` and has to
    be told out of band which crews exist; with Layer-2 it sees
    ``analyse_powerbi_model`` and a description of when to use it, which is what
    an agent actually selects on.

    Caller-dependent because the list is group-scoped, and a projection of
    ``PublicationService.list_capabilities`` — the SAME query the A2A card's
    skills[] reads, which is what stops the two surfaces advertising different
    capabilities.
    """
    from src.services.mcp.mcp_server.tools import build_crew_tool_definitions

    tools = list(TOOL_DEFINITIONS)
    tools.extend(await build_crew_tool_definitions(caller, session=session))
    return tools


__all__ = ["PROTOCOL_VERSION", "UnknownToolError", "call_tool", "list_tools"]


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
        # Not a fixed tool. It may be a Layer-2 per-crew tool, whose name IS a
        # published capability — resolved against this caller's own
        # publications, so an unpublished or another tenant's name is simply
        # unknown.
        from src.services.mcp.mcp_server.tools import call_crew_tool

        return await call_crew_tool(
            caller, name=name, arguments=arguments or {}, session=session
        )

    logger.info(
        "[mcp-server] %s called %s (groups=%s)", caller.origin, name, caller.group_ids
    )
    return await handler(caller, session=session, **(arguments or {}))
