"""Kasal as an MCP SERVER — the endpoint external agents call.

NOT ``mcp_router.py``. That one is the MCP *client* registry: the remote servers
this workspace may use. This is the opposite direction — what other agents may
call on Kasal. The names are one word apart and will be confused otherwise.

Versioned at ``/mcp/v1`` because external clients pin behaviour, and published
tool names are a stable contract.

Every endpoint resolves an :class:`ExternalCaller` before touching data. That
resolution is the only one in the file, and it happens in a dependency, so a new
endpoint cannot reach a tool without it.
"""

import logging
from typing import Annotated, Any, Dict, Optional

from fastapi import APIRouter, Depends, Header
from pydantic import BaseModel, Field

from src.core.dependencies import SessionDep
from src.core.exceptions import KasalError, NotFoundError
from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    resolve_caller,
)
from src.services.mcp_server import server as mcp_server
from src.services.mcp_server.tools import UnknownCapabilityError, UnknownRunError

router = APIRouter(
    prefix="/mcp/v1",
    tags=["mcp-server"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


class ExternalAuthRequired(KasalError):
    """401 for a caller that could not be resolved to a workspace.

    Its own error rather than a generic 403: an external caller needs to know it
    must authenticate — that is the ``auth_required`` state, the one external
    state with no ExecutionStatus behind it because the invocation never became
    a run.
    """

    status_code = 401
    detail = "Authentication required"


async def get_external_caller(
    x_forwarded_email: Annotated[
        Optional[str], Header(alias="X-Forwarded-Email")
    ] = None,
    x_forwarded_access_token: Annotated[
        Optional[str], Header(alias="X-Forwarded-Access-Token")
    ] = None,
    x_auth_request_email: Annotated[
        Optional[str], Header(alias="X-Auth-Request-Email")
    ] = None,
    x_auth_request_access_token: Annotated[
        Optional[str], Header(alias="X-Auth-Request-Access-Token")
    ] = None,
    x_group_id: Annotated[Optional[str], Header(alias="X-Group-Id")] = None,
) -> ExternalCaller:
    """Resolve the caller, or refuse the request.

    Same header chain as ``core/dependencies.get_group_context`` — OAuth2-Proxy
    headers preferred, Databricks Apps headers as fallback. An external surface
    that authenticated differently from the rest of the app would be a second
    security model to reason about.
    """
    try:
        return await resolve_caller(
            protocol="mcp",
            email=x_auth_request_email or x_forwarded_email,
            access_token=x_auth_request_access_token or x_forwarded_access_token,
            group_id=x_group_id,
        )
    except ExternalAuthError as exc:
        logger.warning("[mcp-server] refused caller: %s", exc.detail)
        raise ExternalAuthRequired(exc.detail)


CallerDep = Annotated[ExternalCaller, Depends(get_external_caller)]


class ToolCallRequest(BaseModel):
    name: str = Field(..., description="The tool to invoke.")
    arguments: Dict[str, Any] = Field(default_factory=dict)


@router.get("/tools")
async def list_tools(caller: CallerDep):
    """The tools this caller may use.

    Resolving the caller is not strictly needed to render a static list today,
    but it is required the moment Layer-2 lands (one tool per published crew,
    group-scoped). Requiring it now means the endpoint never has to GAIN an auth
    requirement later — which is the kind of change that gets deployed without
    every client noticing.
    """
    return {
        "protocolVersion": mcp_server.PROTOCOL_VERSION,
        "tools": mcp_server.list_tools(),
    }


@router.post("/tools/call")
async def call_tool(request: ToolCallRequest, caller: CallerDep, session: SessionDep):
    """Invoke a tool as the resolved caller."""
    try:
        result = await mcp_server.call_tool(
            caller=caller,
            name=request.name,
            arguments=request.arguments,
            session=session,
        )
    except mcp_server.UnknownToolError as exc:
        raise NotFoundError(str(exc))
    except (UnknownCapabilityError, UnknownRunError) as exc:
        # 404 for both "does not exist" and "not yours". They must stay
        # indistinguishable, or run ids and capability names become an oracle
        # for other workspaces.
        raise NotFoundError(str(exc))
    except ExternalAuthError as exc:
        # Raised mid-call by require_obo_token(): the caller is known, but this
        # operation needs their Databricks token and they presented none. That
        # is the auth_required state — reported before any run is created,
        # rather than as a run that dies inside an agent.
        raise ExternalAuthRequired(exc.detail)
    except TypeError as exc:
        # Wrong/missing arguments for a known tool. A 422 rather than a 500:
        # this is the caller's mistake and it can fix it.
        from src.core.exceptions import UnprocessableEntityError

        raise UnprocessableEntityError(f"Invalid arguments for {request.name}: {exc}")

    return {"content": result, "isError": False}
