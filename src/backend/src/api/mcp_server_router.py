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
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src.core.dependencies import SessionDep
from src.core.exceptions import KasalError, NotFoundError
from src.services.external import streaming
from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    resolve_caller,
)
from src.services.external.permissions import ExternalPermissionError
from src.services.mcp.mcp_server import server as mcp_server
from src.services.mcp.mcp_server.tools import UnknownCapabilityError, UnknownRunError

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
    stream: bool = Field(
        default=False,
        description=(
            "Stream progress as NDJSON instead of returning one result. The "
            "response is a chunked application/x-ndjson body, one JSON object "
            "per line, ending with the terminal frame."
        ),
    )


@router.get("/tools")
async def list_tools(caller: CallerDep, session: SessionDep):
    """The tools this caller may use.

    Group-scoped: the fixed control tools plus one per crew this caller's
    workspace has published. Requiring identity was already the case before
    Layer-2 needed it, precisely so the endpoint never had to GAIN an auth
    requirement — the kind of change that ships without every client noticing.
    """
    return {
        "protocolVersion": mcp_server.PROTOCOL_VERSION,
        "tools": await mcp_server.list_tools(caller, session=session),
    }


@router.post("/tools/call")
async def call_tool(
    request: ToolCallRequest,
    caller: CallerDep,
    session: SessionDep,
    accept: Annotated[Optional[str], Header()] = None,
):
    """Invoke a tool as the resolved caller.

    ``stream: true`` asks for progress; the ``Accept`` header chooses the
    framing. ``text/event-stream`` gets SSE, anything else gets NDJSON. The
    frames are identical either way — a caller picks a wire format, not a
    feature — because both come from the same generator in
    ``services/external/streaming.py``.

    Streaming matters most for the tools that start a crew: those take minutes,
    and the alternative is a caller that either blocks blind or polls in a loop.
    """
    try:
        result = await mcp_server.call_tool(
            caller=caller,
            name=request.name,
            arguments=request.arguments,
            session=session,
        )
    except mcp_server.UnknownToolError as exc:
        raise NotFoundError(str(exc))
    except ExternalPermissionError as exc:
        # 403, not 401: the caller IS authenticated, their role is the problem.
        # Answering 401 would send them round the auth loop with the same
        # credentials forever.
        from src.core.exceptions import ForbiddenError

        raise ForbiddenError(exc.detail)
    except (UnknownCapabilityError, UnknownRunError) as exc:
        # 404 for both "does not exist" and "not yours". They must stay
        # indistinguishable, or run ids and capability names become an oracle
        # for other workspaces.
        raise NotFoundError(str(exc))
    except ExternalAuthError as exc:
        # A genuine failure of the Databricks auth chain mid-call — not merely a
        # missing header, which falls back like everywhere else in Kasal. This
        # is the auth_required state.
        raise ExternalAuthRequired(exc.detail)
    except TypeError as exc:
        # Wrong/missing arguments for a known tool. A 422 rather than a 500:
        # this is the caller's mistake and it can fix it.
        from src.core.exceptions import UnprocessableEntityError

        raise UnprocessableEntityError(f"Invalid arguments for {request.name}: {exc}")

    if not request.stream:
        return {"content": result, "isError": False}

    run_id = result.get("run_id") if isinstance(result, dict) else None
    if not run_id:
        # A tool with no run to follow (list_crews is not a run at all). Emit
        # the single result as one frame so a streaming caller gets the same
        # shape whichever tool it called.
        async def _single():
            yield result

        frames = _single()
    else:
        frames = streaming.stream_run(caller, str(run_id), session=session)

    # `stream: true` asks for progress; Accept chooses the framing. Identical
    # frames either way — a caller picks a wire format, not a feature.
    wants_sse = bool(accept and "text/event-stream" in accept)

    return StreamingResponse(
        streaming.to_sse(frames) if wants_sse else streaming.to_ndjson(frames),
        media_type="text/event-stream" if wants_sse else "application/x-ndjson",
        headers={
            # Chunked and unbuffered: a proxy that buffers turns a live stream
            # into one delivery at the end, which is indistinguishable from no
            # streaming at all.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
