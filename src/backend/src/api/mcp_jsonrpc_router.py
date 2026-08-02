"""The MCP Streamable HTTP transport — what a real MCP client connects to.

Claude Code, Claude Desktop and Cursor do not speak the REST shape in
``mcp_server_router.py``. They POST JSON-RPC 2.0 to a single endpoint with
``Accept: application/json, text/event-stream`` and expect either a JSON body or
an SSE stream back. Without this, registering Kasal as an MCP server 404s at
``initialize`` and nothing else is ever attempted.

Mounted at the DOMAIN ROOT (``/mcp``), not under the API prefix, because that is
where a client configured with ``https://host/mcp`` looks — the same reasoning
as the A2A Agent Card's well-known path.

Three shapes now coexist deliberately, and they are all one implementation:

* ``POST /mcp``                       JSON-RPC — real MCP clients
* ``POST /api/v1/mcp/v1/tools/call``  REST, one JSON result
* the same with ``stream: true``      REST, NDJSON chunked

All three dispatch through ``services/mcp_server/server.py`` and resolve the
caller through ``services/external/identity.py``. A transport is a transport;
none of them owns policy.
"""

import json
import logging
from typing import Annotated, Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse

from src.core.dependencies import SessionDep
from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    resolve_caller,
)
from src.services.external.permissions import ExternalPermissionError
from src.services.mcp.mcp_server import server as mcp_server
from src.services.mcp.mcp_server import sessions

router = APIRouter(tags=["mcp-server"])

logger = logging.getLogger(__name__)

#: Protocol revisions this server can speak. A client proposes one in
#: ``initialize``; we echo it back when we know it, otherwise we answer with our
#: newest and let the client decide whether to continue.
SUPPORTED_PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26")
DEFAULT_PROTOCOL_VERSION = SUPPORTED_PROTOCOL_VERSIONS[0]

SERVER_INFO = {"name": "kasal", "version": "1.0.0"}

# JSON-RPC error codes, from the spec.
_PARSE_ERROR = -32700
_INVALID_REQUEST = -32600
_METHOD_NOT_FOUND = -32601
_INVALID_PARAMS = -32602
_INTERNAL_ERROR = -32603


def _result(request_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": payload}


def _error(request_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": code, "message": message},
    }


async def _resolve(
    email: Optional[str], token: Optional[str], group_id: Optional[str]
) -> ExternalCaller:
    return await resolve_caller(
        protocol="mcp", email=email, access_token=token, group_id=group_id
    )


def _whoami(caller: ExternalCaller, pinned: Optional[str] = None) -> str:
    """Who the server thinks is calling, and which teamspaces that covers.

    Returned in the ``initialize`` instructions so a misconfigured client is
    obvious in the client itself. Without it, the only symptom of the wrong
    identity is a tool list that looks empty or unfamiliar, and the only way to
    diagnose it is the server log.

    Identity is the whole configuration: the forwarded email decides which
    teamspaces the caller is a member of, and the tool list is exactly those
    teamspaces' publications. ``X-Group-Id`` remains available to PIN one, but
    nothing needs it — which is why it is reported only when it was sent.
    """
    groups = caller.group_ids
    if not groups:
        return f"authenticated as {caller.identifier}, in no teamspace"
    if pinned:
        return f"authenticated as {caller.identifier}, pinned to teamspace {pinned}"
    return (
        f"authenticated as {caller.identifier}; the tools below are the "
        f"capabilities published in your teamspace(s): {', '.join(groups)}"
    )


async def _handle(
    message: Dict[str, Any],
    caller: ExternalCaller,
    session: Any,
    pinned_group: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Dispatch one JSON-RPC message. None means "notification, no reply"."""
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params") or {}

    # Notifications carry no id and MUST NOT be answered.
    if request_id is None and str(method).startswith("notifications/"):
        logger.debug("[mcp] notification %s", method)
        return None

    if method == "initialize":
        requested = params.get("protocolVersion")
        version = (
            requested
            if requested in SUPPORTED_PROTOCOL_VERSIONS
            else DEFAULT_PROTOCOL_VERSION
        )
        client = (params.get("clientInfo") or {}).get("name", "unknown")
        logger.info("[mcp] initialize from %s (protocol %s)", client, version)
        return _result(
            request_id,
            {
                "protocolVersion": version,
                # Only tools. Kasal exposes no prompts or resources over MCP, and
                # advertising a capability that answers empty wastes a round trip
                # on every client that believes it.
                #
                # listChanged is TRUE, and it is backed: the tool list changes
                # whenever a capability is published or withdrawn, and the GET
                # stream below carries `notifications/tools/list_changed` to the
                # sessions it concerns. It was false for as long as there was no
                # channel to push down — promising a notification that never
                # arrives is worse than admitting there is none.
                "capabilities": {"tools": {"listChanged": True}},
                "serverInfo": SERVER_INFO,
                "instructions": (
                    "Kasal runs multi-agent crews and flows. Each published "
                    "capability is its own tool: call it to start a run, which "
                    "returns a run id — runs take minutes, so poll "
                    "get_run_status. A flow may pause for a human, which shows "
                    "as input_required and is answered with respond_to_run. "
                    "ask_kasal answers a question immediately, without a run. "
                    "The tool list changes as capabilities are published; open "
                    "the GET stream to be told when.\n\n"
                    # Which teamspaces this connection covers, stated up front.
                    # The forwarded identity is the whole configuration, and the
                    # symptom of getting it wrong is a tool list that is simply
                    # someone else's — indistinguishable, from the client, from a
                    # teamspace with nothing published.
                    f"This connection is {_whoami(caller, pinned_group)}."
                ),
            },
        )

    if method == "ping":
        return _result(request_id, {})

    if method == "tools/list":
        tools = await mcp_server.list_tools(caller, session=session)
        return _result(request_id, {"tools": tools})

    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not name:
            return _error(request_id, _INVALID_PARAMS, "Missing tool name")
        try:
            output = await mcp_server.call_tool(
                caller=caller, name=name, arguments=arguments, session=session
            )
        except mcp_server.UnknownToolError as exc:
            return _error(request_id, _METHOD_NOT_FOUND, str(exc))
        except ExternalPermissionError as exc:
            # -32600 invalid request rather than method-not-found: the tool
            # exists, and telling the caller otherwise would send it looking for
            # a name that is right there in tools/list.
            return _error(request_id, _INVALID_REQUEST, exc.detail)
        except ExternalAuthError as exc:
            return _error(request_id, _INVALID_REQUEST, exc.detail)
        except TypeError as exc:
            return _error(request_id, _INVALID_PARAMS, str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("[mcp] tool %s failed", name)
            return _error(request_id, _INTERNAL_ERROR, str(exc))

        # MCP tool results are content blocks. The JSON goes in a text block
        # because that is the block type every client renders; a structured-only
        # result is invisible in most of them.
        return _result(
            request_id,
            {
                "content": [
                    {"type": "text", "text": json.dumps(output, default=str, indent=2)}
                ],
                "isError": False,
            },
        )

    return _error(request_id, _METHOD_NOT_FOUND, f"Unknown method: {method}")


@router.post("/mcp")
async def mcp_endpoint(
    request: Request,
    session: SessionDep,
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
    accept: Annotated[Optional[str], Header()] = None,
    mcp_session_id: Annotated[Optional[str], Header(alias="Mcp-Session-Id")] = None,
):
    """The MCP Streamable HTTP endpoint.

    Accepts a single JSON-RPC message or a batch. Answers with JSON, or with SSE
    when the client asked for ``text/event-stream`` — both are permitted by the
    transport, and clients differ in which they prefer.

    An ``initialize`` also opens a SESSION and returns its id in
    ``Mcp-Session-Id``. The client hands that back on the GET stream, which is
    how a ``tools/list_changed`` notification finds it.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(_error(None, _PARSE_ERROR, "Invalid JSON"), status_code=400)

    try:
        caller = await _resolve(
            email=x_auth_request_email or x_forwarded_email,
            token=x_auth_request_access_token or x_forwarded_access_token,
            group_id=x_group_id,
        )
    except ExternalAuthError as exc:
        # 401 with a JSON-RPC error body: the status code is what an HTTP client
        # acts on, the body is what an MCP client shows the user.
        return JSONResponse(
            _error(
                body.get("id") if isinstance(body, dict) else None,
                _INVALID_REQUEST,
                exc.detail,
            ),
            status_code=401,
        )

    messages = body if isinstance(body, list) else [body]
    responses = []
    initialized = False
    for message in messages:
        if not isinstance(message, dict):
            responses.append(_error(None, _INVALID_REQUEST, "Expected an object"))
            continue
        if message.get("method") == "initialize":
            initialized = True
        reply = await _handle(message, caller, session, pinned_group=x_group_id)
        if reply is not None:
            responses.append(reply)

    # Every message was a notification. The transport says: no body, 202.
    if not responses:
        return JSONResponse(None, status_code=202)

    payload = responses if isinstance(body, list) else responses[0]

    headers: Dict[str, str] = {}
    if initialized:
        # The session exists so a later notification has somewhere to go. Bound
        # to the caller's groups, so a publish in one workspace does not make
        # every other workspace's client refetch.
        headers["Mcp-Session-Id"] = (
            sessions.adopt_session(mcp_session_id, caller.group_ids)
            if mcp_session_id
            else sessions.open_session(caller.group_ids)
        )

    if accept and "text/event-stream" in accept and "application/json" not in accept:
        # The client asked ONLY for SSE, so give it SSE. When it accepts both —
        # which Claude Code does — plain JSON is the cheaper answer and every
        # client handles it.
        async def _sse():
            yield f"data: {json.dumps(payload, default=str)}\n\n".encode("utf-8")

        return StreamingResponse(
            _sse(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                **headers,
            },
        )

    return JSONResponse(payload, headers=headers or None)


@router.get("/mcp")
async def mcp_endpoint_get(
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
    accept: Annotated[Optional[str], Header()] = None,
    mcp_session_id: Annotated[Optional[str], Header(alias="Mcp-Session-Id")] = None,
):
    """The server-initiated SSE stream.

    Kasal has exactly one thing to say on it — ``tools/list_changed`` — and that
    one thing is what makes a long-lived client usable: its tool list is a
    snapshot from ``initialize``, and every capability published afterwards is
    invisible to it until something says otherwise. This used to answer 405,
    which is why the generic ``list_crews``/``start_crew`` pair had to exist.

    A client that asked for JSON rather than an event stream still gets the 405:
    it is not asking to hold a stream open.
    """
    if not accept or "text/event-stream" not in accept:
        return JSONResponse(
            _error(None, _METHOD_NOT_FOUND, "This endpoint streams; ask for SSE."),
            status_code=405,
        )

    try:
        caller = await _resolve(
            email=x_auth_request_email or x_forwarded_email,
            token=x_auth_request_access_token or x_forwarded_access_token,
            group_id=x_group_id,
        )
    except ExternalAuthError as exc:
        return JSONResponse(_error(None, _INVALID_REQUEST, exc.detail), status_code=401)

    session_id = (
        sessions.adopt_session(mcp_session_id, caller.group_ids)
        if mcp_session_id
        else sessions.open_session(caller.group_ids)
    )
    logger.info("[mcp] session %s opened a notification stream", session_id)

    return StreamingResponse(
        sessions.stream(session_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Mcp-Session-Id": session_id,
        },
    )


@router.delete("/mcp")
async def mcp_endpoint_delete(
    mcp_session_id: Annotated[Optional[str], Header(alias="Mcp-Session-Id")] = None,
):
    """Session termination, as the transport defines it.

    Nothing is authorised by a session id — the caller is resolved from headers
    on every request — so this only forgets a queue. Answering 200 for an
    unknown id keeps a client's shutdown path simple.
    """
    if mcp_session_id:
        sessions.close_session(mcp_session_id)
    return JSONResponse({"ok": True})
