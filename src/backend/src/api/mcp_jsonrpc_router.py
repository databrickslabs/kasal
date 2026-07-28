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


async def _handle(
    message: Dict[str, Any],
    caller: ExternalCaller,
    session: Any,
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
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": SERVER_INFO,
                "instructions": (
                    "Kasal runs multi-agent crews. Tools named after a crew start "
                    "that crew and return a run id — crews take minutes, so poll "
                    "get_run_status. ask_kasal answers a question immediately."
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
):
    """The MCP Streamable HTTP endpoint.

    Accepts a single JSON-RPC message or a batch. Answers with JSON, or with SSE
    when the client asked for ``text/event-stream`` — both are permitted by the
    transport, and clients differ in which they prefer.
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
    for message in messages:
        if not isinstance(message, dict):
            responses.append(_error(None, _INVALID_REQUEST, "Expected an object"))
            continue
        reply = await _handle(message, caller, session)
        if reply is not None:
            responses.append(reply)

    # Every message was a notification. The transport says: no body, 202.
    if not responses:
        return JSONResponse(None, status_code=202)

    payload = responses if isinstance(body, list) else responses[0]

    if accept and "text/event-stream" in accept and "application/json" not in accept:
        # The client asked ONLY for SSE, so give it SSE. When it accepts both —
        # which Claude Code does — plain JSON is the cheaper answer and every
        # client handles it.
        async def _sse():
            yield f"data: {json.dumps(payload, default=str)}\n\n".encode("utf-8")

        return StreamingResponse(
            _sse(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return JSONResponse(payload)


@router.get("/mcp")
async def mcp_endpoint_get():
    """The transport allows a GET to open a server-initiated SSE stream.

    Kasal has nothing to push on an idle session — no server-initiated requests,
    no resource subscriptions — so this answers 405 rather than holding a
    connection open forever that will never carry a message. Clients treat that
    as "no server-initiated stream" and proceed.
    """
    return JSONResponse(
        _error(None, _METHOD_NOT_FOUND, "This server does not offer a GET stream."),
        status_code=405,
    )
