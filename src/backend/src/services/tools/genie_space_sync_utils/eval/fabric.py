"""Ask a Microsoft Fabric data agent over its MCP server (JSON-RPC over HTTPS).

Ported from the reference ``fabric_runner`` (``fabric_mcp`` mode). The agent's MCP
URL must be ``https://api.fabric.microsoft.com/v1/mcp/workspaces/{ws}/dataagents/{id}/agent``;
anything else is refused before a request is made. The bearer token comes in as a tool
input and is only ever placed in the ``Authorization`` header: it is not stored,
logged, or echoed in an error. The MCP route returns text only, so there are no result
tables and no token counts for this agent.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

import requests

from src.services.tools.genie_space_sync_utils.eval.answer import ERROR, Answer
from src.services.tools.genie_space_sync_utils.verdicts import short

FABRIC_HOST = "api.fabric.microsoft.com"
_GUID = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
_MCP_PATH = re.compile(rf"^/v1/mcp/workspaces/{_GUID}/dataagents/{_GUID}/agent$")
_PROTOCOL_VERSION = "2025-03-26"
_CLIENT_INFO = {"name": "kasal-genie-space-sync-eval", "version": "1.0"}
_TIMEOUT_S = 120
_JSON = "application/json"


@dataclass(frozen=True)
class McpReply:
    status: int
    content_type: str
    text: str
    session_id: str = ""


#: (JSON-RPC body, extra headers) -> reply. The real one posts with requests; tests pass a fake.
McpSend = Callable[[dict[str, object], dict[str, str]], McpReply]


def validate_mcp_url(url: str) -> str:
    """The URL when it is a Fabric data agent MCP endpoint; raises ValueError otherwise."""
    parts = urlsplit(url.strip())
    if (
        parts.scheme != "https"
        or parts.hostname != FABRIC_HOST
        or not _MCP_PATH.match(parts.path)
        or parts.query
        or parts.fragment
    ):
        raise ValueError(
            "fabric_agent_url must be https://"
            f"{FABRIC_HOST}/v1/mcp/workspaces/<id>/dataagents/<id>/agent"
        )
    return parts.geturl()


def requests_mcp_send(url: str, token: str) -> McpSend:
    """The real transport: one POST per JSON-RPC message to the validated MCP URL."""
    target = validate_mcp_url(url)

    def _send(body: dict[str, object], headers: dict[str, str]) -> McpReply:
        request_headers = {
            **headers,
            "Authorization": f"Bearer {token}",
            "Content-Type": _JSON,
            "Accept": f"{_JSON}, text/event-stream",
        }
        response = requests.post(  # target validated by validate_mcp_url
            target, headers=request_headers, json=body, timeout=_TIMEOUT_S
        )
        return McpReply(
            status=response.status_code,
            content_type=response.headers.get("content-type", ""),
            text=response.text,
            session_id=response.headers.get("mcp-session-id", ""),
        )

    return _send


def parse_rpc_body(content_type: str, text: str) -> dict[str, object]:
    """The JSON-RPC message of a reply: a JSON body, or the last ``data:`` line of an event stream."""
    if "text/event-stream" not in content_type:
        return _object(json.loads(text)) if text.strip() else {}
    messages: list[dict[str, object]] = []
    for line in text.splitlines():
        if line.startswith("data:"):
            try:
                messages.append(_object(json.loads(line[5:].strip())))
            except ValueError:
                continue
    return messages[-1] if messages else {}


def _object(value: object) -> dict[str, object]:
    return {str(k): v for k, v in value.items()} if isinstance(value, dict) else {}


def answer_text(result: dict[str, object]) -> str:
    """The text blocks of a ``tools/call`` result, joined by blank lines."""
    content = result.get("content")
    blocks = content if isinstance(content, list) else []
    texts = [
        str(b.get("text", "")).strip()
        for b in blocks
        if isinstance(b, dict) and b.get("type") == "text"
    ]
    return "\n\n".join(t for t in texts if t)


def question_argument(tool: dict[str, object]) -> str:
    """The name of the tool's one required argument, which carries the question."""
    schema = tool.get("inputSchema")
    schema = schema if isinstance(schema, dict) else {}
    required = schema.get("required")
    properties = schema.get("properties")
    names = required if isinstance(required, list) else list(properties or {})
    if len(names) != 1 or not isinstance(names[0], str):
        raise ValueError(
            "the Fabric agent tool does not take exactly one question argument"
        )
    return names[0]


class _Session:
    """The MCP session of one question: the server's session header and the next JSON-RPC id."""

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.next_id = 0


def _rpc(
    send: McpSend,
    session: _Session,
    method: str,
    params: dict[str, object] | None = None,
    notify: bool = False,
) -> dict[str, object]:
    body: dict[str, object] = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        body["params"] = params
    if not notify:
        session.next_id += 1
        body["id"] = session.next_id
    reply = send(body, dict(session.headers))
    if reply.session_id:
        session.headers["Mcp-Session-Id"] = reply.session_id
    if reply.status >= 400:
        raise RuntimeError(f"Fabric agent {method} returned HTTP {reply.status}")
    if notify:
        return {}
    message = parse_rpc_body(reply.content_type, reply.text)
    error = message.get("error")
    if error:
        detail = error.get("message", "error") if isinstance(error, dict) else error
        raise RuntimeError(f"Fabric agent {method} failed: {short(detail)}")
    result = message.get("result")
    return _object(result)


def ask_fabric(
    send: McpSend,
    question: str,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> Answer:
    """One question to the Fabric agent. Failures become an ERROR answer, never an exception."""
    session = _Session()
    started = clock()
    try:
        _rpc(
            send,
            session,
            "initialize",
            {
                "protocolVersion": _PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": _CLIENT_INFO,
            },
        )
        _rpc(send, session, "notifications/initialized", notify=True)
        listed = _rpc(send, session, "tools/list")
        tools = listed.get("tools")
        if (
            not isinstance(tools, list)
            or len(tools) != 1
            or not isinstance(tools[0], dict)
        ):
            raise RuntimeError("the Fabric agent must expose exactly one tool")
        tool = tools[0]
        argument = question_argument(tool)
        result = _rpc(
            send,
            session,
            "tools/call",
            {"name": str(tool.get("name", "")), "arguments": {argument: question}},
        )
    except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
        return Answer(
            agent="fabric",
            status=ERROR,
            error=short(exc),
            latency_s=round(clock() - started, 2),
        )
    latency = round(clock() - started, 2)
    failed = bool(result.get("isError"))
    text = answer_text(result)
    return Answer(
        agent="fabric",
        status="FAILED" if failed else "COMPLETED",
        text="" if failed else text,
        error=short(text) if failed else "",
        latency_s=latency,
    )
