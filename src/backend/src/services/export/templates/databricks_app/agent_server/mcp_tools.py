"""MCP servers as Kasal tools, on the raw ``mcp`` SDK.

Replaces ``crewai_tools.MCPServerAdapter``. The SDK was already a bundle
dependency — the exporter ships it whenever a crew has MCP servers, because
``crewai-tools[mcp]`` is exactly ``mcp`` + ``mcpadapt`` — so this removes a
layer rather than adding one, and drops ``mcpadapt`` entirely.

**The async/sync problem, and how it is solved.** The SDK is async; a crew is
synchronous and runs in a worker thread. A session also has to stay open across
many tool calls: reconnecting per call would re-run the MCP handshake every
time. So each server gets a background thread running its own event loop, which
holds the session open until the connection is closed; tool calls are submitted
to that loop with ``run_coroutine_threadsafe`` and block for the result. This
was prototyped against a live streamable-HTTP server before being written:
calls work from the main thread and from worker threads, the session survives
across calls, and shutdown is clean.

**Per-server isolation is deliberate.** One unreachable or unauthorized server
(a Genie space the service principal cannot see) must not take down every other
server's tools — that was the failure mode of the single all-or-nothing
``MCPServerAdapter([all])`` call this replaces.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Type

from agent_server.kasal_runtime.services.tools.base import BaseTool
from pydantic import BaseModel, Field, create_model

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 60

# JSON Schema type -> Python type for the generated args model. Anything not
# here becomes ``Any``: advertising a wrong type is worse than advertising none,
# because the model then formats an argument the server will reject.
_JSON_TYPES: Dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def _unwrap(exc: BaseException) -> BaseException:
    """The useful error inside anyio's ExceptionGroup.

    The SDK runs its transport in a task group, so a plain connection refusal
    arrives as "unhandled errors in a TaskGroup (1 sub-exception)" — which tells
    an operator nothing about which server failed or why."""
    seen = set()
    while isinstance(exc, BaseExceptionGroup) and exc.exceptions:
        if id(exc) in seen:
            break
        seen.add(id(exc))
        exc = exc.exceptions[0]
    return exc


def args_model_from_schema(
    name: str, schema: Optional[Dict[str, Any]]
) -> Type[BaseModel]:
    """A pydantic model mirroring an MCP tool's ``inputSchema``.

    This is what the agent is shown, so it decides whether the model can call
    the tool correctly at all.
    """
    properties = (schema or {}).get("properties") or {}
    required = set((schema or {}).get("required") or [])
    fields: Dict[str, Any] = {}
    for field, spec in properties.items():
        if not isinstance(spec, dict):
            continue
        python_type = _JSON_TYPES.get(spec.get("type"), Any)
        description = spec.get("description") or spec.get("title") or ""
        if field in required:
            fields[field] = (python_type, Field(..., description=description))
        else:
            fields[field] = (
                Optional[python_type],
                Field(default=spec.get("default"), description=description),
            )
    safe = "".join(c if c.isalnum() else "_" for c in name).strip("_") or "Tool"
    return create_model(f"{safe}Args", **fields)  # type: ignore[call-overload]


def coerce_arguments(
    arguments: Dict[str, Any], schema: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Give the server the types its schema asks for.

    Models routinely emit ``"limit": "3"`` for an integer parameter, and a
    strict MCP server rejects the call. Empty/None values are dropped rather
    than sent, since an absent optional is what the server expects.
    """
    properties = (schema or {}).get("properties") or {}
    out: Dict[str, Any] = {}
    for key, value in (arguments or {}).items():
        if value is None or value == "" or value == "null":
            continue
        wanted = (properties.get(key) or {}).get("type")
        try:
            if wanted == "integer" and not isinstance(value, bool):
                out[key] = int(float(value))
            elif wanted == "number" and not isinstance(value, bool):
                out[key] = float(value)
            elif wanted == "boolean" and isinstance(value, str):
                out[key] = value.strip().lower() in ("1", "true", "yes", "on")
            else:
                out[key] = value
        except (TypeError, ValueError):
            # Send it as-is and let the server say what it does not like; a
            # coercion failure here is not a reason to drop the argument.
            out[key] = value
    return out


def render_result(result: Any) -> str:
    """An MCP ``CallToolResult`` as text an agent can read."""
    parts: List[str] = []
    for item in getattr(result, "content", None) or []:
        text = getattr(item, "text", None)
        if text:
            parts.append(str(text))
            continue
        data = getattr(item, "data", None)
        if data is not None:
            parts.append(f"[{getattr(item, 'type', 'binary')} content]")
    body = "\n".join(parts).strip()
    if getattr(result, "isError", False):
        # Returned, not raised: a failing tool is information the agent can act
        # on (fix the arguments, try another tool), not a reason to end the run.
        return f"Tool error: {body or 'the MCP server reported an error'}"
    return body or "(the tool returned no content)"


class MCPServerConnection:
    """One MCP server, held open for the life of a ``with`` block."""

    def __init__(
        self,
        name: str,
        params: Dict[str, Any],
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.name = name
        self.url = params.get("url", "")
        self.headers = dict(params.get("headers") or {})
        self.transport = params.get("transport") or "streamable-http"
        self.timeout = timeout
        self.tools: List[BaseTool] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._stop: Optional[asyncio.Event] = None
        self._session: Any = None
        self._error: Optional[BaseException] = None

    # ------------------------------------------------------------ the loop thread

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._serve())
        finally:
            self._loop.close()

    @asynccontextmanager
    async def _connect(self) -> Any:
        """Open the streamable-HTTP transport.

        Uses ``streamable_http_client(url, http_client=...)``, which supersedes
        ``streamablehttp_client(url, headers=..., timeout=...)``. That is NOT a
        rename — the signatures are incompatible, headers and timeout moved onto
        an httpx client you construct — and the old spelling now emits a
        DeprecationWarning on every connect.

        No fallback to the old name. The exporter pins ``mcp>=1.26,<1.27`` and
        the modern API exists throughout that range, so a fallback would be a
        branch that can never run and can never be tested (the legacy function
        is itself implemented in terms of this one, so it cannot even be
        simulated). If the pin is ever loosened downward, this raises a plain
        AttributeError naming the missing function, which is a better failure
        than a silently different code path.
        """
        import httpx
        from mcp.client.streamable_http import streamable_http_client

        async with httpx.AsyncClient(
            headers=self.headers, timeout=self.timeout
        ) as http_client:
            async with streamable_http_client(
                self.url, http_client=http_client
            ) as streams:
                yield streams

    async def _serve(self) -> None:
        """Hold the session open until ``__exit__`` sets the stop event."""
        from mcp import ClientSession

        self._stop = asyncio.Event()
        try:
            async with self._connect() as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    self._session = session
                    listed = await session.list_tools()
                    self.tools = [self._build_tool(t) for t in listed.tools]
                    self._ready.set()
                    await self._stop.wait()
        except BaseException as exc:  # noqa: BLE001 — reported to __enter__
            self._error = _unwrap(exc)
        finally:
            self._ready.set()

    # ------------------------------------------------------------------- tools

    def _call(self, tool_name: str, schema: Dict[str, Any], **kwargs: Any) -> str:
        if self._session is None or self._loop is None:
            return f"Tool error: the MCP server '{self.name}' is not connected."
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._session.call_tool(tool_name, coerce_arguments(kwargs, schema)),
                self._loop,
            )
            return render_result(future.result(timeout=self.timeout))
        except Exception as exc:  # noqa: BLE001
            # Returned as text for the same reason as isError above.
            return f"Tool error: {self.name}.{tool_name} failed: {_unwrap(exc)}"

    def _build_tool(self, mcp_tool: Any) -> BaseTool:
        schema = getattr(mcp_tool, "inputSchema", None) or {}
        tool_name = mcp_tool.name
        connection = self

        class _MCPTool(BaseTool):
            name: str = tool_name
            description: str = (
                getattr(mcp_tool, "description", None) or f"The {tool_name} MCP tool."
            )
            args_schema: Type[BaseModel] = args_model_from_schema(tool_name, schema)

            def _run(self, **kwargs: Any) -> str:
                return connection._call(tool_name, schema, **kwargs)

        return _MCPTool()

    # --------------------------------------------------------- context manager

    def __enter__(self) -> "MCPServerConnection":
        self._thread = threading.Thread(
            target=self._run_loop, name=f"mcp-{self.name}", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(timeout=self.timeout):
            raise TimeoutError(
                f"MCP server '{self.name}' did not respond within {self.timeout}s"
            )
        if self._error is not None:
            raise ConnectionError(
                f"MCP server '{self.name}' ({self.url}): {self._error}"
            ) from self._error
        return self

    def __exit__(self, *exc_info: Any) -> bool:
        if self._loop is not None and self._stop is not None:
            try:
                self._loop.call_soon_threadsafe(self._stop.set)
            except RuntimeError:
                pass  # the loop already stopped
        if self._thread is not None:
            self._thread.join(timeout=self.timeout)
        self._session = None
        return False


def open_mcp_server(
    stack: Any,
    name: str,
    params: Dict[str, Any],
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> List[BaseTool]:
    """Connect one server via the caller's ExitStack and return its tools.

    Raises on failure; the caller decides whether to skip that server. Keeping
    that decision at the call site is what makes per-server isolation possible.
    """
    connection = stack.enter_context(MCPServerConnection(name, params, timeout))
    return connection.tools
