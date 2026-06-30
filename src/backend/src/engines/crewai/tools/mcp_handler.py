import logging
import os
import asyncio
import json
import sys
import hashlib
import subprocess
import concurrent.futures
import time
import traceback
import aiohttp
from typing import Optional
from src.utils.databricks_auth import get_databricks_auth_headers, get_mcp_auth_headers

logger = logging.getLogger(__name__)


def format_mcp_exception(exc: BaseException) -> str:
    """Render an MCP error into its real underlying cause(s).

    The MCP client runs over ``anyio`` task groups, so a failed connection OR a
    failed tool call (403, PERMISSION_DENIED, timeout, …) surfaces as an
    ``ExceptionGroup`` whose ``str()`` is the useless "unhandled errors in a
    TaskGroup (1 sub-exception)". This walks the group (duck-typed on
    ``.exceptions`` so it also works with the 3.9 backport) and joins the leaf
    messages, so logs/traces show e.g. "PERMISSION_DENIED: Unable to get space …"
    instead of the wrapper.
    """
    msgs = []

    def _walk(e: BaseException) -> None:
        subs = getattr(e, "exceptions", None)
        if subs and isinstance(subs, (list, tuple)):
            for sub in subs:
                _walk(sub)
            return
        text = str(e).strip()
        msgs.append(f"{type(e).__name__}: {text}" if text else type(e).__name__)

    _walk(exc)
    seen = set()
    unique = [m for m in msgs if not (m in seen or seen.add(m))]
    return "; ".join(unique) or f"{type(exc).__name__}: {exc}"


def _is_image_mime(mime: Optional[str]) -> bool:
    return bool(mime) and str(mime).lower().startswith("image/")


def _format_resource_link(uri: str, name: Optional[str], mime: Optional[str]) -> str:
    """A markdown image line for image resources, else a plain link line, so the
    agent can carry it into the final deliverable (A2UI image/album components or
    inline links) instead of losing it."""
    label = name or uri
    return f"![{label}]({uri})" if _is_image_mime(mime) else f"[{label}]({uri})"


def _format_content_block(block) -> Optional[str]:
    """Render a single MCP content block as agent-friendly text.

    Duck-typed (the exact classes vary by MCP SDK version):
    - text                                  -> the text
    - embedded resource with text           -> the text
    - resource link / resource with a uri   -> markdown image line (images) or link
    - inline image / audio / blob (no uri)  -> a compact placeholder; NEVER the
      base64 payload, which would flood the LLM context window
    """
    text = getattr(block, "text", None)
    if isinstance(text, str):
        return text

    btype = str(getattr(block, "type", "") or "").lower()

    # Embedded resource: may carry inline text, a uri, or an inline blob.
    resource = getattr(block, "resource", None)
    if resource is not None:
        rtext = getattr(resource, "text", None)
        if isinstance(rtext, str):
            return rtext
        ruri = getattr(resource, "uri", None)
        rmime = getattr(resource, "mimeType", None)
        if ruri:
            return _format_resource_link(str(ruri), getattr(block, "name", None), rmime)
        if getattr(resource, "blob", None) is not None:
            return f"[resource: {rmime or 'application/octet-stream'}]"

    # Resource link (newer MCP) — carries a uri directly.
    uri = getattr(block, "uri", None)
    if uri:
        return _format_resource_link(
            str(uri), getattr(block, "name", None), getattr(block, "mimeType", None)
        )

    # Inline binary (image/audio) — placeholder only, never the base64 data.
    if btype in ("image", "audio") or getattr(block, "data", None) is not None:
        mime = getattr(block, "mimeType", None) or btype or "binary"
        return f"[{btype or 'binary'}: {mime}]"

    return None


def _format_mcp_tool_result(result) -> str:
    """Normalize an MCP CallToolResult into agent-friendly text.

    Prefers structured JSON output, surfaces tool errors (``isError``), preserves
    resource links/images as markdown image lines, and replaces inline binary
    payloads with compact placeholders — so nothing is silently dropped and the
    LLM context is never flooded with base64. Falls back to ``str(result)`` for
    unknown shapes.
    """
    if result is None:
        return ""

    is_error = bool(getattr(result, "isError", False))

    def _flag(body: str) -> str:
        return f"Tool error: {body}" if is_error else body

    # Modern MCP servers can return structured JSON alongside/instead of content.
    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        try:
            return _flag(json.dumps(structured, default=str))
        except Exception:
            return _flag(str(structured))

    content = getattr(result, "content", None)
    if content:
        parts = [r for block in content if (r := _format_content_block(block))]
        return _flag("\n".join(parts) if parts else str(result))

    return _flag(str(result))


# --- Managed Databricks Genie MCP auto-poll --------------------------------
# The Databricks-managed Genie MCP server (URL .../api/2.0/mcp/genie/<space>)
# splits a question into TWO tools: "query_space_<space>" returns immediately
# with an in-progress status envelope, and "poll_response_<space>" fetches the
# latest status. That leaves the LLM agent to drive the poll loop — and in
# practice agents give up after a poll or two (while the query is still
# ASKING_AI / PENDING_WAREHOUSE / EXECUTING_QUERY) and fabricate a "placeholder"
# answer, and sometimes pass the wrong id (conversation_id as message_id),
# crashing the poll. To match the blocking behaviour of the built-in GenieTool,
# we poll internally until the message reaches a terminal status, so the agent
# gets the finished result from a single query_space call and never has to
# manage (or bail out of) the loop itself.
_GENIE_TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
_GENIE_POLL_TIMEOUT_SECONDS = 300
_GENIE_POLL_INTERVAL_SECONDS = 3


def _is_managed_genie_adapter(adapter) -> bool:
    """True for the Databricks-managed Genie MCP server (one space per server)."""
    return "/mcp/genie/" in str(getattr(adapter, "server_url", "") or "")


def _genie_poll_tool_name(query_tool_name: str) -> Optional[str]:
    """Derive the 'poll_response_<space>' tool name from 'query_space_<space>'."""
    if "query_space" in query_tool_name:
        return query_tool_name.replace("query_space", "poll_response", 1)
    return None


def _genie_status_envelope(result) -> Optional[dict]:
    """Extract a Genie status envelope ({status, conversationId, messageId, ...})
    from an MCP result, or None if it isn't one (e.g. an error or unknown shape).

    Managed Genie returns the envelope as structuredContent, but fall back to a
    JSON text content block in case a transport delivers it that way."""
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict) and "status" in structured:
        return structured
    for block in (getattr(result, "content", None) or []):
        text = getattr(block, "text", None)
        if isinstance(text, str):
            try:
                data = json.loads(text)
            except Exception:
                continue
            if isinstance(data, dict) and "status" in data:
                return data
    return None


async def _genie_autopoll(wrapper, params):
    """Execute an MCP tool; for a managed-Genie 'query_space' call, keep polling
    'poll_response' internally until the Genie message reaches a terminal status,
    so the agent never sees (and bails on) an in-progress snapshot.

    Returns the final MCP result object, or — on internal-poll timeout — a short
    directive string telling the agent the query timed out so it does not
    fabricate results. Any non-Genie tool just executes once, unchanged."""
    result = await wrapper.execute(params)

    adapter = getattr(wrapper, "adapter", None)
    if adapter is None or not _is_managed_genie_adapter(adapter):
        return result

    poll_tool = _genie_poll_tool_name(getattr(wrapper, "name", "") or "")
    # Only poll if the sibling poll tool actually exists on this server.
    if not poll_tool or not any(
        (t.get("name") if isinstance(t, dict) else getattr(t, "name", None)) == poll_tool
        for t in (getattr(adapter, "tools", None) or [])
    ):
        return result

    envelope = _genie_status_envelope(result)
    if envelope is None:
        return result  # not a status envelope — already-final answer or unknown shape

    deadline = time.monotonic() + _GENIE_POLL_TIMEOUT_SECONDS
    polls = 0
    while True:
        status = str(envelope.get("status") or "").upper()
        if not status or status in _GENIE_TERMINAL_STATUSES:
            return result

        # Pull the ids straight from the envelope so the agent never has to —
        # this is also what eliminates the conversation_id/message_id mix-up.
        conversation_id = envelope.get("conversationId") or envelope.get("conversation_id")
        message_id = envelope.get("messageId") or envelope.get("message_id")
        if not conversation_id or not message_id:
            return result  # can't poll without both ids

        if time.monotonic() >= deadline:
            logger.warning(
                f"Genie auto-poll timed out after {_GENIE_POLL_TIMEOUT_SECONDS}s "
                f"(last status={status}) via {poll_tool}"
            )
            return (
                f"The Genie query did not finish within {_GENIE_POLL_TIMEOUT_SECONDS} seconds "
                f"(last status: {status}). The results are NOT available. Do not fabricate or "
                f"estimate values — report that the Genie query timed out."
            )

        await asyncio.sleep(_GENIE_POLL_INTERVAL_SECONDS)
        polls += 1
        logger.info(f"Genie auto-poll #{polls} (status={status}) via {poll_tool}")
        try:
            result = await adapter.execute_tool(
                poll_tool,
                {"conversation_id": conversation_id, "message_id": message_id},
            )
        except Exception as e:
            logger.warning(
                f"Genie auto-poll request failed, returning last snapshot: {e}"
            )
            return result  # hand back the last good snapshot rather than a hard error

        next_envelope = _genie_status_envelope(result)
        if next_envelope is None:
            return result  # poll returned an error/unknown shape — surface it as-is
        envelope = next_envelope


# Dictionary to track all active MCP adapters
_active_mcp_adapters = {}

# Connection pool for MCP adapters to reuse connections
_mcp_connection_pool = {}

async def get_or_create_mcp_adapter(server_params, adapter_id=None):
    """
    Get an existing MCP adapter from the connection pool or create a new one.
    This improves performance by reusing existing connections when possible.
    
    Args:
        server_params: Dictionary containing MCP server configuration
        adapter_id: Optional adapter ID for registration tracking
        
    Returns:
        MCPAdapter instance (reused from pool or newly created)
    """
    global _mcp_connection_pool
    
    # Create a unique key for this server configuration
    # Include URL and auth type to ensure different auth contexts get different adapters
    server_url = server_params.get('url', 'stdio')
    auth_type = server_params.get('auth_type', 'default')
    
    # For stdio transport, include the command in the key
    if server_params.get('transport') == 'stdio' and server_params.get('command'):
        command_str = ' '.join(server_params['command']) if isinstance(server_params['command'], list) else server_params['command']
        pool_key = f"stdio_{command_str}"
    else:
        # For HTTP-based servers, key by URL + auth type + a fingerprint of the
        # ACTUAL credential being sent. A pooled connection's server-side identity
        # is fixed when it is opened, so sharing one connection across callers is
        # only safe when they authenticate as the SAME principal. For OBO this is
        # per-USER — without the fingerprint, user A's pooled Genie connection is
        # reused for user B, and B's query against a conversation A created fails
        # with "PERMISSION_DENIED: ... does not own conversation". The fingerprint
        # also rotates the key when an OBO token refreshes, so a stale (expired)
        # connection is never reused. Hashing keeps the raw token out of keys/logs.
        auth_material = (
            (server_params.get('headers') or {}).get('Authorization')
            or server_params.get('user_token')
            or ''
        )
        identity_fp = (
            hashlib.sha256(auth_material.encode()).hexdigest()[:12]
            if auth_material else 'noauth'
        )
        pool_key = f"{server_url}_{auth_type}_{identity_fp}"
    
    # Check if we have a valid adapter in the pool
    if pool_key in _mcp_connection_pool:
        adapter = _mcp_connection_pool[pool_key]
        # Verify the adapter is still initialized and functional
        if hasattr(adapter, '_initialized') and adapter._initialized:
            logger.info(f"Reusing MCP adapter from pool for key: {pool_key}")
            # Still register it with the specific adapter_id if provided
            if adapter_id:
                register_mcp_adapter(adapter_id, adapter)
            return adapter
        else:
            # Remove stale adapter from pool
            logger.warning(f"Removing stale adapter from pool for key: {pool_key}")
            del _mcp_connection_pool[pool_key]
    
    # Create new adapter
    logger.info(f"Creating new MCP adapter for key: {pool_key}")
    from src.engines.common.mcp_adapter import MCPAdapter
    
    adapter = MCPAdapter(server_params)
    await adapter.initialize()
    
    # Add to connection pool for reuse
    _mcp_connection_pool[pool_key] = adapter
    
    # Also register it for tracking if adapter_id provided
    if adapter_id:
        register_mcp_adapter(adapter_id, adapter)
    
    return adapter

def register_mcp_adapter(adapter_id, adapter):
    """
    Register an MCP adapter for tracking
    
    Args:
        adapter_id: A unique identifier for the adapter
        adapter: The MCP adapter to register
    """
    global _active_mcp_adapters
    _active_mcp_adapters[adapter_id] = adapter
    logger.info(f"Registered MCP adapter with ID {adapter_id}")

async def stop_all_adapters():
    """
    Stop all active MCP adapters that have been registered (async version)
    
    This function is used during cleanup to ensure that all MCP resources
    are properly released, especially important for stdio adapters that
    could otherwise leave lingering processes.
    """
    global _active_mcp_adapters, _mcp_connection_pool
    logger.info(f"Stopping all MCP adapters, count: {len(_active_mcp_adapters)}")
    
    # First, stop all pooled adapters
    for pool_key, adapter in list(_mcp_connection_pool.items()):
        try:
            logger.info(f"Stopping pooled MCP adapter: {pool_key}")
            await stop_mcp_adapter(adapter)
        except Exception as e:
            logger.error(f"Error stopping pooled adapter {pool_key}: {str(e)}")
    
    # Clear the connection pool
    _mcp_connection_pool.clear()
    
    # Make a copy of the keys since we'll be modifying the dictionary
    adapter_ids = list(_active_mcp_adapters.keys())
    
    for adapter_id in adapter_ids:
        adapter = _active_mcp_adapters.get(adapter_id)
        if adapter:
            try:
                logger.info(f"Stopping MCP adapter: {adapter_id}")
                await stop_mcp_adapter(adapter)
                # Remove from tracked adapters
                del _active_mcp_adapters[adapter_id]
            except Exception as e:
                logger.error(f"Error stopping MCP adapter {adapter_id}: {str(e)}")
                # Still try to remove from tracking
                try:
                    del _active_mcp_adapters[adapter_id]
                except:
                    pass
                
    # Reset the dictionary
    _active_mcp_adapters.clear()
    logger.info("All MCP adapters stopped")

async def get_databricks_workspace_host():
    """
    Get the Databricks workspace host from the configuration.
    
    Returns:
        Tuple[Optional[str], Optional[str]]: (workspace_host, error_message)
    """
    try:
        from src.services.databricks_service import DatabricksService
        from src.db.session import request_scoped_session

        async with request_scoped_session() as session:
            service = DatabricksService(session)
            config = await service.get_databricks_config()
            
            if config and config.workspace_url:
                # Remove https:// prefix if present for consistency
                workspace_host = config.workspace_url.rstrip('/')
                if workspace_host.startswith("https://"):
                    workspace_host = workspace_host[8:]
                elif workspace_host.startswith("http://"):
                    workspace_host = workspace_host[7:]
                return workspace_host, None
            else:
                return None, "No workspace URL found in configuration"
                
    except Exception as e:
        logger.error(f"Error getting workspace host: {e}")
        return None, str(e)

async def call_databricks_api(endpoint, method="GET", data=None, params=None):
    """
    Call the Databricks API directly as a fallback when MCP fails (async version)
    
    Args:
        endpoint: The API endpoint path (without host)
        method: HTTP method (GET, POST, etc.)
        data: Optional request body for POST/PUT requests
        params: Optional query parameters
        
    Returns:
        The API response (parsed JSON)
    """
    try:
        # Get authentication headers (already async)
        headers, error = await get_databricks_auth_headers()
        if error:
            raise ValueError(f"Authentication error: {error}")
        if not headers:
            raise ValueError("Failed to get authentication headers")
        
        # Get the workspace host (already async)
        workspace_host, host_error = await get_databricks_workspace_host()
        if host_error:
            raise ValueError(f"Configuration error: {host_error}")
        
        # Construct the API URL
        url = f"https://{workspace_host}{endpoint}"
        
        # Make the async API call
        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(url, headers=headers, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method.upper() == "POST":
                async with session.post(url, headers=headers, json=data, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method.upper() == "PUT":
                async with session.put(url, headers=headers, json=data, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method.upper() == "DELETE":
                async with session.delete(url, headers=headers, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
    except Exception as e:
        logger.error(f"Error calling Databricks API: {e}")
        return {"error": f"API error: {str(e)}"}

def create_crewai_tool_from_mcp(mcp_tool_dict):
    """
    Create a CrewAI tool from an MCP tool dictionary.
    
    Args:
        mcp_tool_dict: Dictionary containing MCP tool information
        
    Returns:
        CrewAI tool instance
    """
    from crewai.tools import BaseTool
    from pydantic import BaseModel, Field
    from typing import Type, Dict, Any
    from src.engines.common.mcp_adapter import MCPTool
    
    # Create MCPTool wrapper
    mcp_tool_wrapper = MCPTool(mcp_tool_dict)
    
    # Create a dynamic input schema based on the MCP tool's input schema
    input_schema = mcp_tool_wrapper.input_schema or {}
    
    # Create fields for the Pydantic model
    fields = {}
    annotations = {}
    properties = input_schema.get('properties', {})
    required = input_schema.get('required', [])
    
    for field_name, field_info in properties.items():
        field_type = str  # Default to string
        field_description = field_info.get('description', f'{field_name} parameter')
        # A param the MCP server lists as "required" but that ALSO declares a
        # `default` (incl. null) or is nullable is effectively OPTIONAL. Forcing
        # it makes the LLM fabricate a value: managed-Genie `query_space` marks
        # `conversation_id` required despite `default: null`, so the agent invents
        # e.g. "discovery-session"; Genie then rejects it with "PERMISSION_DENIED:
        # ... does not own conversation discovery-session", which surfaces to the
        # user as "unhandled errors in a TaskGroup (1 sub-exception)". Models that
        # happen to omit/null it (e.g. gpt-5.3) work; Claude does not. Drop such
        # params from `required` so the agent can omit them (→ None → server uses
        # its default / starts a fresh conversation).
        has_default = 'default' in field_info
        is_nullable = field_info.get('type') == 'null' or any(
            isinstance(s, dict) and s.get('type') == 'null'
            for s in field_info.get('anyOf', [])
        )
        is_required = (field_name in required) and not has_default and not is_nullable

        # Add type annotation
        annotations[field_name] = field_type

        if is_required:
            fields[field_name] = Field(..., description=field_description)
        else:
            fields[field_name] = Field(default=None, description=field_description)
    
    # If no fields, add a dummy field
    if not fields:
        annotations['dummy'] = str
        fields['dummy'] = Field(default='', description='Dummy field')
    
    # Create dynamic Pydantic model with annotations
    DynamicToolInput = type(
        f"{mcp_tool_wrapper.name}_Input",
        (BaseModel,),
        {
            '__annotations__': annotations,
            **fields
        }
    )
    
    # Create the custom tool class
    class MCPCrewAITool(BaseTool):
        name: str = mcp_tool_wrapper.name
        description: str = mcp_tool_wrapper.description
        args_schema: Type[BaseModel] = DynamicToolInput
        _mcp_tool_wrapper: MCPTool = None
        
        def __init__(self):
            super().__init__()
            self._mcp_tool_wrapper = mcp_tool_wrapper
        
        def _run(self, **kwargs) -> str:
            """Execute the MCP tool."""
            try:
                # Remove dummy field if it exists
                kwargs.pop('dummy', None)

                # Helper function to run async code in a fresh event loop
                def run_async_in_new_loop(params):
                    """Run the async function in a completely isolated event loop."""
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        # _genie_autopoll executes the tool and, for managed-Genie
                        # query_space calls, blocks until the query completes —
                        # for every other tool it's a single execute(), unchanged.
                        return new_loop.run_until_complete(
                            _genie_autopoll(self._mcp_tool_wrapper, params)
                        )
                    except Exception as e:
                        logger.error(f"Error in async execution for {self._mcp_tool_wrapper.name}: {e}")
                        logger.error(traceback.format_exc())
                        raise
                    finally:
                        new_loop.close()

                # Check if there's already an event loop running
                try:
                    loop = asyncio.get_running_loop()
                    # We're in an async context (CrewAI is running), use thread pool to isolate
                    logger.debug(f"Detected running event loop, using thread pool for {self._mcp_tool_wrapper.name}")
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        result = executor.submit(run_async_in_new_loop, kwargs).result()
                except RuntimeError:
                    # No event loop running, we can execute directly
                    logger.debug(f"No running event loop, executing directly for {self._mcp_tool_wrapper.name}")
                    result = run_async_in_new_loop(kwargs)

                # Normalize the MCP result into agent-friendly text: prefer
                # structured JSON, surface errors, keep resource/image links as
                # markdown image lines, and replace inline binary with compact
                # placeholders (never raw base64).
                if hasattr(result, "content") or hasattr(result, "structuredContent"):
                    return _format_mcp_tool_result(result)
                return str(result)
            except Exception as e:
                logger.error(f"Error executing MCP tool {self._mcp_tool_wrapper.name}: {e}")
                logger.error(traceback.format_exc())
                return f"Error: {str(e)}"
    
    # Return an instance of the tool
    return MCPCrewAITool()

def wrap_mcp_tool(tool):
    """
    Wrap an MCP tool to handle event loop issues by using process isolation
    
    Args:
        tool: The MCP tool to wrap
        
    Returns:
        Wrapped tool with proper event loop handling
    """
    # Store the original _run method and tool information
    original_run = tool._run
    tool_name = tool.name
    
    logger.info(f"Wrapping MCP tool: {tool_name}")
    
    # Add special handling for Databricks Genie tools
    if tool_name in ["get_space", "start_conversation", "create_message"]:
        logger.debug(f"Using Databricks Genie specific wrapper for {tool_name}")
        def wrapped_run(*args, **kwargs):
            try:
                # First try executing directly
                logger.debug(f"Attempting direct execution of {tool_name}")
                return original_run(*args, **kwargs)
            except Exception as direct_error:
                # If we get an error, try the process isolation approach
                logger.warning(f"Using alternate approach for MCP tool {tool_name} due to event loop issue: {direct_error}")
                
                try:
                    logger.debug(f"Running {tool_name} in separate process")
                    # Use a new event loop to avoid conflicts
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(run_in_separate_process(tool_name, kwargs))
                    finally:
                        loop.close()
                    
                    # If result indicates an error, try direct API call
                    if isinstance(result, str) and result.startswith("Error:"):
                        logger.warning(f"Process isolation failed for {tool_name}, attempting direct API call")
                        
                        # Try the direct API approach based on the tool (now async)
                        # Note: We can't use asyncio.run here as we're already in an async context
                        # Instead, we'll need to run this in a new event loop or use a different approach
                        try:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                if tool_name == "get_space" and "space_id" in kwargs:
                                    space_id = kwargs["space_id"]
                                    return loop.run_until_complete(call_databricks_api(f"/api/2.0/genie/spaces/{space_id}"))
                                    
                                elif tool_name == "start_conversation" and "space_id" in kwargs and "content" in kwargs:
                                    space_id = kwargs["space_id"]
                                    content = kwargs["content"]
                                    return loop.run_until_complete(call_databricks_api(
                                        f"/api/2.0/genie/spaces/{space_id}/conversations",
                                        method="POST",
                                        data={"content": content}
                                    ))
                                    
                                elif tool_name == "create_message" and "space_id" in kwargs and "conversation_id" in kwargs and "content" in kwargs:
                                    space_id = kwargs["space_id"]
                                    conversation_id = kwargs["conversation_id"]
                                    content = kwargs["content"]
                                    return loop.run_until_complete(call_databricks_api(
                                        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                                        method="POST",
                                        data={"content": content}
                                    ))
                            finally:
                                loop.close()
                        except Exception as api_error:
                            logger.error(f"Error with direct API call for {tool_name}: {api_error}")
                            return f"API call failed: {str(api_error)}"
                    
                    return result
                except Exception as e:
                    cause = format_mcp_exception(e)
                    logger.error(f"All approaches failed for MCP tool {tool_name}: {cause}")
                    return f"Error executing tool '{tool_name}': {cause}"
        
        # Replace the original _run method with our wrapped version
        tool._run = wrapped_run
        return tool
    
    # For other tools, use the standard approach
    logger.debug(f"Using standard wrapper for {tool_name}")
    def wrapped_run(*args, **kwargs):
        try:
            # First try executing directly - this might work for some cases
            logger.debug(f"Attempting direct execution of {tool_name}")
            return original_run(*args, **kwargs)
        except Exception as direct_error:
            # If we get an error about event loop, use process isolation
            error_message = str(direct_error)
            logger.warning(f"Error during direct execution of {tool_name}: {error_message}")
            
            if "Event loop is closed" in error_message or isinstance(direct_error, RuntimeError):
                logger.warning(f"Using alternate approach for MCP tool {tool_name} due to event loop issue")
                
                # Start a fresh process with a new MCP connection
                try:
                    logger.debug(f"Running {tool_name} in separate process")
                    # Use a new event loop to avoid conflicts
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(run_in_separate_process(tool_name, kwargs))
                    finally:
                        loop.close()
                except Exception as e:
                    logger.error(f"Error running MCP tool {tool_name} in separate process: {e}")
                    return f"Error executing tool: {str(e)}"
            else:
                # For other errors, just log and return the error
                cause = format_mcp_exception(direct_error)
                logger.error(f"Error running MCP tool {tool_name}: {cause}")
                return f"Error executing tool '{tool_name}': {cause}"
        except Exception as e:
            # For any other exception, log and return error message
            logger.error(f"Error running MCP tool {tool_name}: {e}")
            return f"Error executing tool: {str(e)}"
    
    # Replace the original _run method with our wrapped version
    tool._run = wrapped_run
    logger.info(f"Successfully wrapped MCP tool: {tool_name}")
    
    return tool

async def run_in_separate_process(tool_name, kwargs):
    """
    Run an MCP tool in a separate process to avoid event loop issues (async version)
    
    Args:
        tool_name: Name of the tool to run
        kwargs: Keyword arguments for the tool
        
    Returns:
        The result of running the tool
    """
    script_path = None
    try:
        # Get the absolute path to the backend directory
        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
        
        # Create a temporary script to run the tool
        script_content = f"""
import asyncio
import json
import sys
import os

# Add the backend directory to Python path
sys.path.insert(0, r"{backend_dir}")

from src.engines.crewai.tools.mcp_handler import create_mcp_adapter

async def run_tool():
    try:
        # Create a new MCP adapter
        adapter = await create_mcp_adapter()
        
        # Get the tool function
        tool_func = getattr(adapter, tool_name)
        
        # Run the tool
        result = await tool_func(**{json.dumps(kwargs)})
        
        # Log the result as JSON
        print(json.dumps(result))
        
    except Exception as e:
        print(json.dumps({{'error': str(e)}}))
    finally:
        # Clean up
        if 'adapter' in locals():
            await adapter.close()

# Run the async function
asyncio.run(run_tool())
"""
        
        # Write the script to a temporary file
        script_path = f"/tmp/mcp_tool_{tool_name}.py"
        with open(script_path, "w") as f:
            f.write(script_content)
        
        # Run the script in a separate process using async subprocess
        env = os.environ.copy()
        env["PYTHONPATH"] = backend_dir
        
        process = await asyncio.create_subprocess_exec(
            sys.executable, script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            return {"error": f"Process error: {stderr.decode()}"}
        
        # Parse the result
        try:
            return json.loads(stdout.decode())
        except json.JSONDecodeError:
            return {"error": f"Failed to parse result: {stdout.decode()}"}
            
    except Exception as e:
        return {"error": f"Error running tool: {str(e)}"}
    finally:
        # Clean up the temporary script
        if script_path:
            try:
                os.remove(script_path)
            except:
                pass


async def stop_mcp_adapter(adapter):
    """
    Safely stop an MCP adapter (async version)
    
    Args:
        adapter: The MCP adapter to stop
    """
    try:
        logger.info("Stopping MCP adapter")
        
        if adapter is None:
            logger.warning("Attempted to stop None adapter")
            return
            
        # Check if this is an async adapter (including OAuthMCPAdapter)
        if hasattr(adapter, 'stop') and asyncio.iscoroutinefunction(adapter.stop):
            # Async adapter
            await adapter.stop()
        elif hasattr(adapter, 'close') and asyncio.iscoroutinefunction(adapter.close):
            # OAuthMCPAdapter uses close()
            await adapter.close()
        elif hasattr(adapter, 'stop'):
            # Sync adapter - run in thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, adapter.stop)
        
        # Add extra cleanup steps to ensure clean shutdown
        if hasattr(adapter, '_connections'):
            for conn in adapter._connections:
                try:
                    if hasattr(conn, 'close'):
                        conn.close()
                except Exception as conn_error:
                    logger.warning(f"Error closing connection: {conn_error}")
        
        logger.info("MCP adapter stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping MCP adapter: {e}")
        import traceback
        logger.error(traceback.format_exc()) 