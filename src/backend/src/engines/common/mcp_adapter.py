"""
MCP Adapter using the official MCP client library.

This adapter supports both SSE and Streamable MCP protocols using the official
MCP client library with Databricks OAuth authentication.
"""

import asyncio
import logging
import time
import traceback
from collections import deque
from typing import Dict, Optional, Any, List

from src.core.exceptions import MCPConnectionError

logger = logging.getLogger(__name__)


def _extract_error_summary(exc: Exception) -> str:
    """Extract a concise error summary from an exception, unwrapping ExceptionGroups."""
    # ExceptionGroup: recurse into sub-exceptions
    if hasattr(exc, 'exceptions'):
        parts = [_extract_error_summary(sub) for sub in exc.exceptions]
        return "; ".join(parts)
    # HTTP status from httpx / aiohttp / requests — include response body
    if hasattr(exc, 'response') and hasattr(exc.response, 'status_code'):
        body = ""
        try:
            body = exc.response.text[:500] if exc.response.text else ""
        except Exception:
            pass
        return f"HTTP {exc.response.status_code} - {exc}" + (f" | body: {body}" if body else "")
    if hasattr(exc, 'status'):
        return f"HTTP {exc.status} - {exc}"
    return str(exc)


def _is_http_auth_error(exc: Exception) -> bool:
    """Check if an exception (or any sub-exception in an ExceptionGroup) is a 401/403 HTTP error."""
    if hasattr(exc, 'exceptions'):
        return any(_is_http_auth_error(sub) for sub in exc.exceptions)
    if hasattr(exc, 'response') and hasattr(exc.response, 'status_code'):
        return exc.response.status_code in (401, 403)
    if hasattr(exc, 'status'):
        return exc.status in (401, 403)
    error_str = str(exc)
    return '403' in error_str or '401' in error_str


def _log_exception_group(exc: Exception, context: str) -> None:
    """Log ExceptionGroup sub-exceptions for visibility."""
    logger.error(f"{context}: {exc}")
    logger.error(f"{context} traceback:\n{traceback.format_exc()}")
    # Python 3.11+ ExceptionGroup / BaseExceptionGroup
    if hasattr(exc, 'exceptions'):
        for i, sub_exc in enumerate(exc.exceptions):
            logger.error(f"{context} sub-exception [{i}]: {type(sub_exc).__name__}: {sub_exc}")
            # Log HTTP response body for 4xx/5xx errors
            if hasattr(sub_exc, 'response'):
                try:
                    body = sub_exc.response.text[:500] if sub_exc.response.text else "(empty)"
                    logger.error(f"{context} sub-exception [{i}] response body: {body}")
                except Exception:
                    pass
            if hasattr(sub_exc, '__traceback__'):
                tb = ''.join(traceback.format_exception(type(sub_exc), sub_exc, sub_exc.__traceback__))
                logger.error(f"{context} sub-exception [{i}] traceback:\n{tb}")


class MCPAdapter:
    """
    MCP Adapter for both SSE and Streamable protocols.
    
    Uses the official MCP client library with proper authentication fallback.
    """
    
    def __init__(self, server_params: Dict[str, Any]):
        """
        Initialize the MCP adapter.

        Args:
            server_params: Server configuration parameters
        """
        self.server_params = server_params
        self.server_url = server_params.get('url', '').strip()
        self.timeout_seconds = server_params.get('timeout_seconds', 30)
        self.max_retries = server_params.get('max_retries', 3)
        self.rate_limit = server_params.get('rate_limit', 60)

        self._tools = []
        self._tool_schemas = {}  # Store schemas for parameter type conversion
        self._initialized = False
        self._call_timestamps: deque = deque()
        self._transport: str = "streamable_http"  # Track which transport works
        self.initialization_error: Optional[MCPConnectionError] = None  # Structured error for UI
        self._spn_fallback_headers: Optional[Dict[str, str]] = None  # SPN headers that worked during discovery
        
    async def initialize(self):
        """Initialize the adapter and discover tools using the working MCP client approach."""
        try:
            logger.info(f"Initializing MCPAdapter for {self.server_url}")

            # Get authentication headers using our mechanism
            headers = await self._get_authentication_headers()
            if not headers:
                self.initialization_error = MCPConnectionError(
                    server_name=self.server_url,
                    server_url=self.server_url,
                    detail=f"MCP server '{self.server_url}': failed to get authentication headers",
                )
                logger.error(self.initialization_error.detail)
                self._tools = []
                self._initialized = True
                return

            logger.info("Successfully obtained authentication headers")

            # Use the working MCP client approach
            tools_list = await self._discover_tools_with_mcp_client(headers)
            self._tools = tools_list

            self._initialized = True
            logger.info(f"MCPAdapter initialized with {len(self._tools)} tools")

        except Exception as e:
            summary = _extract_error_summary(e)
            self.initialization_error = MCPConnectionError(
                server_name=self.server_url,
                server_url=self.server_url,
                detail=f"MCP server '{self.server_url}': {summary}",
                cause=e,
            )
            logger.error(f"Error initializing MCPAdapter: {e}")
            self._tools = []
            self._initialized = True
            
    async def _discover_tools_with_mcp_client(self, headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Discover tools using the MCP client with streamable HTTP → SSE fallback."""
        clean_headers = {"Authorization": headers["Authorization"]}
        last_error: Optional[Exception] = None

        # Log auth type for debugging (no token content)
        auth_value = headers.get("Authorization", "")
        if auth_value.startswith("Bearer "):
            logger.info(f"MCP discovery using Bearer token (length={len(auth_value) - 7})")
        else:
            logger.warning(f"MCP discovery using non-Bearer auth type")

        # Try streamable HTTP first
        try:
            tools = await self._discover_via_streamable_http(clean_headers)
            if tools:
                self._transport = "streamable_http"
                return tools
        except Exception as e:
            last_error = e
            _log_exception_group(e, "Streamable HTTP discovery failed")
            logger.info("Falling back to SSE transport for tool discovery")

        # Fallback: try SSE transport
        try:
            tools = await self._discover_via_sse(clean_headers)
            if tools:
                self._transport = "sse"
                return tools
        except Exception as e:
            last_error = e
            _log_exception_group(e, "SSE discovery also failed")

        # If auth error (401/403) and using Databricks auth, retry with SPN fallback
        # This handles both databricks_obo (legacy) and databricks_spn (unified auth chain
        # where OBO was tried first but rejected by the MCP server)
        if last_error and _is_http_auth_error(last_error) and self.server_params.get('auth_type') in ('databricks_obo', 'databricks_spn'):
            logger.warning(f"OBO authentication rejected by MCP server (403/401). Attempting SPN fallback...")
            spn_headers = await self._get_spn_fallback_headers()
            if spn_headers:
                spn_clean = {"Authorization": spn_headers["Authorization"]}
                auth_value = spn_clean.get("Authorization", "")
                if auth_value.startswith("Bearer "):
                    logger.info(f"MCP SPN fallback using Bearer token (length={len(auth_value) - 7})")

                try:
                    tools = await self._discover_via_streamable_http(spn_clean)
                    if tools:
                        self._transport = "streamable_http"
                        self._spn_fallback_headers = spn_clean
                        logger.info(f"SPN fallback succeeded via streamable HTTP: {len(tools)} tools. Headers saved for tool execution.")
                        return tools
                except Exception as e:
                    _log_exception_group(e, "SPN fallback streamable HTTP failed")

                try:
                    tools = await self._discover_via_sse(spn_clean)
                    if tools:
                        self._transport = "sse"
                        self._spn_fallback_headers = spn_clean
                        logger.info(f"SPN fallback succeeded via SSE: {len(tools)} tools. Headers saved for tool execution.")
                        return tools
                except Exception as e:
                    last_error = e
                    _log_exception_group(e, "SPN fallback SSE also failed")
            else:
                logger.warning("No SPN credentials available for fallback")

        # All transports failed — capture error details for UI
        summary = _extract_error_summary(last_error) if last_error else "unknown error"
        self.initialization_error = MCPConnectionError(
            server_name=self.server_url,
            server_url=self.server_url,
            detail=f"MCP server '{self.server_url}': {summary}",
            cause=last_error,
        )
        logger.error(f"All MCP transports failed for {self.server_url}: {summary}")
        return []

    async def _discover_via_streamable_http(self, clean_headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Discover tools using streamable HTTP transport."""
        from mcp.client.streamable_http import streamablehttp_client as connect

        async with connect(self.server_url, headers=clean_headers, timeout=self.timeout_seconds) as (read_stream, write_stream, _):
            logger.info("Connected to MCP streamable HTTP endpoint for tool discovery")
            return await self._list_tools_from_session(read_stream, write_stream)

    async def _discover_via_sse(self, clean_headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Discover tools using SSE transport (fallback)."""
        from mcp.client.sse import sse_client

        async with sse_client(self.server_url, headers=clean_headers, timeout=self.timeout_seconds) as (read_stream, write_stream):
            logger.info("Connected to MCP SSE endpoint for tool discovery")
            return await self._list_tools_from_session(read_stream, write_stream)

    async def _list_tools_from_session(self, read_stream, write_stream) -> List[Dict[str, Any]]:
        """List tools from an established MCP session."""
        from mcp import ClientSession

        tools_list: List[Dict[str, Any]] = []
        async with ClientSession(read_stream, write_stream) as session:
            logger.info("Created MCP client session")
            await session.initialize()
            logger.info("MCP session initialized")

            tools_result = await session.list_tools()
            logger.info("Retrieved tools from MCP server")

            if hasattr(tools_result, 'tools') and tools_result.tools:
                logger.info(f"Found {len(tools_result.tools)} tools")
                for mcp_tool in tools_result.tools:
                    tool_wrapper = {
                        "name": mcp_tool.name,
                        "description": mcp_tool.description,
                        "mcp_tool": mcp_tool,
                        "input_schema": mcp_tool.inputSchema,
                        "adapter": self,
                    }
                    tools_list.append(tool_wrapper)
                    self._tool_schemas[mcp_tool.name] = mcp_tool.inputSchema
                    logger.debug(f"Added tool: {mcp_tool.name}")
            else:
                logger.warning("No tools found in MCP server response")

        return tools_list

    def _convert_parameters(self, tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert parameter types according to the tool's input schema.

        Args:
            tool_name: Name of the tool
            parameters: Raw parameters from CrewAI (often strings)

        Returns:
            Parameters with correct types for MCP server
        """
        try:
            # Get the schema for this tool
            schema = self._tool_schemas.get(tool_name)
            if not schema:
                logger.warning(f"No schema found for tool {tool_name}, using parameters as-is")
                return parameters

            # Get properties from schema
            properties = schema.get('properties', {})
            if not properties:
                logger.debug(f"No properties in schema for {tool_name}")
                return parameters

            logger.info(f"Converting parameters for {tool_name}")
            logger.info(f"  Input parameters: {parameters}")
            logger.info(f"  Expected schema properties: {list(properties.keys())}")

            converted = {}

            for param_name, param_value in parameters.items():
                # Skip None, null, empty strings
                if param_value is None or param_value == '' or param_value == 'null':
                    logger.debug(f"Skipping null/empty parameter: {param_name}")
                    continue

                # Get the property schema
                prop_schema = properties.get(param_name, {})
                param_type = prop_schema.get('type')

                try:
                    # Convert based on type
                    if param_type == 'number':
                        # Convert to float
                        if isinstance(param_value, str):
                            converted[param_name] = float(param_value)
                        else:
                            converted[param_name] = float(param_value)
                    elif param_type == 'integer':
                        # Convert to int
                        if isinstance(param_value, str):
                            converted[param_name] = int(param_value)
                        else:
                            converted[param_name] = int(param_value)
                    elif param_type == 'boolean':
                        # Convert to bool
                        if isinstance(param_value, str):
                            converted[param_name] = param_value.lower() in ('true', '1', 'yes')
                        else:
                            converted[param_name] = bool(param_value)
                    elif param_type == 'array':
                        # Keep arrays as-is, or parse JSON if string
                        if isinstance(param_value, str):
                            import json
                            try:
                                converted[param_name] = json.loads(param_value)
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    f"Failed to parse array parameter {param_name}='{param_value}' as JSON: {e}. Skipping parameter."
                                )
                                # Skip parameter - don't send invalid data to MCP server
                                continue
                        else:
                            converted[param_name] = param_value
                    elif param_type == 'object':
                        # Keep objects as-is, or parse JSON if string
                        if isinstance(param_value, str):
                            import json
                            try:
                                converted[param_name] = json.loads(param_value)
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    f"Failed to parse object parameter {param_name}='{param_value}' as JSON: {e}. Skipping parameter."
                                )
                                # Skip parameter - don't send invalid data to MCP server
                                continue
                        else:
                            converted[param_name] = param_value
                    else:
                        # For string or unknown types, keep as-is
                        converted[param_name] = param_value

                    # Validate enum if present
                    if 'enum' in prop_schema:
                        allowed_values = prop_schema['enum']
                        if converted[param_name] not in allowed_values:
                            logger.warning(
                                f"Parameter {param_name}={converted[param_name]} not in allowed enum values: {allowed_values}"
                            )
                            # Remove invalid enum value
                            del converted[param_name]

                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to convert parameter {param_name}={param_value} to {param_type}: {e}")
                    # Skip parameters that fail conversion
                    continue

            # Log the conversion results
            skipped_params = set(parameters.keys()) - set(converted.keys()) - {None, '', 'null'}
            if skipped_params:
                logger.warning(f"Skipped parameters for {tool_name}: {skipped_params}")
            logger.info(f"Final converted parameters for {tool_name}: {converted}")
            return converted

        except Exception as e:
            logger.error(f"Error converting parameters for {tool_name}: {e}")
            # Return original parameters as fallback
            return parameters

    async def execute_tool(self, tool_name: str, parameters: Dict[str, Any]) -> Any:
        """Execute a tool by creating a new MCP session (stateless approach).

        Includes rate limiting and retry with exponential backoff for transient errors.
        Uses the same transport (streamable HTTP or SSE) that succeeded during discovery.
        """
        # Rate limiting: enforce max calls per 60-second window
        await self._wait_for_rate_limit()

        # Convert parameters according to schema before execution
        converted_params = self._convert_parameters(tool_name, parameters)

        # Get authentication headers
        headers = await self._get_authentication_headers()
        if not headers:
            raise ValueError("No authentication headers available")

        clean_headers = {"Authorization": headers["Authorization"]}

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                result = await self._execute_with_transport(
                    tool_name, converted_params, clean_headers
                )
                self._call_timestamps.append(time.monotonic())
                return result

            except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    backoff = 2 ** attempt
                    logger.warning(
                        f"Transient error executing MCP tool {tool_name} "
                        f"(attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {backoff}s: {e}"
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        f"MCP tool {tool_name} failed after {self.max_retries} attempts: {e}"
                    )
            except Exception as e:
                _log_exception_group(e, f"Error executing MCP tool {tool_name}")
                raise

        raise last_error  # type: ignore[misc]

    async def _execute_with_transport(
        self, tool_name: str, params: Dict[str, Any], clean_headers: Dict[str, str]
    ) -> Any:
        """Execute a tool using the transport that succeeded during discovery."""
        from mcp import ClientSession

        if self._transport == "sse":
            from mcp.client.sse import sse_client
            async with sse_client(self.server_url, headers=clean_headers, timeout=self.timeout_seconds) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    logger.info(f"Executing MCP tool (SSE): {tool_name} with parameters: {params}")
                    result = await session.call_tool(tool_name, params)
                    logger.info(f"Tool {tool_name} executed successfully (SSE)")
                    return result
        else:
            from mcp.client.streamable_http import streamablehttp_client as connect
            async with connect(self.server_url, headers=clean_headers, timeout=self.timeout_seconds) as (read_stream, write_stream, _):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    logger.info(f"Executing MCP tool (streamable HTTP): {tool_name} with parameters: {params}")
                    result = await session.call_tool(tool_name, params)
                    logger.info(f"Tool {tool_name} executed successfully (streamable HTTP)")
                    return result
            

    async def _wait_for_rate_limit(self) -> None:
        """Enforce sliding-window rate limiting (max calls per 60-second window)."""
        if self.rate_limit <= 0:
            return

        now = time.monotonic()
        window = 60.0

        # Remove timestamps outside the window
        while self._call_timestamps and (now - self._call_timestamps[0]) > window:
            self._call_timestamps.popleft()

        if len(self._call_timestamps) >= self.rate_limit:
            oldest = self._call_timestamps[0]
            wait_time = window - (now - oldest)
            if wait_time > 0:
                logger.info(
                    f"Rate limit reached ({self.rate_limit} calls/min), "
                    f"waiting {wait_time:.1f}s"
                )
                await asyncio.sleep(wait_time)
                # Clean up again after waiting
                now = time.monotonic()
                while self._call_timestamps and (now - self._call_timestamps[0]) > window:
                    self._call_timestamps.popleft()

    async def _get_spn_fallback_headers(self) -> Optional[Dict[str, str]]:
        """Get SPN (Service Principal) authentication headers as fallback when OBO fails."""
        try:
            from src.utils.databricks_auth import get_auth_context
            # Call get_auth_context WITHOUT user_token so it skips OBO and tries PAT → SPN
            group_id = self.server_params.get('group_id')
            auth_context = await get_auth_context(user_token=None, group_id=group_id)
            if auth_context and auth_context.token:
                logger.info(f"SPN/PAT fallback obtained token via auth method: {auth_context.auth_method}")
                return {"Authorization": f"Bearer {auth_context.token}"}
            else:
                logger.warning("SPN/PAT fallback: no auth context available")
                return None
        except Exception as e:
            logger.error(f"Error getting SPN fallback headers: {e}")
            return None

    async def _get_authentication_headers(self) -> Optional[Dict[str, str]]:
        """Get authentication headers using our fallback mechanism."""
        try:
            # If SPN fallback headers were obtained during discovery (OBO was rejected),
            # use them directly — the OBO token is known to be rejected by this server.
            if self._spn_fallback_headers:
                logger.info("Using SPN fallback headers (OBO was rejected during discovery)")
                return self._spn_fallback_headers

            # First try using provided headers
            provided_headers = self.server_params.get('headers', {})
            if provided_headers and 'Authorization' in provided_headers:
                auth_type = self.server_params.get('auth_type', 'unknown')
                has_obo = bool(self.server_params.get('user_token'))
                logger.info(f"Using provided authentication headers (auth_type={auth_type}, has_obo_token={has_obo})")
                return provided_headers
                
            # If no provided headers, try to get them using our auth mechanism
            from src.utils.databricks_auth import get_mcp_auth_headers
            
            logger.info("Getting authentication headers using fallback mechanism")
            headers, error = await get_mcp_auth_headers(
                self.server_url,
                user_token=self.server_params.get('user_token'),
                api_key=self.server_params.get('api_key'),
                include_sse_headers=False  # Don't include extra headers
            )
            
            if headers:
                logger.info("Successfully obtained authentication headers via fallback")
                return headers
            else:
                logger.error(f"Failed to get authentication headers: {error}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting authentication headers: {e}")
            return None
    
    @property
    def tools(self) -> List[Any]:
        """Get the tools from the adapter."""
        return self._tools if self._tools is not None else []
        
    async def stop(self):
        """Stop the adapter and clean up resources."""
        try:
            logger.info("MCPAdapter stopped")
        except Exception as e:
            logger.error(f"Error stopping MCPAdapter: {e}")
            
    async def close(self):
        """Alias for stop() for compatibility."""
        await self.stop()


class MCPTool:
    """
    Wrapper for MCP tools that can be executed via the MCP adapter.
    """
    
    def __init__(self, tool_wrapper: Dict[str, Any]):
        """Initialize the tool wrapper."""
        self.name = tool_wrapper.get('name', 'unknown')
        self.description = tool_wrapper.get('description', '')
        self.input_schema = tool_wrapper.get('input_schema', {})
        self.mcp_tool = tool_wrapper.get('mcp_tool')
        self.adapter = tool_wrapper.get('adapter')
        
    async def execute(self, parameters: Dict[str, Any]) -> Any:
        """Execute the tool with the given parameters."""
        try:
            if not self.adapter:
                raise ValueError("No MCP adapter available for tool execution")
                
            result = await self.adapter.execute_tool(self.name, parameters)
            return result
            
        except Exception as e:
            logger.error(f"Error executing MCP tool {self.name}: {e}")
            raise
            
    def __str__(self):
        return f"MCPTool(name={self.name}, description={self.description})"