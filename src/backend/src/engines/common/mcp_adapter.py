"""
MCP Adapter using the official MCP client library.

This adapter supports both SSE and Streamable MCP protocols using the official
MCP client library with Databricks OAuth authentication.
"""

import logging
from typing import Dict, Optional, Any, List

logger = logging.getLogger(__name__)


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
        self.server_url = server_params.get('url', '')
        self.timeout_seconds = server_params.get('timeout_seconds', 30)
        self.max_retries = server_params.get('max_retries', 3)
        self.rate_limit = server_params.get('rate_limit', 60)

        self._tools = []
        self._tool_schemas = {}  # Store schemas for parameter type conversion
        self._initialized = False
        
    async def initialize(self):
        """Initialize the adapter and discover tools using the working MCP client approach."""
        try:
            logger.info(f"Initializing MCPAdapter for {self.server_url}")
            
            # Get authentication headers using our mechanism
            headers = await self._get_authentication_headers()
            if not headers:
                logger.error("Failed to get authentication headers")
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
            logger.error(f"Error initializing MCPAdapter: {e}")
            self._tools = []
            self._initialized = True
            
    async def _discover_tools_with_mcp_client(self, headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Discover tools using the working MCP client approach."""
        try:
            # Import MCP client dependencies
            from mcp.client.streamable_http import streamablehttp_client as connect
            from mcp import ClientSession
            
            tools_list = []
            
            # Use only the Authorization header (this is what works)
            clean_headers = {"Authorization": headers["Authorization"]}
            
            # Connect using MCP streamable HTTP client (the working approach)
            async with connect(self.server_url, headers=clean_headers) as (read_stream, write_stream, _):
                logger.info("Connected to MCP streamable endpoint for tool discovery")
                
                # Create a session using the client streams
                async with ClientSession(read_stream, write_stream) as session:
                    logger.info("Created MCP client session")
                    
                    # Initialize the connection
                    await session.initialize()
                    logger.info("MCP session initialized")
                    
                    # List available tools
                    tools_result = await session.list_tools()
                    logger.info(f"Retrieved tools from MCP server")
                    
                    if hasattr(tools_result, 'tools') and tools_result.tools:
                        logger.info(f"Found {len(tools_result.tools)} tools")
                        
                        # Convert MCP tools to our format
                        for mcp_tool in tools_result.tools:
                            tool_wrapper = {
                                "name": mcp_tool.name,
                                "description": mcp_tool.description,
                                "mcp_tool": mcp_tool,
                                "input_schema": mcp_tool.inputSchema,
                                "adapter": self  # Store adapter reference for tool execution
                            }
                            tools_list.append(tool_wrapper)
                            # Store schema for parameter type conversion
                            self._tool_schemas[mcp_tool.name] = mcp_tool.inputSchema
                            logger.debug(f"Added tool: {mcp_tool.name}")
                    else:
                        logger.warning("No tools found in MCP server response")
                        
            return tools_list
            
        except Exception as e:
            logger.error(f"Error discovering tools with MCP client: {e}")
            return []

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
        """Execute a tool by creating a new MCP session (stateless approach)."""
        try:
            # Convert parameters according to schema before execution
            converted_params = self._convert_parameters(tool_name, parameters)

            # Get authentication headers
            headers = await self._get_authentication_headers()
            if not headers:
                raise ValueError("No authentication headers available")

            # Import MCP client dependencies
            from mcp.client.streamable_http import streamablehttp_client as connect
            from mcp import ClientSession

            # Use only the Authorization header
            clean_headers = {"Authorization": headers["Authorization"]}

            # Connect using MCP streamable HTTP client
            async with connect(self.server_url, headers=clean_headers) as (read_stream, write_stream, _):
                logger.debug(f"Connected to MCP for tool execution: {tool_name}")

                # Create a session using the client streams
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()

                    # Execute the tool with converted parameters
                    logger.info(f"Executing MCP tool: {tool_name} with parameters: {converted_params}")
                    result = await session.call_tool(tool_name, converted_params)
                    logger.info(f"Tool {tool_name} executed successfully")
                    return result
                    
        except Exception as e:
            logger.error(f"Error executing MCP tool {tool_name}: {e}")
            raise
            
    async def _get_authentication_headers(self) -> Optional[Dict[str, str]]:
        """Get authentication headers using our fallback mechanism."""
        try:
            # First try using provided headers
            provided_headers = self.server_params.get('headers', {})
            if provided_headers and 'Authorization' in provided_headers:
                logger.info("Using provided authentication headers")
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