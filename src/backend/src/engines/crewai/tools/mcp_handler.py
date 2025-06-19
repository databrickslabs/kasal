import logging
import os
import asyncio
import json
import sys
import subprocess
import concurrent.futures
import traceback
import aiohttp
from typing import Optional
from src.utils.databricks_auth import get_databricks_auth_headers, get_mcp_auth_headers

logger = logging.getLogger(__name__)

# Dictionary to track all active MCP adapters
_active_mcp_adapters = {}

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
    global _active_mcp_adapters
    logger.info(f"Stopping all MCP adapters, count: {len(_active_mcp_adapters)}")
    
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
        from src.core.unit_of_work import UnitOfWork
        
        async with UnitOfWork() as uow:
            service = await DatabricksService.from_unit_of_work(uow)
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
        is_required = field_name in required
        
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
                
                # Check if there's already an event loop running
                try:
                    loop = asyncio.get_running_loop()
                    # We're in an async context, create a task
                    future = asyncio.create_task(self._mcp_tool_wrapper.execute(kwargs))
                    # Use asyncio.run_coroutine_threadsafe to handle it properly
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        result = executor.submit(asyncio.run, self._mcp_tool_wrapper.execute(kwargs)).result()
                except RuntimeError:
                    # No event loop running, we can create one
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(self._mcp_tool_wrapper.execute(kwargs))
                    finally:
                        loop.close()
                
                # Extract text content if it's an MCP result object
                if hasattr(result, 'content') and result.content:
                    text_contents = []
                    for content in result.content:
                        if hasattr(content, 'text'):
                            text_contents.append(content.text)
                    return ' '.join(text_contents) if text_contents else str(result)
                return str(result)
            except Exception as e:
                logger.error(f"Error executing MCP tool {self._mcp_tool_wrapper.name}: {e}")
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
                    logger.error(f"All approaches failed for MCP tool {tool_name}: {e}")
                    return f"Error executing tool: {str(e)}"
        
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
                logger.error(f"Error running MCP tool {tool_name}: {direct_error}")
                return f"Error executing tool: {str(direct_error)}"
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