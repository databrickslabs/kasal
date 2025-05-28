import logging
import os
import asyncio
import json
import sys
import subprocess
import concurrent.futures
import traceback
import requests
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

def stop_all_adapters():
    """
    Stop all active MCP adapters that have been registered
    
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
                stop_mcp_adapter(adapter)
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
    _active_mcp_adapters = {}
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

def call_databricks_api(endpoint, method="GET", data=None, params=None):
    """
    Call the Databricks API directly as a fallback when MCP fails
    
    Args:
        endpoint: The API endpoint path (without host)
        method: HTTP method (GET, POST, etc.)
        data: Optional request body for POST/PUT requests
        params: Optional query parameters
        
    Returns:
        The API response (parsed JSON)
    """
    try:
        # Get authentication headers (this will also get the host internally)
        headers, error = asyncio.run(get_databricks_auth_headers())
        if error:
            raise ValueError(f"Authentication error: {error}")
        if not headers:
            raise ValueError("Failed to get authentication headers")
        
        # Get the workspace host
        workspace_host, host_error = asyncio.run(get_databricks_workspace_host())
        if host_error:
            raise ValueError(f"Configuration error: {host_error}")
        
        # Construct the API URL
        url = f"https://{workspace_host}{endpoint}"
        
        # Make the API call
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data, params=params)
        elif method.upper() == "PUT":
            response = requests.put(url, headers=headers, json=data, params=params)
        elif method.upper() == "DELETE":
            response = requests.delete(url, headers=headers, params=params)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status()
        
        # Return the response as a dictionary
        return response.json()
    except Exception as e:
        logger.error(f"Error calling Databricks API: {e}")
        return {"error": f"API error: {str(e)}"}

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
                    result = run_in_separate_process(tool_name, kwargs)
                    
                    # If result indicates an error, try direct API call
                    if isinstance(result, str) and result.startswith("Error:"):
                        logger.warning(f"Process isolation failed for {tool_name}, attempting direct API call")
                        
                        # Try the direct API approach based on the tool
                        if tool_name == "get_space" and "space_id" in kwargs:
                            space_id = kwargs["space_id"]
                            return call_databricks_api(f"/api/2.0/genie/spaces/{space_id}")
                            
                        elif tool_name == "start_conversation" and "space_id" in kwargs and "content" in kwargs:
                            space_id = kwargs["space_id"]
                            content = kwargs["content"]
                            return call_databricks_api(
                                f"/api/2.0/genie/spaces/{space_id}/conversations",
                                method="POST",
                                data={"content": content}
                            )
                            
                        elif tool_name == "create_message" and "space_id" in kwargs and "conversation_id" in kwargs and "content" in kwargs:
                            space_id = kwargs["space_id"]
                            conversation_id = kwargs["conversation_id"]
                            content = kwargs["content"]
                            return call_databricks_api(
                                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                                method="POST",
                                data={"content": content}
                            )
                    
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
                    return run_in_separate_process(tool_name, kwargs)
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

def run_in_separate_process(tool_name, kwargs):
    """
    Run an MCP tool in a separate process to avoid event loop issues
    
    Args:
        tool_name: Name of the tool to run
        kwargs: Keyword arguments for the tool
        
    Returns:
        The result of running the tool
    """
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
        
        # Print the result as JSON
        print(json.dumps(result))
        
    except Exception as e:
        print(json.dumps({{"error": str(e)}}))
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
        
        # Run the script in a separate process
        env = os.environ.copy()
        env["PYTHONPATH"] = backend_dir
        
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env,
            check=True
        )
        
        # Parse the result
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {"error": f"Failed to parse result: {result.stdout}"}
            
    except subprocess.CalledProcessError as e:
        return {"error": f"Process error: {e.stderr}"}
    except Exception as e:
        return {"error": f"Error running tool: {str(e)}"}
    finally:
        # Clean up the temporary script
        try:
            os.remove(script_path)
        except:
            pass

async def create_mcp_adapter():
    """
    Create a new MCP adapter with proper authentication.
    
    Returns:
        MCPAdapter: A new MCP adapter instance
    """
    try:
        # Get the MCP server URL
        mcp_url = "https://mcpgenie-1444828305810485.aws.databricksapps.com/sse"
        
        # Get MCP authentication headers
        headers, error = await get_mcp_auth_headers(mcp_url)
        if error:
            raise ValueError(f"Failed to get MCP auth headers: {error}")
            
        # Create the adapter
        from src.engines.crewai.tools.mcp_adapter import MCPAdapter
        adapter = MCPAdapter(mcp_url, headers)
        
        # Initialize the adapter
        await adapter.initialize()
        
        # Register the adapter for tracking
        adapter_id = id(adapter)
        register_mcp_adapter(adapter_id, adapter)
        
        return adapter
        
    except Exception as e:
        logger.error(f"Error creating MCP adapter: {e}")
        raise

def stop_mcp_adapter(adapter):
    """
    Safely stop an MCP adapter
    
    Args:
        adapter: The MCP adapter to stop
    """
    try:
        logger.info("Stopping MCP adapter")
        
        if adapter is None:
            logger.warning("Attempted to stop None adapter")
            return
            
        # Force close any connections and resources
        adapter.stop()
        
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