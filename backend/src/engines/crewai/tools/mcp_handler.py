import logging
import os
import asyncio
import json
import sys
import subprocess
import concurrent.futures
import traceback
import requests

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
        # Get credentials from environment
        token = os.environ.get("DATABRICKS_API_KEY")
        host = os.environ.get("DATABRICKS_HOST", "your-workspace.cloud.databricks.com")
        
        if not token:
            # Try to get API key from service
            try:
                from src.services.api_keys_service import ApiKeysService
                from src.utils.encryption_utils import EncryptionUtils
                from src.core.unit_of_work import UnitOfWork
                
                async def get_api_key_async():
                    async with UnitOfWork() as uow:
                        api_keys_service = ApiKeysService(uow.session)
                        api_key = await api_keys_service.find_by_name("DATABRICKS_API_KEY")
                        if api_key and api_key.encrypted_value:
                            return EncryptionUtils.decrypt_value(api_key.encrypted_value)
                        return None
                
                # Use asyncio to run the async method
                try:
                    # Check if we're already in an event loop
                    asyncio.get_running_loop()
                    # We are in an event loop, use a different approach
                    import nest_asyncio
                    nest_asyncio.apply()
                    token = asyncio.run(get_api_key_async())
                except RuntimeError:
                    # No running event loop, we can create a new one
                    loop = asyncio.new_event_loop()
                    try:
                        asyncio.set_event_loop(loop)
                        token = loop.run_until_complete(get_api_key_async())
                    finally:
                        loop.close()
            except Exception as e:
                logger.error(f"Error getting API key: {e}")
                pass
        
        if not token:
            raise ValueError("No Databricks API key available")
        
        # Construct the API URL
        url = f"https://{host}{endpoint}"
        
        # Set headers with authentication
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
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
        tool_name: The name of the tool to run
        kwargs: The arguments to pass to the tool
        
    Returns:
        The result of the tool execution
    """
    import subprocess
    import json
    import sys
    import os
    import concurrent.futures
    
    # Get tool configs
    from src.seeds.tools import get_tool_configs
    tool_configs = get_tool_configs()
    mcp_config = tool_configs.get("69", {})
    
    # Create a script that runs the tool in a new process
    tool_runner_script = """
import sys, json, asyncio, traceback
from crewai_tools import MCPServerAdapter

# Custom JSON encoder to handle non-serializable objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except:
            # Convert any non-serializable objects to strings
            return str(obj)

# Function to ensure a value is JSON serializable
def ensure_serializable(obj):
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [ensure_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {str(k): ensure_serializable(v) for k, v in obj.items()}
    else:
        # For any other type, convert to string
        return str(obj)

async def run_tool():
    # Parse arguments
    tool_name = sys.argv[1]
    args_json = sys.argv[2]
    server_url = sys.argv[3]
    
    # Create event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Setup MCP adapter
    adapter = MCPServerAdapter({"url": server_url})
    
    try:
        # Find matching tool
        for tool in adapter.tools:
            if tool.name == tool_name:
                # Execute tool with args
                args = json.loads(args_json)
                print(f"Running {tool_name} with args: {args}", file=sys.stderr)
                
                try:
                    result = tool._run(**args)
                    
                    # Make sure the result is serializable
                    serializable_result = ensure_serializable(result)
                    
                    # Try to encode to JSON and print
                    try:
                        print(json.dumps({"success": True, "result": serializable_result}, cls=CustomJSONEncoder))
                    except Exception as json_error:
                        # If JSON encoding fails, return the result as a string
                        print(json.dumps({"success": True, "result": str(serializable_result)}))
                except Exception as e:
                    # If tool execution fails
                    error_msg = f"Error executing tool: {str(e)}\\n{traceback.format_exc()}"
                    print(json.dumps({"success": False, "error": error_msg}))
                break
        else:
            print(json.dumps({"success": False, "error": f"Tool {tool_name} not found"}))
    except Exception as e:
        # If any other error occurs
        error_msg = f"Unexpected error: {str(e)}\\n{traceback.format_exc()}"
        print(json.dumps({"success": False, "error": error_msg}))
    finally:
        # Cleanup
        try:
            adapter.stop()
        except:
            pass

if __name__ == "__main__":
    asyncio.run(run_tool())
"""
    
    # Write script to a temporary file
    import tempfile
    with tempfile.NamedTemporaryFile('w', suffix='.py', delete=False) as f:
        f.write(tool_runner_script)
        temp_script = f.name
    
    try:
        # Get server URL from config
        server_url = mcp_config.get("server_url", "http://localhost:8000/sse")
        
        # Execute the script in a separate process
        cmd = [sys.executable, temp_script, tool_name, json.dumps(kwargs), server_url]
        
        # Log the command for debugging
        logger.debug(f"Running MCP tool in separate process: {' '.join(cmd)}")
        
        # Run in executor to avoid blocking
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                lambda: subprocess.run(cmd, text=True, capture_output=True)
            )
            try:
                process = future.result(timeout=60)  # 60 second timeout
                
                # Process result
                if process.returncode == 0:
                    stdout = process.stdout.strip()
                    stderr = process.stderr.strip()
                    
                    # Log stderr for debugging if not empty
                    if stderr:
                        logger.debug(f"MCP tool stderr: {stderr}")
                    
                    # If no output, return error
                    if not stdout:
                        return f"Error: No output from MCP tool {tool_name}"
                    
                    # Try parsing as JSON first
                    try:
                        output = json.loads(stdout)
                        if output.get("success"):
                            result = output.get("result")
                            
                            # Try to parse the result if it's a string that looks like JSON
                            if isinstance(result, str) and result.strip().startswith("{") and result.strip().endswith("}"):
                                try:
                                    # Maybe it's a nested JSON string that needs parsing
                                    parsed_result = json.loads(result)
                                    return parsed_result
                                except:
                                    # If that fails, just return the string as is
                                    return result
                            else:
                                # Return as is
                                return result
                        else:
                            error_msg = output.get('error', 'Unknown error')
                            logger.error(f"Tool process error: {error_msg}")
                            return f"Error: {error_msg}"
                    except json.JSONDecodeError:
                        # Failed to parse as JSON - look for patterns in the output
                        logger.warning(f"Failed to parse JSON from stdout: {stdout[:200]}")
                        
                        # Check if the response contains valid JSON embedded in it
                        import re
                        json_pattern = r'({.*})'
                        if re.search(json_pattern, stdout):
                            # Try to extract JSON from the output
                            json_match = re.search(json_pattern, stdout, re.DOTALL)
                            if json_match:
                                try:
                                    json_str = json_match.group(1)
                                    return json.loads(json_str)
                                except:
                                    # If that fails, return the captured text
                                    return json_match.group(1)
                        
                        # If no JSON pattern, maybe it's simple text
                        if len(stdout) < 1000:  # Reasonable size for text output
                            return stdout
                        
                        # Fall back to a more user-friendly error
                        return f"Result from {tool_name}: " + stdout[:200] + "..." 
                else:
                    logger.error(f"Process error: {process.stderr}")
                    return f"Error: Process failed with code {process.returncode}: {process.stderr}"
            except concurrent.futures.TimeoutError:
                return "Error: Tool execution timed out"
    finally:
        # Clean up temp file
        try:
            os.unlink(temp_script)
        except:
            pass

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