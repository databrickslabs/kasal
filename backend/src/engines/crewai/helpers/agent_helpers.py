"""
Utilities for Agent configuration, validation, and setup.

This module provides helper functions for working with CrewAI agents.
"""
import os
from typing import Dict, Any, Optional, Tuple, List

from crewai import Agent
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.engines.crewai.helpers.tool_helpers import resolve_tool_ids_to_names

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew

def process_knowledge_sources(knowledge_sources: List[Any]) -> List[str]:
    """
    Process knowledge sources and return paths.
    
    Args:
        knowledge_sources: List of knowledge sources, which can be strings, 
                          dictionaries with 'path' property, or objects with 'path' property
                          
    Returns:
        List of string paths
    """
    if not knowledge_sources:
        return knowledge_sources
    
    logger.info(f"Processing knowledge sources: {knowledge_sources}")
    
    # If knowledge_sources is a list of strings (paths), return as is
    if all(isinstance(source, str) for source in knowledge_sources):
        return knowledge_sources
        
    # If knowledge_sources contains objects with a 'path' property, extract just the paths
    paths = []
    for source in knowledge_sources:
        if isinstance(source, dict) and 'path' in source:
            paths.append(source['path'])
        elif hasattr(source, 'path'):
            paths.append(source.path)
        elif isinstance(source, str):
            paths.append(source)
    
    logger.info(f"Processed paths: {paths}")
    return paths

async def create_agent(
    agent_key: str, 
    agent_config: Dict, 
    tools: List[Any] = None, 
    config: Dict = None,
    tool_service = None,
    tool_factory = None
) -> Agent:
    """
    Creates an Agent instance from the provided configuration.
    
    Args:
        agent_key: The unique identifier for the agent
        agent_config: Dictionary containing agent configuration
        tools: List of tools to be available to the agent
        config: Global configuration dictionary containing API keys
        tool_service: Optional tool service for resolving tool IDs to names
        tool_factory: Optional tool factory for creating tools
        
    Returns:
        Agent: A configured CrewAI Agent instance
        
    Raises:
        ValueError: If required fields are missing
    """
    logger.info(f"Creating agent {agent_key} with config: {agent_config}")
    
    # Validate required fields
    required_fields = ['role', 'goal', 'backstory']
    for field in required_fields:
        if field not in agent_config:
            raise ValueError(f"Missing required field '{field}' in agent configuration")
        if not agent_config[field]:  # Check if field is empty
            raise ValueError(f"Field '{field}' cannot be empty in agent configuration")
    
    # Process knowledge sources if present
    if 'knowledge_sources' in agent_config:
        agent_config['knowledge_sources'] = process_knowledge_sources(agent_config['knowledge_sources'])
    
    # Handle LLM configuration
    llm = None
    try:
        # Import LLMManager
        from src.core.llm_manager import LLMManager
        
        if 'llm' in agent_config:
            # Check if LLM is a string (model name) or a dictionary (LLM config)
            if isinstance(agent_config['llm'], str):
                # Use LLMManager to configure the LLM with proper provider prefix
                model_name = agent_config['llm']
                logger.info(f"Configuring agent {agent_key} LLM using LLMManager for model: {model_name}")
                llm = await LLMManager.configure_crewai_llm(model_name)
                logger.info(f"Successfully configured LLM for agent {agent_key} using model: {model_name}")
            elif isinstance(agent_config['llm'], dict):
                # If a dictionary is provided with LLM parameters, use crewai LLM directly
                from crewai import LLM
                
                llm_config = agent_config['llm']
                
                # If a model name is specified, configure it through LLMManager
                if 'model' in llm_config:
                    model_name = llm_config['model']
                    # Get properly configured LLM for the model
                    configured_llm = await LLMManager.configure_crewai_llm(model_name)
                    
                    # Extract the configured parameters
                    if hasattr(configured_llm, 'model'):
                        # Apply the configured parameters but allow overrides from llm_config
                        llm_kwargs = vars(configured_llm)
                    else:
                        # Fallback if we can't extract params
                        llm_kwargs = {'model': model_name}
                    
                    # Apply any additional parameters from llm_config
                    for key, value in llm_config.items():
                        if value is not None:
                            llm_kwargs[key] = value
                            
                    # Create the LLM with the merged parameters
                    llm = LLM(**llm_kwargs)
                    logger.info(f"Created LLM instance for agent {agent_key} with model {llm_kwargs.get('model')}")
                else:
                    # No model specified, use default with additional parameters
                    logger.warning(f"LLM config missing 'model', using default with additional parameters")
                    default_llm = await LLMManager.configure_crewai_llm("gpt-4o")
                    
                    # Extract and merge parameters
                    llm_kwargs = vars(default_llm)
                    for key, value in llm_config.items():
                        if value is not None:
                            llm_kwargs[key] = value
                    
                    llm = LLM(**llm_kwargs)
        else:
            # Use default model
            logger.info(f"No LLM specified for agent {agent_key}, using default")
            llm = await LLMManager.configure_crewai_llm("gpt-4o")
            
    except Exception as e:
        # Fallback to simple string if configuration fails
        logger.error(f"Error configuring LLM: {e}")
        llm = agent_config.get('llm', "gpt-4o")
        logger.warning(f"Using string model name as fallback for agent {agent_key}: {llm}")
    
    # Log detailed LLM info for debugging
    logger.info(f"Final LLM configuration for agent {agent_key}: {llm}")
    
    # Handle tool resolution if tool_service is provided and agent has tool_ids
    agent_tools = tools if tools else []
    
    # New code: Always check for enabled MCP servers regardless of tool configuration
    try:
        import os
        from src.core.unit_of_work import UnitOfWork
        from src.services.mcp_service import MCPService
        
        logger.info(f"Checking for enabled MCP servers for agent {agent_key}")
        async with UnitOfWork() as uow:
            mcp_service = await MCPService.from_unit_of_work(uow)
            enabled_servers_response = await mcp_service.get_enabled_servers()
            
            if enabled_servers_response and enabled_servers_response.servers and len(enabled_servers_response.servers) > 0:
                logger.info(f"Found {len(enabled_servers_response.servers)} enabled MCP server(s) for agent {agent_key}")
                
                # Create MCP tools for each enabled server
                for server in enabled_servers_response.servers:
                    # Get the full server details with API key
                    server_detail = await mcp_service.get_server_by_id(server.id)
                    
                    if not server_detail:
                        logger.warning(f"Could not fetch details for MCP server ID {server.id}")
                        continue
                    
                    logger.info(f"Adding MCP server '{server_detail.name}' (type: {server_detail.server_type}) to agent {agent_key}")
                    
                    # Use the MCP handler to create tools for this server
                    if server_detail.server_type.lower() == 'sse':
                        from src.engines.crewai.tools.mcp_handler import wrap_mcp_tool
                        from crewai_tools import MCPServerAdapter
                        
                        # Get the server URL
                        server_url = server_detail.server_url
                        if not server_url:
                            logger.error(f"Server URL not provided for MCP server {server_detail.name}")
                            continue
                        
                        # Fix the URL for Databricks Apps - ensure it has /sse endpoint
                        if "databricksapps.com" in server_url and not server_url.endswith("/sse"):
                            server_url = server_url.rstrip("/") + "/sse"
                            logger.info(f"Added /sse endpoint to Databricks Apps URL: {server_url}")
                        
                        # Check if this is a Databricks server and use OAuth authentication
                        headers = {}
                        if "databricks.com" in server_url or "databricksapps.com" in server_url:
                            logger.info(f"Detected Databricks server, using OAuth authentication for {server_detail.name}")
                            try:
                                from src.utils.databricks_auth import get_mcp_auth_headers
                                from urllib.parse import urlparse
                                
                                # For Databricks Apps, we need to authenticate against the workspace host
                                if "databricksapps.com" in server_url:
                                    # Use the new authentication system which handles workspace config internally
                                    logger.info(f"Using Databricks Apps authentication for {server_detail.name}")
                                    oauth_headers, error = await get_mcp_auth_headers(server_url)
                                    
                                    if oauth_headers:
                                        headers = oauth_headers
                                        logger.info(f"Successfully authenticated with Databricks Apps server {server_detail.name}")
                                    else:
                                        logger.error(f"Failed to authenticate with Apps server {server_detail.name}: {error}")
                                        # Fall back to API key if available
                                        if server_detail.api_key:
                                            headers["Authorization"] = f"Bearer {server_detail.api_key}"
                                            logger.warning(f"Falling back to API key authentication for {server_detail.name}")
                                else:
                                    # For regular Databricks servers, use the new system
                                    oauth_headers, error = await get_mcp_auth_headers(server_url)
                                    
                                    if oauth_headers:
                                        headers = oauth_headers
                                        logger.info(f"Using MCP authentication for Databricks server {server_detail.name}")
                                    else:
                                        logger.error(f"Failed to get MCP headers for {server_detail.name}: {error}")
                                        # Fall back to API key if available
                                        if server_detail.api_key:
                                            headers["Authorization"] = f"Bearer {server_detail.api_key}"
                                            logger.warning(f"Falling back to API key authentication for {server_detail.name}")
                                    
                            except Exception as e:
                                logger.error(f"Error getting OAuth authentication for {server_detail.name}: {e}")
                                # Fall back to API key if available
                                if server_detail.api_key:
                                    headers["Authorization"] = f"Bearer {server_detail.api_key}"
                                    logger.warning(f"Falling back to API key authentication for {server_detail.name}")
                        else:
                            # For non-Databricks servers, use API key if available
                            if server_detail.api_key:
                                headers["Authorization"] = f"Bearer {server_detail.api_key}"
                        
                        # Create server parameters
                        server_params = {"url": server_url}
                        if headers:
                            server_params["headers"] = headers
                        
                        # Initialize the SSE adapter
                        try:
                            logger.info(f"Creating MCP SSE adapter for server {server_detail.name} at {server_url}")
                            mcp_adapter = MCPServerAdapter(server_params)
                            
                            # Register adapter for cleanup
                            from src.engines.crewai.tools.mcp_handler import register_mcp_adapter
                            adapter_id = f"agent_{agent_key}_server_{server_detail.id}"
                            register_mcp_adapter(adapter_id, mcp_adapter)
                            
                            # Get tools from the adapter
                            tools = mcp_adapter.tools
                            logger.info(f"Got {len(tools)} tools from MCP server '{server_detail.name}'")
                            
                            # Wrap tools for proper event loop handling
                            for tool in tools:
                                wrapped_tool = wrap_mcp_tool(tool)
                                
                                # Add server name to tool name for identification
                                if hasattr(tool, 'name'):
                                    original_name = tool.name
                                    # Avoid duplicate prefixes
                                    if not original_name.startswith(f"{server_detail.name}_"):
                                        tool.name = f"{server_detail.name}_{original_name}"
                                
                                # Add tool to agent tools
                                agent_tools.append(wrapped_tool)
                                logger.info(f"Added MCP tool '{tool.name}' from server '{server_detail.name}' to agent {agent_key}")
                        except Exception as e:
                            logger.error(f"Error creating MCP adapter for server '{server_detail.name}': {str(e)}")
                    
                    elif server_detail.server_type.lower() == 'stdio':
                        from src.engines.crewai.tools.mcp_handler import wrap_mcp_tool
                        from crewai_tools import MCPServerAdapter
                        
                        try:
                            # Get the command and arguments
                            command = server_detail.command
                            args = server_detail.args or []
                            
                            if not command:
                                logger.error(f"Command not provided for MCP server {server_detail.name}")
                                continue
                            
                            # Create environment with API key if available
                            env = {}
                            if server_detail.api_key:
                                env["MCP_API_KEY"] = server_detail.api_key
                            
                            # Add any additional configuration as environment variables
                            if server_detail.additional_config:
                                for key, value in server_detail.additional_config.items():
                                    if isinstance(value, str):
                                        env[f"MCP_{key.upper()}"] = value
                            
                            logger.info(f"Creating MCP STDIO adapter for server {server_detail.name}")
                            from mcp import StdioServerParameters
                            
                            server_params = StdioServerParameters(
                                command=command,
                                args=args,
                                env={**os.environ, **env}
                            )
                            
                            mcp_adapter = MCPServerAdapter(server_params)
                            
                            # Register adapter for cleanup
                            from src.engines.crewai.tools.mcp_handler import register_mcp_adapter
                            adapter_id = f"agent_{agent_key}_server_{server_detail.id}"
                            register_mcp_adapter(adapter_id, mcp_adapter)
                            
                            # Get tools from the adapter
                            tools = mcp_adapter.tools
                            logger.info(f"Got {len(tools)} tools from MCP server '{server_detail.name}'")
                            
                            # Wrap tools for proper event loop handling
                            for tool in tools:
                                wrapped_tool = wrap_mcp_tool(tool)
                                
                                # Add server name to tool name for identification
                                if hasattr(tool, 'name'):
                                    original_name = tool.name
                                    # Avoid duplicate prefixes
                                    if not original_name.startswith(f"{server_detail.name}_"):
                                        tool.name = f"{server_detail.name}_{original_name}"
                                
                                # Add tool to agent tools
                                agent_tools.append(wrapped_tool)
                                logger.info(f"Added MCP tool '{tool.name}' from server '{server_detail.name}' to agent {agent_key}")
                        except Exception as e:
                            logger.error(f"Error creating MCP adapter for server '{server_detail.name}': {str(e)}")
                    
                    else:
                        logger.warning(f"Unsupported MCP server type: {server_detail.server_type}")
                
            else:
                logger.info(f"No enabled MCP servers found for agent {agent_key}")
    except Exception as e:
        logger.error(f"Error fetching MCP servers: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Continue with normal tool resolution
    if tool_service and 'tools' in agent_config and agent_config['tools']:
        logger.info(f"Resolving tool IDs for agent {agent_key}: {agent_config['tools']}")
        try:
            # Resolve tool IDs to names
            tool_names = await resolve_tool_ids_to_names(agent_config['tools'], tool_service)
            logger.info(f"Resolved tool names for agent {agent_key}: {tool_names}")
            
            # Create actual tool instances using the tool factory if available
            if tool_factory:
                for tool_name in tool_names:
                    if not tool_name:
                        continue
                    
                    # Get the tool configuration if available
                    tool_config = {}
                    if hasattr(tool_service, 'get_tool_config_by_name'):
                        tool_config = await tool_service.get_tool_config_by_name(tool_name) or {}
                    
                    # Create the tool instance with result_as_answer from config
                    tool_instance = tool_factory.create_tool(
                        tool_name, 
                        result_as_answer=tool_config.get('result_as_answer', False)
                    )
                    
                    if tool_instance:
                        # Check if this is a special MCP tool that returns a tuple with (is_mcp, tools_list)
                        if isinstance(tool_instance, tuple) and len(tool_instance) == 2 and tool_instance[0] is True:
                            # This is an MCP tool - Add all the individual tools from the list
                            mcp_tools = tool_instance[1]
                            
                            # Special case for mcp_service_adapter - async fetch from service
                            if mcp_tools == 'mcp_service_adapter':
                                # Skip this case since we've removed the service adapter
                                logger.info(f"MCP service adapter requested but not supported anymore")
                                continue
                            elif isinstance(mcp_tools, list):
                                # Regular MCP tools list
                                for mcp_tool in mcp_tools:
                                    agent_tools.append(mcp_tool)
                                logger.info(f"Added {len(mcp_tools)} MCP tools from {tool_name} to agent {agent_key}")
                            else:
                                logger.warning(f"Unexpected MCP tools format: {mcp_tools}")
                        else:
                            # Normal tool
                            agent_tools.append(tool_instance)
                            logger.info(f"Added tool instance {tool_name} to agent {agent_key}")
                    else:
                        logger.warning(f"Could not create tool instance for {tool_name}")
            else:
                # Without tool_factory, just append the tool names (this won't work for CrewAI)
                agent_tools.extend([name for name in tool_names if name])
                logger.warning("No tool_factory provided, using tool names which may not work with CrewAI")
                
        except Exception as e:
            logger.error(f"Error resolving tool IDs for agent {agent_key}: {str(e)}")
    
    # Log tool information
    if agent_tools:
        logger.info(f"Agent {agent_key} will have access to {len(agent_tools)} tools:")
        for tool in agent_tools:
            if isinstance(tool, str):
                logger.info(f"  - Tool name: {tool}")
            else:
                tool_name = getattr(tool, "name", str(tool.__class__.__name__))
                logger.info(f"  - Tool: {tool_name}")
                # Try to get more details about the tool
                tool_details = {}
                if hasattr(tool, "description"):
                    tool_details["description"] = tool.description
                if hasattr(tool, "api_key") and tool.api_key:
                    # Don't log the actual API key, just note that it exists
                    tool_details["has_api_key"] = True
                
                logger.debug(f"  - Tool details: {tool_details}")
    else:
        logger.info(f"Agent {agent_key} will not have any tools")
    
    # Create agent with all available configuration options
    agent_kwargs = {
        'role': agent_config['role'],
        'goal': agent_config['goal'],
        'backstory': agent_config['backstory'],
        'tools': agent_tools or [],
        'llm': llm,
        'verbose': agent_config.get('verbose', True),
        'allow_delegation': agent_config.get('allow_delegation', True),
        'cache': agent_config.get('cache', False),
        'allow_code_execution': agent_config.get('allow_code_execution', False),
        'max_retry_limit': agent_config.get('max_retry_limit', 3),
        'use_system_prompt': True,
        'respect_context_window': True,
    }

    # Add additional agent configuration parameters
    additional_params = [
        'max_iter', 'max_rpm', 'memory', 'code_execution_mode', 
        'knowledge_sources', 'max_context_window_size', 'max_tokens',
        'reasoning', 'max_reasoning_attempts'
    ]
    
    for param in additional_params:
        if param in agent_config and agent_config[param] is not None:
            agent_kwargs[param] = agent_config[param]
            logger.info(f"Setting additional parameter '{param}' to {agent_config[param]} for agent {agent_key}")

    # Handle prompt templates
    if 'system_template' in agent_config and agent_config['system_template']:
        agent_kwargs['system_prompt'] = agent_config['system_template']
    if 'prompt_template' in agent_config and agent_config['prompt_template']:
        agent_kwargs['task_prompt'] = agent_config['prompt_template']
    if 'response_template' in agent_config and agent_config['response_template']:
        agent_kwargs['format_prompt'] = agent_config['response_template']
    
    # Note: Embedder configuration is handled at the Crew level, not Agent level
    # The embedder_config from agents will be used by CrewPreparation to configure the crew
    
    # Create and return the agent
    agent = Agent(**agent_kwargs)
    
    # Explicitly check if the llm attribute was set correctly
    if hasattr(agent, 'llm'):
        logger.info(f"Confirmed agent {agent_key} has llm attribute set to: {agent.llm}")
    else:
        logger.warning(f"Agent {agent_key} does not have llm attribute after creation!")
        
    logger.info(f"Successfully created agent {agent_key} with role '{agent_config['role']}' using model {llm}")
    return agent 