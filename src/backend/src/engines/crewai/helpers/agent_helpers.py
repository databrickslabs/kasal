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
        # Import LLMManager and handlers
        from src.core.llm_manager import LLMManager
        from src.core.llm_handlers.gpt5_handler import GPT5Handler
        from src.core.llm_handlers.gpt5_llm_wrapper import GPT5CompatibleLLM
        
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
                        llm_kwargs = {}
                        # Copy relevant parameters from configured_llm
                        for attr in ['model', 'api_key', 'api_base', 'temperature', 'max_completion_tokens', 'max_tokens']:
                            if hasattr(configured_llm, attr):
                                value = getattr(configured_llm, attr)
                                if value is not None:
                                    llm_kwargs[attr] = value
                    else:
                        # Fallback if we can't extract params
                        llm_kwargs = {'model': model_name}
                    
                    # Apply any additional parameters from llm_config
                    for key, value in llm_config.items():
                        if value is not None:
                            llm_kwargs[key] = value
                    
                    # Use GPT5Handler to transform parameters if needed
                    model_name = llm_kwargs.get('model', '')
                    if GPT5Handler.is_gpt5_model(model_name):
                        llm_kwargs = GPT5Handler.transform_params(llm_kwargs)
                        logger.info(f"Applied GPT-5 parameter transformations for model: {model_name}")
                        # Use GPT5CompatibleLLM for GPT-5 models
                        llm = GPT5CompatibleLLM(**llm_kwargs)
                    else:
                        # Create the standard LLM for non-GPT-5 models
                        llm = LLM(**llm_kwargs)
                    logger.info(f"Created LLM instance for agent {agent_key} with model {llm_kwargs.get('model')}")
                else:
                    # No model specified, use default with additional parameters
                    logger.warning(f"LLM config missing 'model', using default with additional parameters")
                    default_llm = await LLMManager.configure_crewai_llm("gpt-4o")
                    
                    # Extract and merge parameters
                    llm_kwargs = {}
                    # Copy relevant parameters from default_llm
                    for attr in ['model', 'api_key', 'api_base', 'temperature', 'max_completion_tokens', 'max_tokens']:
                        if hasattr(default_llm, attr):
                            value = getattr(default_llm, attr)
                            if value is not None:
                                llm_kwargs[attr] = value
                    
                    for key, value in llm_config.items():
                        if value is not None:
                            llm_kwargs[key] = value
                    
                    # Use GPT5Handler to transform parameters if needed
                    model_name = llm_kwargs.get('model', '')
                    if GPT5Handler.is_gpt5_model(model_name):
                        llm_kwargs = GPT5Handler.transform_params(llm_kwargs)
                        logger.info(f"Applied GPT-5 parameter transformations for model: {model_name}")
                        # Use GPT5CompatibleLLM for GPT-5 models
                        llm = GPT5CompatibleLLM(**llm_kwargs)
                    else:
                        # Create the standard LLM for non-GPT-5 models
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
        
        logger.info(f"Checking MCP global settings for agent {agent_key}")
        async with UnitOfWork() as uow:
            mcp_service = await MCPService.from_unit_of_work(uow)
            
            # First check if MCP is globally enabled
            mcp_settings = await mcp_service.get_settings()
            if not mcp_settings.global_enabled:
                logger.info(f"MCP is globally disabled, skipping MCP server setup for agent {agent_key}")
            else:
                logger.info(f"MCP is globally enabled, checking for enabled MCP servers for agent {agent_key}")
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
                        
                            # Get the server URL
                            server_url = server_detail.server_url
                            if not server_url:
                                logger.error(f"Server URL not provided for MCP server {server_detail.name}")
                                continue
                        
                            # Fix the URL for Databricks Apps - ensure it has /sse endpoint
                            if "databricksapps.com" in server_url and not server_url.endswith("/sse"):
                                server_url = server_url.rstrip("/") + "/sse"
                                logger.info(f"Added /sse endpoint to Databricks Apps URL: {server_url}")
                        
                            # Get authentication headers based on server configuration
                            headers = {}
                            
                            # Auto-detect Databricks Apps URLs regardless of configured auth_type
                            if 'databricksapps.com' in server_url:
                                auth_type = 'databricks_obo'
                                logger.info(f"Auto-detected Databricks Apps URL for {server_detail.name}, using OAuth authentication")
                                
                                # For Databricks Apps, check if we have authentication configured
                                if server_detail.api_key:
                                    logger.info(f"Using configured API key for OAuth authentication with {server_detail.name}")
                                else:
                                    logger.info(f"No API key configured for {server_detail.name}, will attempt to use environment credentials")
                            else:
                                auth_type = getattr(server_detail, 'auth_type', 'api_key')  # Default to api_key for backward compatibility
                            
                            # For any server with authentication, get appropriate headers
                            if auth_type == 'databricks_obo':
                                # Get OAuth headers for Databricks OBO
                                try:
                                    from src.utils.databricks_auth import get_mcp_auth_headers
                                    
                                    # Get user token from tool_factory if available
                                    user_token = tool_factory.user_token if tool_factory else None
                                    
                                    logger.info(f"Getting Databricks OBO authentication for {server_detail.name}")
                                    oauth_headers, error = await get_mcp_auth_headers(
                                        server_url, 
                                        user_token=user_token,
                                        api_key=server_detail.api_key
                                    )
                                    
                                    if oauth_headers:
                                        headers = oauth_headers
                                        logger.info(f"Successfully obtained authentication headers for {server_detail.name}")
                                    else:
                                        logger.error(f"Failed to get authentication headers for {server_detail.name}: {error}")
                                        
                                except Exception as e:
                                    logger.error(f"Error getting OBO authentication for {server_detail.name}: {e}")
                            else:
                                # For API key authentication
                                if server_detail.api_key:
                                    headers["Authorization"] = f"Bearer {server_detail.api_key}"
                                    logger.info(f"Using API key authentication for {server_detail.name}")
                        
                            # Create server parameters with all configuration
                            server_params = {
                                "url": server_url,
                                "timeout_seconds": server_detail.timeout_seconds,
                                "max_retries": server_detail.max_retries,
                                "rate_limit": server_detail.rate_limit,
                                "auth_type": auth_type  # Pass auth_type to adapter
                            }
                            if headers:
                                server_params["headers"] = headers
                        
                            logger.debug(f"MCP server params for {server_detail.name}: timeout={server_detail.timeout_seconds}s, max_retries={server_detail.max_retries}, rate_limit={server_detail.rate_limit}/min, auth_type={auth_type}")
                        
                            # Initialize the appropriate adapter based on auth type
                            try:
                                logger.info(f"Getting MCP adapter for server {server_detail.name} at {server_url}")
                            
                                # Use connection pooling for MCP adapters
                                from src.engines.crewai.tools.mcp_handler import get_or_create_mcp_adapter
                                adapter_id = f"agent_{agent_key}_server_{server_detail.id}"
                                mcp_adapter = await get_or_create_mcp_adapter(server_params, adapter_id)
                            
                                # Get tools from the adapter
                                tools = mcp_adapter.tools
                                logger.info(f"Got {len(tools)} tools from MCP server '{server_detail.name}'")
                            
                                # Create CrewAI tools from MCP tool dictionaries
                                for tool in tools:
                                    from src.engines.crewai.tools.mcp_handler import create_crewai_tool_from_mcp
                                    wrapped_tool = create_crewai_tool_from_mcp(tool)
                                
                                    # Add server name to tool name for identification
                                    tool_name = tool.get('name', 'unknown') if isinstance(tool, dict) else getattr(tool, 'name', 'unknown')
                                    # Avoid duplicate prefixes
                                    if not tool_name.startswith(f"{server_detail.name}_"):
                                        prefixed_name = f"{server_detail.name}_{tool_name}"
                                        # Update the wrapped tool's name
                                        if hasattr(wrapped_tool, 'name'):
                                            wrapped_tool.name = prefixed_name
                                
                                    # Add tool to agent tools
                                    agent_tools.append(wrapped_tool)
                                    final_tool_name = prefixed_name if not tool_name.startswith(f"{server_detail.name}_") else tool_name
                                    logger.info(f"Added MCP tool '{final_tool_name}' from server '{server_detail.name}' to agent {agent_key}")
                            except Exception as e:
                                import traceback
                                logger.error(f"Error creating MCP adapter for server '{server_detail.name}': {str(e)}")
                                logger.error(f"Traceback: {traceback.format_exc()}")
                    
                        elif server_detail.server_type.lower() == 'streamable':
                        
                            try:
                                # Get the server URL
                                server_url = server_detail.server_url
                                if not server_url:
                                    logger.error(f"Server URL not provided for Streamable server {server_detail.name}")
                                    continue
                            
                                # Prepare headers for Streamable API
                                headers = {
                                    "Accept": "application/json",
                                    "User-Agent": "Kasal-MCP-Client/1.0"
                                }
                                
                                # Auto-detect Databricks Apps URLs regardless of configured auth_type
                                if 'databricksapps.com' in server_url:
                                    auth_type = 'databricks_obo'
                                    logger.info(f"Auto-detected Databricks Apps URL for Streamable server {server_detail.name}, using OAuth authentication")
                                else:
                                    auth_type = getattr(server_detail, 'auth_type', 'api_key')  # Default to api_key for backward compatibility
                                
                                # For any server with authentication, get appropriate headers
                                if auth_type == 'databricks_obo':
                                    # Get OAuth headers for Databricks OBO
                                    try:
                                        from src.utils.databricks_auth import get_mcp_auth_headers
                                        
                                        # Get user token from tool_factory if available
                                        user_token = tool_factory.user_token if tool_factory else None
                                        
                                        logger.info(f"Getting Databricks OBO authentication for Streamable server {server_detail.name}")
                                        oauth_headers, error = await get_mcp_auth_headers(
                                            server_url, 
                                            user_token=user_token,
                                            api_key=server_detail.api_key
                                        )
                                        
                                        if oauth_headers:
                                            headers.update(oauth_headers)
                                            logger.info(f"Successfully obtained authentication headers for Streamable server {server_detail.name}")
                                        else:
                                            logger.error(f"Failed to get authentication headers for Streamable server {server_detail.name}: {error}")
                                            
                                    except Exception as e:
                                        logger.error(f"Error getting OBO authentication for Streamable server {server_detail.name}: {e}")
                                else:
                                    # For API key authentication
                                    if server_detail.api_key:
                                        headers["Authorization"] = f"Bearer {server_detail.api_key}"
                                        logger.info(f"Using API key authentication for Streamable server {server_detail.name}")
                            
                                # Create server parameters with all configuration
                                server_params = {
                                    "url": server_url,
                                    "timeout_seconds": server_detail.timeout_seconds,
                                    "max_retries": server_detail.max_retries,
                                    "rate_limit": server_detail.rate_limit,
                                    "auth_type": auth_type,  # Pass auth_type to adapter
                                    "headers": headers
                                }
                            
                                # Add any additional configuration
                                if server_detail.additional_config:
                                    server_params["additional_config"] = server_detail.additional_config
                            
                                logger.info(f"Creating Streamable adapter for server {server_detail.name} at {server_url}")
                                logger.debug(f"Streamable server params: timeout={server_detail.timeout_seconds}s, max_retries={server_detail.max_retries}, rate_limit={server_detail.rate_limit}/min, auth_type={auth_type}")
                            
                                # For streamable servers, use connection pooling
                                from src.engines.crewai.tools.mcp_handler import get_or_create_mcp_adapter
                                adapter_id = f"agent_{agent_key}_server_{server_detail.id}"
                                mcp_adapter = await get_or_create_mcp_adapter(server_params, adapter_id)
                            
                                # Get tools from the adapter
                                tools = mcp_adapter.tools
                                logger.info(f"Got {len(tools)} tools from Streamable server '{server_detail.name}'")
                            
                                # Create CrewAI tools from MCP tool dictionaries
                                for tool in tools:
                                    from src.engines.crewai.tools.mcp_handler import create_crewai_tool_from_mcp
                                    wrapped_tool = create_crewai_tool_from_mcp(tool)
                                
                                    # Add server name to tool name for identification
                                    tool_name = tool.get('name', 'unknown') if isinstance(tool, dict) else getattr(tool, 'name', 'unknown')
                                    # Avoid duplicate prefixes
                                    if not tool_name.startswith(f"{server_detail.name}_"):
                                        prefixed_name = f"{server_detail.name}_{tool_name}"
                                        # Update the wrapped tool's name
                                        if hasattr(wrapped_tool, 'name'):
                                            wrapped_tool.name = prefixed_name
                                
                                    # Add tool to agent tools
                                    agent_tools.append(wrapped_tool)
                                    final_tool_name = prefixed_name if not tool_name.startswith(f"{server_detail.name}_") else tool_name
                                    logger.info(f"Added Streamable tool '{final_tool_name}' from server '{server_detail.name}' to agent {agent_key}")
                            except Exception as e:
                                import traceback
                                logger.error(f"Error creating Streamable adapter for server '{server_detail.name}': {str(e)}")
                                logger.error(f"Traceback: {traceback.format_exc()}")
                    
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