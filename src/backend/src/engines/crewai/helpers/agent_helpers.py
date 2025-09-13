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

def process_knowledge_sources(knowledge_sources: List[Any]) -> List[Any]:
    """
    Process knowledge sources and return CrewAI-compatible knowledge source objects.
    
    Args:
        knowledge_sources: List of knowledge sources, which can be strings, 
                          dictionaries with metadata, or objects with 'path' property
                          
    Returns:
        List of knowledge source objects compatible with CrewAI
    """
    if not knowledge_sources:
        logger.info("[CREW] No knowledge sources to process")
        return knowledge_sources
    
    logger.info(f"[CREW] Processing {len(knowledge_sources)} knowledge sources: {knowledge_sources}")
    
    # Import knowledge source classes
    from src.engines.crewai.knowledge.databricks_volume_knowledge_source import DatabricksVolumeKnowledgeSource
    
    processed_sources = []
    
    for source in knowledge_sources:
        try:
            # Handle Databricks volume sources
            if isinstance(source, dict) and source.get('type') == 'databricks_volume':
                # Create DatabricksVolumeKnowledgeSource instance
                metadata = source.get('metadata', {})
                source_path = source.get('source', '')  # This is the FULL Databricks path
                logger.info(f"[CREW] Creating DatabricksVolumeKnowledgeSource with full path: {source_path}, metadata: {metadata}")
                
                # The source_path is the complete path: /Volumes/catalog/schema/volume/path/to/file
                # We need to extract the volume path and pass the full file path
                
                if source_path.startswith('/Volumes/'):
                    # Extract catalog.schema.volume from the path
                    path_parts = source_path[9:].split('/')  # Remove /Volumes/ prefix
                    if len(path_parts) >= 3:
                        # First 3 parts are catalog, schema, volume
                        volume_path = '.'.join(path_parts[:3])
                        
                        logger.info(f"[CREW] Extracted volume_path: {volume_path} from full path: {source_path}")
                        
                        # Create the DatabricksVolumeKnowledgeSource
                        # Pass the FULL path in file_paths so it can be downloaded directly
                        databricks_source = DatabricksVolumeKnowledgeSource(
                            volume_path=volume_path,
                            execution_id=metadata.get('execution_id', ''),
                            group_id=metadata.get('group_id', ''),
                            file_paths=[source_path],  # Pass the FULL path here
                            workspace_url=os.environ.get('DATABRICKS_HOST'),
                            token=os.environ.get('DATABRICKS_TOKEN')
                        )
                        processed_sources.append(databricks_source)
                        logger.info(f"[CREW] Created DatabricksVolumeKnowledgeSource for: {source_path}")
                    else:
                        logger.error(f"[CREW] Invalid Databricks volume path format: {source_path}")
                        continue
                else:
                    logger.error(f"[CREW] Path doesn't start with /Volumes/: {source_path}")
                    continue
            
            # Handle string paths (backwards compatibility)
            elif isinstance(source, str):
                processed_sources.append(source)
            
            # Handle dictionaries with 'path' property
            elif isinstance(source, dict) and 'path' in source:
                processed_sources.append(source['path'])
            
            # Handle objects with 'path' property
            elif hasattr(source, 'path'):
                processed_sources.append(source.path)
            
            else:
                logger.warning(f"Unknown knowledge source format: {source}")
                
        except Exception as e:
            logger.error(f"Error processing knowledge source {source}: {str(e)}")
    
    logger.info(f"Processed {len(processed_sources)} knowledge sources")
    return processed_sources


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
        logger.info(f"[CREW] Agent {agent_key} has {len(agent_config.get('knowledge_sources', []))} knowledge sources")
        if agent_config.get('knowledge_sources'):
            logger.info(f"[CREW] Knowledge sources for {agent_key}: {agent_config['knowledge_sources']}")
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
                
                # Check if agent has temperature override
                temperature = None
                if 'temperature' in agent_config and agent_config['temperature'] is not None:
                    # Convert from 0-100 to 0.0-1.0 range
                    temperature = agent_config['temperature'] / 100.0
                    logger.info(f"Using temperature override {temperature} for agent {agent_key}")
                
                llm = await LLMManager.configure_crewai_llm(model_name, temperature)
                logger.info(f"Successfully configured LLM for agent {agent_key} using model: {model_name}")
            elif isinstance(agent_config['llm'], dict):
                # If a dictionary is provided with LLM parameters, use crewai LLM directly
                from crewai import LLM
                
                llm_config = agent_config['llm']
                
                # If a model name is specified, configure it through LLMManager
                if 'model' in llm_config:
                    model_name = llm_config['model']
                    
                    # Check if agent has temperature override
                    temperature = None
                    if 'temperature' in agent_config and agent_config['temperature'] is not None:
                        # Convert from 0-100 to 0.0-1.0 range
                        temperature = agent_config['temperature'] / 100.0
                        logger.info(f"Using temperature override {temperature} for agent {agent_key}")
                    
                    # Get properly configured LLM for the model
                    configured_llm = await LLMManager.configure_crewai_llm(model_name, temperature)
                    
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
    
    # Add MCP tools using the centralized integration module
    try:
        from src.core.unit_of_work import UnitOfWork
        from src.services.mcp_service import MCPService
        from src.engines.crewai.tools.mcp_integration import MCPIntegration
        
        async with UnitOfWork() as uow:
            mcp_service = await MCPService.from_unit_of_work(uow)
            mcp_tools = await MCPIntegration.create_mcp_tools_for_agent(
                agent_config, agent_key, mcp_service
            )
            agent_tools.extend(mcp_tools)
            logger.info(f"Added {len(mcp_tools)} MCP tools to agent {agent_key}")
    except Exception as e:
        logger.error(f"Error adding MCP tools to agent {agent_key}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    
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
                    
                    # Get agent-specific tool config overrides
                    agent_tool_configs = agent_config.get('tool_configs', {})
                    tool_override = agent_tool_configs.get(tool_name, {})
                    
                    # Debug logging for GenieTool spaceId
                    if tool_name == "GenieTool":
                        logger.info(f"Agent {agent_key} - GenieTool agent_tool_configs: {agent_tool_configs}")
                        logger.info(f"Agent {agent_key} - GenieTool tool_override: {tool_override}")
                    
                    # Create the tool instance with result_as_answer from config and overrides
                    tool_instance = tool_factory.create_tool(
                        tool_name, 
                        result_as_answer=tool_config.get('result_as_answer', False),
                        tool_config_override=tool_override
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
        # SECURITY: Always force allow_code_execution to False for safety
        'allow_code_execution': False,  # Hardcoded to False - ignoring agent_config
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
            if param == 'knowledge_sources':
                logger.info(f"[CREW] Setting knowledge_sources for agent {agent_key}: {len(agent_config[param])} sources")
            else:
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
    
    # Store the agent key as a custom attribute using object.__setattr__ to bypass Pydantic validation
    # This allows task_helpers.py to access the agent name properly
    object.__setattr__(agent, '_agent_key', agent_key)
    
    # Explicitly check if the llm attribute was set correctly
    if hasattr(agent, 'llm'):
        logger.info(f"Confirmed agent {agent_key} has llm attribute set to: {agent.llm}")
    else:
        logger.warning(f"Agent {agent_key} does not have llm attribute after creation!")
        
    logger.info(f"Successfully created agent {agent_key} with role '{agent_config['role']}' using model {llm}")
    return agent 