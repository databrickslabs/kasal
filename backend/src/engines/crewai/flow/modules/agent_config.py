"""
Agent configuration module for CrewAI flow execution.

This module handles the configuration of agents for CrewAI flows.
"""
import logging
import json
from typing import Dict, List, Optional, Any, Union
from crewai import Agent

from src.core.logger import LoggerManager
from src.engines.crewai.tools.tool_factory import ToolFactory

# Initialize logger
logger = LoggerManager.get_instance().crew

class AgentConfig:
    """
    Helper class for configuring agents in CrewAI flows.
    """
    
    @staticmethod
    async def configure_agent_and_tools(agent_data, flow_data=None, repositories=None):
        """
        Configure an agent with its associated tools.
        
        Args:
            agent_data: Agent data from the database
            flow_data: Flow data for context (optional)
            repositories: Dictionary of repositories (optional)
            
        Returns:
            Agent: A properly configured CrewAI Agent instance
        """
        if not agent_data:
            logger.warning("No agent data provided for configuration")
            return None
            
        try:
            logger.info(f"Configuring agent: {agent_data.name}")
            
            # Initialize tools list
            tools = []
            
            # Initialize the ToolFactory
            tool_factory = ToolFactory({})
            try:
                await tool_factory.initialize()
            except Exception as e:
                logger.warning(f"Error initializing ToolFactory: {e}")
            
            # First check for tools directly on the agent_data object
            if hasattr(agent_data, 'tools') and agent_data.tools:
                agent_tools = AgentConfig._normalize_tools_list(agent_data.tools)
                logger.info(f"Found tools on agent {agent_data.name}: {agent_tools}")
                
                # Create tools from IDs
                tools = await AgentConfig._create_tools_from_ids(agent_tools, tool_factory, f"agent {agent_data.name}")
            
            # Check if we need to look for tools in flow nodes
            if len(tools) == 0 and flow_data and hasattr(flow_data, 'nodes'):
                logger.info(f"No tools found directly on agent, checking flow nodes for agent {agent_data.name}")
                tools = await AgentConfig._get_tools_from_flow_nodes(agent_data, flow_data, tool_factory)
            
            # Get LLM for the agent
            llm = await AgentConfig._get_agent_llm(agent_data)
            
            # Create the agent with all configurations
            agent_kwargs = AgentConfig._prepare_agent_kwargs(agent_data, tools, llm)
            
            # Create and return the configured agent
            from crewai import Agent
            agent = Agent(**agent_kwargs)
            logger.info(f"Successfully configured agent: {agent_data.name} with {len(tools)} tools")
            
            # We no longer add default tools - respect the agent configuration
            # If an agent has no tools assigned, we don't add any by default
                    
            return agent
            
        except Exception as e:
            logger.error(f"Error configuring agent {getattr(agent_data, 'name', 'unknown')}: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _normalize_tools_list(tools_data):
        """Convert tools data to a normalized list of tool IDs"""
        agent_tools = []
        
        if isinstance(tools_data, list):
            agent_tools = [str(tool_id) for tool_id in tools_data]
        elif isinstance(tools_data, str):
            # Try to convert to list if it's a string (e.g., JSON)
            try:
                agent_tools = [str(tool_id) for tool_id in json.loads(tools_data)]
            except Exception as e:
                logger.error(f"Error parsing tools string: {e}")
        
        return agent_tools
    
    @staticmethod
    async def _create_tools_from_ids(tool_ids, tool_factory, owner_desc):
        """Create tool instances from tool IDs using ToolFactory"""
        tools = []
        
        for tool_id in tool_ids:
            try:
                # Try to create the tool using the factory
                tool = tool_factory.create_tool(tool_id)
                if tool:
                    tools.append(tool)
                    logger.info(f"Added tool with ID {tool_id} for {owner_desc}")
                else:
                    logger.warning(f"Tool factory couldn't create tool with ID: {tool_id} for {owner_desc}")
            except Exception as tool_error:
                logger.warning(f"Error creating tool {tool_id}: {tool_error}")
        
        # No longer adding default tools if none were explicitly assigned
        if len(tools) == 0:
            logger.info(f"No valid tools could be created for {owner_desc}, not adding any default tools")
        
        return tools
    
    @staticmethod
    async def _get_tools_from_flow_nodes(agent_data, flow_data, tool_factory):
        """Extract tools from flow nodes for a specific agent"""
        tools = []
        
        try:
            nodes = flow_data.nodes
            # Check if nodes is a string and try to parse it
            if isinstance(nodes, str):
                nodes = json.loads(nodes)
            
            # Get the agent ID to look for
            agent_id = str(getattr(agent_data, 'id', ''))
            
            if agent_id:
                # Find the agent node
                agent_node_id = f"agent-{agent_id}"
                for node in nodes:
                    if node.get('id') == agent_node_id and 'data' in node:
                        node_data = node.get('data', {})
                        node_tools = node_data.get('tools', [])
                        
                        if node_tools:
                            logger.info(f"Found tools in node data for agent {agent_id}: {node_tools}")
                            tools = await AgentConfig._create_tools_from_ids(
                                node_tools, 
                                tool_factory, 
                                f"agent {agent_data.name} (from node)"
                            )
                        break
        except Exception as e:
            logger.error(f"Error looking for tools in flow nodes: {e}", exc_info=True)
        
        return tools
    
    @staticmethod
    async def _get_agent_llm(agent_data):
        """Get an LLM instance for the agent"""
        llm = None
        
        try:
            # Check if agent has a specific LLM configuration
            if hasattr(agent_data, 'llm') and agent_data.llm:
                agent_llm = agent_data.llm
                # Import LLMManager to get proper LLM configuration
                try:
                    from src.core.llm_manager import LLMManager
                    if isinstance(agent_llm, str):
                        # Just a model name
                        llm = await LLMManager.get_llm(agent_llm)
                    elif isinstance(agent_llm, dict):
                        # Dictionary with LLM configuration
                        model_name = agent_llm.get('model', 'databricks-llama-4-maverick')
                        llm = await LLMManager.get_llm(model_name)
                    logger.info(f"Configured custom LLM for agent: {agent_data.name}")
                except ImportError:
                    logger.warning("Could not import LLMManager, using default model string")
                    llm = agent_llm if isinstance(agent_llm, str) else agent_llm.get('model', 'databricks-llama-4-maverick')
                    logger.info(f"Using model name '{llm}' for agent: {agent_data.name}")
            else:
                # Use default LLM
                try:
                    from src.core.llm_manager import LLMManager
                    model_name = getattr(agent_data, 'model', 'databricks-llama-4-maverick')
                    if not isinstance(model_name, str):
                        model_name = 'databricks-llama-4-maverick'
                    llm = await LLMManager.get_llm(model_name)
                    logger.info(f"Using default LLM from LLMManager for agent: {agent_data.name}")
                except ImportError:
                    logger.warning("Could not import LLMManager, using default model string")
                    model_name = getattr(agent_data, 'model', 'gpt-4o')
                    if not isinstance(model_name, str):
                        model_name = 'gpt-4o'
                    llm = model_name
                    logger.info(f"Using model name '{model_name}' for agent: {agent_data.name}")
        except Exception as e:
            logger.error(f"Error configuring LLM for agent {agent_data.name}: {e}", exc_info=True)
            # Continue without LLM, it will use environment defaults
        
        return llm
    
    @staticmethod
    def _prepare_agent_kwargs(agent_data, tools, llm):
        """Prepare kwargs dictionary for creating an agent"""
        # Start with required and default fields
        agent_kwargs = {
            "role": agent_data.role,
            "goal": agent_data.goal,
            "backstory": agent_data.backstory,
            "verbose": True,
            "allow_delegation": getattr(agent_data, 'allow_delegation', True),
            "config": {}  # Always ensure there's an empty dict for config
        }
        
        # Add tools if available - directly assign instead of modifying config
        if tools:
            agent_kwargs["tools"] = tools
            
        # Add LLM if available
        if llm:
            agent_kwargs["llm"] = llm
            
        # Add additional properties if they exist
        for prop in ['memory', 'max_iter', 'max_rpm']:
            if hasattr(agent_data, prop) and getattr(agent_data, prop) is not None:
                agent_kwargs[prop] = getattr(agent_data, prop)
        
        # Handle config as the last step to avoid validation issues
        if hasattr(agent_data, 'config') and agent_data.config is not None:
            try:
                if isinstance(agent_data.config, dict):
                    # If it's already a dict, use it directly
                    agent_kwargs["config"] = agent_data.config
                elif isinstance(agent_data.config, str) and agent_data.config.strip():
                    # Try to parse JSON string
                    try:
                        parsed_config = json.loads(agent_data.config)
                        if isinstance(parsed_config, dict):
                            agent_kwargs["config"] = parsed_config
                        else:
                            logger.warning(f"Parsed config is not a dictionary for agent: {agent_data.name}")
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse config string for agent {agent_data.name}")
            except Exception as e:
                logger.warning(f"Error processing config for agent {agent_data.name}: {e}")
                # Keep the default empty dict
        
        return agent_kwargs 