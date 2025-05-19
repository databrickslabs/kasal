"""
Tool helper functions for CrewAI engine.

This module provides helper functions for handling tool-related operations
in the CrewAI engine service.
"""
import logging
from typing import Dict, List, Any, Optional, Union

# Import CrewAI components
from crewai.tools import BaseTool

# Import services
from src.services.tool_service import ToolService

logger = logging.getLogger(__name__)

async def resolve_tool_ids_to_names(tool_ids: List[Union[str, int]], tool_service: ToolService) -> List[str]:
    """
    Resolve tool IDs to their corresponding names using the tool service.
    
    Args:
        tool_ids: List of tool IDs to resolve
        tool_service: Tool service instance
        
    Returns:
        List of tool names (empty strings for IDs that couldn't be resolved)
    """
    tool_names = []
    
    for tool_id in tool_ids:
        try:
            # Convert string ID to integer if needed
            numeric_id = int(tool_id) if isinstance(tool_id, str) else tool_id
            
            # Get tool from service
            tool = await tool_service.get_tool_by_id(numeric_id)
            
            # Add the tool title as the name
            tool_names.append(tool.title)
            logger.info(f"Resolved tool ID {tool_id} to name: {tool.title}")
        except Exception as e:
            logger.error(f"Error resolving tool ID {tool_id}: {str(e)}")
            tool_names.append("")  # Add empty string for unresolved IDs
    
    return tool_names

async def get_tool_instances(tool_names: List[str], tool_registry) -> List[BaseTool]:
    """
    Get tool instances from the tool registry based on tool names.
    
    Args:
        tool_names: List of tool names to get instances for
        tool_registry: Tool registry instance to get tools from
        
    Returns:
        List of actual tool instances
    """
    tools = []
    
    for tool_name in tool_names:
        if not tool_name:
            continue
            
        try:
            # Get tool from registry
            tool = tool_registry.get_tool(tool_name)
            if tool:
                tools.append(tool)
                logger.info(f"Got tool instance for: {tool_name}")
            else:
                logger.warning(f"Tool {tool_name} not found in registry")
        except Exception as e:
            logger.error(f"Error getting tool instance for {tool_name}: {str(e)}")
    
    return tools

async def prepare_tools(
    tool_registry, 
    tool_configs: List[Dict[str, Any]], 
    agent_id: Optional[str] = None,
    task_id: Optional[str] = None
) -> List[BaseTool]:
    """
    Prepare tools for use in agents or tasks
    
    Args:
        tool_registry: The tool registry instance
        tool_configs: List of tool configurations
        agent_id: Optional agent ID requesting the tools
        task_id: Optional task ID requesting the tools
        
    Returns:
        List of instantiated tools
    """
    tools = []
    
    # Create context information for tracking
    context = {}
    if agent_id:
        context["_agent_info"] = {"name": agent_id}
    if task_id:
        context["_task_info"] = {"id": task_id}
    
    for tool_config in tool_configs:
        tool_name = tool_config.get("name")
        tool_params = tool_config.get("parameters", {})
        
        if not tool_name:
            continue
            
        try:
            # Merge context with tool parameters
            tool_params_with_context = {**tool_params, **context}
            
            # Get tool from registry
            tool = tool_registry.get_tool(tool_name, **tool_params_with_context)
            if tool:
                tools.append(tool)
                logger.info(f"Added tool {tool_name} to execution")
            else:
                logger.warning(f"Tool {tool_name} not found in registry")
        except Exception as e:
            logger.error(f"Error preparing tool {tool_name}: {str(e)}")
    
    return tools

async def get_tools_for_agent(
    tool_registry,
    agent_id: str,
    agent_tool_ids: List[str],
    all_tools: List[BaseTool]
) -> List[BaseTool]:
    """
    Get tools for a specific agent

    Args:
        tool_registry: The tool registry instance
        agent_id: ID of the agent
        agent_tool_ids: List of tool IDs assigned to the agent
        all_tools: List of all available tools
        
    Returns:
        List of tools for the agent
    """
    if not agent_tool_ids:
        # No specific tools, use all available tools
        return all_tools
    
    # Agent has specific tools assigned
    agent_tool_configs = []
    for tool_id in agent_tool_ids:
        # Create a tool config entry for each tool
        agent_tool_configs.append({
            "name": tool_id,
            "parameters": {}
        })
    
    # Prepare agent-specific tools with agent context
    return await prepare_tools(
        tool_registry=tool_registry,
        tool_configs=agent_tool_configs,
        agent_id=agent_id,
        task_id=None
    ) 