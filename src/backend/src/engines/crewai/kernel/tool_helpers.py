"""
Tool helper functions for CrewAI engine.

This module provides helper functions for handling tool-related operations
in the CrewAI engine service.
"""
import logging
from typing import List, Union

# Import services
from src.services.tool_service import ToolService

logger = logging.getLogger(__name__)

async def resolve_tool_ids_to_names(tool_ids: List[Union[str, int]], tool_service: ToolService) -> List[str]:
    """
    Resolve tool IDs to their corresponding names using the tool service.
    Handles both numeric IDs and tool names.

    Args:
        tool_ids: List of tool IDs or tool names to resolve
        tool_service: Tool service instance

    Returns:
        List of tool names (empty strings for IDs that couldn't be resolved)
    """
    tool_names = []

    for tool_id in tool_ids:
        try:
            # Special handling for known custom tools that are not in the database
            if isinstance(tool_id, str) and tool_id == "DatabricksKnowledgeSearchTool":
                # This is a custom tool name, pass it through directly
                tool_names.append(tool_id)
                logger.info(f"Passing through custom tool name: {tool_id}")
                continue

            # Try to convert string ID to integer
            if isinstance(tool_id, str):
                try:
                    numeric_id = int(tool_id)
                except ValueError:
                    # Not a numeric ID - for invalid strings, add empty string
                    logger.error(f"Error resolving tool ID {tool_id}: Invalid numeric format")
                    tool_names.append("")
                    continue
            else:
                numeric_id = tool_id

            # Get tool from service by ID
            tool = await tool_service.get_tool_by_id(numeric_id)

            # Add the tool title as the name
            tool_names.append(tool.title)
            logger.info(f"Resolved tool ID {tool_id} to name: {tool.title}")
        except Exception as e:
            logger.error(f"Error resolving tool ID {tool_id}: {str(e)}")
            tool_names.append("")  # Add empty string for unresolved IDs

    return tool_names

