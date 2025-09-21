from typing import List, Optional, Dict, Any
import logging

from fastapi import HTTPException, status

from src.repositories.tool_repository import ToolRepository
from src.schemas.tool import ToolCreate, ToolUpdate, ToolResponse, ToolListResponse, ToggleResponse
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

class ToolService:
    """
    Service for Tool business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    Uses dependency injection for better testability and modularity.
    """

    def __init__(self, session):
        """
        Initialize service with session.
        Uses dependency injection pattern for clean architecture.

        Args:
            session: Database session from FastAPI DI
        """
        from src.repositories.tool_repository import ToolRepository
        self.repository = ToolRepository(session)

    # Removed factory method - using dependency injection instead
    
    async def get_all_tools(self) -> ToolListResponse:
        """
        Get all tools.
        
        Returns:
            ToolListResponse with list of all tools and count
        """
        tools = await self.repository.list()
        return ToolListResponse(
            tools=[ToolResponse.model_validate(tool) for tool in tools],
            count=len(tools)
        )
    
    async def get_all_tools_for_group(self, group_context: GroupContext) -> ToolListResponse:
        """
        Get all tools for a specific group.
        
        Shows:
        1. Default tools (group_id = null) - visible to everyone
        2. Group-specific tools - visible only to members of that group
        3. If a tool has both default and group versions, the group version takes precedence
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            ToolListResponse with list of tools for the group
        """
        all_tools = await self.repository.list()
        
        # If no group context, show only default tools
        if not group_context or not group_context.group_ids:
            default_tools = [
                tool for tool in all_tools
                if tool.group_id is None
            ]
            return ToolListResponse(
                tools=[ToolResponse.model_validate(tool) for tool in default_tools],
                count=len(default_tools)
            )
        
        # Build a dictionary to handle overrides: tool_title -> tool
        tools_by_title = {}
        
        # First, add all default tools (group_id = null)
        for tool in all_tools:
            if tool.group_id is None:
                tools_by_title[tool.title] = tool
        
        # Then, override with group-specific tools if they exist
        for tool in all_tools:
            if tool.group_id in group_context.group_ids:
                # This will override the default if it exists
                tools_by_title[tool.title] = tool
        
        # Convert back to list
        final_tools = list(tools_by_title.values())
        
        return ToolListResponse(
            tools=[ToolResponse.model_validate(tool) for tool in final_tools],
            count=len(final_tools)
        )
    
    async def get_enabled_tools(self) -> ToolListResponse:
        """
        Get all enabled tools.
        
        Returns:
            ToolListResponse with list of enabled tools and count
        """
        tools = await self.repository.find_enabled()
        return ToolListResponse(
            tools=[ToolResponse.model_validate(tool) for tool in tools],
            count=len(tools)
        )
    
    async def get_enabled_tools_for_group(self, group_context: GroupContext) -> ToolListResponse:
        """
        Get all enabled tools for a specific group.
        
        Shows:
        1. Default enabled tools (group_id = null) - visible to everyone
        2. Group-specific enabled tools - visible only to members of that group
        3. If a tool has both default and group versions, the group version takes precedence
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            ToolListResponse with list of enabled tools for the group
        """
        enabled_tools = await self.repository.find_enabled()
        
        # If no group context, show only default enabled tools
        if not group_context or not group_context.group_ids:
            default_tools = [
                tool for tool in enabled_tools
                if tool.group_id is None
            ]
            return ToolListResponse(
                tools=[ToolResponse.model_validate(tool) for tool in default_tools],
                count=len(default_tools)
            )
        
        # Build a dictionary to handle overrides: tool_title -> tool
        tools_by_title = {}
        
        # First, add all default enabled tools (group_id = null)
        for tool in enabled_tools:
            if tool.group_id is None:
                tools_by_title[tool.title] = tool
        
        # Then, override with group-specific enabled tools if they exist
        for tool in enabled_tools:
            if tool.group_id in group_context.group_ids:
                # This will override the default if it exists
                tools_by_title[tool.title] = tool
        
        # Convert back to list
        final_tools = list(tools_by_title.values())
        
        return ToolListResponse(
            tools=[ToolResponse.model_validate(tool) for tool in final_tools],
            count=len(final_tools)
        )
    
    async def get_tool_by_id(self, tool_id: int) -> ToolResponse:
        """
        Get a tool by ID.
        
        Args:
            tool_id: ID of the tool to retrieve
            
        Returns:
            ToolResponse if found
            
        Raises:
            HTTPException: If tool not found
        """
        tool = await self.repository.get(tool_id)
        if not tool:
            logger.warning(f"Tool with ID {tool_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool with ID {tool_id} not found"
            )
        return ToolResponse.model_validate(tool)
    
    async def get_tool_with_group_check(self, tool_id: int, group_context: GroupContext) -> ToolResponse:
        """
        Get a tool by ID with group verification.
        
        Allows access to:
        1. Default tools (group_id = null) - accessible to everyone
        2. Group-specific tools - accessible only to members of that group
        
        Args:
            tool_id: ID of the tool to retrieve
            group_context: Group context with group IDs
            
        Returns:
            ToolResponse if found and authorized
            
        Raises:
            HTTPException: If tool not found or not authorized
        """
        tool = await self.repository.get(tool_id)
        if not tool:
            logger.warning(f"Tool with ID {tool_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool with ID {tool_id} not found"
            )
        
        # Check group authorization
        # Allow access if:
        # 1. Tool is a default tool (group_id is None)
        # 2. User belongs to the tool's group
        if tool.group_id is not None:  # Only check authorization for non-default tools
            if not group_context or not group_context.group_ids or tool.group_id not in group_context.group_ids:
                logger.warning(f"Tool with ID {tool_id} not authorized for group")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,  # Return 404 not 403 to avoid information leakage
                    detail=f"Tool with ID {tool_id} not found"
                )
        
        return ToolResponse.model_validate(tool)
    
    async def create_tool(self, tool_data: ToolCreate) -> ToolResponse:
        """
        Create a new tool.
        
        Args:
            tool_data: Tool data for creation
            
        Returns:
            ToolResponse of the created tool
            
        Raises:
            HTTPException: If tool creation fails
        """
        try:
            # Create tool
            tool = await self.repository.create(tool_data.model_dump())
            return ToolResponse.model_validate(tool)
        except Exception as e:
            logger.error(f"Failed to create tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create tool: {str(e)}"
            )
    
    async def create_tool_with_group(self, tool_data: ToolCreate, group_context: GroupContext) -> ToolResponse:
        """
        Create a new tool with group assignment.
        
        Args:
            tool_data: Tool data for creation
            group_context: Group context with group IDs
            
        Returns:
            ToolResponse of the created tool
            
        Raises:
            HTTPException: If tool creation fails
        """
        try:
            tool_dict = tool_data.model_dump()
            
            # Add group information
            if group_context and group_context.is_valid():
                tool_dict['group_id'] = group_context.primary_group_id
                tool_dict['created_by_email'] = group_context.group_email
            
            # Create tool
            tool = await self.repository.create(tool_dict)
            return ToolResponse.model_validate(tool)
        except Exception as e:
            logger.error(f"Failed to create tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create tool: {str(e)}"
            )
    
    async def update_tool(self, tool_id: int, tool_data: ToolUpdate) -> ToolResponse:
        """
        Update an existing tool.
        
        Args:
            tool_id: ID of tool to update
            tool_data: Tool data for update
            
        Returns:
            ToolResponse of the updated tool
            
        Raises:
            HTTPException: If tool not found or update fails
        """
        # Check if tool exists
        tool = await self.repository.get(tool_id)
        if not tool:
            logger.warning(f"Tool with ID {tool_id} not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool with ID {tool_id} not found"
            )
        
        try:
            # Update tool
            update_data = tool_data.model_dump(exclude_unset=True)
            updated_tool = await self.repository.update(tool_id, update_data)
            return ToolResponse.model_validate(updated_tool)
        except Exception as e:
            logger.error(f"Failed to update tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update tool: {str(e)}"
            )
    
    async def update_tool_with_group_check(self, tool_id: int, tool_data: ToolUpdate, group_context: GroupContext) -> ToolResponse:
        """
        Update a tool with group verification.
        
        Args:
            tool_id: ID of tool to update
            tool_data: Tool data for update
            group_context: Group context with group IDs
            
        Returns:
            ToolResponse of the updated tool
            
        Raises:
            HTTPException: If tool not found, not authorized, or update fails
        """
        # Check if tool exists and belongs to group
        tool = await self.repository.get(tool_id)
        if not tool:
            logger.warning(f"Tool with ID {tool_id} not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool with ID {tool_id} not found"
            )
        
        # Check group authorization
        if group_context and group_context.group_ids:
            if tool.group_id is not None and tool.group_id not in group_context.group_ids:
                logger.warning(f"Tool with ID {tool_id} not authorized for group")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,  # Return 404 not 403 to avoid information leakage
                    detail=f"Tool with ID {tool_id} not found"
                )
        
        try:
            # Update tool
            update_data = tool_data.model_dump(exclude_unset=True)
            updated_tool = await self.repository.update(tool_id, update_data)
            return ToolResponse.model_validate(updated_tool)
        except Exception as e:
            logger.error(f"Failed to update tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update tool: {str(e)}"
            )
    
    async def delete_tool(self, tool_id: int) -> bool:
        """
        Delete a tool by ID.
        
        Args:
            tool_id: ID of tool to delete
            
        Returns:
            True if deleted successfully
            
        Raises:
            HTTPException: If tool not found or deletion fails
        """
        # Check if tool exists
        tool = await self.repository.get(tool_id)
        if not tool:
            logger.warning(f"Tool with ID {tool_id} not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool with ID {tool_id} not found"
            )
        
        try:
            # Delete tool
            await self.repository.delete(tool_id)
            return True
        except Exception as e:
            logger.error(f"Failed to delete tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete tool: {str(e)}"
            )
    
    async def delete_tool_with_group_check(self, tool_id: int, group_context: GroupContext) -> bool:
        """
        Delete a tool with group verification.
        
        Args:
            tool_id: ID of tool to delete
            group_context: Group context with group IDs
            
        Returns:
            True if deleted successfully
            
        Raises:
            HTTPException: If tool not found, not authorized, or deletion fails
        """
        # Check if tool exists and belongs to group
        tool = await self.repository.get(tool_id)
        if not tool:
            logger.warning(f"Tool with ID {tool_id} not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool with ID {tool_id} not found"
            )
        
        # Check group authorization
        if group_context and group_context.group_ids:
            if tool.group_id is not None and tool.group_id not in group_context.group_ids:
                logger.warning(f"Tool with ID {tool_id} not authorized for group")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,  # Return 404 not 403 to avoid information leakage
                    detail=f"Tool with ID {tool_id} not found"
                )
        
        try:
            # Delete tool
            await self.repository.delete(tool_id)
            return True
        except Exception as e:
            logger.error(f"Failed to delete tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete tool: {str(e)}"
            )
    
    async def toggle_tool_enabled(self, tool_id: int) -> ToggleResponse:
        """
        Toggle the enabled status of a tool.
        
        Args:
            tool_id: ID of tool to toggle
            
        Returns:
            ToggleResponse with message and current enabled state
            
        Raises:
            HTTPException: If tool not found or toggle fails
        """
        try:
            # Toggle tool enabled status using repository
            tool = await self.repository.toggle_enabled(tool_id)
            if not tool:
                logger.warning(f"Tool with ID {tool_id} not found for toggle")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Tool with ID {tool_id} not found"
                )
            
            status_text = "enabled" if tool.enabled else "disabled"
            return ToggleResponse(
                message=f"Tool {status_text} successfully",
                enabled=tool.enabled
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to toggle tool: {str(e)}"
            )
    
    async def toggle_tool_enabled_with_group_check(self, tool_id: int, group_context: GroupContext) -> ToggleResponse:
        """
        Toggle the enabled status of a tool with group verification.
        
        For default tools (group_id = null):
        - Creates a group-specific copy with the toggled state
        - Ensures each group has their own enabled/disabled settings
        
        For group-specific tools:
        - Only the owning group can toggle them
        
        Args:
            tool_id: ID of tool to toggle
            group_context: Group context with group IDs
            
        Returns:
            ToggleResponse with message and current enabled state
            
        Raises:
            HTTPException: If tool not found, not authorized, or toggle fails
        """
        try:
            # First get the tool
            tool = await self.repository.get(tool_id)
            if not tool:
                logger.warning(f"Tool with ID {tool_id} not found for toggle")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Tool with ID {tool_id} not found"
                )
            
            # Must have a valid group context to toggle tools
            if not group_context or not group_context.group_ids:
                logger.warning(f"No group context provided for toggling tool {tool_id}")
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Group context required to toggle tools"
                )
            
            primary_group_id = group_context.primary_group_id
            
            # If it's a default tool (group_id = null), create a group-specific copy
            if tool.group_id is None:
                # Check if a group-specific version already exists
                existing_group_tool = await self.repository.find_by_title_and_group(
                    tool.title, 
                    primary_group_id
                )
                
                if existing_group_tool:
                    # Toggle the existing group-specific tool
                    toggled_tool = await self.repository.toggle_enabled(existing_group_tool.id)
                else:
                    # Create a new group-specific copy with toggled state
                    # Don't include 'id' to let the database auto-generate it
                    tool_data = {
                        'title': tool.title,
                        'description': tool.description,
                        'icon': tool.icon if hasattr(tool, 'icon') else None,
                        'config': tool.config if hasattr(tool, 'config') else {},
                        'enabled': not tool.enabled,  # Toggle the state
                        'group_id': primary_group_id,
                        'created_by_email': group_context.group_email
                    }
                    toggled_tool = await self.repository.create(tool_data)
                
                status_text = "enabled" if toggled_tool.enabled else "disabled"
                return ToggleResponse(
                    message=f"Tool {status_text} successfully for your group",
                    enabled=toggled_tool.enabled
                )
            
            # For group-specific tools, check authorization
            if tool.group_id is not None and tool.group_id not in group_context.group_ids:
                logger.warning(f"Tool with ID {tool_id} not authorized for group")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,  # Return 404 not 403 to avoid information leakage
                    detail=f"Tool with ID {tool_id} not found"
                )
            
            # Toggle the group-specific tool
            toggled_tool = await self.repository.toggle_enabled(tool_id)
            
            status_text = "enabled" if toggled_tool.enabled else "disabled"
            return ToggleResponse(
                message=f"Tool {status_text} successfully",
                enabled=toggled_tool.enabled
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle tool: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to toggle tool: {str(e)}"
            )

    # Removed enable_all_tools and disable_all_tools methods for security reasons
    # Individual tool enabling now requires security disclaimer confirmation

    async def get_tool_config_by_name(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a tool's configuration by its name/title.
        
        Args:
            tool_name: Name/title of the tool
            
        Returns:
            Tool configuration dictionary or None if not found
        """
        try:
            # Get tool by title
            tool = await self.repository.find_by_title(tool_name)
            if not tool:
                logger.warning(f"Tool with name '{tool_name}' not found")
                return None
            
            # Return tool configuration
            return tool.config if hasattr(tool, 'config') else {}
        except Exception as e:
            logger.error(f"Error getting tool config for '{tool_name}': {str(e)}")
            return None

    async def update_tool_configuration_by_title(self, title: str, config: Dict[str, Any]) -> ToolResponse:
        """
        Update configuration for a tool identified by its title.

        Args:
            title: Title of the tool to update
            config: New configuration dictionary

        Returns:
            ToolResponse of the updated tool

        Raises:
            HTTPException: If tool not found or update fails
        """
        try:
            updated_tool = await self.repository.update_configuration_by_title(title, config)
            if not updated_tool:
                logger.warning(f"Tool with title '{title}' not found for configuration update")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Tool with title '{title}' not found"
                )
            return ToolResponse.model_validate(updated_tool)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to update tool configuration by title: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update tool configuration by title: {str(e)}"
            ) 