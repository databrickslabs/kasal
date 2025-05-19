from typing import List, Optional, Dict, Any
import logging

from fastapi import HTTPException, status

from src.repositories.tool_repository import ToolRepository
from src.schemas.tool import ToolCreate, ToolUpdate, ToolResponse, ToolListResponse, ToggleResponse

logger = logging.getLogger(__name__)

class ToolService:
    """
    Service for Tool business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    """
    
    def __init__(self, session=None, repository=None):
        """
        Initialize service with database session or repository.
        
        Args:
            session: SQLAlchemy async session (for backwards compatibility)
            repository: ToolRepository instance (preferred way)
        """
        if repository is not None:
            self.repository = repository
        elif session is not None:
            self.repository = ToolRepository(session)
        else:
            raise ValueError("Either session or repository must be provided")
    
    @classmethod
    async def from_unit_of_work(cls, uow):
        """
        Create a service instance from a UnitOfWork.
        
        Args:
            uow: UnitOfWork instance
            
        Returns:
            ToolService: Service instance using the UnitOfWork's repository
        """
        return cls(repository=uow.tool_repository)
    
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

    async def enable_all_tools(self) -> List[ToolResponse]:
        """
        Enable all tools.
        
        Returns:
            List of all tools with their updated enabled status
            
        Raises:
            HTTPException: If operation fails
        """
        try:
            # Enable all tools using repository
            tools = await self.repository.enable_all()
            
            # Return updated tools
            return [ToolResponse.model_validate(tool) for tool in tools]
        except Exception as e:
            logger.error(f"Failed to enable all tools: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to enable all tools: {str(e)}"
            )

    async def disable_all_tools(self) -> List[ToolResponse]:
        """
        Disable all tools.
        
        Returns:
            List of all tools with their updated enabled status
            
        Raises:
            HTTPException: If operation fails
        """
        try:
            # Disable all tools using repository
            tools = await self.repository.disable_all()
            
            # Return updated tools
            return [ToolResponse.model_validate(tool) for tool in tools]
        except Exception as e:
            logger.error(f"Failed to disable all tools: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to disable all tools: {str(e)}"
            )

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