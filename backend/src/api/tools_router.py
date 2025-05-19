"""
API router for tool operations.

This module provides endpoints for managing and interacting with tools.
"""
from typing import Annotated, Dict, Any, List
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.tool import ToolCreate, ToolUpdate, ToolResponse, ToolListResponse, ToggleResponse
from src.db.session import get_db
from src.services.tool_service import ToolService
from src.engines.factory import EngineFactory

# Create router instance
router = APIRouter(
    prefix="/tools",
    tags=["tools"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)


@router.get("", response_model=List[ToolResponse])
async def get_tools(
    db: AsyncSession = Depends(get_db)
) -> List[ToolResponse]:
    """
    Get all tools.
    
    Returns:
        List of tools
    """
    try:
        service = ToolService(db)
        tools = await service.get_all_tools()
        return tools.tools
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enabled", response_model=ToolListResponse)
async def get_enabled_tools(
    db: AsyncSession = Depends(get_db)
) -> ToolListResponse:
    """
    Get all enabled tools.
    """
    logger.info("Getting enabled tools")
    service = ToolService(db)
    tools_response = await service.get_enabled_tools()
    logger.info(f"Found {tools_response.count} enabled tools")
    return tools_response


@router.get("/{tool_id}", response_model=ToolResponse)
async def get_tool_by_id(
    tool_id: int,
    db: AsyncSession = Depends(get_db)
) -> ToolResponse:
    """
    Get a tool by ID.
    """
    logger.info(f"Getting tool with ID {tool_id}")
    try:
        service = ToolService(db)
        tool = await service.get_tool_by_id(tool_id)
        logger.info(f"Found tool with ID {tool_id}")
        return tool
    except HTTPException as e:
        logger.warning(f"Tool retrieval failed: {str(e)}")
        raise


@router.post("/", response_model=ToolResponse, status_code=status.HTTP_201_CREATED)
async def create_tool(
    tool_data: ToolCreate,
    db: AsyncSession = Depends(get_db)
) -> ToolResponse:
    """
    Create a new tool.
    """
    logger.info(f"Creating tool with title '{tool_data.title}'")
    try:
        service = ToolService(db)
        tool = await service.create_tool(tool_data)
        logger.info(f"Created tool with ID {tool.id}")
        return tool
    except HTTPException as e:
        logger.warning(f"Tool creation failed: {str(e)}")
        raise


@router.put("/{tool_id}", response_model=ToolResponse)
async def update_tool(
    tool_id: int,
    tool_data: ToolUpdate,
    db: AsyncSession = Depends(get_db)
) -> ToolResponse:
    """
    Update an existing tool.
    """
    logger.info(f"Updating tool with ID {tool_id}")
    try:
        service = ToolService(db)
        tool = await service.update_tool(tool_id, tool_data)
        logger.info(f"Updated tool with ID {tool_id}")
        return tool
    except HTTPException as e:
        logger.warning(f"Tool update failed: {str(e)}")
        raise


@router.delete("/{tool_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tool(
    tool_id: int,
    db: AsyncSession = Depends(get_db)
) -> None:
    """
    Delete a tool.
    """
    logger.info(f"Deleting tool with ID {tool_id}")
    try:
        service = ToolService(db)
        await service.delete_tool(tool_id)
        logger.info(f"Deleted tool with ID {tool_id}")
    except HTTPException as e:
        logger.warning(f"Tool deletion failed: {str(e)}")
        raise


@router.patch("/{tool_id}/toggle-enabled", response_model=ToggleResponse)
async def toggle_tool_enabled(
    tool_id: int,
    db: AsyncSession = Depends(get_db)
) -> ToggleResponse:
    """
    Toggle the enabled status of a tool.
    """
    logger.info(f"Toggling enabled status for tool with ID {tool_id}")
    try:
        service = ToolService(db)
        response = await service.toggle_tool_enabled(tool_id)
        status_text = "enabled" if response.enabled else "disabled"
        logger.info(f"Tool with ID {tool_id} {status_text}")
        return response
    except HTTPException as e:
        logger.warning(f"Tool toggle failed: {str(e)}")
        raise

@router.patch("/enable-all", response_model=List[ToolResponse])
async def enable_all_tools(
    db: AsyncSession = Depends(get_db)
) -> List[ToolResponse]:
    """
    Enable all tools.
    """
    logger.info("Enabling all tools")
    try:
        service = ToolService(db)
        tools = await service.enable_all_tools()
        logger.info(f"Enabled {len(tools)} tools")
        return tools
    except HTTPException as e:
        logger.warning(f"Enable all tools failed: {str(e)}")
        raise

@router.patch("/disable-all", response_model=List[ToolResponse])
async def disable_all_tools(
    db: AsyncSession = Depends(get_db)
) -> List[ToolResponse]:
    """
    Disable all tools.
    """
    logger.info("Disabling all tools")
    try:
        service = ToolService(db)
        tools = await service.disable_all_tools()
        logger.info(f"Disabled {len(tools)} tools")
        return tools
    except HTTPException as e:
        logger.warning(f"Disable all tools failed: {str(e)}")
        raise

@router.get("/configurations/all", response_model=Dict[str, Dict[str, Any]])
async def get_all_tool_configurations(
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Dict[str, Any]]:
    """
    Get configurations for all tools.
    
    Returns:
        Dictionary mapping tool names to their configurations
    """
    logger.info("Getting all tool configurations")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=db,
            initialize=True
        )
        
        # Get tool registry from engine
        tool_registry = engine.tool_registry
        
        # Get all configurations
        configs = tool_registry.get_all_tool_configurations()
        logger.info(f"Retrieved configurations for {len(configs)} tools")
        return configs
    except Exception as e:
        logger.error(f"Error getting tool configurations: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tool configurations: {str(e)}"
        )

@router.get("/configurations/{tool_name}", response_model=Dict[str, Any])
async def get_tool_configuration(
    tool_name: str,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get configuration for a specific tool.
    
    Args:
        tool_name: Name of the tool
        
    Returns:
        Tool configuration dictionary
    """
    logger.info(f"Getting configuration for tool: {tool_name}")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=db,
            initialize=True
        )
        
        # Get tool registry from engine
        tool_registry = engine.tool_registry
        
        # Get configuration
        config = tool_registry.get_tool_configuration(tool_name)
        if not config:
            # Return empty config if none exists
            logger.info(f"No configuration found for tool: {tool_name}")
            return {}
            
        logger.info(f"Retrieved configuration for tool: {tool_name}")
        return config
    except Exception as e:
        logger.error(f"Error getting tool configuration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tool configuration: {str(e)}"
        )

@router.put("/configurations/{tool_name}", response_model=Dict[str, Any])
async def update_tool_configuration(
    tool_name: str,
    config: Dict[str, Any],
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Update configuration for a specific tool.
    
    Args:
        tool_name: Name of the tool
        config: New configuration dictionary
        
    Returns:
        Updated tool configuration dictionary
    """
    logger.info(f"Updating configuration for tool: {tool_name}")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=db,
            initialize=True
        )
        
        # Get tool registry from engine
        tool_registry = engine.tool_registry
        
        # Update configuration
        success = await tool_registry.update_tool_configuration(tool_name, config)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tool {tool_name} not found or configuration update failed"
            )
            
        # Get updated configuration
        updated_config = tool_registry.get_tool_configuration(tool_name)
        logger.info(f"Updated configuration for tool: {tool_name}")
        return updated_config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating tool configuration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating tool configuration: {str(e)}"
        )

@router.get("/configurations/{tool_name}/schema", response_model=Dict[str, Any])
async def get_tool_configuration_schema(
    tool_name: str,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get configuration schema for a specific tool.
    
    Args:
        tool_name: Name of the tool
        
    Returns:
        JSON schema dictionary describing the tool's configuration format
    """
    logger.info(f"Getting configuration schema for tool: {tool_name}")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=db,
            initialize=True
        )
        
        # Get tool registry from engine
        tool_registry = engine.tool_registry
        
        # Get schema
        schema = tool_registry.get_tool_configuration_schema(tool_name)
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema for tool {tool_name} not found"
            )
            
        logger.info(f"Retrieved configuration schema for tool: {tool_name}")
        return schema
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting tool configuration schema: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tool configuration schema: {str(e)}"
        )

@router.patch("/configurations/{tool_name}/in-memory", response_model=Dict[str, Any])
async def update_tool_configuration_in_memory(
    tool_name: str,
    config: Dict[str, Any],
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Update a tool's configuration in memory without requiring a database entry.
    
    This is useful for runtime configuration of tools that don't have a database record.
    The changes won't be persisted after server restart.
    
    Args:
        tool_name: Name of the tool
        config: New configuration dictionary
        
    Returns:
        Updated tool configuration dictionary
    """
    logger.info(f"Updating in-memory configuration for tool: {tool_name}")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=db,
            initialize=True
        )
        
        # Get tool registry from engine
        tool_registry = engine.tool_registry
        
        # Update configuration in memory
        success = tool_registry.update_tool_configuration_in_memory(tool_name, config)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update in-memory configuration for tool {tool_name}"
            )
            
        # Get updated configuration
        updated_config = tool_registry.get_tool_configuration(tool_name)
        logger.info(f"Updated in-memory configuration for tool: {tool_name}")
        return updated_config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating in-memory tool configuration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating in-memory tool configuration: {str(e)}"
        ) 