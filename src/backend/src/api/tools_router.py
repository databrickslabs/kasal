"""
API router for tool operations.

This module provides endpoints for managing and interacting with tools.
"""
from typing import Annotated, Dict, Any, List
import logging

from fastapi import APIRouter, Depends, HTTPException, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context, require_admin
from src.schemas.tool import ToolCreate, ToolUpdate, ToolResponse, ToolListResponse, ToggleResponse
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


async def get_tool_service(session: SessionDep) -> ToolService:
    """
    Dependency provider for ToolService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        ToolService instance with session
    """
    return ToolService(session)


# Type alias for cleaner function signatures
ToolServiceDep = Annotated[ToolService, Depends(get_tool_service)]


@router.get("", response_model=List[ToolResponse])
async def get_tools(
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> List[ToolResponse]:
    """
    Get all tools for the current group.

    Uses dependency injection to get ToolService with repository.

    Args:
        service: Injected ToolService instance
        group_context: Group context from headers

    Returns:
        List of tools for the current group
    """
    try:
        tools = await service.get_all_tools_for_group(group_context)
        return tools.tools
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enabled", response_model=ToolListResponse)
async def get_enabled_tools(
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> ToolListResponse:
    """
    Get all enabled tools for the current group.

    Uses dependency injection to get ToolService with repository.

    Args:
        service: Injected ToolService instance
        group_context: Group context from headers
    """
    logger.info("Getting enabled tools")
    tools_response = await service.get_enabled_tools_for_group(group_context)
    logger.info(f"Found {tools_response.count} enabled tools")
    return tools_response



@router.get("/global", response_model=ToolListResponse)
@require_admin()
async def list_global_tools(
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> ToolListResponse:
    """List globally cataloged tools (base tools with no group_id)."""
    all_tools = await service.get_all_tools()
    base_tools = [t for t in all_tools.tools if getattr(t, "group_id", None) is None]
    return ToolListResponse(tools=base_tools, count=len(base_tools))

@router.get("/{tool_id}", response_model=ToolResponse)
async def get_tool_by_id(
    tool_id: int,
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> ToolResponse:
    """
    Get a tool by ID with group isolation.

    Uses dependency injection to get ToolService with repository.

    Args:
        tool_id: ID of the tool to get
        service: Injected ToolService instance
        group_context: Group context from headers
    """
    logger.info(f"Getting tool with ID {tool_id}")
    try:
        tool = await service.get_tool_with_group_check(tool_id, group_context)
        logger.info(f"Found tool with ID {tool_id}")
        return tool
    except HTTPException as e:
        logger.warning(f"Tool retrieval failed: {str(e)}")
        raise


@router.post("/", response_model=ToolResponse, status_code=status.HTTP_201_CREATED)
async def create_tool(
    tool_data: ToolCreate,
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> ToolResponse:
    """
    Create a new tool with group isolation.
    Only Editors and Admins can create tools.

    Uses dependency injection to get ToolService with repository.

    Args:
        tool_data: Tool data for creation
        service: Injected ToolService instance
        group_context: Group context from headers
    """
    # Check permissions - only editors and admins can create tools
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only editors and admins can create tools"
        )

    logger.info(f"Creating tool with title '{tool_data.title}'")
    try:
        tool = await service.create_tool_with_group(tool_data, group_context)
        logger.info(f"Created tool with ID {tool.id}")
        return tool
    except HTTPException as e:
        logger.warning(f"Tool creation failed: {str(e)}")
        raise


@router.put("/{tool_id}", response_model=ToolResponse)
async def update_tool(
    tool_id: int,
    tool_data: ToolUpdate,
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> ToolResponse:
    """
    Update an existing tool with group isolation.
    Only Editors and Admins can update tools.

    Uses dependency injection to get ToolService with repository.

    Args:
        tool_id: ID of the tool to update
        tool_data: Tool data for update
        service: Injected ToolService instance
        group_context: Group context from headers
    """
    # Check permissions - only editors and admins can update tools
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only editors and admins can update tools"
        )

    logger.info(f"Updating tool with ID {tool_id}")
    try:
        tool = await service.update_tool_with_group_check(tool_id, tool_data, group_context)
        logger.info(f"Updated tool with ID {tool_id}")
        return tool
    except HTTPException as e:
        logger.warning(f"Tool update failed: {str(e)}")
        raise


@router.delete("/{tool_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tool(
    tool_id: int,
    session: SessionDep,
    group_context: GroupContextDep = None,
) -> None:
    """
    Delete a tool with group isolation.
    Only Editors and Admins can delete tools.

    Args:
        tool_id: ID of the tool to delete
        db: Database session
        group_context: Group context from headers
    """
    # Check permissions - only editors and admins can delete tools
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only editors and admins can delete tools"
        )

    logger.info(f"Deleting tool with ID {tool_id}")
    try:
        service = ToolService(session)
        await service.delete_tool_with_group_check(tool_id, group_context)
        logger.info(f"Deleted tool with ID {tool_id}")
    except HTTPException as e:
        logger.warning(f"Tool deletion failed: {str(e)}")
        raise


@router.patch("/{tool_id}/toggle-enabled", response_model=ToggleResponse)
async def toggle_tool_enabled(
    tool_id: int,
    session: SessionDep,
    group_context: GroupContextDep = None,
) -> ToggleResponse:
    """
    Toggle the enabled status of a tool with group isolation.

    Args:
        tool_id: ID of the tool to toggle
        db: Database session
        group_context: Group context from headers
    """
    logger.info(f"Toggling enabled status for tool with ID {tool_id}")
    try:
        service = ToolService(session)
        response = await service.toggle_tool_enabled_with_group_check(tool_id, group_context)
        status_text = "enabled" if response.enabled else "disabled"
        logger.info(f"Tool with ID {tool_id} {status_text}")
        return response
    except HTTPException as e:
        logger.warning(f"Tool toggle failed: {str(e)}")
        raise

# Removed enable-all and disable-all endpoints for security reasons
# Individual tool enabling now requires security disclaimer confirmation

@router.get("/configurations/all", response_model=Dict[str, Dict[str, Any]])
async def get_all_tool_configurations(
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Get configurations for all tools for the current group using group-first override.
    """
    logger.info("Getting all tool configurations (group-aware)")
    try:
        configs = await service.get_all_tool_configurations_for_group(group_context)
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
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> Dict[str, Any]:
    """
    Get configuration for a specific tool with group-first fallback to base.
    """
    logger.info(f"Getting configuration for tool: {tool_name}")
    try:
        config = await service.get_tool_configuration_with_group_check(tool_name, group_context)
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
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> Dict[str, Any]:
    """
    Update configuration for a specific tool, scoped to the caller's group.
    Only Admins can configure tools.
    """
    # Enforce admin-only configuration changes
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can configure tools"
        )

    logger.info(f"Updating configuration for tool: {tool_name}")
    try:
        updated = await service.update_tool_configuration_group_scoped(tool_name, config, group_context)
        return updated.config or {}
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
    session: SessionDep,
    group_context: GroupContextDep = None,
) -> Dict[str, Any]:
    """
    Get configuration schema for a specific tool with group isolation.

    Args:
        tool_name: Name of the tool
        db: Database session
        group_context: Group context from headers

    Returns:
        JSON schema dictionary describing the tool's configuration format
    """
    logger.info(f"Getting configuration schema for tool: {tool_name}")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=session,
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
    session: SessionDep,
    group_context: GroupContextDep = None,
) -> Dict[str, Any]:
    """
    Update a tool's configuration in memory without requiring a database entry.
    Only Admins can use this ephemeral override.
    """
    # Enforce admin-only configuration changes
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can configure tools"
        )

    logger.info(f"Updating in-memory configuration for tool: {tool_name}")
    try:
        # Get the CrewAI engine
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            db=session,
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



@router.patch("/{tool_id}/global-availability", response_model=ToolResponse)
@require_admin()
async def set_global_availability(
    tool_id: int,
    payload: Dict[str, Any],
    service: ToolServiceDep,
    group_context: GroupContextDep = None,
) -> ToolResponse:
    """System admin: set global availability (enabled) for a base tool.

    Rejects if the tool is group-scoped (must be a base tool with group_id=None).
    """
    if "enabled" not in payload or not isinstance(payload["enabled"], bool):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'enabled' boolean is required")

    # Ensure the tool exists and is a base (global) tool
    tool = await service.get_tool_by_id(tool_id)
    if getattr(tool, "group_id", None) is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Not a global tool")

    # Update enabled on the base tool
    updated = await service.update_tool(tool_id, ToolUpdate(enabled=bool(payload["enabled"])) )
    return updated
