"""
API router for MCP server operations.

This module provides endpoints for managing MCP (Model Context Protocol) servers.
"""
from typing import Annotated, Dict, Any, List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, status

from src.schemas.mcp import (
    MCPServerCreate,
    MCPServerUpdate,
    MCPServerResponse,
    MCPServerListResponse,
    MCPToggleResponse,
    MCPTestConnectionRequest,
    MCPTestConnectionResponse,
    MCPSettingsResponse,
    MCPSettingsUpdate
)
from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context
from src.services.mcp_service import MCPService

# Create router instance
router = APIRouter(
    prefix="/mcp",
    tags=["mcp"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)

async def get_mcp_service(session: SessionDep) -> MCPService:
    """
    Dependency provider for MCPService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        MCPService instance with injected session
    """
    return MCPService(session=session)

# Type alias for cleaner function signatures
MCPServiceDep = Annotated[MCPService, Depends(get_mcp_service)]


@router.get("/servers", response_model=MCPServerListResponse)
async def get_mcp_servers(
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get all MCP servers.
    
    Returns:
        List of MCP servers and count
    """
    try:
        return await service.get_all_servers()
    except Exception as e:
        logger.error(f"Error getting MCP servers: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/servers/enabled", response_model=MCPServerListResponse)
async def get_enabled_mcp_servers(
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get all enabled MCP servers.
    """
    logger.info("Getting enabled MCP servers")
    servers_response = await service.get_enabled_servers()
    logger.info(f"Found {servers_response.count} enabled MCP servers")
    return servers_response


@router.get("/servers/global", response_model=MCPServerListResponse)
async def get_global_mcp_servers(
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get all globally enabled MCP servers.
    """
    logger.info("Getting globally enabled MCP servers")
    servers_response = await service.get_global_servers()
    logger.info(f"Found {servers_response.count} globally enabled MCP servers")
    return servers_response


@router.get("/servers/{server_id}", response_model=MCPServerResponse)
async def get_mcp_server(
    server_id: int,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPServerResponse:
    """
    Get an MCP server by ID.
    """
    logger.info(f"Getting MCP server with ID {server_id}")
    try:
        server = await service.get_server_by_id(server_id)
        logger.info(f"Found MCP server with ID {server_id}")
        return server
    except HTTPException:
        raise


@router.post("/servers", response_model=MCPServerResponse, status_code=status.HTTP_201_CREATED)
async def create_mcp_server(
    server_data: MCPServerCreate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPServerResponse:
    """
    Create a new MCP server.
    Only Admins can create MCP servers.
    """
    # Check permissions - only admins can create MCP servers
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can create MCP servers"
        )

    logger.info(f"Creating MCP server with name '{server_data.name}'")
    try:
        server = await service.create_server(server_data)
        logger.info(f"Created MCP server with ID {server.id}")
        return server
    except HTTPException:
        raise


@router.put("/servers/{server_id}", response_model=MCPServerResponse)
async def update_mcp_server(
    server_id: int,
    server_data: MCPServerUpdate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPServerResponse:
    """
    Update an existing MCP server.
    Only Admins can update MCP servers.
    """
    # Check permissions - only admins can update MCP servers
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can update MCP servers"
        )

    logger.info(f"Updating MCP server with ID {server_id}")
    try:
        server = await service.update_server(server_id, server_data)
        logger.info(f"Updated MCP server with ID {server_id}")
        return server
    except HTTPException:
        raise


@router.delete("/servers/{server_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_mcp_server(
    server_id: int,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> None:
    """
    Delete an MCP server.
    Only Admins can delete MCP servers.
    """
    # Check permissions - only admins can delete MCP servers
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can delete MCP servers"
        )

    logger.info(f"Deleting MCP server with ID {server_id}")
    try:
        await service.delete_server(server_id)
        logger.info(f"Deleted MCP server with ID {server_id}")
    except HTTPException:
        raise


@router.patch("/servers/{server_id}/toggle-enabled", response_model=MCPToggleResponse)
async def toggle_mcp_server_enabled(
    server_id: int,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPToggleResponse:
    """
    Toggle the enabled status of an MCP server.
    Only Admins can toggle MCP server status.
    """
    # Check permissions - only admins can toggle MCP servers
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can toggle MCP server status"
        )

    logger.info(f"Toggling enabled status for MCP server with ID {server_id}")
    try:
        response = await service.toggle_server_enabled(server_id)
        status_text = "enabled" if response.enabled else "disabled"
        logger.info(f"MCP server with ID {server_id} {status_text}")
        return response
    except HTTPException:
        raise


@router.patch("/servers/{server_id}/toggle-global-enabled", response_model=MCPToggleResponse)
async def toggle_mcp_server_global_enabled(
    server_id: int,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPToggleResponse:
    """
    Toggle the global enabled status of an MCP server.
    Only Admins can toggle global MCP server status.
    """
    # Check permissions - only admins can toggle global MCP server status
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can toggle global MCP server status"
        )

    logger.info(f"Toggling global enabled status for MCP server with ID {server_id}")
    try:
        response = await service.toggle_server_global_enabled(server_id)
        status_text = "globally enabled" if response.enabled else "globally disabled"
        logger.info(f"MCP server with ID {server_id} {status_text}")
        return response
    except HTTPException:
        raise


@router.post("/test-connection", response_model=MCPTestConnectionResponse)
async def test_mcp_connection(
    test_data: MCPTestConnectionRequest,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPTestConnectionResponse:
    """
    Test connection to an MCP server.
    Only Admins can test MCP server connections.
    """
    # Check permissions - only admins can test MCP connections
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can test MCP server connections"
        )

    logger.info(f"Testing connection to MCP server at {test_data.server_url}")
    try:
        response = await service.test_connection(test_data)
        success_text = "successful" if response.success else "failed"
        logger.info(f"Connection test {success_text}: {response.message}")
        return response
    except Exception as e:
        logger.error(f"Error testing MCP server connection: {str(e)}")
        return MCPTestConnectionResponse(
            success=False,
            message=f"Error testing connection: {str(e)}"
        )


@router.get("/settings", response_model=MCPSettingsResponse)
async def get_mcp_settings(
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPSettingsResponse:
    """
    Get global MCP settings.
    """
    logger.info("Getting global MCP settings")
    try:
        settings = await service.get_settings()
        logger.info(f"Retrieved global MCP settings (enabled: {settings.global_enabled})")
        return settings
    except HTTPException:
        raise


@router.put("/settings", response_model=MCPSettingsResponse)
async def update_mcp_settings(
    settings_data: MCPSettingsUpdate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None
) -> MCPSettingsResponse:
    """
    Update global MCP settings.
    Only Admins can update global MCP settings.
    """
    # Check permissions - only admins can update global MCP settings
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can update global MCP settings"
        )

    logger.info(f"Updating global MCP settings (enabled: {settings_data.global_enabled})")
    try:
        settings = await service.update_settings(settings_data)
        logger.info(f"Updated global MCP settings (enabled: {settings.global_enabled})")
        return settings
    except HTTPException:
        raise 