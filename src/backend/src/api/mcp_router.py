"""
API router for MCP server operations.

This module provides endpoints for managing MCP (Model Context Protocol) servers.
"""
from typing import Annotated, Dict, Any, List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

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
from src.db.session import get_db
from src.services.mcp_service import MCPService

# Create router instance
router = APIRouter(
    prefix="/mcp",
    tags=["mcp"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)


@router.get("/servers", response_model=MCPServerListResponse)
async def get_mcp_servers(
    db: AsyncSession = Depends(get_db)
) -> MCPServerListResponse:
    """
    Get all MCP servers.
    
    Returns:
        List of MCP servers and count
    """
    try:
        service = MCPService(db)
        return await service.get_all_servers()
    except Exception as e:
        logger.error(f"Error getting MCP servers: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/servers/enabled", response_model=MCPServerListResponse)
async def get_enabled_mcp_servers(
    db: AsyncSession = Depends(get_db)
) -> MCPServerListResponse:
    """
    Get all enabled MCP servers.
    """
    logger.info("Getting enabled MCP servers")
    service = MCPService(db)
    servers_response = await service.get_enabled_servers()
    logger.info(f"Found {servers_response.count} enabled MCP servers")
    return servers_response


@router.get("/servers/{server_id}", response_model=MCPServerResponse)
async def get_mcp_server(
    server_id: int,
    db: AsyncSession = Depends(get_db)
) -> MCPServerResponse:
    """
    Get an MCP server by ID.
    """
    logger.info(f"Getting MCP server with ID {server_id}")
    try:
        service = MCPService(db)
        server = await service.get_server_by_id(server_id)
        logger.info(f"Found MCP server with ID {server_id}")
        return server
    except HTTPException:
        raise


@router.post("/servers", response_model=MCPServerResponse, status_code=status.HTTP_201_CREATED)
async def create_mcp_server(
    server_data: MCPServerCreate,
    db: AsyncSession = Depends(get_db)
) -> MCPServerResponse:
    """
    Create a new MCP server.
    """
    logger.info(f"Creating MCP server with name '{server_data.name}'")
    try:
        service = MCPService(db)
        server = await service.create_server(server_data)
        logger.info(f"Created MCP server with ID {server.id}")
        return server
    except HTTPException:
        raise


@router.put("/servers/{server_id}", response_model=MCPServerResponse)
async def update_mcp_server(
    server_id: int,
    server_data: MCPServerUpdate,
    db: AsyncSession = Depends(get_db)
) -> MCPServerResponse:
    """
    Update an existing MCP server.
    """
    logger.info(f"Updating MCP server with ID {server_id}")
    try:
        service = MCPService(db)
        server = await service.update_server(server_id, server_data)
        logger.info(f"Updated MCP server with ID {server_id}")
        return server
    except HTTPException:
        raise


@router.delete("/servers/{server_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_mcp_server(
    server_id: int,
    db: AsyncSession = Depends(get_db)
) -> None:
    """
    Delete an MCP server.
    """
    logger.info(f"Deleting MCP server with ID {server_id}")
    try:
        service = MCPService(db)
        await service.delete_server(server_id)
        logger.info(f"Deleted MCP server with ID {server_id}")
    except HTTPException:
        raise


@router.patch("/servers/{server_id}/toggle-enabled", response_model=MCPToggleResponse)
async def toggle_mcp_server_enabled(
    server_id: int,
    db: AsyncSession = Depends(get_db)
) -> MCPToggleResponse:
    """
    Toggle the enabled status of an MCP server.
    """
    logger.info(f"Toggling enabled status for MCP server with ID {server_id}")
    try:
        service = MCPService(db)
        response = await service.toggle_server_enabled(server_id)
        status_text = "enabled" if response.enabled else "disabled"
        logger.info(f"MCP server with ID {server_id} {status_text}")
        return response
    except HTTPException:
        raise


@router.post("/test-connection", response_model=MCPTestConnectionResponse)
async def test_mcp_connection(
    test_data: MCPTestConnectionRequest,
    db: AsyncSession = Depends(get_db)
) -> MCPTestConnectionResponse:
    """
    Test connection to an MCP server.
    """
    logger.info(f"Testing connection to MCP server at {test_data.server_url}")
    try:
        service = MCPService(db)
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
    db: AsyncSession = Depends(get_db)
) -> MCPSettingsResponse:
    """
    Get global MCP settings.
    """
    logger.info("Getting global MCP settings")
    try:
        service = MCPService(db)
        settings = await service.get_settings()
        logger.info(f"Retrieved global MCP settings (enabled: {settings.global_enabled})")
        return settings
    except HTTPException:
        raise


@router.put("/settings", response_model=MCPSettingsResponse)
async def update_mcp_settings(
    settings_data: MCPSettingsUpdate,
    db: AsyncSession = Depends(get_db)
) -> MCPSettingsResponse:
    """
    Update global MCP settings.
    """
    logger.info(f"Updating global MCP settings (enabled: {settings_data.global_enabled})")
    try:
        service = MCPService(db)
        settings = await service.update_settings(settings_data)
        logger.info(f"Updated global MCP settings (enabled: {settings.global_enabled})")
        return settings
    except HTTPException:
        raise 