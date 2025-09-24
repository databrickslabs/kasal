"""
API router for workspace (group) tool mappings.

Provides endpoints for workspace admins to:
- See globally available tools that can be added to their group
- See tools already added to their group
- Add/remove a tool to/from the group
- Enable/disable and configure a tool within the group
"""
from typing import Annotated, Dict, Any
import logging

from fastapi import APIRouter, Depends, HTTPException, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import require_admin
from src.schemas.group_tool import GroupToolResponse, GroupToolListResponse
from src.schemas.tool import ToolListResponse
from src.services.group_tool_service import GroupToolService

router = APIRouter(
    prefix="/group-tools",
    tags=["group-tools"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


async def get_group_tool_service(session: SessionDep) -> GroupToolService:
    return GroupToolService(session)

GroupToolServiceDep = Annotated[GroupToolService, Depends(get_group_tool_service)]


@router.get("/available", response_model=ToolListResponse)
@require_admin()
async def list_available_to_add(
    service: GroupToolServiceDep,
    group_context: GroupContextDep,
) -> ToolListResponse:
    """List global tools that are available to add to the current group."""
    return await service.list_available_to_add_for_group(group_context)


@router.get("", response_model=GroupToolListResponse)
@require_admin()
async def list_added(
    service: GroupToolServiceDep,
    group_context: GroupContextDep,
) -> GroupToolListResponse:
    """List tools already added to the current group."""
    return await service.list_added_for_group(group_context)


@router.post("/{tool_id}", response_model=GroupToolResponse, status_code=status.HTTP_201_CREATED)
@require_admin()
async def add_tool(
    tool_id: int,
    service: GroupToolServiceDep,
    group_context: GroupContextDep,
) -> GroupToolResponse:
    """Add a globally available tool to the current group."""
    return await service.add_tool_to_group(tool_id, group_context)


@router.patch("/{tool_id}/enabled", response_model=GroupToolResponse)
@require_admin()
async def set_enabled(
    tool_id: int,
    payload: Dict[str, Any],
    service: GroupToolServiceDep,
    group_context: GroupContextDep,
) -> GroupToolResponse:
    """Enable or disable a tool within the current group."""
    if "enabled" not in payload or not isinstance(payload["enabled"], bool):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'enabled' boolean is required")
    return await service.set_group_tool_enabled(tool_id, bool(payload["enabled"]), group_context)


@router.patch("/{tool_id}/config", response_model=GroupToolResponse)
@require_admin()
async def update_config(
    tool_id: int,
    config: Dict[str, Any],
    service: GroupToolServiceDep,
    group_context: GroupContextDep,
) -> GroupToolResponse:
    """Update group-scoped configuration for a tool in the current group."""
    return await service.update_group_tool_config(tool_id, config, group_context)


@router.delete("/{tool_id}", status_code=status.HTTP_204_NO_CONTENT)
@require_admin()
async def remove_tool(
    tool_id: int,
    service: GroupToolServiceDep,
    group_context: GroupContextDep,
) -> None:
    """Remove a tool from the current group."""
    deleted = await service.remove_tool_from_group(tool_id, group_context)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group tool mapping not found")
    return None

