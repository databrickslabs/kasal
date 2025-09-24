from typing import List, Optional, Dict, Any
import logging

from fastapi import HTTPException, status

from src.repositories.tool_repository import ToolRepository
from src.repositories.group_tool_repository import GroupToolRepository
from src.schemas.group_tool import (
    GroupToolCreate,
    GroupToolUpdate,
    GroupToolResponse,
    GroupToolListResponse,
)
from src.schemas.tool import ToolResponse, ToolListResponse
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class GroupToolService:
    """
    Service managing the relationship between global tools and group availability/enabling.

    Semantics:
    - Base tool (tools.group_id is NULL) + enabled=True => globally available
    - GroupTool row => tool is added to the group (explicit opt-in)
    - GroupTool.enabled => whether the tool is enabled within the group
    """

    def __init__(self, session):
        self.tool_repo = ToolRepository(session)
        self.group_tool_repo = GroupToolRepository(session)

    async def list_added_for_group(self, group_context: GroupContext) -> GroupToolListResponse:
        if not group_context or not group_context.primary_group_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Group context required")
        group_id = group_context.primary_group_id
        mappings = await self.group_tool_repo.list_for_group(group_id)
        return GroupToolListResponse(
            items=[GroupToolResponse.model_validate(m) for m in mappings],
            count=len(mappings),
        )

    async def list_available_to_add_for_group(self, group_context: GroupContext) -> ToolListResponse:
        """
        List global tools that are globally available (base tools with enabled=True, group_id=NULL)
        but not yet added to this group (no GroupTool row).
        """
        if not group_context or not group_context.primary_group_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Group context required")
        group_id = group_context.primary_group_id

        # All global tools
        all_tools = await self.tool_repo.list()
        base_global_available = [t for t in all_tools if (t.group_id is None and getattr(t, "enabled", False))]

        # Fetch existing mappings for the group
        mappings = await self.group_tool_repo.list_for_group(group_id)
        mapped_tool_ids = {m.tool_id for m in mappings}

        to_add = [t for t in base_global_available if t.id not in mapped_tool_ids]
        return ToolListResponse(tools=[ToolResponse.model_validate(t) for t in to_add], count=len(to_add))

    async def add_tool_to_group(self, tool_id: int, group_context: GroupContext, defaults: Optional[Dict[str, Any]] = None) -> GroupToolResponse:
        if not group_context or not group_context.primary_group_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Group context required")
        group_id = group_context.primary_group_id

        # Validate that tool exists and is a base/global tool
        tool = await self.tool_repo.get(tool_id)
        if not tool:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tool not found")
        if tool.group_id is not None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only global tools can be added to a group")
        if not getattr(tool, "enabled", False):
            # Global availability is controlled by base tool enabled flag (for now)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Tool is not globally available")

        mapping = await self.group_tool_repo.upsert(tool_id=tool_id, group_id=group_id, defaults=defaults)
        return GroupToolResponse.model_validate(mapping)

    async def set_group_tool_enabled(self, tool_id: int, enabled: bool, group_context: GroupContext) -> GroupToolResponse:
        if not group_context or not group_context.primary_group_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Group context required")
        group_id = group_context.primary_group_id

        updated = await self.group_tool_repo.set_enabled(tool_id=tool_id, group_id=group_id, enabled=enabled)
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group tool mapping not found")
        return GroupToolResponse.model_validate(updated)

    async def update_group_tool_config(self, tool_id: int, config: Dict[str, Any], group_context: GroupContext) -> GroupToolResponse:
        if not group_context or not group_context.primary_group_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Group context required")
        group_id = group_context.primary_group_id

        updated = await self.group_tool_repo.update_config(tool_id=tool_id, group_id=group_id, config=config)
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group tool mapping not found")
        return GroupToolResponse.model_validate(updated)

    async def remove_tool_from_group(self, tool_id: int, group_context: GroupContext) -> bool:
        if not group_context or not group_context.primary_group_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Group context required")
        group_id = group_context.primary_group_id
        deleted = await self.group_tool_repo.delete_mapping(tool_id=tool_id, group_id=group_id)
        return deleted > 0

