from typing import List, Optional, Dict, Any

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.group_tool import GroupTool


class GroupToolRepository:
    """
    Repository for GroupTool mappings (global Tool -> group availability/enabled/config).
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, mapping_id: int) -> Optional[GroupTool]:
        result = await self.session.execute(select(GroupTool).where(GroupTool.id == mapping_id))
        return result.scalars().first()

    async def find_by_tool_and_group(self, tool_id: int, group_id: str) -> Optional[GroupTool]:
        result = await self.session.execute(
            select(GroupTool).where((GroupTool.tool_id == tool_id) & (GroupTool.group_id == group_id))
        )
        return result.scalars().first()

    async def list_for_group(self, group_id: str) -> List[GroupTool]:
        result = await self.session.execute(select(GroupTool).where(GroupTool.group_id == group_id))
        return list(result.scalars().all())

    async def list_enabled_for_group(self, group_id: str) -> List[GroupTool]:
        result = await self.session.execute(
            select(GroupTool).where((GroupTool.group_id == group_id) & (GroupTool.enabled == True))  # noqa: E712
        )
        return list(result.scalars().all())

    async def create(self, payload: Dict[str, Any]) -> GroupTool:
        mapping = GroupTool(**payload)
        self.session.add(mapping)
        await self.session.flush()
        await self.session.refresh(mapping)
        return mapping

    async def upsert(self, tool_id: int, group_id: str, defaults: Optional[Dict[str, Any]] = None) -> GroupTool:
        mapping = await self.find_by_tool_and_group(tool_id, group_id)
        if mapping:
            return mapping
        payload = {"tool_id": tool_id, "group_id": group_id}
        if defaults:
            payload.update(defaults)
        return await self.create(payload)

    async def update_config(self, tool_id: int, group_id: str, config: Dict[str, Any]) -> Optional[GroupTool]:
        mapping = await self.find_by_tool_and_group(tool_id, group_id)
        if not mapping:
            return None
        await self.session.execute(
            update(GroupTool)
            .where((GroupTool.tool_id == tool_id) & (GroupTool.group_id == group_id))
            .values(config=config)
        )
        await self.session.flush()
        await self.session.refresh(mapping)
        return mapping

    async def set_enabled(self, tool_id: int, group_id: str, enabled: bool) -> Optional[GroupTool]:
        mapping = await self.find_by_tool_and_group(tool_id, group_id)
        if not mapping:
            return None
        await self.session.execute(
            update(GroupTool)
            .where((GroupTool.tool_id == tool_id) & (GroupTool.group_id == group_id))
            .values(enabled=enabled)
        )
        await self.session.flush()
        await self.session.refresh(mapping)
        return mapping

    async def delete_mapping(self, tool_id: int, group_id: str) -> int:
        result = await self.session.execute(
            delete(GroupTool).where((GroupTool.tool_id == tool_id) & (GroupTool.group_id == group_id))
        )
        await self.session.flush()
        return result.rowcount or 0

