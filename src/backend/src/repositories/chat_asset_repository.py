"""Data access for chat assets (uploaded images)."""

from typing import Any, Dict, List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from src.models.chat_asset import ChatAsset


class ChatAssetRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: Dict[str, Any]) -> ChatAsset:
        asset = ChatAsset(**data)
        self.session.add(asset)
        await self.session.flush()
        return asset

    async def get(
        self, asset_id: str, group_ids: Optional[List[str]]
    ) -> Optional[ChatAsset]:
        """The asset WITH its bytes, when it belongs to one of ``group_ids``
        (None skips the tenant filter — only for trusted internal callers; an
        empty list matches nothing)."""
        stmt = select(ChatAsset).where(ChatAsset.id == asset_id)
        if group_ids is not None:
            if not group_ids:
                return None
            stmt = stmt.where(ChatAsset.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_for_session(
        self, session_id: str, group_ids: Optional[List[str]]
    ) -> List[ChatAsset]:
        """Metadata only — the bytes stay in the database until one is served."""
        stmt = (
            select(ChatAsset)
            .options(defer(ChatAsset.data))
            .where(ChatAsset.session_id == session_id)
            .order_by(ChatAsset.created_at)
        )
        if group_ids is not None:
            if not group_ids:
                return []
            stmt = stmt.where(ChatAsset.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete(self, asset_id: str, group_ids: Optional[List[str]]) -> bool:
        stmt = delete(ChatAsset).where(ChatAsset.id == asset_id)
        if group_ids is not None:
            if not group_ids:
                return False
            stmt = stmt.where(ChatAsset.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        return bool(result.rowcount)
