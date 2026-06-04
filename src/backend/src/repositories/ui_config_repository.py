from typing import Optional
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.ui_config import UIConfig

logger = logging.getLogger(__name__)


class UIConfigRepository(BaseRepository[UIConfig]):
    """
    Repository for the per-workspace Predefined UI configuration.
    There is at most one row per group.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(UIConfig, session)

    async def get_for_group(self, group_id: Optional[str]) -> Optional[UIConfig]:
        """Return the UI config row for a group (most recent if duplicated)."""
        query = select(self.model)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        else:
            query = query.where(self.model.group_id.is_(None))
        query = query.order_by(self.model.updated_at.desc())
        result = await self.session.execute(query)
        return result.scalars().first()
