import logging
from typing import Optional

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
        """This group's OWN row, or the global default when it has none.

        Two levels, like MCP servers and model configs: a row with
        ``group_id IS NULL`` is the default for every workspace, and a row with a
        ``group_id`` overrides it for that workspace only.

        Without the fallback an exact match meant a new teamspace saw nothing an
        admin had configured globally — it silently got the schema defaults
        instead, so "set it once for everyone" was not expressible.

        ``exact_only=True`` on the write path: an update must never mistake the
        global default for this workspace's own row and edit it in place.
        """
        own = await self.get_for_group_exact(group_id)
        if own is not None or group_id is None:
            return own
        # Fall back to the global default (group_id IS NULL).
        return await self.get_for_group_exact(None)

    async def add(self, config: UIConfig) -> UIConfig:
        """Stage a new UI-config row.

        No flush: the caller sets the payload fields next and commits once, so
        flushing here would write a half-populated row for no benefit.
        """
        self.session.add(config)
        return config

    async def reload(self, config: UIConfig) -> UIConfig:
        """Re-read a row after commit, so server-side defaults are populated."""
        await self.session.refresh(config)
        return config

    async def get_for_group_exact(self, group_id: Optional[str]) -> Optional[UIConfig]:
        """The row belonging to EXACTLY this group (most recent if duplicated).

        No fallback — used by the write path, where treating the global default as
        the workspace's own row would edit every workspace's config.
        """
        query = select(self.model)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        else:
            query = query.where(self.model.group_id.is_(None))
        query = query.order_by(self.model.updated_at.desc())
        result = await self.session.execute(query)
        return result.scalars().first()
