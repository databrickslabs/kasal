from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.a2a_agent import A2AAgent


class A2AAgentRepository(BaseRepository[A2AAgent]):
    """Data access for remote A2A agents.

    Every read here takes a group. A remote agent row carries a credential and
    a URL Kasal will POST to on behalf of whoever triggers it, so an unscoped
    read is not a listing bug — it is one workspace's agents being callable with
    another workspace's data.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(A2AAgent, session)

    async def list_for_group(self, group_ids: List[str]) -> List[A2AAgent]:
        if not group_ids:
            return []
        result = await self.session.execute(
            select(self.model)
            .where(self.model.group_id.in_(group_ids))
            .order_by(self.model.name)
        )
        return list(result.scalars().all())

    async def list_enabled_for_group(self, group_ids: List[str]) -> List[A2AAgent]:
        """The remotes an agent run may actually use.

        Disabled rows are filtered in SQL rather than skipped later, so a remote
        an operator turned off cannot be reached by a stale tool config.
        """
        if not group_ids:
            return []
        result = await self.session.execute(
            select(self.model)
            .where(self.model.group_id.in_(group_ids), self.model.enabled.is_(True))
            .order_by(self.model.name)
        )
        return list(result.scalars().all())

    async def find_by_name(self, name: str, group_ids: List[str]) -> Optional[A2AAgent]:
        if not group_ids:
            return None
        result = await self.session.execute(
            select(self.model).where(
                self.model.name == name, self.model.group_id.in_(group_ids)
            )
        )
        return result.scalars().first()

    async def find_for_group(
        self, agent_id: int, group_ids: List[str]
    ) -> Optional[A2AAgent]:
        """One agent, or None — including when it exists but belongs elsewhere.

        The caller turns None into a 404 rather than a 403, so an id cannot be
        probed to learn what other workspaces have configured.
        """
        if not group_ids:
            return None
        result = await self.session.execute(
            select(self.model).where(
                self.model.id == agent_id, self.model.group_id.in_(group_ids)
            )
        )
        return result.scalars().first()
