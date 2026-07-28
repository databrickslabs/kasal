from typing import List, Optional

from sqlalchemy import delete, select
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

    async def find_all_base(self) -> List[A2AAgent]:
        """The system-admin catalogue: base rows (``group_id IS NULL``).

        A base row is "available to all workspaces" when ``enabled``. Only a
        Kasal admin creates or edits one — a remote agent carries a credential
        and an outbound URL, so registering one is a system-administration act,
        not something each workspace redoes.
        """
        result = await self.session.execute(
            select(self.model)
            .where(self.model.group_id.is_(None))
            .order_by(self.model.name)
        )
        return list(result.scalars().all())

    async def find_base_by_name(self, name: str) -> Optional[A2AAgent]:
        result = await self.session.execute(
            select(self.model).where(
                self.model.name == name, self.model.group_id.is_(None)
            )
        )
        return result.scalars().first()

    async def list_for_group_scope(self, group_id: Optional[str]) -> List[A2AAgent]:
        """What a workspace SEES: globally-available agents plus its own rows.

        Same override model as MCP servers, and deliberately so — an operator who
        has learned one should not have to learn a second:

        - A base row is visible only when a Kasal admin made it available.
        - The group's own row of the same name SHADOWS the base for that group,
          carrying this workspace's enabled/disabled state. Other workspaces keep
          seeing the base.
        - A base a Kasal admin turned OFF hides the workspace override too, so a
          global disable cascades rather than leaving workspaces still calling it.
        """
        if not group_id:
            result = await self.session.execute(
                select(self.model).where(
                    self.model.group_id.is_(None), self.model.enabled.is_(True)
                )
            )
            return list(result.scalars().all())

        overridden = (
            select(self.model.name).where(self.model.group_id == group_id).distinct()
        )
        disabled_base = select(self.model.name).where(
            self.model.group_id.is_(None), self.model.enabled.is_(False)
        )
        result = await self.session.execute(
            select(self.model)
            .where(
                (
                    (self.model.group_id == group_id)
                    & (~self.model.name.in_(disabled_base))
                )
                | (
                    self.model.group_id.is_(None)
                    & self.model.enabled.is_(True)
                    & (~self.model.name.in_(overridden))
                )
            )
            .order_by(self.model.name)
        )
        return list(result.scalars().all())

    async def list_enabled_for_group(self, group_ids: List[str]) -> List[A2AAgent]:
        """The remotes an agent run may actually use — OPT-IN only.

        A workspace's OWN enabled rows, and only while the base of the same name
        is not disabled. A globally-available agent does NOT auto-resolve: a
        workspace admin has to turn it on, which creates the override row this
        reads. Availability is a Kasal admin's decision; use is the workspace's,
        and neither substitutes for the other.
        """
        if not group_ids:
            return []
        disabled_base = select(self.model.name).where(
            self.model.group_id.is_(None), self.model.enabled.is_(False)
        )
        result = await self.session.execute(
            select(self.model)
            .where(
                self.model.group_id.in_(group_ids),
                self.model.enabled.is_(True),
                ~self.model.name.in_(disabled_base),
            )
            .order_by(self.model.name)
        )
        return list(result.scalars().all())

    async def find_by_name_and_group(
        self, name: str, group_id: str
    ) -> Optional[A2AAgent]:
        """A workspace's own override row for a name, if it has one."""
        result = await self.session.execute(
            select(self.model).where(
                self.model.name == name, self.model.group_id == group_id
            )
        )
        return result.scalars().first()

    async def delete_overrides_by_name(self, name: str) -> int:
        """Remove every workspace override for a name.

        Runs when a Kasal admin deletes the base: leaving orphaned overrides
        behind would keep the agent callable in workspaces that had enabled it,
        long after the row that defined it was gone.
        """
        result = await self.session.execute(
            delete(self.model).where(
                self.model.name == name, self.model.group_id.isnot(None)
            )
        )
        return result.rowcount or 0

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
