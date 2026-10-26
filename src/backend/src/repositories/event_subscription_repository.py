"""Data access for event choreography config (``eventsubscription``, ``emitrule``).

Owns the SQL for subscriptions (event_type → target) and emit rules
(target completion → event_type). No method commits — the calling service owns
the transaction. Target matching for emit rules is done in Python (the target is
a JSON blob, so a portable ``==`` filter beats dialect-specific JSON SQL).
"""

from typing import Any, List, Optional

from sqlalchemy import delete, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.event_subscription import EmitRule, EventSubscription


class EventSubscriptionRepository(BaseRepository[EventSubscription]):
    def __init__(self, session: AsyncSession):
        super().__init__(EventSubscription, session)

    async def insert(self, **fields: Any) -> EventSubscription:
        row = EventSubscription(**fields)
        self.session.add(row)
        await self.session.flush()
        return row

    async def list_for_groups(
        self, group_ids: List[str], limit: int = 100
    ) -> List[EventSubscription]:
        if not group_ids:
            return []
        result = await self.session.execute(
            select(EventSubscription)
            .where(EventSubscription.group_id.in_(group_ids))
            .order_by(desc(EventSubscription.created_at))
            .limit(limit)
        )
        return list(result.scalars().all())

    async def find_enabled_by_event_type(
        self, event_type: str, group_ids: List[str]
    ) -> List[EventSubscription]:
        if not group_ids:
            return []
        result = await self.session.execute(
            select(EventSubscription).where(
                EventSubscription.event_type == event_type,
                EventSubscription.group_id.in_(group_ids),
                EventSubscription.enabled.is_(True),
            )
        )
        return list(result.scalars().all())

    async def delete_by_id(self, row_id: int) -> None:
        await self.session.execute(
            delete(EventSubscription).where(EventSubscription.id == row_id)
        )


class EmitRuleRepository(BaseRepository[EmitRule]):
    def __init__(self, session: AsyncSession):
        super().__init__(EmitRule, session)

    async def insert(self, **fields: Any) -> EmitRule:
        row = EmitRule(**fields)
        self.session.add(row)
        await self.session.flush()
        return row

    async def list_for_groups(
        self, group_ids: List[str], limit: int = 100
    ) -> List[EmitRule]:
        if not group_ids:
            return []
        result = await self.session.execute(
            select(EmitRule)
            .where(EmitRule.group_id.in_(group_ids))
            .order_by(desc(EmitRule.created_at))
            .limit(limit)
        )
        return list(result.scalars().all())

    async def find_enabled_for_target(
        self, kind: str, target_id: str, group_ids: Optional[List[str]] = None
    ) -> List[EmitRule]:
        """Enabled emit rules whose ``on_target`` matches this crew/flow.

        ``group_ids`` is optional: emit runs from a completed execution, which
        already carries its own group, so a None here means "any group" (the
        caller has already scoped to the run's group).
        """
        query = select(EmitRule).where(EmitRule.enabled.is_(True))
        if group_ids:
            query = query.where(EmitRule.group_id.in_(group_ids))
        result = await self.session.execute(query)
        matches = []
        for rule in result.scalars().all():
            target = rule.on_target or {}
            if target.get("kind") == kind and str(target.get("id")) == str(target_id):
                matches.append(rule)
        return matches

    async def delete_by_id(self, row_id: int) -> None:
        await self.session.execute(delete(EmitRule).where(EmitRule.id == row_id))
