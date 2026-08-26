"""CRUD for event choreography config — subscriptions and emit rules.

Request-scoped service behind the ``/triggers/subscriptions`` +
``/triggers/emit-rules`` routes. Group-scoped for tenant isolation; all queries
live in the repositories. The consumer/emit engine read these at dispatch time.
"""

from typing import List

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import NotFoundError
from src.core.logger import LoggerManager
from src.models.event_subscription import EmitRule, EventSubscription
from src.repositories.event_subscription_repository import (
    EmitRuleRepository,
    EventSubscriptionRepository,
)
from src.schemas.triggers import EmitRuleCreate, SubscriptionCreate
from src.utils.user_context import GroupContext

logger = LoggerManager.get_instance().system


class SubscriptionService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.subscriptions = EventSubscriptionRepository(session)
        self.emit_rules = EmitRuleRepository(session)

    # ---------------------------------------------------------- subscriptions
    async def create_subscription(
        self, payload: SubscriptionCreate, group_context: GroupContext
    ) -> EventSubscription:
        row = await self.subscriptions.insert(
            group_id=group_context.primary_group_id,
            event_type=payload.event_type,
            target=payload.target.model_dump(),
            harness=payload.harness,
            input_mapping=payload.input_mapping,
            schema_ref=payload.schema_ref,
            enabled=payload.enabled,
        )
        logger.info(
            "[Triggers] subscription %s: %s -> %s",
            row.id,
            payload.event_type,
            payload.target.kind,
        )
        return row

    async def list_subscriptions(
        self, group_context: GroupContext
    ) -> List[EventSubscription]:
        return await self.subscriptions.list_for_groups(group_context.group_ids or [])

    async def delete_subscription(
        self, sub_id: int, group_context: GroupContext
    ) -> None:
        row = await self.subscriptions.get(sub_id)
        if row is None or row.group_id not in (group_context.group_ids or []):
            raise NotFoundError(f"Subscription {sub_id} not found")
        await self.subscriptions.delete_by_id(sub_id)

    # ------------------------------------------------------------- emit rules
    async def create_emit_rule(
        self, payload: EmitRuleCreate, group_context: GroupContext
    ) -> EmitRule:
        row = await self.emit_rules.insert(
            group_id=group_context.primary_group_id,
            on_target=payload.on_target.model_dump(),
            event_type=payload.event_type,
            schema_ref=payload.schema_ref,
            condition=payload.condition,
            enabled=payload.enabled,
        )
        logger.info(
            "[Triggers] emit rule %s: %s -> %s",
            row.id,
            payload.on_target.kind,
            payload.event_type,
        )
        return row

    async def list_emit_rules(self, group_context: GroupContext) -> List[EmitRule]:
        return await self.emit_rules.list_for_groups(group_context.group_ids or [])

    async def delete_emit_rule(
        self, rule_id: int, group_context: GroupContext
    ) -> None:
        row = await self.emit_rules.get(rule_id)
        if row is None or row.group_id not in (group_context.group_ids or []):
            raise NotFoundError(f"Emit rule {rule_id} not found")
        await self.emit_rules.delete_by_id(rule_id)
