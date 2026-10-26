"""CRUD service for the trigger queue — the API's view of ``triggerqueue``.

Distinct from ``TriggerQueueConsumerService`` (the background worker that drains
the queue): this is the request-scoped service the ``/triggers`` router uses to
enqueue events and inspect/delete them. Group-scoped for tenant isolation; all
queries live in ``TriggerQueueRepository``.
"""

from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import NotFoundError
from src.core.logger import LoggerManager
from src.models.trigger_queue import STATUS_PENDING, TriggerQueue
from src.repositories.trigger_queue_repository import TriggerQueueRepository
from src.schemas.triggers import EnqueueTrigger
from src.utils.user_context import GroupContext

logger = LoggerManager.get_instance().system


class TriggerQueueService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = TriggerQueueRepository(session)

    async def enqueue(
        self, payload: EnqueueTrigger, group_context: GroupContext
    ) -> TriggerQueue:
        """Insert a pending event, stamped with the caller's tenant.

        Does not commit — the request session is committed by the router's DI at
        the end of the request (repository ``enqueue`` flushes to assign the id).
        """
        row = await self.repository.enqueue(
            group_id=group_context.primary_group_id,
            event_type=payload.event_type,
            target=payload.target.model_dump() if payload.target else None,
            payload=payload.payload or {},
            correlation_id=payload.correlation_id,
            causation_run_id=payload.causation_run_id,
            idempotency_key=payload.idempotency_key,
            status=STATUS_PENDING,
        )
        logger.info(
            "[TriggerQueue] enqueued event %s (group=%s, target=%s)",
            row.id,
            row.group_id,
            (row.target or {}).get("kind"),
        )
        return row

    async def list_events(
        self,
        group_context: GroupContext,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[TriggerQueue]:
        return await self.repository.list_for_groups(
            group_context.group_ids or [], status=status, limit=limit
        )

    async def get_event(
        self, event_id: int, group_context: GroupContext
    ) -> TriggerQueue:
        row = await self.repository.get(event_id)
        if row is None or row.group_id not in (group_context.group_ids or []):
            raise NotFoundError(f"Trigger event {event_id} not found")
        return row

    async def delete_event(self, event_id: int, group_context: GroupContext) -> None:
        await self.get_event(event_id, group_context)  # 404s if not visible
        await self.repository.delete_by_id(event_id)
