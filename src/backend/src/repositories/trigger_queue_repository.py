"""Data access for the event-trigger queue (``triggerqueue`` table).

Owns all SQL for the queue, including the transactional CLAIM. No method commits
— the calling service owns the transaction (so the claim's row locks are held
until the service decides the row is safely in-flight). See
``services/triggers/queue_consumer_service.py``.
"""

from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import delete, desc, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.trigger_queue import (
    STATUS_CLAIMED,
    STATUS_DEAD,
    STATUS_DISPATCHED,
    STATUS_FAILED,
    STATUS_PENDING,
    TriggerQueue,
)


class TriggerQueueRepository(BaseRepository[TriggerQueue]):
    def __init__(self, session: AsyncSession):
        super().__init__(TriggerQueue, session)

    def _dialect(self) -> str:
        """Dialect name of the bound engine (``postgresql`` / ``sqlite`` / …).

        Defensive: if it can't be determined we return ``""`` and the caller
        omits ``FOR UPDATE SKIP LOCKED`` — correct everywhere, just without the
        multi-worker skip (single-worker semantics), which is the safe default.
        """
        try:
            bind = self.session.get_bind()
        except Exception:  # noqa: BLE001
            return ""
        dialect = getattr(bind, "dialect", None)
        if dialect is None:
            dialect = getattr(getattr(bind, "sync_engine", None), "dialect", None)
        return getattr(dialect, "name", "") or ""

    async def enqueue(self, **fields: Any) -> TriggerQueue:
        """Insert a pending event. Does NOT commit (caller owns the transaction)."""
        row = TriggerQueue(**fields)
        self.session.add(row)
        await self.session.flush()  # assign the id
        return row

    async def claim(
        self,
        limit: int = 5,
        now: Optional[datetime] = None,
        group_ids: Optional[List[str]] = None,
    ) -> List[TriggerQueue]:
        """Atomically claim up to ``limit`` due pending rows and mark them claimed.

        Postgres/Lakebase uses ``FOR UPDATE SKIP LOCKED`` so multiple app replicas
        never claim the same row. On SQLite (dev/tests) the lock clause is omitted
        — single-worker, which is all local dev needs. The claimed rows' locks are
        held until the caller commits, so the follow-up UPDATE is race-free.

        ``group_ids`` scopes the claim to those tenants (the on-demand
        ``/triggers/dispatch`` drain); None (the background consumer) claims
        across all tenants. An EMPTY list claims nothing — a caller with no
        groups must not fall through to the global scan.
        """
        now = now or datetime.utcnow()
        if group_ids is not None and not group_ids:
            return []
        stmt = (
            select(TriggerQueue.id)
            .where(
                TriggerQueue.status == STATUS_PENDING,
                or_(
                    TriggerQueue.available_at.is_(None),
                    TriggerQueue.available_at <= now,
                ),
            )
            .order_by(TriggerQueue.created_at, TriggerQueue.id)
            .limit(limit)
        )
        if group_ids:
            stmt = stmt.where(TriggerQueue.group_id.in_(group_ids))
        if self._dialect() == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)
        id_result = await self.session.execute(stmt)
        ids = [row[0] for row in id_result.fetchall()]
        if not ids:
            return []
        await self.session.execute(
            update(TriggerQueue)
            .where(TriggerQueue.id.in_(ids))
            .values(
                status=STATUS_CLAIMED,
                claimed_at=now,
                attempts=TriggerQueue.attempts + 1,
            )
        )
        rows = await self.session.execute(
            select(TriggerQueue)
            .where(TriggerQueue.id.in_(ids))
            .order_by(TriggerQueue.created_at, TriggerQueue.id)
        )
        return list(rows.scalars().all())

    async def mark_dispatched(self, row_id: int) -> None:
        await self.session.execute(
            update(TriggerQueue)
            .where(TriggerQueue.id == row_id)
            .values(status=STATUS_DISPATCHED)
        )

    async def mark_failed(self, row_id: int, error: str, dead: bool = False) -> None:
        await self.session.execute(
            update(TriggerQueue)
            .where(TriggerQueue.id == row_id)
            .values(
                status=STATUS_DEAD if dead else STATUS_FAILED,
                last_error=(error or "")[:1000],
            )
        )

    async def requeue(
        self, row_id: int, available_at: datetime, error: Optional[str] = None
    ) -> None:
        """Return a row to ``pending`` (with backoff via ``available_at``)."""
        await self.session.execute(
            update(TriggerQueue)
            .where(TriggerQueue.id == row_id)
            .values(
                status=STATUS_PENDING,
                available_at=available_at,
                claimed_at=None,
                last_error=(error or "")[:1000] if error else None,
            )
        )

    async def reclaim_stuck(self, claimed_before: datetime) -> int:
        """Reset rows stuck in ``claimed`` (crashed worker) back to ``pending``."""
        result = await self.session.execute(
            update(TriggerQueue)
            .where(
                TriggerQueue.status == STATUS_CLAIMED,
                TriggerQueue.claimed_at < claimed_before,
            )
            .values(status=STATUS_PENDING, claimed_at=None)
        )
        return result.rowcount or 0

    async def purge_finished(self, older_than: datetime) -> int:
        """Delete finished rows (``dispatched``/``dead``) created before the
        cutoff — the retention sweep. The queue is a work log, not an archive;
        run history lives in ``executionhistory``."""
        result = await self.session.execute(
            delete(TriggerQueue).where(
                TriggerQueue.status.in_((STATUS_DISPATCHED, STATUS_DEAD)),
                TriggerQueue.created_at < older_than,
            )
        )
        return result.rowcount or 0

    # --------------------------------------------------------------- reads (API)
    async def list_for_groups(
        self,
        group_ids: List[str],
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[TriggerQueue]:
        """Most-recent-first events visible to ``group_ids`` (tenant scoping)."""
        if not group_ids:
            return []
        query = select(TriggerQueue).where(TriggerQueue.group_id.in_(group_ids))
        if status:
            query = query.where(TriggerQueue.status == status)
        query = query.order_by(desc(TriggerQueue.created_at)).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def delete_by_id(self, row_id: int) -> None:
        await self.session.execute(
            delete(TriggerQueue).where(TriggerQueue.id == row_id)
        )
