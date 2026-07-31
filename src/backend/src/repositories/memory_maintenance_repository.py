"""Data access for the memory-maintenance watermark.

Answers one question for the sweep — "which scopes are due, oldest first?" — and
records the outcome. Deliberately thin: the decision about WHAT maintenance does
lives in ``services/memory``, and this layer only owns the queries.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.memory_backend import MemoryBackend
from src.models.memory_maintenance import MemoryMaintenanceWatermark

logger = logging.getLogger(__name__)


class MemoryMaintenanceRepository:
    """Repository for memory-maintenance watermarks."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def groups_with_memory(self) -> List[str]:
        """Group ids with an ACTIVE memory backend configured.

        The sweep's population. A group with no configured backend falls back
        to the local per-group SQLite store, which is a dev-scale store on the
        app host — it is still maintained at run teardown, it just is not worth
        a scheduled sweep.
        """
        result = await self.session.execute(
            select(MemoryBackend.group_id)
            .where(MemoryBackend.is_active.is_(True))
            .distinct()
        )
        return [row[0] for row in result.fetchall() if row[0]]

    async def get(self, group_id: str) -> Optional[MemoryMaintenanceWatermark]:
        result = await self.session.execute(
            select(MemoryMaintenanceWatermark).where(
                MemoryMaintenanceWatermark.group_id == group_id
            )
        )
        return result.scalar_one_or_none()

    async def due_groups(self, interval_hours: float, limit: int) -> List[str]:
        """Groups whose scope has gone longest without maintenance, oldest first.

        A group with no watermark row has never been maintained by the sweep and
        sorts FIRST — a newly configured workspace should not wait a full
        interval for its first pass.
        """
        configured = await self.groups_with_memory()
        if not configured:
            return []

        result = await self.session.execute(
            select(
                MemoryMaintenanceWatermark.group_id,
                MemoryMaintenanceWatermark.last_maintained_at,
            ).where(MemoryMaintenanceWatermark.group_id.in_(configured))
        )
        seen: Dict[str, Optional[datetime]] = {
            row[0]: row[1] for row in result.fetchall()
        }

        cutoff = datetime.utcnow() - timedelta(hours=interval_hours)
        never: List[str] = []
        overdue: List[tuple] = []
        for group_id in configured:
            last = seen.get(group_id, "missing")
            if last == "missing" or last is None:
                never.append(group_id)
            elif last <= cutoff:
                overdue.append((last, group_id))

        overdue.sort(key=lambda pair: pair[0])
        return (never + [group_id for _, group_id in overdue])[:limit]

    async def record_result(
        self,
        group_id: str,
        status: str,
        stats: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Stamp the watermark for ``group_id``. Upserts the row.

        ``last_maintained_at`` advances even on FAILURE. That is deliberate: a
        scope whose backend is unreachable would otherwise stay permanently at
        the front of the due queue and starve every other scope behind it. The
        failure is recorded in ``last_status``/``last_error`` instead, so it is
        visible without being able to monopolise the sweep.
        """
        watermark = await self.get(group_id)
        now = datetime.utcnow()
        if watermark is None:
            watermark = MemoryMaintenanceWatermark(group_id=group_id)
            self.session.add(watermark)
        watermark.last_maintained_at = now
        watermark.last_status = status
        watermark.last_error = (error or None) and str(error)[:500]
        watermark.last_stats = stats or {}
        watermark.updated_at = now
