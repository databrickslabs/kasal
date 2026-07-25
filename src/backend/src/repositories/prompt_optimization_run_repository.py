"""
Repository for durable prompt-optimization runs. Group scoping is enforced
on every read — a run carries a proposal for someone's templates or crew.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.prompt_optimization_run import PromptOptimizationRun


class PromptOptimizationRunRepository:
    """Data access for the `prompt_optimization_runs` table."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: Dict[str, Any]) -> PromptOptimizationRun:
        record = PromptOptimizationRun(**data)
        self.session.add(record)
        await self.session.flush()
        return record

    async def get(self, run_id: str) -> Optional[PromptOptimizationRun]:
        """Fetch by id WITHOUT group filtering — callers that serve a request
        must use `get_by_group` instead."""
        result = await self.session.execute(
            select(PromptOptimizationRun).where(PromptOptimizationRun.id == run_id)
        )
        return result.scalars().first()

    async def get_by_group(
        self, run_id: str, group_id: Optional[str]
    ) -> Optional[PromptOptimizationRun]:
        """Fetch by id, visible only to the owning group.

        `group_id=None` matches rows recorded without a group (the single-user
        / no-auth path), mirroring the registry's old visibility rule.
        """
        result = await self.session.execute(
            select(PromptOptimizationRun).where(
                PromptOptimizationRun.id == run_id,
                (
                    PromptOptimizationRun.group_id.is_(None)
                    if group_id is None
                    else PromptOptimizationRun.group_id == group_id
                ),
            )
        )
        return result.scalars().first()

    async def list_by_group(
        self, group_id: Optional[str], limit: int = 50
    ) -> List[PromptOptimizationRun]:
        """Newest-first runs for one group."""
        result = await self.session.execute(
            select(PromptOptimizationRun)
            .where(
                PromptOptimizationRun.group_id.is_(None)
                if group_id is None
                else PromptOptimizationRun.group_id == group_id
            )
            .order_by(PromptOptimizationRun.created_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def update_fields(self, run_id: str, changes: Dict[str, Any]) -> bool:
        """Patch a run row. Returns False when the row is gone (a run whose
        record was pruned must not resurrect itself on a status write)."""
        if not changes:
            return False
        changes = dict(changes)
        changes["updated_at"] = datetime.utcnow()
        result = await self.session.execute(
            update(PromptOptimizationRun)
            .where(PromptOptimizationRun.id == run_id)
            .values(**changes)
        )
        return bool(getattr(result, "rowcount", 0) or 0)

    async def find_stale_active(
        self, group_id: Optional[str], stale_after_seconds: int
    ) -> List[PromptOptimizationRun]:
        """Runs still marked pending/running whose heartbeat has gone stale.

        A live run bumps `updated_at` from its heartbeat, so a stale one was
        orphaned by a backend restart. Reported so reads can settle them
        instead of showing a run that will never finish (which also keeps the
        UI's "run in progress" lock stuck forever).
        """
        cutoff = datetime.utcnow() - timedelta(seconds=stale_after_seconds)
        result = await self.session.execute(
            select(PromptOptimizationRun).where(
                PromptOptimizationRun.status.in_(("pending", "running")),
                PromptOptimizationRun.updated_at < cutoff,
                (
                    PromptOptimizationRun.group_id.is_(None)
                    if group_id is None
                    else PromptOptimizationRun.group_id == group_id
                ),
            )
        )
        return list(result.scalars().all())
