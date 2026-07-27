"""Repository for workflow-recipe trials — the reuse measurement ledger.

Reads are group-scoped like the recipe repository: a trial carries the prompt a
workspace typed and the shape of the crew it got back, which is the same class of
information as the recipe itself.

The one deliberate exception is ``list_unlinked``, the linker's own work queue.
It is a background writer, not a tenant-visible read — the same reasoning that
makes ``WorkflowRecipeRepository.list_missing_embeddings`` unscoped.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.workflow_recipe_trial import WorkflowRecipeTrial


class WorkflowRecipeTrialRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: Dict[str, Any]) -> WorkflowRecipeTrial:
        record = WorkflowRecipeTrial(**data)
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_unlinked(
        self, limit: int = 200, max_age_days: int = 30
    ) -> List[WorkflowRecipeTrial]:
        """Trials with no run attached yet, newest first.

        Bounded by age because most trials never link at all — a generated crew
        that was discarded, or edited past recognition, has no run to find — and
        without the bound the linker would rescan the same permanent misses on
        every sweep forever.
        """
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        stmt = (
            select(WorkflowRecipeTrial)
            .where(
                WorkflowRecipeTrial.linked_job_id.is_(None),
                WorkflowRecipeTrial.created_at >= cutoff,
            )
            .order_by(WorkflowRecipeTrial.created_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_for_report(
        self,
        group_ids: List[str],
        since: Optional[datetime] = None,
        limit: int = 5000,
    ) -> List[WorkflowRecipeTrial]:
        """Every trial the report aggregates over.

        Returns rows rather than pre-aggregating in SQL: the arms are small (a
        workspace generates crews in the hundreds, not millions), and the medians
        and rates are clearer — and identically computed across SQLite,
        PostgreSQL and Lakebase — in Python than in three dialects of SQL.
        """
        if not group_ids:
            return []
        stmt = select(WorkflowRecipeTrial).where(
            WorkflowRecipeTrial.group_id.in_(group_ids)
        )
        if since is not None:
            stmt = stmt.where(WorkflowRecipeTrial.created_at >= since)
        stmt = stmt.order_by(WorkflowRecipeTrial.created_at.desc()).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete_by_groups(self, group_ids: Optional[List[str]] = None) -> int:
        """Delete a workspace's trials — or every trial when ``group_ids`` is
        omitted, matching the unscoped (admin) arm of run deletion.

        Trials measure runs; once the runs they describe are gone the ledger
        reports on nothing, and its ``linked_job_id`` values point at rows that
        no longer exist.

        An empty LIST means "these zero workspaces" and deletes nothing —
        only omitting the argument is the deliberate delete-everything.
        """
        if group_ids is not None and not group_ids:
            return 0
        stmt = delete(WorkflowRecipeTrial)
        if group_ids is not None:
            stmt = stmt.where(WorkflowRecipeTrial.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount or 0
