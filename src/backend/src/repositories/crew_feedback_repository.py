"""
Repository for crew thumbs feedback. Group scoping enforced on all reads.
"""

from typing import Any, Dict, List

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.crew_feedback import CrewFeedback


class CrewFeedbackRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: Dict[str, Any]) -> CrewFeedback:
        record = CrewFeedback(**data)
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_by_crew_and_group(
        self, crew_id: str, group_ids: List[str]
    ) -> List[CrewFeedback]:
        if not group_ids:
            return []
        stmt = (
            select(CrewFeedback)
            .where(
                CrewFeedback.crew_id == crew_id,
                CrewFeedback.group_id.in_(group_ids),
            )
            .order_by(CrewFeedback.created_at.desc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def summary_by_group(self, group_ids: List[str]) -> List[Dict[str, Any]]:
        """Per-crew up/down counts for the workspace's catalog view."""
        if not group_ids:
            return []
        stmt = (
            select(
                CrewFeedback.crew_id,
                CrewFeedback.rating,
                func.count().label("n"),
            )
            .where(CrewFeedback.group_id.in_(group_ids))
            .group_by(CrewFeedback.crew_id, CrewFeedback.rating)
        )
        result = await self.session.execute(stmt)
        counts: Dict[str, Dict[str, int]] = {}
        for crew_id, rating, n in result.all():
            entry = counts.setdefault(str(crew_id), {"up": 0, "down": 0})
            if rating in ("up", "down"):
                entry[rating] = int(n)
        return [
            {"crew_id": cid, "up": c["up"], "down": c["down"]}
            for cid, c in counts.items()
        ]
