"""Exact workspace lookup: a missing opt-in never inherits a global setting."""

from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.decision_config import DecisionConfig


class DecisionConfigRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, group_id: str) -> Optional[DecisionConfig]:
        return await self.session.get(DecisionConfig, group_id)

    async def save(self, group_id: str, enabled: bool) -> DecisionConfig:
        row = await self.get(group_id)
        if row is None:
            row = DecisionConfig(group_id=group_id)
            self.session.add(row)
        row.enabled = enabled
        await self.session.flush()
        return row
