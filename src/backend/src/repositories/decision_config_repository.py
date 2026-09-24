"""Exact workspace lookup: a missing opt-in never inherits a global setting."""

from src.models.decision_config import DecisionConfig


class DecisionConfigRepository:
    def __init__(self, session):
        self.session = session

    async def get(self, group_id: str):
        return await self.session.get(DecisionConfig, group_id)

    async def save(self, group_id: str, enabled: bool):
        row = await self.get(group_id)
        if row is None:
            row = DecisionConfig(group_id=group_id)
            self.session.add(row)
        row.enabled = enabled
        await self.session.flush()
        return row
