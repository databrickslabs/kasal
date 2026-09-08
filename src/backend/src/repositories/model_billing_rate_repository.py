"""Async, teamspace-scoped pricing storage."""

from sqlalchemy import select

from src.core.base_repository import BaseRepository
from src.models.model_billing_rate import ModelBillingRate


class ModelBillingRateRepository(BaseRepository[ModelBillingRate]):
    def __init__(self, session):
        super().__init__(ModelBillingRate, session)

    async def for_group(self, group_id: str):
        result = await self.session.execute(
            select(ModelBillingRate)
            .where(ModelBillingRate.group_id == group_id)
            .order_by(ModelBillingRate.model)
        )
        return result.scalars().all()

    async def put(self, group_id: str, values: dict):
        row = await self.session.get(ModelBillingRate, (group_id, values["model"]))
        if row is None:
            row = ModelBillingRate(group_id=group_id, **values)
            self.session.add(row)
        else:
            for key, value in values.items():
                setattr(row, key, value)
        await self.session.flush()
        return row
