from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.flow_state import FlowState


class FlowStateRepository(BaseRepository[FlowState]):
    """Repository for CrewAI flow-state checkpoints (``@persist``).

    Inherits base CRUD from :class:`BaseRepository`; the session (unit of work) is
    owned and committed by the caller.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(FlowState, session)

    async def add_state(self, flow_uuid: str, method_name: str, state_json: str) -> FlowState:
        """Append a new flow-state snapshot (history is kept; latest wins on load)."""
        db_obj = FlowState(
            flow_uuid=flow_uuid,
            method_name=method_name,
            state_json=state_json,
        )
        self.session.add(db_obj)
        await self.session.flush()
        return db_obj

    async def get_latest_state_json(self, flow_uuid: str) -> Optional[str]:
        """Return the most recent serialized state for a flow UUID, or None."""
        query = (
            select(FlowState.state_json)
            .where(FlowState.flow_uuid == flow_uuid)
            .order_by(desc(FlowState.id))
            .limit(1)
        )
        result = await self.session.execute(query)
        return result.scalars().first()
