from typing import List, Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.flow_state import FlowState


class FlowStateRepository(BaseRepository[FlowState]):
    """Repository for flow-state checkpoints (``@persist``).

    Inherits base CRUD from :class:`BaseRepository`; the session (unit of work) is
    owned and committed by the caller.

    The table is APPEND-ONLY: one row per completed method, newest wins on load.
    So the whole history of a run — and of a conversation, now that one lineage
    can span turns — is already stored. The reads below are what make it
    reachable: previously only ``get_latest_state_json`` existed, and everything
    older was written and never looked at again.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(FlowState, session)

    def _scope(self, query, group_id: Optional[str]):
        """Narrow a query to exactly one tenant. Never widen it.

        A flow's checkpoints hold everything the run saw — crew outputs, the
        user's messages, whatever the conversation carried — so a read that can
        reach another group's rows is a data leak, not an inconvenience.

        The match is EXACT in both directions:

        - a run that carries a group sees only that group's rows,
        - a run that carries none sees only rows that carry none.

        The symmetry is what closes the hole. An earlier version let NULL rows
        match every group, so that historical checkpoints stayed resumable — but
        that made one untenanted row readable from every teamspace, which is
        precisely what this column exists to prevent. Checkpoints written before
        the column existed are therefore unreadable from a tenanted run; they
        cannot be attributed to a group after the fact, and a run that fails to
        resume is a far better outcome than one that resumes on somebody else's
        state.
        """
        if group_id:
            return query.where(FlowState.group_id == group_id)
        return query.where(FlowState.group_id.is_(None))

    async def add_state(
        self,
        flow_uuid: str,
        method_name: str,
        state_json: str,
        group_id: Optional[str] = None,
    ) -> FlowState:
        """Append a new flow-state snapshot (history is kept; latest wins on load)."""
        db_obj = FlowState(
            flow_uuid=flow_uuid,
            method_name=method_name,
            state_json=state_json,
            group_id=group_id,
        )
        self.session.add(db_obj)
        await self.session.flush()
        return db_obj

    async def get_latest_state_json(
        self, flow_uuid: str, group_id: Optional[str] = None
    ) -> Optional[str]:
        """Return the most recent serialized state for a flow UUID, or None.

        Scoped to ``group_id`` — see :meth:`_scope`. A lineage id alone is not
        an authorisation to read a lineage.
        """
        query = self._scope(
            select(FlowState.state_json).where(FlowState.flow_uuid == flow_uuid),
            group_id,
        )
        query = query.order_by(desc(FlowState.id)).limit(1)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def get_history(
        self,
        flow_uuid: str,
        group_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[FlowState]:
        """Every checkpoint in a lineage, oldest first.

        Oldest first because this is read as a timeline — the order the methods
        completed in, and for a conversation the order the turns happened in.
        Bounded, because a long-lived thread's lineage grows without limit and
        nothing downstream wants all of it at once.
        """
        query = self._scope(
            select(FlowState).where(FlowState.flow_uuid == flow_uuid), group_id
        )
        query = query.order_by(FlowState.id).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())
