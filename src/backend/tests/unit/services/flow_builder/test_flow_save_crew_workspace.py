"""A saved flow links only a crew of its own workspace.

``crew_id`` on a flow comes from the request. At save time it is kept when the
crew is in the workspace the flow is saved into (the caller's current one), and
dropped, as for a crew that does not exist, otherwise.

Real FlowService and CrewService over a real SQLite database.
"""

import pytest

from src.models.flow import Flow
from src.schemas.flow import FlowCreate
from src.services.flow_builder.flow_service import FlowService
from tests.unit.services.catalog.crew_workspaces import (
    A_CREW,
    B_CREW,
    CALLER_A,
    seeded_database,
)


async def _save(maker, crew_id):
    async with maker() as session:
        connection = await session.connection()
        await connection.run_sync(
            lambda sync: Flow.__table__.create(sync, checkfirst=True)
        )
        flow = await FlowService(session).create_flow_with_group(
            FlowCreate(name=f"flow {crew_id}", crew_id=crew_id), CALLER_A
        )
        await session.commit()
        return flow


@pytest.mark.asyncio
async def test_a_flow_keeps_a_crew_of_its_workspace_and_drops_another():
    async with seeded_database() as maker:
        own = await _save(maker, A_CREW)
        other = await _save(maker, B_CREW)

    assert (own.group_id, own.crew_id) == (CALLER_A.primary_group_id, A_CREW)
    assert other.group_id == CALLER_A.primary_group_id
    assert other.crew_id is None
