"""Publications, for the WORKSPACE's own UI.

Distinct from the external surfaces on purpose. ``/mcp/v1`` and ``/a2a/v1``
answer external agents and shape their output for them — an MCP tool result
carries a name and a description because that is what a calling agent selects
on, and deliberately not internal ids.

The catalogue needs the opposite: entity ids, so it can mark the right cards.
Driving it off the MCP tool result meant the UI was pretending to be an external
agent and reading a payload shaped for someone else — which broke the moment the
tool result did not happen to carry an id.

Group-scoped like every other internal endpoint, through the same repository the
external surfaces read.
"""

import logging
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.crew_publication import CrewPublicationResponse
from src.services.external.publication import PublicationService

router = APIRouter(
    prefix="/publications",
    tags=["publications"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


def get_publication_service(session: SessionDep) -> PublicationService:
    return PublicationService(session)


@router.get("", response_model=List[CrewPublicationResponse])
async def list_publications(
    service: Annotated[PublicationService, Depends(get_publication_service)],
    group_context: GroupContextDep,
    entity_type: Annotated[
        str | None, Query(description='Filter to "crew" or "flow".')
    ] = None,
):
    """Everything this workspace has published, with entity ids.

    One request for the whole catalogue rather than one per card: a workspace
    with fifty crews should not make fifty round trips just to draw an icon.
    """
    rows = await service.repository.list_published_for_group(
        group_ids=group_context.group_ids or []
    )
    if entity_type:
        rows = [r for r in rows if (r.entity_type or "crew") == entity_type]
    return [CrewPublicationResponse.model_validate(row) for row in rows]
