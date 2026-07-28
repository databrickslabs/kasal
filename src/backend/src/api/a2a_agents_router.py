"""Remote A2A agents — the workspace's own configuration UI.

The outbound registry, not the inbound protocol. ``/a2a/v1`` answers external
agents; this is where an operator attaches one for Kasal's agents to call, and
it behaves like every other internal endpoint: group-scoped, role-checked,
domain exceptions.
"""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.schemas.a2a_agent import (
    A2AAgentCreate,
    A2AAgentListResponse,
    A2AAgentResponse,
    A2AAgentUpdate,
    A2AConnectionTest,
)
from src.services.a2a import client as a2a_client
from src.services.a2a_agent_service import A2AAgentService

router = APIRouter(
    prefix="/a2a-agents",
    tags=["a2a-agents"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

#: Attaching a remote agent means storing a credential and pointing Kasal at a
#: URL it will call with the workspace's data. Admin and Editor configure;
#: Operator runs what exists.
CONFIG_ROLES = ["admin", "editor"]


def get_service(session: SessionDep) -> A2AAgentService:
    return A2AAgentService(session)


ServiceDep = Annotated[A2AAgentService, Depends(get_service)]


def _require_config_role(group_context) -> None:
    if not check_role_in_context(group_context, CONFIG_ROLES):
        raise ForbiddenError(
            "Only workspace admins and editors can configure remote agents."
        )


def _to_response(agent) -> A2AAgentResponse:
    """Row -> response. Skills come from the cached card, the key never does."""
    return A2AAgentResponse(
        id=agent.id,
        name=agent.name,
        card_url=agent.card_url,
        description=agent.description,
        auth_type=agent.auth_type or "obo",
        enabled=bool(agent.enabled),
        global_enabled=bool(agent.global_enabled),
        timeout_seconds=agent.timeout_seconds or 300,
        has_api_key=bool(agent.encrypted_api_key),
        skills=a2a_client.skills_of(agent.cached_card or {}),
        card_fetched_at=agent.card_fetched_at,
        last_error=agent.last_error,
        created_at=agent.created_at,
        updated_at=agent.updated_at,
    )


@router.get("", response_model=A2AAgentListResponse)
async def list_agents(service: ServiceDep, group_context: GroupContextDep):
    """Remote agents this workspace has attached."""
    agents = await service.list_agents(group_context)
    return A2AAgentListResponse(
        agents=[_to_response(a) for a in agents], count=len(agents)
    )


@router.post("", response_model=A2AAgentResponse, status_code=status.HTTP_201_CREATED)
async def create_agent(
    body: A2AAgentCreate, service: ServiceDep, group_context: GroupContextDep
):
    """Attach a remote agent. Its card is fetched immediately.

    A 201 with ``last_error`` set is the normal outcome for a URL that does not
    answer: the row is worth keeping so the operator can correct it, and
    refusing to save would lose what they typed.
    """
    _require_config_role(group_context)
    try:
        agent = await service.create_agent(body, group_context)
    except ValueError as exc:
        raise BadRequestError(str(exc))
    return _to_response(agent)


@router.put("/{agent_id}", response_model=A2AAgentResponse)
async def update_agent(
    agent_id: int,
    body: A2AAgentUpdate,
    service: ServiceDep,
    group_context: GroupContextDep,
):
    _require_config_role(group_context)
    try:
        agent = await service.update_agent(agent_id, body, group_context)
    except ValueError as exc:
        raise BadRequestError(str(exc))
    if not agent:
        raise NotFoundError(f"No remote agent {agent_id}.")
    return _to_response(agent)


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: int, service: ServiceDep, group_context: GroupContextDep
):
    _require_config_role(group_context)
    if not await service.delete_agent(agent_id, group_context):
        raise NotFoundError(f"No remote agent {agent_id}.")


@router.post("/{agent_id}/test", response_model=A2AConnectionTest)
async def test_agent(
    agent_id: int, service: ServiceDep, group_context: GroupContextDep
):
    """Fetch the card now and report what happened.

    Answers 200 with ``connected: false`` rather than an error status: an
    unreachable remote is a result the operator asked for, not a failure of
    their request.
    """
    result = await service.test_connection(agent_id, group_context)
    if result is None:
        raise NotFoundError(f"No remote agent {agent_id}.")
    return result
