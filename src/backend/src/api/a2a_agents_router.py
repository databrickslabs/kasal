"""Remote A2A agents — the workspace's own configuration UI.

The outbound registry, not the inbound protocol. ``/a2a/v1`` answers external
agents; this is where a remote is attached for Kasal's agents to call.

Two gates, mirroring MCP servers exactly:

- ``/base`` and the write endpoints are **Kasal admin** only. A remote agent row
  carries an outbound URL and a credential, so registering one is a
  system-administration act.
- ``/{id}/workspace-enabled`` is **workspace admin**. They can turn a
  globally-available agent on or off for their own workspace and nothing else —
  no adding, no editing a URL, no reading a key.
"""

import logging
from typing import Annotated, Any, Dict

from fastapi import APIRouter, Body, Depends, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context, get_effective_role
from src.schemas.a2a_agent import (
    A2AAgentCreate,
    A2AAgentListResponse,
    A2AAgentResponse,
    A2AAgentUpdate,
    A2AConnectionTest,
)
from src.services.a2a.a2a_client import client as a2a_client
from src.services.a2a.a2a_client.agent_service import A2AAgentService

router = APIRouter(
    prefix="/a2a-agents",
    tags=["a2a-agents"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


def get_service(session: SessionDep) -> A2AAgentService:
    return A2AAgentService(session)


ServiceDep = Annotated[A2AAgentService, Depends(get_service)]


def _is_global_admin(group_context) -> bool:
    """Whether the caller may manage GLOBAL remote agents.

    The same gate ``mcp_router`` uses, and the same reasoning: a global row is
    available to every workspace, so changing one is system administration.
    """
    try:
        role = get_effective_role(group_context) if group_context else None
        if role and role.lower() == "admin":
            return True
    except Exception:  # noqa: BLE001
        pass
    return bool(
        group_context is not None
        and getattr(
            getattr(group_context, "current_user", None), "is_system_admin", False
        )
    )


def _require_global_admin(group_context) -> None:
    if not _is_global_admin(group_context):
        raise ForbiddenError("Only Kasal admins can manage global remote agents.")


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
        group_id=agent.group_id,
        created_at=agent.created_at,
        updated_at=agent.updated_at,
    )


def _enabled_flag(payload: Dict[str, Any]) -> bool:
    value = payload.get("enabled")
    if not isinstance(value, bool):
        raise BadRequestError("'enabled' must be true or false.")
    return value


@router.get("", response_model=A2AAgentListResponse)
async def list_agents(service: ServiceDep, group_context: GroupContextDep):
    """What this workspace sees: globally-available agents plus its own rows.

    A row with no ``group_id`` is an inherited global one — toggleable here,
    editable only in the global view.
    """
    agents = await service.list_agents(group_context)
    return A2AAgentListResponse(
        agents=[_to_response(a) for a in agents], count=len(agents)
    )


@router.get("/base", response_model=A2AAgentListResponse)
async def list_base_agents(service: ServiceDep, group_context: GroupContextDep):
    """The Kasal admin catalogue. Registered here, offered to workspaces."""
    _require_global_admin(group_context)
    agents = await service.list_base_agents()
    return A2AAgentListResponse(
        agents=[_to_response(a) for a in agents], count=len(agents)
    )


@router.post("", response_model=A2AAgentResponse, status_code=status.HTTP_201_CREATED)
async def create_agent(
    body: A2AAgentCreate, service: ServiceDep, group_context: GroupContextDep
):
    """Register a remote agent globally. Its card is fetched immediately.

    A 201 with ``last_error`` set is the normal outcome for a URL that does not
    answer: the row is worth keeping so the admin can correct it, and refusing
    to save would lose what they typed.
    """
    _require_global_admin(group_context)
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
    _require_global_admin(group_context)
    try:
        agent = await service.update_agent(agent_id, body, group_context)
    except ValueError as exc:
        raise BadRequestError(str(exc))
    if not agent:
        raise NotFoundError(f"No global remote agent {agent_id}.")
    return _to_response(agent)


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: int, service: ServiceDep, group_context: GroupContextDep
):
    """Remove a global agent, and every workspace's opt-in along with it."""
    _require_global_admin(group_context)
    if not await service.delete_agent(agent_id):
        raise NotFoundError(f"No global remote agent {agent_id}.")


@router.patch("/{agent_id}/global-availability", response_model=A2AAgentResponse)
async def set_global_availability(
    agent_id: int,
    service: ServiceDep,
    group_context: GroupContextDep,
    payload: Annotated[Dict[str, Any], Body()],
):
    """Kasal admin: offer a remote agent to all workspaces, or withdraw it.

    Withdrawing cascades immediately, whatever workspaces had enabled.
    """
    _require_global_admin(group_context)
    agent = await service.set_global_availability(agent_id, _enabled_flag(payload))
    if not agent:
        raise NotFoundError(f"No global remote agent {agent_id}.")
    return _to_response(agent)


@router.patch("/{agent_id}/workspace-enabled", response_model=A2AAgentResponse)
async def set_workspace_enabled(
    agent_id: int,
    service: ServiceDep,
    group_context: GroupContextDep,
    payload: Annotated[Dict[str, Any], Body()],
):
    """Workspace admin: turn an agent on or off for THIS workspace only.

    Toggling an inherited global agent creates a workspace-scoped copy carrying
    that choice. The global row is never touched, so one workspace's decision
    cannot reach another's.
    """
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError(
            "Only workspace admins can change remote agents for a workspace."
        )
    group_id = getattr(group_context, "primary_group_id", None)
    if not group_id:
        raise BadRequestError("No workspace selected.")

    agent = await service.set_enabled_for_group(
        agent_id, group_id, _enabled_flag(payload)
    )
    if not agent:
        raise NotFoundError(f"No remote agent {agent_id}.")
    return _to_response(agent)


@router.post("/{agent_id}/test", response_model=A2AConnectionTest)
async def test_agent(
    agent_id: int, service: ServiceDep, group_context: GroupContextDep
):
    """Fetch the card now and report what happened.

    Answers 200 with ``connected: false`` rather than an error status: an
    unreachable remote is a result the admin asked for, not a failure of their
    request.
    """
    _require_global_admin(group_context)
    result = await service.test_connection(agent_id, group_context)
    if result is None:
        raise NotFoundError(f"No remote agent {agent_id}.")
    return result
