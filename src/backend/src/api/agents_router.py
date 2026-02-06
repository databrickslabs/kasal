import logging
from typing import Annotated, List

from fastapi import APIRouter, Depends, Path, status
from sqlalchemy.exc import IntegrityError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ConflictError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.models.agent import Agent
from src.schemas.agent import Agent as AgentSchema
from src.schemas.agent import AgentCreate, AgentLimitedUpdate, AgentUpdate
from src.services.agent_service import AgentService

router = APIRouter(
    prefix="/agents",
    tags=["agents"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)


async def get_agent_service(session: SessionDep) -> AgentService:
    """
    Dependency provider for AgentService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        AgentService instance with injected session
    """
    return AgentService(session=session)


# Type alias for cleaner function signatures
AgentServiceDep = Annotated[AgentService, Depends(get_agent_service)]


@router.post("", response_model=AgentSchema, status_code=status.HTTP_201_CREATED)
async def create_agent(
    agent_in: AgentCreate,
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Create a new agent with group isolation.
    Only Editors and Admins can create agents.

    Args:
        agent_in: Agent data for creation
        service: Agent service injected by dependency
        group_context: Group context from headers

    Returns:
        Created agent
    """
    # Check permissions - only editors and admins can create agents
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can create agents")

    if group_context and group_context.is_valid():
        return await service.create_with_group(agent_in, group_context)
    else:
        raise BadRequestError("No valid group context provided")


@router.get("", response_model=List[AgentSchema])
async def list_agents(
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Retrieve all agents for the current group.

    Args:
        service: Agent service injected by dependency
        group_context: Group context from headers

    Returns:
        List of agents for the current group
    """
    if group_context and group_context.is_valid():
        return await service.find_by_group(group_context)
    else:
        # If no context available, return empty list for security
        return []


@router.get("/{agent_id}", response_model=AgentSchema)
async def get_agent(
    agent_id: Annotated[str, Path(title="The ID of the agent to get")],
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Get a specific agent by ID with group isolation.

    Args:
        agent_id: ID of the agent to get
        service: Agent service injected by dependency
        group_context: Group context from headers

    Returns:
        Agent if found and belongs to user's group

    Raises:
        HTTPException: If agent not found or not authorized
    """
    agent = await service.get_with_group_check(agent_id, group_context)
    if not agent:
        raise NotFoundError("Agent not found")
    return agent


@router.put("/{agent_id}/full", response_model=AgentSchema)
async def update_agent_full(
    agent_id: Annotated[str, Path(title="The ID of the agent to update")],
    agent_in: AgentUpdate,
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Update all fields of an existing agent with group isolation.
    Only Editors and Admins can update agents.

    Args:
        agent_id: ID of the agent to update
        agent_in: Agent data for full update
        service: Agent service injected by dependency
        group_context: Group context from headers

    Returns:
        Updated agent

    Raises:
        HTTPException: If agent not found or not authorized
    """
    # Check permissions - only editors and admins can update agents
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can update agents")

    agent = await service.update_with_group_check(agent_id, agent_in, group_context)
    if not agent:
        raise NotFoundError("Agent not found")
    return agent


@router.put("/{agent_id}", response_model=AgentSchema)
async def update_agent(
    agent_id: Annotated[str, Path(title="The ID of the agent to update")],
    agent_in: AgentLimitedUpdate,
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Update limited fields of an existing agent with group isolation.
    Only Editors and Admins can update agents.

    Args:
        agent_id: ID of the agent to update
        agent_in: Agent data for limited update
        service: Agent service injected by dependency
        group_context: Group context from headers

    Returns:
        Updated agent

    Raises:
        HTTPException: If agent not found or not authorized
    """
    # Check permissions - only editors and admins can update agents
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can update agents")

    agent = await service.update_limited_with_group_check(
        agent_id, agent_in, group_context
    )
    if not agent:
        raise NotFoundError("Agent not found")
    return agent


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: Annotated[str, Path(title="The ID of the agent to delete")],
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete an agent with group isolation.
    Only Editors and Admins can delete agents.

    Args:
        agent_id: ID of the agent to delete
        service: Agent service injected by dependency
        group_context: Group context from headers

    Raises:
        HTTPException: If agent not found or not authorized
    """
    # Check permissions - only editors and admins can delete agents
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can delete agents")

    deleted = await service.delete_with_group_check(agent_id, group_context)
    if not deleted:
        raise NotFoundError("Agent not found")


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_agents(
    service: AgentServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete all agents for the current group.
    Only Admins can delete all agents.

    Args:
        service: Agent service injected by dependency
        group_context: Group context from headers
    """
    # Check permissions - only admins can delete all agents
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can delete all agents")

    try:
        await service.delete_all_for_group(group_context)
    except IntegrityError as ie:
        logger.warning(f"Attempted to delete agents referenced by tasks: {ie}")
        raise ConflictError("Cannot delete agents because some are still referenced by tasks. Please delete or reassign the associated tasks first.")
