from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException, Path, status
import logging
from sqlalchemy.exc import IntegrityError

from src.core.dependencies import SessionDep, get_service
from src.models.agent import Agent
from src.repositories.agent_repository import AgentRepository
from src.schemas.agent import Agent as AgentSchema
from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate
from src.services.agent_service import AgentService

router = APIRouter(
    prefix="/agents",
    tags=["agents"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get AgentService
get_agent_service = get_service(AgentService, AgentRepository, Agent)


@router.post("", response_model=AgentSchema, status_code=status.HTTP_201_CREATED)
async def create_agent(
    agent_in: AgentCreate,
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Create a new agent.
    
    Args:
        agent_in: Agent data for creation
        service: Agent service injected by dependency
        
    Returns:
        Created agent
    """
    try:
        return await service.create(agent_in)
    except Exception as e:
        logger.error(f"Error creating agent: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[AgentSchema])
async def list_agents(
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Retrieve all agents.
    
    Args:
        service: Agent service injected by dependency
        
    Returns:
        List of agents
    """
    try:
        return await service.find_all()
    except Exception as e:
        logger.error(f"Error listing agents: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{agent_id}", response_model=AgentSchema)
async def get_agent(
    agent_id: Annotated[str, Path(title="The ID of the agent to get")],
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Get a specific agent by ID.
    
    Args:
        agent_id: ID of the agent to get
        service: Agent service injected by dependency
        
    Returns:
        Agent if found
        
    Raises:
        HTTPException: If agent not found
    """
    try:
        agent = await service.get(agent_id)
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Agent not found",
            )
        return agent
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting agent: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{agent_id}/full", response_model=AgentSchema)
async def update_agent_full(
    agent_id: Annotated[str, Path(title="The ID of the agent to update")],
    agent_in: AgentUpdate,
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Update all fields of an existing agent.
    
    Args:
        agent_id: ID of the agent to update
        agent_in: Agent data for full update
        service: Agent service injected by dependency
        
    Returns:
        Updated agent
        
    Raises:
        HTTPException: If agent not found
    """
    try:
        agent = await service.update_with_partial_data(agent_id, agent_in)
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Agent not found",
            )
        return agent
    except Exception as e:
        logger.error(f"Error updating agent: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{agent_id}", response_model=AgentSchema)
async def update_agent(
    agent_id: Annotated[str, Path(title="The ID of the agent to update")],
    agent_in: AgentLimitedUpdate,
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Update limited fields of an existing agent.
    
    Args:
        agent_id: ID of the agent to update
        agent_in: Agent data for limited update
        service: Agent service injected by dependency
        
    Returns:
        Updated agent
        
    Raises:
        HTTPException: If agent not found
    """
    try:
        agent = await service.update_limited_fields(agent_id, agent_in)
        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Agent not found",
            )
        return agent
    except Exception as e:
        logger.error(f"Error updating agent: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: Annotated[str, Path(title="The ID of the agent to delete")],
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Delete an agent.
    
    Args:
        agent_id: ID of the agent to delete
        service: Agent service injected by dependency
        
    Raises:
        HTTPException: If agent not found
    """
    try:
        deleted = await service.delete(agent_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Agent not found",
            )
    except Exception as e:
        logger.error(f"Error deleting agent: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_agents(
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    """
    Delete all agents.
    
    Args:
        service: Agent service injected by dependency
    """
    try:
        await service.delete_all()
    except IntegrityError as ie:
        logger.warning(f"Attempted to delete agents referenced by tasks: {ie}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, 
            detail="Cannot delete agents because some are still referenced by tasks. Please delete or reassign the associated tasks first."
        )
    except Exception as e:
        logger.error(f"Error deleting all agents: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 