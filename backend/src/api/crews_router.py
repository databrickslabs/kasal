from typing import Annotated, List, Dict, Any
import logging
import json
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import ValidationError

from src.core.dependencies import SessionDep, GroupContextDep
from src.schemas.crew import CrewCreate, CrewUpdate, CrewResponse
from src.services.crew_service import CrewService

router = APIRouter(
    prefix="/crews",
    tags=["crews"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get CrewService
def get_crew_service(session: SessionDep) -> CrewService:
    return CrewService(session)


@router.get("", response_model=List[CrewResponse])
async def list_crews(
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Retrieve all crews for the current group.
    
    Args:
        service: Crew service injected by dependency
        group_context: Group context from headers
        
    Returns:
        List of crews for current group
    """
    try:
        crews = await service.find_by_group(group_context)
        return [
            CrewResponse(
                id=crew.id,
                name=crew.name,
                agent_ids=crew.agent_ids,
                task_ids=crew.task_ids,
                nodes=crew.nodes or [],
                edges=crew.edges or [],
                created_at=crew.created_at.isoformat(),
                updated_at=crew.updated_at.isoformat()
            )
            for crew in crews
        ]
    except Exception as e:
        logger.error(f"Error listing crews: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{crew_id}", response_model=CrewResponse)
async def get_crew(
    crew_id: Annotated[UUID, Path(title="The ID of the crew to get")],
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Get a specific crew by ID for the current group.
    
    Args:
        crew_id: ID of the crew to get
        service: Crew service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Crew if found and belongs to group
        
    Raises:
        HTTPException: If crew not found or doesn't belong to group
    """
    try:
        crew = await service.get_by_group(crew_id, group_context)
        if not crew:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Crew not found",
            )
        return CrewResponse(
            id=crew.id,
            name=crew.name,
            agent_ids=crew.agent_ids,
            task_ids=crew.task_ids,
            nodes=crew.nodes or [],
            edges=crew.edges or [],
            created_at=crew.created_at.isoformat(),
            updated_at=crew.updated_at.isoformat()
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting crew: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=CrewResponse, status_code=status.HTTP_201_CREATED)
async def create_crew(
    crew_in: CrewCreate,
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Create a new crew for the current group.
    
    Args:
        crew_in: Crew data for creation
        service: Crew service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Created crew
    """
    try:
        # Use the group-aware create method
        crew = await service.create_with_group(crew_in, group_context)
        
        # Format the response
        return CrewResponse(
            id=crew.id,
            name=crew.name,
            agent_ids=crew.agent_ids,
            task_ids=crew.task_ids,
            nodes=crew.nodes or [],
            edges=crew.edges or [],
            created_at=crew.created_at.isoformat(),
            updated_at=crew.updated_at.isoformat()
        )
    except ValidationError as e:
        logger.error(f"Validation error: {e.json()}")
        raise HTTPException(status_code=422, detail=json.loads(e.json()))
    except Exception as e:
        logger.error(f"Error creating crew: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/debug")
async def debug_crew_data(
    crew_in: CrewCreate,
):
    """
    Debug endpoint to validate crew data structure without saving.
    
    Args:
        crew_in: Crew data to validate
        
    Returns:
        Validation result
    """
    try:
        # Convert to dict and back to ensure it's valid
        data_dict = crew_in.model_dump()
        logger.info("Data validation successful")
        logger.info(f"Crew name: {data_dict['name']}")
        logger.info(f"Agent IDs: {data_dict['agent_ids']}")
        logger.info(f"Task IDs: {data_dict['task_ids']}")
        logger.info(f"Number of nodes: {len(data_dict['nodes'])}")
        logger.info(f"Number of edges: {len(data_dict['edges'])}")
        return {
            "status": "success",
            "message": "Data validation successful",
            "data": {
                "name": data_dict["name"],
                "agent_ids": data_dict["agent_ids"],
                "task_ids": data_dict["task_ids"],
                "node_count": len(data_dict["nodes"]),
                "edge_count": len(data_dict["edges"])
            }
        }
    except ValidationError as e:
        logger.error(f"Validation error: {e.json()}")
        return {
            "status": "error",
            "message": "Validation failed",
            "errors": json.loads(e.json())
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }


@router.put("/{crew_id}", response_model=CrewResponse)
async def update_crew(
    crew_id: Annotated[UUID, Path(title="The ID of the crew to update")],
    crew_update: CrewUpdate,
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Update a crew for the current group.
    
    Args:
        crew_id: ID of the crew to update
        crew_update: Crew data for update (only provided fields will be updated)
        service: Crew service injected by dependency
        group_context: Group context from headers
        
    Returns:
        Updated crew
        
    Raises:
        HTTPException: If crew not found or doesn't belong to group
    """
    try:
        updated_crew = await service.update_with_partial_data_by_group(crew_id, crew_update, group_context)
        if not updated_crew:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Crew not found",
            )
        return CrewResponse(
            id=updated_crew.id,
            name=updated_crew.name,
            agent_ids=updated_crew.agent_ids,
            task_ids=updated_crew.task_ids,
            nodes=updated_crew.nodes or [],
            edges=updated_crew.edges or [],
            created_at=updated_crew.created_at.isoformat(),
            updated_at=updated_crew.updated_at.isoformat()
        )
    except HTTPException as he:
        raise he
    except ValidationError as e:
        logger.error(f"Validation error: {e.json()}")
        raise HTTPException(status_code=422, detail=json.loads(e.json()))
    except Exception as e:
        logger.error(f"Error updating crew: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{crew_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_crew(
    crew_id: Annotated[UUID, Path(title="The ID of the crew to delete")],
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Delete a crew for the current group.
    
    Args:
        crew_id: ID of the crew to delete
        service: Crew service injected by dependency
        group_context: Group context from headers
        
    Raises:
        HTTPException: If crew not found or doesn't belong to group
    """
    try:
        deleted = await service.delete_by_group(crew_id, group_context)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Crew not found",
            )
    except Exception as e:
        logger.error(f"Error deleting crew: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_crews(
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Delete all crews for the current group.
    
    Args:
        service: Crew service injected by dependency
        group_context: Group context from headers
    """
    try:
        await service.delete_all_by_group(group_context)
    except Exception as e:
        logger.error(f"Error deleting all crews: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 