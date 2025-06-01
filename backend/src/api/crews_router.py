from typing import Annotated, List, Dict, Any
import logging
import json
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import ValidationError

from src.core.dependencies import SessionDep
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
):
    """
    Retrieve all crews.
    
    Args:
        service: Crew service injected by dependency
        
    Returns:
        List of crews
    """
    try:
        crews = await service.find_all()
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
):
    """
    Get a specific crew by ID.
    
    Args:
        crew_id: ID of the crew to get
        service: Crew service injected by dependency
        
    Returns:
        Crew if found
        
    Raises:
        HTTPException: If crew not found
    """
    try:
        crew = await service.get(crew_id)
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
):
    """
    Create a new crew.
    
    Args:
        crew_in: Crew data for creation
        service: Crew service injected by dependency
        
    Returns:
        Created crew
    """
    try:
        # Use the custom create method to handle complex JSON serialization
        crew = await service.create_crew(crew_in)
        
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
):
    """
    Update a crew.
    
    Args:
        crew_id: ID of the crew to update
        crew_update: Crew data for update (only provided fields will be updated)
        service: Crew service injected by dependency
        
    Returns:
        Updated crew
        
    Raises:
        HTTPException: If crew not found
    """
    try:
        updated_crew = await service.update_with_partial_data(crew_id, crew_update)
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
):
    """
    Delete a crew.
    
    Args:
        crew_id: ID of the crew to delete
        service: Crew service injected by dependency
        
    Raises:
        HTTPException: If crew not found
    """
    try:
        deleted = await service.delete(crew_id)
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
):
    """
    Delete all crews.
    
    Args:
        service: Crew service injected by dependency
    """
    try:
        await service.delete_all()
    except Exception as e:
        logger.error(f"Error deleting all crews: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 