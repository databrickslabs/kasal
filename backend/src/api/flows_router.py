from typing import Annotated, Dict, List, Optional, Any
import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status

from src.core.dependencies import SessionDep
from src.schemas.flow import FlowCreate, FlowUpdate, FlowResponse
from src.services.flow_service import FlowService

router = APIRouter(
    prefix="/flows",
    tags=["flows"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Dependency to get FlowService
def get_flow_service(session: SessionDep) -> FlowService:
    return FlowService(session)


@router.get("", response_model=List[FlowResponse])
async def get_all_flows(
    service: Annotated[FlowService, Depends(get_flow_service)],
):
    """
    Retrieve all flows.
    
    Args:
        service: Flow service injected by dependency
        
    Returns:
        List of flows
    """
    try:
        flows = await service.get_all_flows()
        return [
            FlowResponse(
                id=flow.id,
                name=flow.name,
                crew_id=flow.crew_id,
                nodes=flow.nodes or [],
                edges=flow.edges or [],
                flow_config=flow.flow_config or {},
                created_at=flow.created_at.isoformat(),
                updated_at=flow.updated_at.isoformat()
            )
            for flow in flows
        ]
    except Exception as e:
        logger.error(f"Error listing flows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{flow_id}", response_model=FlowResponse)
async def get_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to get")],
    service: Annotated[FlowService, Depends(get_flow_service)],
):
    """
    Get a specific flow by ID.
    
    Args:
        flow_id: UUID of the flow to get
        service: Flow service injected by dependency
        
    Returns:
        Flow if found
        
    Raises:
        HTTPException: If flow not found
    """
    try:
        flow = await service.get_flow(flow_id)
        return FlowResponse(
            id=flow.id,
            name=flow.name,
            crew_id=flow.crew_id,
            nodes=flow.nodes or [],
            edges=flow.edges or [],
            flow_config=flow.flow_config or {},
            created_at=flow.created_at.isoformat(),
            updated_at=flow.updated_at.isoformat()
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting flow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=FlowResponse, status_code=status.HTTP_201_CREATED)
async def create_flow(
    flow_in: FlowCreate,
    service: Annotated[FlowService, Depends(get_flow_service)],
):
    """
    Create a new flow.
    
    Args:
        flow_in: Flow data for creation
        service: Flow service injected by dependency
        
    Returns:
        Created flow
    """
    try:
        flow = await service.create_flow(flow_in)
        return FlowResponse(
            id=flow.id,
            name=flow.name,
            crew_id=flow.crew_id,
            nodes=flow.nodes or [],
            edges=flow.edges or [],
            flow_config=flow.flow_config or {},
            created_at=flow.created_at.isoformat(),
            updated_at=flow.updated_at.isoformat()
        )
    except Exception as e:
        logger.error(f"Error creating flow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/debug", response_model=Dict)
async def debug_flow_data(
    flow_in: FlowCreate,
    service: Annotated[FlowService, Depends(get_flow_service)],
):
    """
    Debug endpoint to validate flow data without saving.
    
    Args:
        flow_in: Flow data to validate
        service: Flow service injected by dependency
        
    Returns:
        Validation result
    """
    return await service.validate_flow_data(flow_in)


@router.put("/{flow_id}", response_model=FlowResponse)
async def update_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to update")],
    flow_in: FlowUpdate,
    service: Annotated[FlowService, Depends(get_flow_service)],
):
    """
    Update a flow.
    
    Args:
        flow_id: UUID of the flow to update
        flow_in: Flow data for update
        service: Flow service injected by dependency
        
    Returns:
        Updated flow
        
    Raises:
        HTTPException: If flow not found
    """
    try:
        flow = await service.update_flow(flow_id, flow_in)
        return FlowResponse(
            id=flow.id,
            name=flow.name,
            crew_id=flow.crew_id,
            nodes=flow.nodes or [],
            edges=flow.edges or [],
            flow_config=flow.flow_config or {},
            created_at=flow.created_at.isoformat(),
            updated_at=flow.updated_at.isoformat()
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error updating flow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{flow_id}", status_code=status.HTTP_200_OK)
async def delete_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to delete")],
    service: Annotated[FlowService, Depends(get_flow_service)],
    force: Annotated[bool, Query(title="Force delete and remove associated executions")] = False,
):
    """
    Delete a flow.
    
    Args:
        flow_id: UUID of the flow to delete
        service: Flow service injected by dependency
        force: Parameter is kept for backward compatibility but ignored, force delete is always used
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If flow not found
    """
    logger.info(f"Force deleting flow {flow_id} with its executions")
    
    try:
        # Always use force delete to avoid foreign key constraint issues
        result = await service.force_delete_flow_with_executions(flow_id)
        
        # Log success and return response
        logger.info(f"Successfully deleted flow {flow_id}")
        return {"status": "success", "message": "Flow deleted successfully"}
        
    except HTTPException as he:
        # Pass through HTTP exceptions from the service
        logger.warning(f"HTTP error deleting flow {flow_id}: {he.detail}")
        raise
    except Exception as e:
        # Log and convert other exceptions to 500 errors
        error_msg = f"Unexpected error deleting flow {flow_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("", status_code=status.HTTP_200_OK)
async def delete_all_flows(
    service: Annotated[FlowService, Depends(get_flow_service)],
):
    """
    Delete all flows.
    
    Args:
        service: Flow service injected by dependency
        
    Returns:
        Success message
    """
    try:
        await service.delete_all_flows()
        return {"status": "success", "message": "All flows deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting all flows: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 