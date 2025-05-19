"""
API endpoints for flow executions.
"""
import logging
import uuid
from typing import Dict, Any, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from src.core.dependencies import get_db
from src.engines.crewai.crewai_flow_service import CrewAIFlowService

# Configure logger
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/flow-executions",
    tags=["flow executions"],
    responses={404: {"description": "Not found"}},
)


class FlowExecutionRequest(BaseModel):
    """Request model for flow execution"""
    flow_id: Union[str, int, uuid.UUID]
    job_id: str
    config: Optional[Dict[str, Any]] = None


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def execute_flow(
    request: FlowExecutionRequest
):
    """
    Start a flow execution asynchronously.
    
    Args:
        request: Flow execution request details
        
    Returns:
        Flow execution details
    """
    try:
        # Use the CrewAIFlowService instead of FlowRunnerService
        # This service will handle its own session management
        service = CrewAIFlowService()
        
        result = await service.run_flow(
            flow_id=request.flow_id,
            job_id=request.job_id,
            config=request.config
        )
        
        if not result.get("success", True) is False:  # Assume success unless explicitly False
            return result
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result.get("error", "Flow execution failed")
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing flow: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )


@router.get("/{execution_id}")
async def get_flow_execution(
    execution_id: int
):
    """
    Get details of a flow execution.
    
    Args:
        execution_id: ID of the flow execution
        
    Returns:
        Flow execution details
    """
    try:
        # Use the CrewAIFlowService
        service = CrewAIFlowService()
        
        result = await service.get_flow_execution(execution_id)
        
        if not result.get("success", True) is False:  # Assume success unless explicitly False
            # If result contains an 'execution' key, return that, otherwise return the whole result
            return result.get("execution", result)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result.get("error", "Flow execution not found")
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting flow execution: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )


@router.get("/by-flow/{flow_id}")
async def get_flow_executions_by_flow(
    flow_id: str
):
    """
    Get all executions for a specific flow.
    
    Args:
        flow_id: ID of the flow
        
    Returns:
        List of flow executions
    """
    try:
        # Use the CrewAIFlowService
        service = CrewAIFlowService()
        
        result = await service.get_flow_executions_by_flow(flow_id)
        
        if not result.get("success", True) is False:  # Assume success unless explicitly False
            # If result contains an 'executions' key, return that, otherwise return the whole result
            return result.get("executions", result)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result.get("error", "Flow not found")
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting flow executions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        ) 