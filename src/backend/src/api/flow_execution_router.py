"""
API endpoints for flow executions.
"""
import uuid
from typing import Any, Dict, Optional, Union

from fastapi import APIRouter, Depends, status

from src.core.exceptions import BadRequestError, NotFoundError
from pydantic import BaseModel

from src.core.dependencies import GroupContextDep, get_db
from src.engines.crewai.crewai_flow_service import CrewAIFlowService

router = APIRouter(
    prefix="/flow-executions",
    tags=["flow executions"],
    responses={404: {"description": "Not found"}},
)


class FlowExecutionRequest(BaseModel):
    """Request model for flow execution"""

    flow_id: Union[str, int, uuid.UUID]
    job_id: str
    run_name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    # Checkpoint resume fields
    resume_from_flow_uuid: Optional[str] = None  # CrewAI state.id to resume from
    resume_from_execution_id: Optional[
        int
    ] = None  # Execution ID of checkpoint to resume


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def execute_flow(
    request: FlowExecutionRequest, group_context: GroupContextDep, db=Depends(get_db)
):
    """
    Start a flow execution asynchronously.

    Args:
        request: Flow execution request details

    Returns:
        Flow execution details
    """
    # Use the CrewAIFlowService with database session
    service = CrewAIFlowService(db)

    result = await service.run_flow(
        flow_id=request.flow_id,
        job_id=request.job_id,
        run_name=request.run_name,
        config=request.config,
        resume_from_flow_uuid=request.resume_from_flow_uuid,
        resume_from_execution_id=request.resume_from_execution_id,
    )

    if (
        not result.get("success", True) is False
    ):  # Assume success unless explicitly False
        return result
    else:
        raise BadRequestError(result.get("error", "Flow execution failed"))


@router.get("/{execution_id}")
async def get_flow_execution(
    execution_id: int, group_context: GroupContextDep, db=Depends(get_db)
):
    """
    Get details of a flow execution.

    Args:
        execution_id: ID of the flow execution

    Returns:
        Flow execution details
    """
    # Use the CrewAIFlowService with database session
    service = CrewAIFlowService(db)

    result = await service.get_flow_execution(execution_id)

    if (
        not result.get("success", True) is False
    ):  # Assume success unless explicitly False
        # If result contains an 'execution' key, return that, otherwise return the whole result
        return result.get("execution", result)
    else:
        raise NotFoundError(result.get("error", "Flow execution not found"))


@router.get("/by-flow/{flow_id}")
async def get_flow_executions_by_flow(
    flow_id: str, group_context: GroupContextDep, db=Depends(get_db)
):
    """
    Get all executions for a specific flow.

    Args:
        flow_id: ID of the flow

    Returns:
        List of flow executions
    """
    # Use the CrewAIFlowService with database session
    service = CrewAIFlowService(db)

    result = await service.get_flow_executions_by_flow(flow_id)

    if (
        not result.get("success", True) is False
    ):  # Assume success unless explicitly False
        # If result contains an 'executions' key, return that, otherwise return the whole result
        return result.get("executions", result)
    else:
        raise NotFoundError(result.get("error", "Flow not found"))
