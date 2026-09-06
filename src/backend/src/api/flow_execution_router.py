"""
API endpoints for flow executions.
"""


from fastapi import APIRouter, Depends, status

from src.schemas.flow_execution import FlowExecutionRequest
from src.core.dependencies import GroupContextDep, get_db
from src.core.exceptions import BadRequestError, NotFoundError
from src.services.flow_builder.kasal_flow_service import KasalFlowService

router = APIRouter(
    prefix="/flow-executions",
    tags=["flow executions"],
    responses={404: {"description": "Not found"}},
)


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
    # Use the KasalFlowService with database session
    service = KasalFlowService(db)

    # SECURITY: pass the caller's group context so the flow's group ownership is
    # enforced and the execution record is tagged with the caller's group_id.
    result = await service.run_flow(
        flow_id=request.flow_id,
        job_id=request.job_id,
        run_name=request.run_name,
        config=request.config,
        group_context=group_context,
        user_token=group_context.access_token if group_context else None,
        resume_from_flow_uuid=request.resume_from_flow_uuid,
        resume_from_execution_id=request.resume_from_execution_id,
    )

    if (
        result.get("success", True) is not False
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
    # Use the KasalFlowService with database session
    service = KasalFlowService(db)

    # SECURITY: scope to the caller's groups so a user cannot read another
    # tenant's flow execution by enumerating IDs (returns 404 on mismatch).
    group_ids = group_context.group_ids if group_context else None
    result = await service.get_flow_execution(execution_id, group_ids=group_ids)

    if (
        result.get("success", True) is not False
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
    # Use the KasalFlowService with database session
    service = KasalFlowService(db)

    # SECURITY: scope to the caller's groups (cross-tenant isolation)
    group_ids = group_context.group_ids if group_context else None
    result = await service.get_flow_executions_by_flow(flow_id, group_ids=group_ids)

    if (
        result.get("success", True) is not False
    ):  # Assume success unless explicitly False
        # If result contains an 'executions' key, return that, otherwise return the whole result
        return result.get("executions", result)
    else:
        raise NotFoundError(result.get("error", "Flow not found"))
