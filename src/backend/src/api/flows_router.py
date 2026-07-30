import logging
import uuid
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import ForbiddenError
from src.core.permissions import check_role_in_context
from src.repositories.execution_trace_repository import ExecutionTraceRepository
from src.schemas.crew_publication import (
    CrewPublicationCreate,
    CrewPublicationResponse,
    CrewPublicationUpdate,
)
from src.schemas.execution_history import (
    CheckpointInfo,
    CheckpointListResponse,
    CrewCheckpointInfo,
)
from src.schemas.flow import FlowCreate, FlowResponse, FlowUpdate
from src.services.execution.checkpointing.service import CheckpointService
from src.services.execution.history import ExecutionHistoryService
from src.services.external.publication import PublicationService
from src.services.flow_builder.flow_service import FlowService

router = APIRouter(
    prefix="/flows",
    tags=["flows"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


# Dependency to get FlowService
def get_flow_service(session: SessionDep) -> FlowService:
    return FlowService(session)


# Dependency to get ExecutionHistoryService
def get_execution_history_service(session: SessionDep) -> ExecutionHistoryService:
    return ExecutionHistoryService(session)


# Dependency to get ExecutionTraceRepository
def get_execution_trace_repository(session: SessionDep) -> ExecutionTraceRepository:
    return ExecutionTraceRepository(session)


# Dependency to get the shared CheckpointService
def get_checkpoint_service(session: SessionDep) -> CheckpointService:
    return CheckpointService(session)


def clean_null_values(obj: Any) -> Any:
    """
    Recursively remove all None/null values from dictionaries and lists.
    This cleans up the JSON response to avoid sending unnecessary null fields.
    """
    if isinstance(obj, dict):
        return {k: clean_null_values(v) for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        return [clean_null_values(item) for item in obj]
    else:
        return obj


@router.get("", response_model=List[FlowResponse])
async def get_all_flows(
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
):
    """
    Retrieve all flows for the current group.

    Args:
        service: Flow service injected by dependency
        group_context: Group context from headers

    Returns:
        List of flows for the current group
    """
    flows = await service.get_all_flows_for_group(group_context)
    return [
        FlowResponse(
            id=flow.id,
            name=flow.name,
            crew_id=flow.crew_id,
            nodes=clean_null_values(flow.nodes) or [],
            edges=clean_null_values(flow.edges) or [],
            flow_config=flow.flow_config or {},
            created_at=flow.created_at.isoformat(),
            updated_at=flow.updated_at.isoformat(),
        )
        for flow in flows
    ]


@router.get("/{flow_id}", response_model=FlowResponse)
async def get_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to get")],
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
):
    """
    Get a specific flow by ID with group isolation.

    Args:
        flow_id: UUID of the flow to get
        service: Flow service injected by dependency
        group_context: Group context from headers

    Returns:
        Flow if found and belongs to user's group

    Raises:
        HTTPException: If flow not found or not authorized
    """
    flow = await service.get_flow_with_group_check(flow_id, group_context)
    return FlowResponse(
        id=flow.id,
        name=flow.name,
        crew_id=flow.crew_id,
        nodes=clean_null_values(flow.nodes) or [],
        edges=clean_null_values(flow.edges) or [],
        flow_config=flow.flow_config or {},
        created_at=flow.created_at.isoformat(),
        updated_at=flow.updated_at.isoformat(),
    )


@router.post("", response_model=FlowResponse, status_code=status.HTTP_201_CREATED)
async def create_flow(
    flow_in: FlowCreate,
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
):
    """
    Create a new flow with group isolation.
    Only Editors and Admins can create flows.

    Args:
        flow_in: Flow data for creation
        service: Flow service injected by dependency
        group_context: Group context from headers

    Returns:
        Created flow
    """
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can create flows")

    flow = await service.create_flow_with_group(flow_in, group_context)
    return FlowResponse(
        id=flow.id,
        name=flow.name,
        crew_id=flow.crew_id,
        nodes=clean_null_values(flow.nodes) or [],
        edges=clean_null_values(flow.edges) or [],
        flow_config=flow.flow_config or {},
        created_at=flow.created_at.isoformat(),
        updated_at=flow.updated_at.isoformat(),
    )


@router.post("/debug", response_model=Dict)
async def debug_flow_data(
    flow_in: FlowCreate,
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
):
    """
    Debug endpoint to validate flow data without saving.

    Args:
        flow_in: Flow data to validate
        service: Flow service injected by dependency
        group_context: Group context from headers

    Returns:
        Validation result
    """
    return await service.validate_flow_data(flow_in)


@router.put("/{flow_id}", response_model=FlowResponse)
async def update_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to update")],
    flow_in: FlowUpdate,
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
):
    """
    Update a flow with group isolation.
    Only Editors and Admins can update flows.

    Args:
        flow_id: UUID of the flow to update
        flow_in: Flow data for update
        service: Flow service injected by dependency
        group_context: Group context from headers

    Returns:
        Updated flow

    Raises:
        HTTPException: If flow not found or not authorized
    """
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can update flows")

    flow = await service.update_flow_with_group_check(flow_id, flow_in, group_context)
    return FlowResponse(
        id=flow.id,
        name=flow.name,
        crew_id=flow.crew_id,
        nodes=clean_null_values(flow.nodes) or [],
        edges=clean_null_values(flow.edges) or [],
        flow_config=flow.flow_config or {},
        created_at=flow.created_at.isoformat(),
        updated_at=flow.updated_at.isoformat(),
    )


@router.delete("/{flow_id}", status_code=status.HTTP_200_OK)
async def delete_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to delete")],
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
    force: Annotated[
        bool, Query(title="Force delete and remove associated executions")
    ] = False,
):
    """
    Delete a flow with group isolation.
    Only Editors and Admins can delete flows.

    Args:
        flow_id: UUID of the flow to delete
        service: Flow service injected by dependency
        group_context: Group context from headers
        force: Parameter is kept for backward compatibility but ignored, force delete is always used

    Returns:
        Success message

    Raises:
        HTTPException: If flow not found or not authorized
    """
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can delete flows")

    logger.info(f"Force deleting flow {flow_id} with its executions")

    try:
        # Always use force delete to avoid foreign key constraint issues
        result = await service.force_delete_flow_with_executions_with_group_check(
            flow_id, group_context
        )

        # Log success and return response
        logger.info(f"Successfully deleted flow {flow_id}")
        return {"status": "success", "message": "Flow deleted successfully"}

    except HTTPException as he:
        # Pass through HTTP exceptions from the service
        logger.warning(f"HTTP error deleting flow {flow_id}: {he.detail}")
        raise


@router.delete("", status_code=status.HTTP_200_OK)
async def delete_all_flows(
    service: Annotated[FlowService, Depends(get_flow_service)],
    group_context: GroupContextDep,
):
    """
    Delete all flows for the current group.
    Only Admins can delete all flows (mirrors delete_all_crews).

    Args:
        service: Flow service injected by dependency
        group_context: Group context from headers

    Returns:
        Success message
    """
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can delete all flows")

    await service.delete_all_flows_for_group(group_context)
    return {"status": "success", "message": "All flows deleted successfully"}


def _parse_completed_at(value):
    """Coerce a stored ISO timestamp to a datetime, tolerating a trailing Z."""
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@router.get("/{flow_id}/checkpoints", response_model=CheckpointListResponse)
async def get_flow_checkpoints(
    flow_id: Annotated[
        uuid.UUID, Path(title="The ID of the flow to get checkpoints for")
    ],
    flow_service: Annotated[FlowService, Depends(get_flow_service)],
    execution_service: Annotated[
        ExecutionHistoryService, Depends(get_execution_history_service)
    ],
    trace_repository: Annotated[
        ExecutionTraceRepository, Depends(get_execution_trace_repository)
    ],
    checkpoint_service: Annotated[CheckpointService, Depends(get_checkpoint_service)],
    group_context: GroupContextDep,
    status_filter: Annotated[
        Optional[str], Query(title="Filter by checkpoint status")
    ] = "active",
):
    """
    Get available checkpoints for a flow.

    **Deprecated.** Checkpoints belong to an EXECUTION, not to the thing that
    was executed, and hanging them off ``/flows`` is why crew executions never
    got an equivalent. Use ``GET /executions/{job_id}/checkpoints``, which
    serves both paths. This endpoint stays for one release so the frontend can
    migrate on its own schedule; it is flow-scoped ("every checkpoint for this
    saved flow"), which the per-execution route deliberately is not.

    Returns checkpoints from previous executions that can be resumed.
    Only returns checkpoints with 'active' status by default.
    Each checkpoint includes a list of completed crews for granular resume.

    Args:
        flow_id: UUID of the flow
        flow_service: Flow service for group check
        execution_service: Execution history service
        trace_repository: Execution trace repository for crew checkpoints
        group_context: Group context from headers
        status_filter: Filter checkpoints by status (default: 'active')

    Returns:
        List of available checkpoints for the flow with crew-level details
    """
    # First verify the flow exists and user has access
    await flow_service.get_flow_with_group_check(flow_id, group_context)

    # One source of truth: the written checkpoint, via the shared service. The
    # trace reconstruction below is reached only for executions that predate the
    # recorder — see ExecutionTraceRepository.get_crew_checkpoints_by_job_id.
    summaries = await checkpoint_service.list_for_flow(
        flow_id=flow_id, group_context=group_context, status_filter=status_filter
    )
    summarised_job_ids = {summary["job_id"] for summary in summaries}

    checkpoint_infos = []
    for summary in summaries:
        checkpoint_infos.append(
            CheckpointInfo(
                execution_id=summary["execution_id"],
                job_id=summary["job_id"],
                flow_uuid=summary.get("flow_uuid"),
                checkpoint_method=summary.get("checkpoint_method"),
                checkpoint_status=summary.get("status") or "active",
                created_at=summary["created_at"],
                run_name=summary.get("run_name"),
                crew_checkpoints=[
                    CrewCheckpointInfo(
                        crew_name=unit.get("name") or "Unknown Crew",
                        sequence=int(unit.get("key") or 0),
                        status="completed",
                        output_preview=unit.get("output_preview"),
                        completed_at=_parse_completed_at(unit.get("completed_at")),
                    )
                    for unit in summary.get("units", [])
                ],
            )
        )

    # Legacy: executions with no written checkpoint at all. Their crews are
    # reconstructed from traces, which cannot be backfilled and is not
    # fidelity-equivalent; the per-execution API reports this as `derived`.
    legacy = await execution_service.get_checkpoints_for_flow(
        flow_id=flow_id,
        group_id=group_context.primary_group_id,
        status_filter=status_filter,
    )
    for cp in legacy:
        if cp.job_id in summarised_job_ids:
            continue

        crew_checkpoints = []
        for crew_cp in await trace_repository.get_crew_checkpoints_by_job_id(cp.job_id):
            try:
                crew_checkpoints.append(
                    CrewCheckpointInfo(
                        crew_name=crew_cp.get("crew_name", "Unknown Crew"),
                        sequence=crew_cp.get("sequence", 0),
                        status=crew_cp.get("status", "completed"),
                        output_preview=crew_cp.get("output_preview"),
                        completed_at=_parse_completed_at(crew_cp.get("completed_at")),
                    )
                )
            except Exception as e:
                logger.warning(f"Error parsing crew checkpoint: {e}")
                continue

        checkpoint_infos.append(
            CheckpointInfo(
                execution_id=cp.id,
                job_id=cp.job_id,
                flow_uuid=cp.flow_uuid,
                checkpoint_method=cp.checkpoint_method,
                checkpoint_status=cp.checkpoint_status,
                created_at=cp.created_at,
                run_name=cp.run_name,
                crew_checkpoints=crew_checkpoints,
            )
        )

    return CheckpointListResponse(
        flow_id=str(flow_id), checkpoints=checkpoint_infos, total=len(checkpoint_infos)
    )


@router.delete("/{flow_id}/checkpoints/{execution_id}", status_code=status.HTTP_200_OK)
async def delete_checkpoint(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow")],
    execution_id: Annotated[
        int, Path(title="The execution ID of the checkpoint to delete")
    ],
    flow_service: Annotated[FlowService, Depends(get_flow_service)],
    execution_service: Annotated[
        ExecutionHistoryService, Depends(get_execution_history_service)
    ],
    group_context: GroupContextDep,
):
    """
    Delete/expire a specific checkpoint.

    Marks the checkpoint as 'expired' so it won't appear in the resume list.

    Args:
        flow_id: UUID of the flow
        execution_id: ID of the execution with the checkpoint
        flow_service: Flow service for group check
        execution_service: Execution history service
        group_context: Group context from headers

    Returns:
        Success message
    """
    # Verify flow access
    await flow_service.get_flow_with_group_check(flow_id, group_context)

    # Expire the checkpoint
    await execution_service.expire_checkpoint(
        execution_id=execution_id, group_id=group_context.primary_group_id
    )

    return {"status": "success", "message": "Checkpoint expired successfully"}


# ---------------------------------------------------------------------------
# Publication — exposing a flow to callers OUTSIDE this Kasal instance.
#
# Identical shape to the crew endpoints in crews_router.py, over the SAME
# PublicationService and the same table. A flow is a capability an external
# agent invokes exactly as a crew is; only entity_type differs, and the
# invocation layer is what routes it to the flow engine.
# ---------------------------------------------------------------------------


def get_publication_service(session: SessionDep) -> PublicationService:
    return PublicationService(session)


@router.post("/{flow_id}/publish", response_model=CrewPublicationResponse)
async def publish_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to publish")],
    publication: CrewPublicationCreate,
    flow_service: Annotated[FlowService, Depends(get_flow_service)],
    service: Annotated[PublicationService, Depends(get_publication_service)],
    group_context: GroupContextDep,
):
    """Expose a flow over the listed external protocols.

    Idempotent: publishing an already-published flow updates its record.

    Admins and editors only, exactly as for crews: making a flow reachable from
    outside the workspace is a higher-consequence action than editing one, and
    the same people who may change a flow are the people who may expose it.
    """
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can publish flows")

    # Resolve the flow through the group-scoped service FIRST, so a caller
    # cannot publish another workspace's flow by id.
    flow = await flow_service.get_flow_with_group_check(flow_id, group_context)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    row = await service.publish(
        str(flow_id), publication, group_context, entity_type="flow"
    )
    return CrewPublicationResponse.model_validate(row)


@router.get("/{flow_id}/publish", response_model=CrewPublicationResponse)
async def get_flow_publication(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow")],
    service: Annotated[PublicationService, Depends(get_publication_service)],
    group_context: GroupContextDep,
):
    """The flow's publication record, or 404 if it is not published."""
    row = await service.repository.find_by_entity(
        entity_type="flow",
        entity_id=str(flow_id),
        group_ids=group_context.group_ids or [],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Flow is not published")
    return CrewPublicationResponse.model_validate(row)


@router.patch("/{flow_id}/publish", response_model=CrewPublicationResponse)
async def update_flow_publication(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow")],
    publication: CrewPublicationUpdate,
    service: Annotated[PublicationService, Depends(get_publication_service)],
    group_context: GroupContextDep,
):
    """Adjust an existing publication. Omitted fields are left alone."""
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can change a publication")

    row = await service.update(
        str(flow_id), publication, group_context, entity_type="flow"
    )
    if not row:
        raise HTTPException(status_code=404, detail="Flow is not published")
    return CrewPublicationResponse.model_validate(row)


@router.delete("/{flow_id}/publish", status_code=status.HTTP_204_NO_CONTENT)
async def unpublish_flow(
    flow_id: Annotated[uuid.UUID, Path(title="The ID of the flow to unpublish")],
    service: Annotated[PublicationService, Depends(get_publication_service)],
    group_context: GroupContextDep,
):
    """Withdraw a flow from every external surface."""
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can unpublish flows")

    if not await service.unpublish(str(flow_id), group_context, entity_type="flow"):
        raise HTTPException(status_code=404, detail="Flow is not published")
