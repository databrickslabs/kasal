"""
Router for execution trace operations.

This module provides API endpoints for retrieving, creating, and managing
execution traces.
"""

from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, Query, status

from src.core.exceptions import NotFoundError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.logger import LoggerManager
from src.schemas.execution_trace import (
    DeleteTraceResponse,
    ExecutionTraceItem,
    ExecutionTraceList,
    ExecutionTraceResponseByJobId,
    ExecutionTraceResponseByRunId,
)
from src.services.execution_trace_service import ExecutionTraceService

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

router = APIRouter(prefix="/traces", tags=["Execution Traces"])


# Dependency to get ExecutionTraceService
def get_execution_trace_service(session: SessionDep) -> ExecutionTraceService:
    """
    Dependency provider for ExecutionTraceService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI (from core.dependencies)

    Returns:
        ExecutionTraceService instance with session
    """
    return ExecutionTraceService(session)


# Type alias for cleaner function signatures
ExecutionTraceServiceDep = Annotated[
    ExecutionTraceService, Depends(get_execution_trace_service)
]


@router.get("/", response_model=ExecutionTraceList)
async def get_all_traces(
    service: ExecutionTraceServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(100, ge=1, le=15000),
    offset: int = Query(0, ge=0),
):
    """
    Get a paginated list of all execution traces for the current group.

    Args:
        group_context: Group context from headers for authorization
        limit: Maximum number of traces to return (1-15000)
        offset: Pagination offset

    Returns:
        ExecutionTraceList with paginated execution traces for the group
    """
    return await service.get_all_traces_for_group(group_context, limit, offset)


@router.get("/execution/{run_id}", response_model=ExecutionTraceResponseByRunId)
async def get_traces_by_run_id(
    run_id: int,
    service: ExecutionTraceServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(100, ge=1, le=15000),
    offset: int = Query(0, ge=0),
):
    """
    Get traces for an execution by run_id.

    Args:
        run_id: Database ID of the execution
        service: Execution trace service dependency
        group_context: Group context from headers for authorization
        limit: Maximum number of traces to return (1-15000)
        offset: Pagination offset

    Returns:
        ExecutionTraceResponseByRunId with traces for the execution
    """
    result = await service.get_traces_by_run_id(
        group_context=group_context, run_id=run_id, limit=limit, offset=offset
    )
    if not result:
        raise NotFoundError(f"Execution with ID {run_id} not found or access denied")
    return result


@router.get("/job/{job_id}", response_model=ExecutionTraceResponseByJobId)
async def get_traces_by_job_id(
    job_id: str,
    service: ExecutionTraceServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(100, ge=1, le=15000),
    offset: int = Query(0, ge=0),
):
    """
    Get traces for an execution by job_id.

    Args:
        job_id: String ID of the execution (job_id)
        service: Execution trace service dependency
        group_context: Group context from headers for authorization
        limit: Maximum number of traces to return (1-15000)
        offset: Pagination offset

    Returns:
        ExecutionTraceResponseByJobId with traces for the execution
    """
    result = await service.get_traces_by_job_id(
        group_context=group_context, job_id=job_id, limit=limit, offset=offset
    )
    if not result:
        raise NotFoundError(f"Execution with job_id {job_id} not found or access denied")
    return result


@router.get("/job/{job_id}/crew-node-states")
async def get_current_crew_node_states(
    job_id: str, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Get current crew node execution states from traces for flow execution.
    Returns which crew nodes are running, completed, or failed.

    Args:
        job_id: String ID of the execution (job_id)
        service: Execution trace service dependency
        group_context: Group context from headers for authorization

    Returns:
        Dictionary mapping crew names to their current states
    """
    # Get all traces for the job with authorization check
    result = await service.get_traces_by_job_id(
        group_context=group_context, job_id=job_id, limit=15000, offset=0
    )
    if not result:
        raise NotFoundError(f"Execution with job_id {job_id} not found or access denied")

    crew_states = {}

    # Process traces to determine current crew node states
    # Track crew execution based on task events grouped by crew
    current_crew = None
    crew_task_counts = {}  # Track total tasks per crew
    crew_completed_tasks = {}  # Track completed tasks per crew
    crew_failed = set()  # Track failed crews

    for trace in result.traces:
        event_type_upper = trace.event_type.upper() if trace.event_type else ""

        # Check for crew-related events from flow execution
        if event_type_upper in ["TASK_STARTED", "TASK_COMPLETED", "TASK_FAILED"]:
            # Extract crew name from metadata if available
            crew_name = None
            if trace.trace_metadata and isinstance(trace.trace_metadata, dict):
                crew_name = trace.trace_metadata.get("crew_name")
                if not crew_name:
                    # Try to get from agent role (crew name is often the first agent's role)
                    agent_role = trace.trace_metadata.get("agent_role")
                    if agent_role:
                        crew_name = agent_role

            if crew_name:
                # Initialize crew state if not exists
                if crew_name not in crew_states:
                    crew_states[crew_name] = {
                        "status": "pending",
                        "started_at": None,
                        "completed_at": None,
                        "task_count": 0,
                        "completed_count": 0,
                    }
                    crew_task_counts[crew_name] = 0
                    crew_completed_tasks[crew_name] = 0

                if event_type_upper == "TASK_STARTED":
                    crew_task_counts[crew_name] = crew_task_counts.get(crew_name, 0) + 1
                    if crew_states[crew_name]["status"] == "pending":
                        crew_states[crew_name]["status"] = "running"
                        crew_states[crew_name]["started_at"] = (
                            trace.created_at.isoformat() if trace.created_at else None
                        )
                    crew_states[crew_name]["task_count"] = crew_task_counts[crew_name]

                elif event_type_upper == "TASK_COMPLETED":
                    crew_completed_tasks[crew_name] = (
                        crew_completed_tasks.get(crew_name, 0) + 1
                    )
                    crew_states[crew_name]["completed_count"] = crew_completed_tasks[
                        crew_name
                    ]
                    # Check if all tasks are completed
                    if (
                        crew_completed_tasks[crew_name]
                        >= crew_task_counts.get(crew_name, 0)
                        and crew_name not in crew_failed
                    ):
                        crew_states[crew_name]["status"] = "completed"
                        crew_states[crew_name]["completed_at"] = (
                            trace.created_at.isoformat() if trace.created_at else None
                        )

                elif event_type_upper == "TASK_FAILED":
                    crew_failed.add(crew_name)
                    crew_states[crew_name]["status"] = "failed"
                    crew_states[crew_name]["failed_at"] = (
                        trace.created_at.isoformat() if trace.created_at else None
                    )
                    # Extract the error message so the UI can display it
                    _error = None
                    if trace.trace_metadata and isinstance(trace.trace_metadata, dict):
                        _error = trace.trace_metadata.get("error")
                    if not _error and trace.output and isinstance(trace.output, dict):
                        _extra = trace.output.get("extra_data")
                        if isinstance(_extra, dict):
                            _error = _extra.get("error")
                    if _error:
                        crew_states[crew_name]["error"] = str(_error)[:500]

    return crew_states


@router.get("/job/{job_id}/task-states")
async def get_current_task_states(
    job_id: str, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Get current task execution states from traces.
    Returns which tasks are running, completed, or failed.

    Args:
        job_id: String ID of the execution (job_id)
        service: Execution trace service dependency
        group_context: Group context from headers for authorization

    Returns:
        Dictionary mapping task IDs to their current states
    """
    # Get all traces for the job with authorization check
    result = await service.get_traces_by_job_id(
        group_context=group_context, job_id=job_id, limit=15000, offset=0
    )
    if not result:
        raise NotFoundError(f"Execution with job_id {job_id} not found or access denied")

    task_states = {}
    task_name_to_id = {}  # Track the proper task ID for each task name

    # First pass: collect all task IDs with proper UUIDs (those that have task_id in metadata)
    for trace in result.traces:
        event_type_upper = trace.event_type.upper() if trace.event_type else ""
        if event_type_upper in ["TASK_STARTED", "TASK_COMPLETED", "TASK_FAILED"]:
            if trace.trace_metadata and isinstance(trace.trace_metadata, dict):
                task_id = trace.trace_metadata.get("task_id")
                if task_id and trace.event_context:
                    # Map task name to its proper ID (prefer UUIDs over generated IDs)
                    if (
                        trace.event_context not in task_name_to_id
                        or not task_name_to_id[trace.event_context].startswith("task_")
                    ):
                        task_name_to_id[trace.event_context] = task_id

    # Second pass: process traces to determine current task states
    for trace in result.traces:
        # Normalize event type to uppercase for consistency
        event_type_upper = trace.event_type.upper() if trace.event_type else ""

        # Check if this is a task-related event
        if event_type_upper in ["TASK_STARTED", "TASK_COMPLETED", "TASK_FAILED"]:
            # Extract task_id from trace_metadata (where it's stored in the database)
            task_id = None
            if trace.trace_metadata and isinstance(trace.trace_metadata, dict):
                task_id = trace.trace_metadata.get("task_id")

            # Try to use the proper task ID for this task name
            if trace.event_context and trace.event_context in task_name_to_id:
                task_id = task_name_to_id[trace.event_context]
            elif not task_id and trace.event_context:
                # Use event_context (task name) as a fallback identifier
                task_id = f"task_{hash(trace.event_context) % 1000000}"

            if task_id:
                # Update task state based on event type (using normalized uppercase)
                if event_type_upper == "TASK_STARTED":
                    # Only create running state if task doesn't exist or isn't completed/failed
                    if (
                        task_id not in task_states
                        or task_states[task_id]["status"] == "running"
                    ):
                        task_states[task_id] = {
                            "status": "running",
                            "started_at": trace.created_at.isoformat()
                            if trace.created_at
                            else None,
                            "task_name": trace.event_context,
                        }
                elif event_type_upper == "TASK_COMPLETED":
                    # Always update to completed (overrides running)
                    if task_id in task_states:
                        task_states[task_id]["status"] = "completed"
                        task_states[task_id]["completed_at"] = (
                            trace.created_at.isoformat() if trace.created_at else None
                        )
                    else:
                        task_states[task_id] = {
                            "status": "completed",
                            "completed_at": trace.created_at.isoformat()
                            if trace.created_at
                            else None,
                            "task_name": trace.event_context,
                        }
                elif event_type_upper == "TASK_FAILED":
                    # Always update to failed (overrides running or completed)
                    if task_id in task_states:
                        task_states[task_id]["status"] = "failed"
                        task_states[task_id]["failed_at"] = (
                            trace.created_at.isoformat() if trace.created_at else None
                        )
                    else:
                        task_states[task_id] = {
                            "status": "failed",
                            "failed_at": trace.created_at.isoformat()
                            if trace.created_at
                            else None,
                            "task_name": trace.event_context,
                        }

    return task_states


@router.get("/{trace_id}", response_model=ExecutionTraceItem)
async def get_trace_by_id(
    trace_id: int, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Get a specific trace by ID with group authorization.

    Args:
        trace_id: ID of the trace to retrieve
        group_context: Group context from headers for authorization

    Returns:
        ExecutionTraceItem with trace details
    """
    trace = await service.get_trace_by_id_with_group_check(trace_id, group_context)
    if not trace:
        raise NotFoundError(f"Trace with ID {trace_id} not found")
    return trace


@router.post(
    "/", response_model=ExecutionTraceItem, status_code=status.HTTP_201_CREATED
)
async def create_trace(
    trace_data: dict, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Create a new execution trace with group assignment.

    Args:
        trace_data: Dictionary with trace data
        group_context: Group context from headers for authorization

    Returns:
        Created ExecutionTraceItem
    """
    return await service.create_trace_with_group(trace_data, group_context)


@router.delete("/execution/{run_id}", response_model=DeleteTraceResponse)
async def delete_traces_by_run_id(
    run_id: int, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Delete all traces for a specific execution with group authorization.

    Args:
        run_id: Database ID of the execution
        group_context: Group context from headers for authorization

    Returns:
        DeleteTraceResponse with information about deleted traces
    """
    return await service.delete_traces_by_run_id_with_group_check(run_id, group_context)


@router.delete("/job/{job_id}", response_model=DeleteTraceResponse)
async def delete_traces_by_job_id(
    job_id: str, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Delete all traces for a specific job with group authorization.

    Args:
        job_id: String ID of the execution (job_id)
        group_context: Group context from headers for authorization

    Returns:
        DeleteTraceResponse with information about deleted traces
    """
    return await service.delete_traces_by_job_id_with_group_check(job_id, group_context)


@router.delete("/{trace_id}", response_model=DeleteTraceResponse)
async def delete_trace(
    trace_id: int, service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Delete a specific trace by ID with group authorization.

    Args:
        trace_id: ID of the trace to delete
        group_context: Group context from headers for authorization

    Returns:
        DeleteTraceResponse with information about the deleted trace
    """
    result = await service.delete_trace_with_group_check(trace_id, group_context)
    if not result:
        raise NotFoundError(f"Trace with ID {trace_id} not found")
    return result


@router.delete("/", response_model=DeleteTraceResponse)
async def delete_all_traces(
    service: ExecutionTraceServiceDep, group_context: GroupContextDep
):
    """
    Delete all execution traces for the current group.

    Args:
        group_context: Group context from headers for authorization

    Returns:
        DeleteTraceResponse with information about deleted traces
    """
    return await service.delete_all_traces_for_group(group_context)
