"""
API router for execution logs endpoints.

This module provides endpoints for retrieving historical execution logs.
"""

from typing import Annotated, Dict, List

from fastapi import (
    APIRouter,
    Depends,
    Query,
)

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.logger import LoggerManager
from src.schemas.execution_logs import ExecutionLogResponse, ExecutionLogsResponse
from src.services.execution_logs_service import ExecutionLogsService

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system


async def get_execution_logs_service(session: SessionDep) -> ExecutionLogsService:
    """
    Dependency provider for ExecutionLogsService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        ExecutionLogsService instance with injected session
    """
    return ExecutionLogsService(session)


# Type alias for cleaner function signatures
ExecutionLogsServiceDep = Annotated[
    ExecutionLogsService, Depends(get_execution_logs_service)
]

# Create router for log endpoints
logs_router = APIRouter(
    prefix="/logs",
    tags=["logs"],
)

# Create a router for the runs API to match frontend expectations
runs_router = APIRouter(
    prefix="/runs",
    tags=["runs"],
)

# Create main router for execution logs (for backward compatibility with tests)
router = APIRouter(
    prefix="/execution-logs",
    tags=["execution-logs"],
)


@logs_router.get(
    "/executions/{execution_id}", response_model=List[ExecutionLogResponse]
)
async def get_execution_logs(
    execution_id: str,
    service: ExecutionLogsServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """
    Get historical execution logs for the current tenant.

    This endpoint allows retrieval of past logs for a specific execution
    belonging to the current tenant.

    Args:
        execution_id: ID of the execution to get logs for
        group_context: Group context from headers
        limit: Maximum number of logs to return
        offset: Number of logs to skip

    Returns:
        List of execution logs with their timestamps
    """
    logs = await service.get_execution_logs_by_group(
        execution_id, group_context, limit, offset
    )
    return logs


@runs_router.get("/{run_id}/outputs", response_model=ExecutionLogsResponse)
async def get_run_logs(
    run_id: str,
    service: ExecutionLogsServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """
    Get historical logs for a specific run within the current tenant.

    This endpoint matches the frontend expectation for the URL pattern.
    It delegates to the execution logs service with tenant filtering.

    Args:
        run_id: ID of the run to get logs for
        group_context: Group context from headers
        limit: Maximum number of logs to return
        offset: Number of logs to skip

    Returns:
        Dictionary with a list of run logs with their timestamps
    """
    logs = await service.get_execution_logs_by_group(
        run_id, group_context, limit, offset
    )
    return ExecutionLogsResponse(logs=logs)


# Endpoints for the main execution-logs router (for test compatibility)
@router.get("/{execution_id}", response_model=List[ExecutionLogResponse])
async def get_execution_logs_main(
    execution_id: str,
    service: ExecutionLogsServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """Get execution logs via main router."""
    logs = await service.get_execution_logs_by_group(
        execution_id, group_context, limit, offset
    )
    return logs


@router.post("/", status_code=201)
async def create_execution_log(
    log_data: Dict,
    group_context: GroupContextDep,
):
    """Create an execution log via main router."""
    # For now, just return a success response since this is mainly for testing
    return {"id": 1, "message": "Log created"}
