"""
Router for execution history operations.

This module provides API endpoints for retrieving, managing, and deleting
execution history records and related data.
"""


from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status

from src.config.settings import settings

from src.core.exceptions import NotFoundError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.logger import LoggerManager
from src.schemas.execution_history import (
    DeleteResponse,
    ExecutionHistoryItem,
    ExecutionHistoryList,
    ExecutionOutputDebugList,
    ExecutionOutputList,
)
from src.services.execution_history_service import (
    ExecutionHistoryService,
    get_execution_history_service,
)
from src.services.group_service import GroupService
from src.services.user_service import UserService

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

router = APIRouter(prefix="/executions", tags=["Execution History"])


@router.get("/history/debug-groups")
async def debug_execution_groups(
    session: SessionDep,
    x_forwarded_email: Optional[str] = Header(None, alias="X-Forwarded-Email"),
    x_auth_request_email: Optional[str] = Header(None, alias="X-Auth-Request-Email"),
) -> Dict[str, Any]:
    """
    Debug endpoint to see all unique group_ids in execution_history table
    and compare with user's groups.
    """
    if not settings.DEBUG_MODE:
        raise HTTPException(status_code=404)
    # Get user email from headers
    user_email = x_auth_request_email or x_forwarded_email

    # Get all unique group_ids from execution_history table via service
    history_service = ExecutionHistoryService(session)
    all_group_ids = await history_service.get_execution_groups_with_counts()

    # Get user's groups if email provided
    user_groups = []
    if user_email:
        user_service = UserService(session)
        user = await user_service.get_or_create_user_by_email(user_email)
        if user:
            group_service = GroupService(session)
            groups = await group_service.get_user_groups(user.id)
            user_groups = [{"id": g.id, "name": g.name} for g in groups]

    return {
        "user_email": user_email,
        "user_groups": user_groups,
        "all_execution_groups": [
            {"group_id": gid, "execution_count": count} for gid, count in all_group_ids
        ],
        "total_unique_groups": len(all_group_ids),
    }


@router.get("/history/all-groups", response_model=ExecutionHistoryList)
async def get_all_groups_execution_history(
    session: SessionDep,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    x_forwarded_email: Optional[str] = Header(None, alias="X-Forwarded-Email"),
    x_auth_request_email: Optional[str] = Header(None, alias="X-Auth-Request-Email"),
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Get execution history from all groups the user belongs to.

    This endpoint fetches executions from all groups where the user is a member,
    eliminating the need to switch between groups to see executions.

    Args:
        session: Database session
        limit: Maximum number of executions to return (1-100)
        offset: Pagination offset
        x_forwarded_email: User email from Databricks Apps
        x_auth_request_email: User email from OAuth2-Proxy
        service: ExecutionHistoryService instance

    Returns:
        ExecutionHistoryList with paginated execution history from all user's groups
    """
    # Get user email from headers (prefer OAuth2-Proxy over direct)
    user_email = x_auth_request_email or x_forwarded_email

    if not user_email:
        logger.warning("No user email found in headers, returning empty list")
        return ExecutionHistoryList(executions=[], total=0, offset=offset, limit=limit)

    # Get all groups the user belongs to
    user_service = UserService(session)
    user = await user_service.get_or_create_user_by_email(user_email)

    if not user:
        logger.warning(f"User not found for email {user_email}, returning empty list")
        return ExecutionHistoryList(executions=[], total=0, offset=offset, limit=limit)

    group_service = GroupService(session)
    user_groups = await group_service.get_user_groups(user.id)

    # Extract group IDs
    group_ids = [group.id for group in user_groups]

    logger.info(f"User {user_email} belongs to {len(user_groups)} groups")
    for group in user_groups:
        logger.info(f"  - Group: {group.id} ({group.name})")

    # Also add the user's personal workspace
    email_parts = user_email.split("@")
    if len(email_parts) == 2:
        email_user = email_parts[0]
        email_domain = email_parts[1].replace(".", "_")
        personal_group_id = f"user_{email_user}_{email_domain}"
        if personal_group_id not in group_ids:
            group_ids.append(personal_group_id)
            logger.info(f"  - Added personal workspace: {personal_group_id}")

    logger.info(
        f"Fetching executions for user {user_email} from {len(group_ids)} total groups: {group_ids}"
    )

    # Fetch executions from all groups
    result = await service.get_execution_history(limit, offset, group_ids=group_ids)

    logger.info(f"Found {result.total} total executions across all groups")
    if result.executions:
        logger.info(
            f"Returning {len(result.executions)} executions (offset={offset}, limit={limit})"
        )
        # Log first few execution group_ids to debug
        for i, exec in enumerate(result.executions[:5]):
            logger.info(
                f"  - Execution {i+1}: job_id={exec.job_id}, group_id={exec.group_id}, status={exec.status}"
            )

    return result


@router.get("/history", response_model=ExecutionHistoryList)
async def get_execution_history(
    group_context: GroupContextDep,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Get a paginated list of execution history with group filtering.

    Args:
        limit: Maximum number of executions to return (1-100)
        offset: Pagination offset
        group_context: Group context for filtering
        service: ExecutionHistoryService instance

    Returns:
        ExecutionHistoryList with paginated execution history
    """
    return await service.get_execution_history(
        limit, offset, group_ids=group_context.group_ids
    )


@router.head("/history/{execution_id}")
async def check_execution_exists(
    execution_id: int,
    group_context: GroupContextDep,
    service: ExecutionHistoryService = Depends(get_execution_history_service),
    response: Response = None,
):
    """
    Check if an execution exists by ID. This is a lightweight HEAD request
    that returns only status code without a response body.

    Args:
        execution_id: Database ID of the execution
        service: ExecutionHistoryService instance
        response: FastAPI Response object

    Returns:
        HTTP 200 OK if the execution exists, HTTP 404 Not Found otherwise
    """
    exists = await service.check_execution_exists(execution_id)
    if not exists:
        raise NotFoundError(f"Execution with ID {execution_id} not found")
    # Just return an empty response with 200 status
    return Response(status_code=status.HTTP_200_OK)


@router.get("/history/{execution_id}", response_model=ExecutionHistoryItem)
async def get_execution_by_id(
    execution_id: int,
    group_context: GroupContextDep,
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Get execution details by ID with group filtering.

    Args:
        execution_id: Database ID of the execution
        group_context: Group context for filtering
        service: ExecutionHistoryService instance

    Returns:
        ExecutionHistoryItem with execution details
    """
    execution = await service.get_execution_by_id(
        execution_id, group_ids=group_context.group_ids
    )
    if not execution:
        raise NotFoundError(f"Execution with ID {execution_id} not found")
    return execution


@router.get("/{execution_id}/outputs", response_model=ExecutionOutputList)
async def get_execution_outputs(
    execution_id: str,
    group_context: GroupContextDep,
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Get outputs for an execution.

    Args:
        execution_id: String ID of the execution
        limit: Maximum number of outputs to return (1-5000)
        offset: Pagination offset
        service: ExecutionHistoryService instance

    Returns:
        ExecutionOutputList with paginated execution outputs
    """
    return await service.get_execution_outputs(
        execution_id, limit, offset, group_ids=group_context.group_ids
    )


@router.get("/{execution_id}/outputs/debug", response_model=ExecutionOutputDebugList)
async def get_execution_debug_outputs(
    execution_id: str,
    group_context: GroupContextDep,
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Get debug information about outputs for an execution.

    Args:
        execution_id: String ID of the execution
        service: ExecutionHistoryService instance

    Returns:
        ExecutionOutputDebugList with debug information
    """
    debug_info = await service.get_debug_outputs(
        execution_id, group_ids=group_context.group_ids
    )
    if not debug_info:
        raise NotFoundError(f"Execution with ID {execution_id} not found")
    return debug_info


@router.delete("/history", response_model=DeleteResponse)
async def delete_all_executions(
    group_context: GroupContextDep,
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Delete all executions and their associated data for the user's groups.

    This endpoint ensures tenant isolation by only deleting executions
    that belong to the groups the user has access to.

    Returns:
        DeleteResponse with information about the deleted data
    """
    return await service.delete_all_executions(group_ids=group_context.group_ids)


@router.delete("/history/{execution_id}", response_model=DeleteResponse)
async def delete_execution(
    execution_id: int,
    group_context: GroupContextDep,
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Delete a specific execution and its associated data.

    Args:
        execution_id: Database ID of the execution
        service: ExecutionHistoryService instance

    Returns:
        DeleteResponse with information about the deleted data
    """
    result = await service.delete_execution(execution_id)
    if not result:
        raise NotFoundError(f"Execution with ID {execution_id} not found")
    return result


@router.delete("/{job_id}", response_model=DeleteResponse)
async def delete_execution_by_job_id(
    job_id: str,
    group_context: GroupContextDep,
    service: ExecutionHistoryService = Depends(get_execution_history_service),
):
    """
    Delete an execution by its job_id.

    Args:
        job_id: String ID (UUID) of the execution
        service: ExecutionHistoryService instance

    Returns:
        DeleteResponse with information about the deleted data
    """
    result = await service.delete_execution_by_job_id(job_id)
    if not result:
        raise NotFoundError(f"Execution with job_id {job_id} not found")
    return result
