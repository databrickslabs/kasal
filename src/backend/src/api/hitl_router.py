"""
HITL (Human in the Loop) API Router.

This module provides API endpoints for managing HITL approvals and webhooks.
"""

import logging
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.core.dependencies import SessionDep, GroupContextDep
from src.services.hitl_service import (
    HITLService,
    HITLServiceError,
    HITLApprovalNotFoundError,
    HITLApprovalAlreadyProcessedError,
    HITLApprovalExpiredError,
    HITLPermissionDeniedError
)
from src.services.hitl_webhook_service import (
    HITLWebhookService,
    HITLWebhookServiceError,
    HITLWebhookNotFoundError
)
from src.schemas.hitl import (
    HITLApprovalResponse,
    HITLApprovalListResponse,
    HITLApproveRequest,
    HITLRejectRequest,
    HITLActionResponse,
    ExecutionHITLStatus,
    HITLWebhookCreate,
    HITLWebhookUpdate,
    HITLWebhookResponse,
    HITLWebhookListResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hitl", tags=["Human in the Loop"])


# =============================================================================
# Dependency Providers
# =============================================================================

async def get_hitl_service(session: SessionDep) -> HITLService:
    """
    Dependency provider for HITLService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        HITLService instance with injected session
    """
    return HITLService(session=session)


async def get_hitl_webhook_service(session: SessionDep) -> HITLWebhookService:
    """
    Dependency provider for HITLWebhookService.

    Args:
        session: Database session from FastAPI DI

    Returns:
        HITLWebhookService instance with injected session
    """
    return HITLWebhookService(session=session)


# Type aliases for cleaner function signatures
HITLServiceDep = Annotated[HITLService, Depends(get_hitl_service)]
HITLWebhookServiceDep = Annotated[HITLWebhookService, Depends(get_hitl_webhook_service)]


# =============================================================================
# Approval Endpoints
# =============================================================================

@router.get("/pending", response_model=HITLApprovalListResponse)
async def get_pending_approvals(
    service: HITLServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> HITLApprovalListResponse:
    """
    Get all pending HITL approvals for the current user's group.

    Returns a paginated list of approval requests that are waiting for
    human decision.
    """
    try:
        return await service.get_pending_approvals(
            group_id=group_context.primary_group_id,
            limit=limit,
            offset=offset
        )
    except HITLServiceError as e:
        logger.error(f"Error getting pending approvals: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/approvals/{approval_id}", response_model=HITLApprovalResponse)
async def get_approval(
    approval_id: int,
    service: HITLServiceDep,
    group_context: GroupContextDep,
) -> HITLApprovalResponse:
    """
    Get a specific HITL approval by ID.
    """
    try:
        # Get execution status which includes the approval
        approval = await service.approval_repo.get_by_id(
            approval_id,
            group_context.primary_group_id
        )

        if not approval:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Approval {approval_id} not found"
            )

        from src.schemas.hitl import HITLApprovalStatusEnum, HITLRejectionActionEnum

        return HITLApprovalResponse(
            id=approval.id,
            execution_id=approval.execution_id,
            flow_id=approval.flow_id,
            gate_node_id=approval.gate_node_id,
            crew_sequence=approval.crew_sequence,
            status=HITLApprovalStatusEnum(approval.status),
            gate_config=approval.gate_config,
            previous_crew_name=approval.previous_crew_name,
            previous_crew_output=approval.previous_crew_output,
            flow_state_snapshot=approval.flow_state_snapshot,
            responded_by=approval.responded_by,
            responded_at=approval.responded_at,
            approval_comment=approval.approval_comment,
            rejection_reason=approval.rejection_reason,
            rejection_action=(
                HITLRejectionActionEnum(approval.rejection_action)
                if approval.rejection_action else None
            ),
            expires_at=approval.expires_at,
            is_expired=approval.is_expired,
            created_at=approval.created_at,
            group_id=approval.group_id
        )

    except HTTPException:
        raise
    except HITLServiceError as e:
        logger.error(f"Error getting approval {approval_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/approvals/{approval_id}/approve", response_model=HITLActionResponse)
async def approve_gate(
    approval_id: int,
    request: HITLApproveRequest,
    service: HITLServiceDep,
    group_context: GroupContextDep,
) -> HITLActionResponse:
    """
    Approve an HITL gate and resume flow execution.

    The flow will continue from where it was paused at the gate.
    """
    try:
        result = await service.approve(
            approval_id=approval_id,
            approved_by=group_context.group_email or "unknown",
            group_id=group_context.primary_group_id,
            comment=request.comment,
            user_token=group_context.access_token  # Pass user's token for OBO auth on resume
        )
        return result

    except HITLApprovalNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except HITLApprovalAlreadyProcessedError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except HITLApprovalExpiredError as e:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=str(e)
        )
    except HITLPermissionDeniedError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )
    except HITLServiceError as e:
        logger.error(f"Error approving gate {approval_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/approvals/{approval_id}/reject", response_model=HITLActionResponse)
async def reject_gate(
    approval_id: int,
    request: HITLRejectRequest,
    service: HITLServiceDep,
    group_context: GroupContextDep,
) -> HITLActionResponse:
    """
    Reject an HITL gate.

    Options:
    - action=reject: Fail the flow execution
    - action=retry: Re-run the previous crew and return to the gate
    """
    try:
        result = await service.reject(
            approval_id=approval_id,
            rejected_by=group_context.group_email or "unknown",
            group_id=group_context.primary_group_id,
            reason=request.reason,
            action=request.action
        )
        return result

    except HITLApprovalNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except HITLApprovalAlreadyProcessedError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except HITLApprovalExpiredError as e:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=str(e)
        )
    except HITLPermissionDeniedError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )
    except HITLServiceError as e:
        logger.error(f"Error rejecting gate {approval_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/execution/{execution_id}", response_model=ExecutionHITLStatus)
async def get_execution_hitl_status(
    execution_id: str,
    service: HITLServiceDep,
    group_context: GroupContextDep,
) -> ExecutionHITLStatus:
    """
    Get HITL status for a specific execution.

    Returns information about any pending or completed HITL gates
    for the given execution.
    """
    try:
        return await service.get_execution_hitl_status(
            execution_id=execution_id,
            group_id=group_context.primary_group_id
        )
    except HITLServiceError as e:
        logger.error(f"Error getting execution HITL status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# =============================================================================
# Webhook Endpoints
# =============================================================================

@router.get("/webhooks", response_model=HITLWebhookListResponse)
async def list_webhooks(
    service: HITLWebhookServiceDep,
    group_context: GroupContextDep,
) -> HITLWebhookListResponse:
    """
    List all HITL webhooks for the current user's group.
    """
    try:
        return await service.list_webhooks(
            group_id=group_context.primary_group_id
        )
    except HITLWebhookServiceError as e:
        logger.error(f"Error listing webhooks: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/webhooks", response_model=HITLWebhookResponse, status_code=status.HTTP_201_CREATED)
async def create_webhook(
    webhook_data: HITLWebhookCreate,
    service: HITLWebhookServiceDep,
    group_context: GroupContextDep,
) -> HITLWebhookResponse:
    """
    Create a new HITL webhook.

    The webhook will be called when HITL events occur (gate_reached,
    gate_approved, gate_rejected, gate_timeout) based on the events
    list in the configuration.
    """
    try:
        return await service.create_webhook(
            webhook_data=webhook_data,
            group_id=group_context.primary_group_id
        )

    except HITLWebhookServiceError as e:
        logger.error(f"Error creating webhook: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/webhooks/{webhook_id}", response_model=HITLWebhookResponse)
async def get_webhook(
    webhook_id: int,
    service: HITLWebhookServiceDep,
    group_context: GroupContextDep,
) -> HITLWebhookResponse:
    """
    Get a specific HITL webhook by ID.
    """
    try:
        return await service.get_webhook(
            webhook_id=webhook_id,
            group_id=group_context.primary_group_id
        )
    except HITLWebhookNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except HITLWebhookServiceError as e:
        logger.error(f"Error getting webhook {webhook_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.patch("/webhooks/{webhook_id}", response_model=HITLWebhookResponse)
async def update_webhook(
    webhook_id: int,
    webhook_data: HITLWebhookUpdate,
    service: HITLWebhookServiceDep,
    group_context: GroupContextDep,
) -> HITLWebhookResponse:
    """
    Update an existing HITL webhook.
    """
    try:
        return await service.update_webhook(
            webhook_id=webhook_id,
            webhook_data=webhook_data,
            group_id=group_context.primary_group_id
        )

    except HITLWebhookNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except HITLWebhookServiceError as e:
        logger.error(f"Error updating webhook {webhook_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/webhooks/{webhook_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_webhook(
    webhook_id: int,
    service: HITLWebhookServiceDep,
    group_context: GroupContextDep,
) -> None:
    """
    Delete an HITL webhook.
    """
    try:
        await service.delete_webhook(
            webhook_id=webhook_id,
            group_id=group_context.primary_group_id
        )

    except HITLWebhookNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except HITLWebhookServiceError as e:
        logger.error(f"Error deleting webhook {webhook_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
