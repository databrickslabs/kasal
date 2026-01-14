"""
Comprehensive unit tests for HITL API router.

Tests all endpoints including:
- GET /hitl/pending - Get pending approvals
- GET /hitl/approvals/{approval_id} - Get specific approval
- POST /hitl/approvals/{approval_id}/approve - Approve a gate
- POST /hitl/approvals/{approval_id}/reject - Reject a gate
- GET /hitl/execution/{execution_id} - Get execution HITL status
- Webhook CRUD endpoints

Tests cover:
- Successful operations
- Error handling (404, 409, 410, 403, 500)
- Authorization checks
- User token passing for OBO authentication
"""

import pytest
from datetime import datetime, timezone, timedelta
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from src.api.hitl_router import (
    router,
    get_pending_approvals,
    get_approval,
    approve_gate,
    reject_gate,
    get_execution_hitl_status,
    list_webhooks,
    create_webhook,
    get_webhook,
    update_webhook,
    delete_webhook,
    get_hitl_service,
    get_hitl_webhook_service,
)
from src.schemas.hitl import (
    HITLApprovalResponse,
    HITLApprovalListResponse,
    HITLApproveRequest,
    HITLRejectRequest,
    HITLActionResponse,
    ExecutionHITLStatus,
    HITLApprovalStatusEnum,
    HITLRejectionActionEnum,
    HITLWebhookCreate,
    HITLWebhookUpdate,
    HITLWebhookResponse,
    HITLWebhookListResponse,
    HITLWebhookEventEnum,
)
from src.services.hitl_service import (
    HITLService,
    HITLServiceError,
    HITLApprovalNotFoundError,
    HITLApprovalAlreadyProcessedError,
    HITLApprovalExpiredError,
    HITLPermissionDeniedError,
)
from src.services.hitl_webhook_service import (
    HITLWebhookService,
    HITLWebhookServiceError,
    HITLWebhookNotFoundError,
)
from src.utils.user_context import GroupContext


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_group_context() -> GroupContext:
    """Create a mock group context for testing."""
    return GroupContext(
        group_ids=["test-group-123"],
        group_email="test@example.com",
        email_domain="example.com",
        user_role="admin",
        access_token="test-obo-token-123"
    )


@pytest.fixture
def mock_group_context_no_token() -> GroupContext:
    """Create a mock group context without access token."""
    return GroupContext(
        group_ids=["test-group-123"],
        group_email="test@example.com",
        email_domain="example.com",
        user_role="admin",
        access_token=None
    )


@pytest.fixture
def mock_hitl_service() -> AsyncMock:
    """Create a mock HITL service."""
    return AsyncMock(spec=HITLService)


@pytest.fixture
def mock_webhook_service() -> AsyncMock:
    """Create a mock webhook service."""
    return AsyncMock(spec=HITLWebhookService)


@pytest.fixture
def sample_approval_response() -> HITLApprovalResponse:
    """Create a sample approval response for testing."""
    now = datetime.now(timezone.utc)
    return HITLApprovalResponse(
        id=1,
        execution_id="exec-123",
        flow_id="flow-456",
        gate_node_id="gate-1",
        crew_sequence=1,
        status=HITLApprovalStatusEnum.PENDING,
        gate_config={
            "message": "Review output before proceeding",
            "timeout_seconds": 3600,
            "timeout_action": "auto_reject",
            "require_comment": False,
        },
        previous_crew_name="Research Crew",
        previous_crew_output="Research findings...",
        flow_state_snapshot={"key": "value"},
        responded_by=None,
        responded_at=None,
        approval_comment=None,
        rejection_reason=None,
        rejection_action=None,
        expires_at=now + timedelta(hours=1),
        is_expired=False,
        created_at=now,
        group_id="test-group-123",
    )


@pytest.fixture
def sample_approval_list_response(
    sample_approval_response: HITLApprovalResponse,
) -> HITLApprovalListResponse:
    """Create a sample approval list response."""
    return HITLApprovalListResponse(
        items=[sample_approval_response],
        total=1,
        limit=50,
        offset=0,
    )


@pytest.fixture
def sample_action_response_approved() -> HITLActionResponse:
    """Create a sample action response for approval."""
    return HITLActionResponse(
        success=True,
        approval_id=1,
        status=HITLApprovalStatusEnum.APPROVED,
        message="Gate approved successfully",
        execution_resumed=True,
    )


@pytest.fixture
def sample_action_response_rejected() -> HITLActionResponse:
    """Create a sample action response for rejection."""
    return HITLActionResponse(
        success=True,
        approval_id=1,
        status=HITLApprovalStatusEnum.REJECTED,
        message="Gate rejected - flow execution failed",
        execution_resumed=False,
    )


@pytest.fixture
def sample_execution_hitl_status(
    sample_approval_response: HITLApprovalResponse,
) -> ExecutionHITLStatus:
    """Create a sample execution HITL status."""
    return ExecutionHITLStatus(
        execution_id="exec-123",
        has_pending_approval=True,
        pending_approval=sample_approval_response,
        approval_history=[sample_approval_response],
        total_gates_passed=0,
    )


@pytest.fixture
def sample_webhook_response() -> HITLWebhookResponse:
    """Create a sample webhook response."""
    now = datetime.now(timezone.utc)
    return HITLWebhookResponse(
        id=1,
        group_id="test-group-123",
        name="Test Webhook",
        url="https://example.com/webhook",
        enabled=True,
        events=[HITLWebhookEventEnum.GATE_REACHED],
        headers={"X-Custom-Header": "value"},
        created_at=now,
        updated_at=None,
    )


@pytest.fixture
def sample_webhook_list_response(
    sample_webhook_response: HITLWebhookResponse,
) -> HITLWebhookListResponse:
    """Create a sample webhook list response."""
    return HITLWebhookListResponse(
        items=[sample_webhook_response],
        total=1,
    )


# =============================================================================
# Router Configuration Tests
# =============================================================================

class TestRouterConfiguration:
    """Tests for router configuration."""

    def test_router_prefix(self):
        """Test that router has correct prefix."""
        assert router.prefix == "/hitl"

    def test_router_tags(self):
        """Test that router has correct tags."""
        assert "Human in the Loop" in router.tags


# =============================================================================
# Dependency Provider Tests
# =============================================================================

class TestDependencyProviders:
    """Tests for dependency providers."""

    @pytest.mark.asyncio
    async def test_get_hitl_service(self):
        """Test HITL service dependency provider."""
        mock_session = AsyncMock()
        service = await get_hitl_service(mock_session)
        assert isinstance(service, HITLService)
        assert service.session == mock_session

    @pytest.mark.asyncio
    async def test_get_hitl_webhook_service(self):
        """Test HITL webhook service dependency provider."""
        mock_session = AsyncMock()
        service = await get_hitl_webhook_service(mock_session)
        assert isinstance(service, HITLWebhookService)
        assert service.session == mock_session


# =============================================================================
# GET /pending Tests
# =============================================================================

class TestGetPendingApprovals:
    """Tests for get_pending_approvals endpoint."""

    @pytest.mark.asyncio
    async def test_get_pending_approvals_success(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_approval_list_response: HITLApprovalListResponse,
    ):
        """Test successful retrieval of pending approvals."""
        mock_hitl_service.get_pending_approvals.return_value = sample_approval_list_response

        result = await get_pending_approvals(
            service=mock_hitl_service,
            group_context=mock_group_context,
            limit=50,
            offset=0,
        )

        assert result.total == 1
        assert len(result.items) == 1
        assert result.items[0].status == HITLApprovalStatusEnum.PENDING
        mock_hitl_service.get_pending_approvals.assert_called_once_with(
            group_id=mock_group_context.primary_group_id,
            limit=50,
            offset=0,
        )

    @pytest.mark.asyncio
    async def test_get_pending_approvals_empty(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test retrieval when no pending approvals exist."""
        mock_hitl_service.get_pending_approvals.return_value = HITLApprovalListResponse(
            items=[],
            total=0,
            limit=50,
            offset=0,
        )

        result = await get_pending_approvals(
            service=mock_hitl_service,
            group_context=mock_group_context,
            limit=50,
            offset=0,
        )

        assert result.total == 0
        assert len(result.items) == 0

    @pytest.mark.asyncio
    async def test_get_pending_approvals_with_pagination(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_approval_list_response: HITLApprovalListResponse,
    ):
        """Test pagination parameters are passed correctly."""
        sample_approval_list_response.limit = 10
        sample_approval_list_response.offset = 20
        mock_hitl_service.get_pending_approvals.return_value = sample_approval_list_response

        result = await get_pending_approvals(
            service=mock_hitl_service,
            group_context=mock_group_context,
            limit=10,
            offset=20,
        )

        assert result.limit == 10
        assert result.offset == 20
        mock_hitl_service.get_pending_approvals.assert_called_once_with(
            group_id=mock_group_context.primary_group_id,
            limit=10,
            offset=20,
        )

    @pytest.mark.asyncio
    async def test_get_pending_approvals_service_error(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_hitl_service.get_pending_approvals.side_effect = HITLServiceError(
            "Database connection failed"
        )

        with pytest.raises(HTTPException) as exc_info:
            await get_pending_approvals(
                service=mock_hitl_service,
                group_context=mock_group_context,
                limit=50,
                offset=0,
            )

        assert exc_info.value.status_code == 500
        assert "Database connection failed" in exc_info.value.detail


# =============================================================================
# GET /approvals/{approval_id} Tests
# =============================================================================

class TestGetApproval:
    """Tests for get_approval endpoint."""

    @pytest.mark.asyncio
    async def test_get_approval_success(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test successful retrieval of a specific approval."""
        now = datetime.now(timezone.utc)
        mock_approval = MagicMock()
        mock_approval.id = 1
        mock_approval.execution_id = "exec-123"
        mock_approval.flow_id = "flow-456"
        mock_approval.gate_node_id = "gate-1"
        mock_approval.crew_sequence = 1
        mock_approval.status = "pending"
        mock_approval.gate_config = {"message": "Review output"}
        mock_approval.previous_crew_name = "Research Crew"
        mock_approval.previous_crew_output = "Research findings..."
        mock_approval.flow_state_snapshot = {"key": "value"}
        mock_approval.responded_by = None
        mock_approval.responded_at = None
        mock_approval.approval_comment = None
        mock_approval.rejection_reason = None
        mock_approval.rejection_action = None
        mock_approval.expires_at = now + timedelta(hours=1)
        mock_approval.is_expired = False
        mock_approval.created_at = now
        mock_approval.group_id = "test-group-123"

        mock_hitl_service.approval_repo = AsyncMock()
        mock_hitl_service.approval_repo.get_by_id = AsyncMock(return_value=mock_approval)

        result = await get_approval(
            approval_id=1,
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.id == 1
        assert result.execution_id == "exec-123"
        assert result.status == HITLApprovalStatusEnum.PENDING
        mock_hitl_service.approval_repo.get_by_id.assert_called_once_with(
            1, mock_group_context.primary_group_id
        )

    @pytest.mark.asyncio
    async def test_get_approval_not_found(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test retrieval of non-existent approval (404)."""
        mock_hitl_service.approval_repo = AsyncMock()
        mock_hitl_service.approval_repo.get_by_id = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await get_approval(
                approval_id=999,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 404
        assert "Approval 999 not found" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_approval_service_error(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_hitl_service.approval_repo = AsyncMock()
        mock_hitl_service.approval_repo.get_by_id = AsyncMock(
            side_effect=HITLServiceError("Database error")
        )

        with pytest.raises(HTTPException) as exc_info:
            await get_approval(
                approval_id=1,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500
        assert "Database error" in exc_info.value.detail


# =============================================================================
# POST /approvals/{approval_id}/approve Tests
# =============================================================================

class TestApproveGate:
    """Tests for approve_gate endpoint."""

    @pytest.mark.asyncio
    async def test_approve_gate_success(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_action_response_approved: HITLActionResponse,
    ):
        """Test successful approval of a gate."""
        mock_hitl_service.approve.return_value = sample_action_response_approved
        request = HITLApproveRequest(comment="Looks good!")

        result = await approve_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.success is True
        assert result.status == HITLApprovalStatusEnum.APPROVED
        assert result.execution_resumed is True
        mock_hitl_service.approve.assert_called_once_with(
            approval_id=1,
            approved_by=mock_group_context.group_email,
            group_id=mock_group_context.primary_group_id,
            comment="Looks good!",
            user_token=mock_group_context.access_token,
        )

    @pytest.mark.asyncio
    async def test_approve_gate_passes_user_token(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_action_response_approved: HITLActionResponse,
    ):
        """Test that user token is passed for OBO authentication."""
        mock_hitl_service.approve.return_value = sample_action_response_approved
        request = HITLApproveRequest(comment=None)

        await approve_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        # Verify the user token was passed
        call_kwargs = mock_hitl_service.approve.call_args[1]
        assert call_kwargs["user_token"] == "test-obo-token-123"

    @pytest.mark.asyncio
    async def test_approve_gate_without_token(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context_no_token: GroupContext,
        sample_action_response_approved: HITLActionResponse,
    ):
        """Test approval when no access token is available."""
        mock_hitl_service.approve.return_value = sample_action_response_approved
        request = HITLApproveRequest(comment=None)

        result = await approve_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=mock_group_context_no_token,
        )

        assert result.success is True
        call_kwargs = mock_hitl_service.approve.call_args[1]
        assert call_kwargs["user_token"] is None

    @pytest.mark.asyncio
    async def test_approve_gate_without_comment(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_action_response_approved: HITLActionResponse,
    ):
        """Test approval without a comment."""
        mock_hitl_service.approve.return_value = sample_action_response_approved
        request = HITLApproveRequest(comment=None)

        result = await approve_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.success is True
        mock_hitl_service.approve.assert_called_once_with(
            approval_id=1,
            approved_by=mock_group_context.group_email,
            group_id=mock_group_context.primary_group_id,
            comment=None,
            user_token=mock_group_context.access_token,
        )

    @pytest.mark.asyncio
    async def test_approve_gate_not_found(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test approval of non-existent gate (404)."""
        mock_hitl_service.approve.side_effect = HITLApprovalNotFoundError(
            "Approval 999 not found"
        )
        request = HITLApproveRequest(comment=None)

        with pytest.raises(HTTPException) as exc_info:
            await approve_gate(
                approval_id=999,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 404
        assert "Approval 999 not found" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_approve_gate_already_processed(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test approval of already processed gate (409)."""
        mock_hitl_service.approve.side_effect = HITLApprovalAlreadyProcessedError(
            "Approval 1 already processed with status: approved"
        )
        request = HITLApproveRequest(comment=None)

        with pytest.raises(HTTPException) as exc_info:
            await approve_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 409
        assert "already processed" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_approve_gate_expired(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test approval of expired gate (410)."""
        mock_hitl_service.approve.side_effect = HITLApprovalExpiredError(
            "Approval 1 has expired"
        )
        request = HITLApproveRequest(comment=None)

        with pytest.raises(HTTPException) as exc_info:
            await approve_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 410
        assert "expired" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_approve_gate_permission_denied(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test approval when user lacks permission (403)."""
        mock_hitl_service.approve.side_effect = HITLPermissionDeniedError(
            "User test@example.com is not allowed to approve this gate"
        )
        request = HITLApproveRequest(comment=None)

        with pytest.raises(HTTPException) as exc_info:
            await approve_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 403
        assert "not allowed to approve" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_approve_gate_service_error(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_hitl_service.approve.side_effect = HITLServiceError(
            "Failed to resume execution"
        )
        request = HITLApproveRequest(comment=None)

        with pytest.raises(HTTPException) as exc_info:
            await approve_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500
        assert "Failed to resume execution" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_approve_gate_unknown_email_fallback(
        self,
        mock_hitl_service: AsyncMock,
        sample_action_response_approved: HITLActionResponse,
    ):
        """Test that 'unknown' is used when group_email is None."""
        mock_hitl_service.approve.return_value = sample_action_response_approved
        request = HITLApproveRequest(comment=None)

        # Create context with None email
        context = GroupContext(
            group_ids=["test-group"],
            group_email=None,
            access_token="token"
        )

        await approve_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=context,
        )

        call_kwargs = mock_hitl_service.approve.call_args[1]
        assert call_kwargs["approved_by"] == "unknown"


# =============================================================================
# POST /approvals/{approval_id}/reject Tests
# =============================================================================

class TestRejectGate:
    """Tests for reject_gate endpoint."""

    @pytest.mark.asyncio
    async def test_reject_gate_success(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_action_response_rejected: HITLActionResponse,
    ):
        """Test successful rejection of a gate."""
        mock_hitl_service.reject.return_value = sample_action_response_rejected
        request = HITLRejectRequest(
            reason="Output quality is poor",
            action=HITLRejectionActionEnum.REJECT,
        )

        result = await reject_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.success is True
        assert result.status == HITLApprovalStatusEnum.REJECTED
        assert result.execution_resumed is False
        mock_hitl_service.reject.assert_called_once_with(
            approval_id=1,
            rejected_by=mock_group_context.group_email,
            group_id=mock_group_context.primary_group_id,
            reason="Output quality is poor",
            action=HITLRejectionActionEnum.REJECT,
        )

    @pytest.mark.asyncio
    async def test_reject_gate_with_retry_action(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test rejection with retry action."""
        mock_hitl_service.reject.return_value = HITLActionResponse(
            success=True,
            approval_id=1,
            status=HITLApprovalStatusEnum.RETRY,
            message="Gate rejected - retrying previous crew",
            execution_resumed=True,
        )
        request = HITLRejectRequest(
            reason="Needs improvement",
            action=HITLRejectionActionEnum.RETRY,
        )

        result = await reject_gate(
            approval_id=1,
            request=request,
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.success is True
        assert result.status == HITLApprovalStatusEnum.RETRY
        assert result.execution_resumed is True
        mock_hitl_service.reject.assert_called_once_with(
            approval_id=1,
            rejected_by=mock_group_context.group_email,
            group_id=mock_group_context.primary_group_id,
            reason="Needs improvement",
            action=HITLRejectionActionEnum.RETRY,
        )

    @pytest.mark.asyncio
    async def test_reject_gate_not_found(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test rejection of non-existent gate (404)."""
        mock_hitl_service.reject.side_effect = HITLApprovalNotFoundError(
            "Approval 999 not found"
        )
        request = HITLRejectRequest(reason="Test reason")

        with pytest.raises(HTTPException) as exc_info:
            await reject_gate(
                approval_id=999,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 404
        assert "Approval 999 not found" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_reject_gate_already_processed(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test rejection of already processed gate (409)."""
        mock_hitl_service.reject.side_effect = HITLApprovalAlreadyProcessedError(
            "Approval 1 already processed"
        )
        request = HITLRejectRequest(reason="Test reason")

        with pytest.raises(HTTPException) as exc_info:
            await reject_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 409
        assert "already processed" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_reject_gate_expired(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test rejection of expired gate (410)."""
        mock_hitl_service.reject.side_effect = HITLApprovalExpiredError(
            "Approval 1 has expired"
        )
        request = HITLRejectRequest(reason="Test reason")

        with pytest.raises(HTTPException) as exc_info:
            await reject_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 410
        assert "expired" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_reject_gate_permission_denied(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test rejection when user lacks permission (403)."""
        mock_hitl_service.reject.side_effect = HITLPermissionDeniedError(
            "User test@example.com is not allowed to reject this gate"
        )
        request = HITLRejectRequest(reason="Test reason")

        with pytest.raises(HTTPException) as exc_info:
            await reject_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 403
        assert "not allowed to reject" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_reject_gate_service_error(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_hitl_service.reject.side_effect = HITLServiceError(
            "Failed to fail execution"
        )
        request = HITLRejectRequest(reason="Test reason")

        with pytest.raises(HTTPException) as exc_info:
            await reject_gate(
                approval_id=1,
                request=request,
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500
        assert "Failed to fail execution" in exc_info.value.detail


# =============================================================================
# GET /execution/{execution_id} Tests
# =============================================================================

class TestGetExecutionHITLStatus:
    """Tests for get_execution_hitl_status endpoint."""

    @pytest.mark.asyncio
    async def test_get_execution_status_success(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_execution_hitl_status: ExecutionHITLStatus,
    ):
        """Test successful retrieval of execution HITL status."""
        mock_hitl_service.get_execution_hitl_status.return_value = (
            sample_execution_hitl_status
        )

        result = await get_execution_hitl_status(
            execution_id="exec-123",
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.execution_id == "exec-123"
        assert result.has_pending_approval is True
        assert result.pending_approval is not None
        assert len(result.approval_history) == 1
        mock_hitl_service.get_execution_hitl_status.assert_called_once_with(
            execution_id="exec-123",
            group_id=mock_group_context.primary_group_id,
        )

    @pytest.mark.asyncio
    async def test_get_execution_status_no_pending(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test execution status when no pending approvals exist."""
        mock_hitl_service.get_execution_hitl_status.return_value = ExecutionHITLStatus(
            execution_id="exec-123",
            has_pending_approval=False,
            pending_approval=None,
            approval_history=[],
            total_gates_passed=2,
        )

        result = await get_execution_hitl_status(
            execution_id="exec-123",
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.has_pending_approval is False
        assert result.pending_approval is None
        assert result.total_gates_passed == 2

    @pytest.mark.asyncio
    async def test_get_execution_status_service_error(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_hitl_service.get_execution_hitl_status.side_effect = HITLServiceError(
            "Failed to get HITL status"
        )

        with pytest.raises(HTTPException) as exc_info:
            await get_execution_hitl_status(
                execution_id="exec-123",
                service=mock_hitl_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500
        assert "Failed to get HITL status" in exc_info.value.detail


# =============================================================================
# Webhook Endpoint Tests
# =============================================================================

class TestListWebhooks:
    """Tests for list_webhooks endpoint."""

    @pytest.mark.asyncio
    async def test_list_webhooks_success(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_webhook_list_response: HITLWebhookListResponse,
    ):
        """Test successful listing of webhooks."""
        mock_webhook_service.list_webhooks.return_value = sample_webhook_list_response

        result = await list_webhooks(
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result.total == 1
        assert len(result.items) == 1
        assert result.items[0].name == "Test Webhook"
        mock_webhook_service.list_webhooks.assert_called_once_with(
            group_id=mock_group_context.primary_group_id
        )

    @pytest.mark.asyncio
    async def test_list_webhooks_empty(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test listing when no webhooks exist."""
        mock_webhook_service.list_webhooks.return_value = HITLWebhookListResponse(
            items=[],
            total=0,
        )

        result = await list_webhooks(
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result.total == 0
        assert len(result.items) == 0

    @pytest.mark.asyncio
    async def test_list_webhooks_service_error(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_webhook_service.list_webhooks.side_effect = HITLWebhookServiceError(
            "Database error"
        )

        with pytest.raises(HTTPException) as exc_info:
            await list_webhooks(
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500


class TestCreateWebhook:
    """Tests for create_webhook endpoint."""

    @pytest.mark.asyncio
    async def test_create_webhook_success(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_webhook_response: HITLWebhookResponse,
    ):
        """Test successful creation of a webhook."""
        mock_webhook_service.create_webhook.return_value = sample_webhook_response
        webhook_data = HITLWebhookCreate(
            name="Test Webhook",
            url="https://example.com/webhook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
        )

        result = await create_webhook(
            webhook_data=webhook_data,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result.id == 1
        assert result.name == "Test Webhook"
        mock_webhook_service.create_webhook.assert_called_once_with(
            webhook_data=webhook_data,
            group_id=mock_group_context.primary_group_id,
        )

    @pytest.mark.asyncio
    async def test_create_webhook_with_secret(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_webhook_response: HITLWebhookResponse,
    ):
        """Test creation of webhook with secret."""
        mock_webhook_service.create_webhook.return_value = sample_webhook_response
        webhook_data = HITLWebhookCreate(
            name="Secure Webhook",
            url="https://example.com/webhook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
            secret="my-secret-key",
        )

        result = await create_webhook(
            webhook_data=webhook_data,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result is not None
        mock_webhook_service.create_webhook.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_webhook_service_error(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_webhook_service.create_webhook.side_effect = HITLWebhookServiceError(
            "Failed to create webhook"
        )
        webhook_data = HITLWebhookCreate(
            name="Test",
            url="https://example.com/webhook",
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_webhook(
                webhook_data=webhook_data,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500


class TestGetWebhook:
    """Tests for get_webhook endpoint."""

    @pytest.mark.asyncio
    async def test_get_webhook_success(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_webhook_response: HITLWebhookResponse,
    ):
        """Test successful retrieval of a webhook."""
        mock_webhook_service.get_webhook.return_value = sample_webhook_response

        result = await get_webhook(
            webhook_id=1,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result.id == 1
        assert result.name == "Test Webhook"
        mock_webhook_service.get_webhook.assert_called_once_with(
            webhook_id=1,
            group_id=mock_group_context.primary_group_id,
        )

    @pytest.mark.asyncio
    async def test_get_webhook_not_found(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test retrieval of non-existent webhook (404)."""
        mock_webhook_service.get_webhook.side_effect = HITLWebhookNotFoundError(
            "Webhook 999 not found"
        )

        with pytest.raises(HTTPException) as exc_info:
            await get_webhook(
                webhook_id=999,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 404
        assert "Webhook 999 not found" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_webhook_service_error(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_webhook_service.get_webhook.side_effect = HITLWebhookServiceError(
            "Database error"
        )

        with pytest.raises(HTTPException) as exc_info:
            await get_webhook(
                webhook_id=1,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500


class TestUpdateWebhook:
    """Tests for update_webhook endpoint."""

    @pytest.mark.asyncio
    async def test_update_webhook_success(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_webhook_response: HITLWebhookResponse,
    ):
        """Test successful update of a webhook."""
        updated_response = sample_webhook_response.model_copy()
        updated_response.name = "Updated Webhook"
        mock_webhook_service.update_webhook.return_value = updated_response

        webhook_data = HITLWebhookUpdate(name="Updated Webhook")

        result = await update_webhook(
            webhook_id=1,
            webhook_data=webhook_data,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result.name == "Updated Webhook"
        mock_webhook_service.update_webhook.assert_called_once_with(
            webhook_id=1,
            webhook_data=webhook_data,
            group_id=mock_group_context.primary_group_id,
        )

    @pytest.mark.asyncio
    async def test_update_webhook_partial(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
        sample_webhook_response: HITLWebhookResponse,
    ):
        """Test partial update of a webhook."""
        updated_response = sample_webhook_response.model_copy()
        updated_response.enabled = False
        mock_webhook_service.update_webhook.return_value = updated_response

        webhook_data = HITLWebhookUpdate(enabled=False)

        result = await update_webhook(
            webhook_id=1,
            webhook_data=webhook_data,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result.enabled is False

    @pytest.mark.asyncio
    async def test_update_webhook_not_found(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test update of non-existent webhook (404)."""
        mock_webhook_service.update_webhook.side_effect = HITLWebhookNotFoundError(
            "Webhook 999 not found"
        )
        webhook_data = HITLWebhookUpdate(name="Updated")

        with pytest.raises(HTTPException) as exc_info:
            await update_webhook(
                webhook_id=999,
                webhook_data=webhook_data,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_update_webhook_service_error(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_webhook_service.update_webhook.side_effect = HITLWebhookServiceError(
            "Database error"
        )
        webhook_data = HITLWebhookUpdate(name="Updated")

        with pytest.raises(HTTPException) as exc_info:
            await update_webhook(
                webhook_id=1,
                webhook_data=webhook_data,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500


class TestDeleteWebhook:
    """Tests for delete_webhook endpoint."""

    @pytest.mark.asyncio
    async def test_delete_webhook_success(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test successful deletion of a webhook."""
        mock_webhook_service.delete_webhook.return_value = None

        result = await delete_webhook(
            webhook_id=1,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )

        assert result is None
        mock_webhook_service.delete_webhook.assert_called_once_with(
            webhook_id=1,
            group_id=mock_group_context.primary_group_id,
        )

    @pytest.mark.asyncio
    async def test_delete_webhook_not_found(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test deletion of non-existent webhook (404)."""
        mock_webhook_service.delete_webhook.side_effect = HITLWebhookNotFoundError(
            "Webhook 999 not found"
        )

        with pytest.raises(HTTPException) as exc_info:
            await delete_webhook(
                webhook_id=999,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_webhook_service_error(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test handling of service error (500)."""
        mock_webhook_service.delete_webhook.side_effect = HITLWebhookServiceError(
            "Database error"
        )

        with pytest.raises(HTTPException) as exc_info:
            await delete_webhook(
                webhook_id=1,
                service=mock_webhook_service,
                group_context=mock_group_context,
            )

        assert exc_info.value.status_code == 500


# =============================================================================
# Integration-style Tests (testing endpoint functions directly)
# =============================================================================

class TestEndpointIntegration:
    """Integration-style tests for HITL router endpoints."""

    @pytest.mark.asyncio
    async def test_full_approval_workflow(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test the complete approval workflow."""
        # Step 1: Get pending approvals
        mock_hitl_service.get_pending_approvals.return_value = HITLApprovalListResponse(
            items=[
                HITLApprovalResponse(
                    id=1,
                    execution_id="exec-123",
                    flow_id="flow-456",
                    gate_node_id="gate-1",
                    crew_sequence=1,
                    status=HITLApprovalStatusEnum.PENDING,
                    gate_config={"message": "Review"},
                    created_at=datetime.now(timezone.utc),
                    group_id="test-group-123",
                    is_expired=False,
                )
            ],
            total=1,
            limit=50,
            offset=0,
        )

        pending = await get_pending_approvals(
            service=mock_hitl_service,
            group_context=mock_group_context,
            limit=50,
            offset=0,
        )
        assert pending.total == 1

        # Step 2: Approve the gate
        mock_hitl_service.approve.return_value = HITLActionResponse(
            success=True,
            approval_id=1,
            status=HITLApprovalStatusEnum.APPROVED,
            message="Gate approved successfully",
            execution_resumed=True,
        )

        result = await approve_gate(
            approval_id=1,
            request=HITLApproveRequest(comment="Approved"),
            service=mock_hitl_service,
            group_context=mock_group_context,
        )
        assert result.success is True
        assert result.execution_resumed is True

        # Step 3: Verify execution status
        mock_hitl_service.get_execution_hitl_status.return_value = ExecutionHITLStatus(
            execution_id="exec-123",
            has_pending_approval=False,
            pending_approval=None,
            approval_history=[],
            total_gates_passed=1,
        )

        status = await get_execution_hitl_status(
            execution_id="exec-123",
            service=mock_hitl_service,
            group_context=mock_group_context,
        )
        assert status.has_pending_approval is False
        assert status.total_gates_passed == 1

    @pytest.mark.asyncio
    async def test_full_rejection_workflow(
        self,
        mock_hitl_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test the complete rejection workflow."""
        # Step 1: Reject the gate
        mock_hitl_service.reject.return_value = HITLActionResponse(
            success=True,
            approval_id=1,
            status=HITLApprovalStatusEnum.REJECTED,
            message="Gate rejected - flow execution failed",
            execution_resumed=False,
        )

        result = await reject_gate(
            approval_id=1,
            request=HITLRejectRequest(
                reason="Quality not acceptable",
                action=HITLRejectionActionEnum.REJECT,
            ),
            service=mock_hitl_service,
            group_context=mock_group_context,
        )

        assert result.success is True
        assert result.status == HITLApprovalStatusEnum.REJECTED
        assert result.execution_resumed is False

    @pytest.mark.asyncio
    async def test_webhook_crud_workflow(
        self,
        mock_webhook_service: AsyncMock,
        mock_group_context: GroupContext,
    ):
        """Test complete webhook CRUD operations."""
        now = datetime.now(timezone.utc)

        # Create
        mock_webhook_service.create_webhook.return_value = HITLWebhookResponse(
            id=1,
            group_id="test-group-123",
            name="Slack Notification",
            url="https://example.com/slack",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
            headers={},
            created_at=now,
            updated_at=None,
        )

        created = await create_webhook(
            webhook_data=HITLWebhookCreate(
                name="Slack Notification",
                url="https://example.com/slack",
            ),
            service=mock_webhook_service,
            group_context=mock_group_context,
        )
        assert created.id == 1

        # Read
        mock_webhook_service.get_webhook.return_value = created

        fetched = await get_webhook(
            webhook_id=1,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )
        assert fetched.name == "Slack Notification"

        # Update
        updated_response = created.model_copy()
        updated_response.name = "Updated Name"
        mock_webhook_service.update_webhook.return_value = updated_response

        updated = await update_webhook(
            webhook_id=1,
            webhook_data=HITLWebhookUpdate(name="Updated Name"),
            service=mock_webhook_service,
            group_context=mock_group_context,
        )
        assert updated.name == "Updated Name"

        # Delete
        mock_webhook_service.delete_webhook.return_value = None

        await delete_webhook(
            webhook_id=1,
            service=mock_webhook_service,
            group_context=mock_group_context,
        )
        mock_webhook_service.delete_webhook.assert_called_once()
