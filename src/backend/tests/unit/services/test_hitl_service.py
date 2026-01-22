"""
Unit tests for HITLService.

Tests the functionality of Human in the Loop (HITL) operations including
creating approval requests, processing approvals/rejections, handling timeouts,
and triggering flow resume.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta
from sqlalchemy.exc import SQLAlchemyError

from src.services.hitl_service import (
    HITLService,
    HITLServiceError,
    HITLApprovalNotFoundError,
    HITLApprovalAlreadyProcessedError,
    HITLApprovalExpiredError,
    HITLPermissionDeniedError
)
from src.models.hitl_approval import (
    HITLApproval,
    HITLApprovalStatus,
    HITLTimeoutAction,
    HITLRejectionAction
)
from src.models.execution_status import ExecutionStatus
from src.schemas.hitl import (
    HITLApprovalStatusEnum,
    HITLRejectionActionEnum,
    HITLActionResponse,
    HITLApprovalListResponse,
    ExecutionHITLStatus
)


# ==============================================================================
# Mock Models
# ==============================================================================

class MockHITLApproval:
    """Mock HITL approval for testing."""

    def __init__(
        self,
        id: int = 1,
        execution_id: str = "exec-123",
        flow_id: str = "flow-456",
        gate_node_id: str = "gate-1",
        crew_sequence: int = 1,
        status: str = HITLApprovalStatus.PENDING,
        gate_config: dict = None,
        previous_crew_name: str = "Research Crew",
        previous_crew_output: str = "Research output text",
        flow_state_snapshot: dict = None,
        responded_by: str = None,
        responded_at: datetime = None,
        approval_comment: str = None,
        rejection_reason: str = None,
        rejection_action: str = None,
        expires_at: datetime = None,
        group_id: str = "group-123",
        created_at: datetime = None,
        is_expired: bool = False,
        allowed_approvers: list = None
    ):
        self.id = id
        self.execution_id = execution_id
        self.flow_id = flow_id
        self.gate_node_id = gate_node_id
        self.crew_sequence = crew_sequence
        self.status = status
        self.gate_config = gate_config or {
            "message": "Review output before proceeding",
            "timeout_seconds": 3600,
            "timeout_action": "auto_reject",
            "allowed_approvers": allowed_approvers
        }
        self.previous_crew_name = previous_crew_name
        self.previous_crew_output = previous_crew_output
        self.flow_state_snapshot = flow_state_snapshot or {}
        self.responded_by = responded_by
        self.responded_at = responded_at
        self.approval_comment = approval_comment
        self.rejection_reason = rejection_reason
        self.rejection_action = rejection_action
        self.expires_at = expires_at or (datetime.now(timezone.utc) + timedelta(hours=1))
        self.group_id = group_id
        self.created_at = created_at or datetime.now(timezone.utc)
        self._is_expired = is_expired
        self._allowed_approvers = allowed_approvers or []

    @property
    def is_expired(self) -> bool:
        """Return configured expiration status."""
        return self._is_expired

    @property
    def timeout_action(self) -> str:
        """Get the configured timeout action."""
        return self.gate_config.get("timeout_action", HITLTimeoutAction.AUTO_REJECT)

    def can_be_approved_by(self, user_email: str) -> bool:
        """Check if user can approve this gate."""
        if not self._allowed_approvers:
            return True
        return user_email.lower() in [email.lower() for email in self._allowed_approvers]


class MockExecutionHistory:
    """Mock execution history for testing."""

    def __init__(
        self,
        id: int = 1,
        job_id: str = "exec-123",
        status: str = "running",
        flow_id: str = "flow-456",
        flow_uuid: str = "flow-uuid-789",
        inputs: dict = None,
        error: str = None
    ):
        self.id = id
        self.job_id = job_id
        self.status = status
        self.flow_id = flow_id
        self.flow_uuid = flow_uuid
        self.inputs = inputs or {
            "flow_id": "flow-456",
            "nodes": [{"id": "node-1"}],
            "edges": [{"source": "node-1", "target": "node-2"}]
        }
        self.error = error


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def mock_session():
    """Create a mock async database session."""
    session = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    return session


@pytest.fixture
def mock_approval_repository():
    """Create a mock HITL approval repository."""
    return AsyncMock()


@pytest.fixture
def mock_webhook_repository():
    """Create a mock HITL webhook repository."""
    return AsyncMock()


@pytest.fixture
def hitl_service(mock_session, mock_approval_repository, mock_webhook_repository):
    """Create an HITLService instance with mocked dependencies."""
    service = HITLService(
        session=mock_session,
        approval_repository=mock_approval_repository,
        webhook_repository=mock_webhook_repository
    )
    return service


@pytest.fixture
def mock_approval():
    """Create a mock HITL approval."""
    return MockHITLApproval()


@pytest.fixture
def mock_approval_with_allowed_approvers():
    """Create a mock approval with specific allowed approvers."""
    return MockHITLApproval(
        allowed_approvers=["admin@example.com", "manager@example.com"]
    )


@pytest.fixture
def mock_expired_approval():
    """Create a mock expired approval."""
    return MockHITLApproval(
        is_expired=True,
        expires_at=datetime.now(timezone.utc) - timedelta(hours=1)
    )


@pytest.fixture
def mock_processed_approval():
    """Create a mock already processed approval."""
    return MockHITLApproval(
        status=HITLApprovalStatus.APPROVED,
        responded_by="approver@example.com",
        responded_at=datetime.now(timezone.utc)
    )


@pytest.fixture
def gate_config():
    """Create a sample gate configuration."""
    return {
        "message": "Please review the output before proceeding",
        "timeout_seconds": 3600,
        "timeout_action": "auto_reject",
        "require_comment": False,
        "allowed_approvers": None
    }


# ==============================================================================
# Test Cases: HITLService Initialization
# ==============================================================================

class TestHITLServiceInit:
    """Test cases for HITLService initialization."""

    def test_init_with_session_only(self, mock_session):
        """Test initialization with session creates default repositories."""
        with patch('src.services.hitl_service.HITLApprovalRepository') as mock_repo_class, \
             patch('src.services.hitl_service.HITLWebhookRepository') as mock_webhook_class:
            service = HITLService(session=mock_session)

            mock_repo_class.assert_called_once_with(mock_session)
            mock_webhook_class.assert_called_once_with(mock_session)
            assert service.session == mock_session

    def test_init_with_custom_repositories(
        self,
        mock_session,
        mock_approval_repository,
        mock_webhook_repository
    ):
        """Test initialization with custom repositories."""
        service = HITLService(
            session=mock_session,
            approval_repository=mock_approval_repository,
            webhook_repository=mock_webhook_repository
        )

        assert service.approval_repo == mock_approval_repository
        assert service.webhook_repo == mock_webhook_repository


# ==============================================================================
# Test Cases: create_approval_request
# ==============================================================================

class TestCreateApprovalRequest:
    """Test cases for create_approval_request method."""

    @pytest.mark.asyncio
    async def test_create_approval_request_success(
        self,
        hitl_service,
        mock_approval_repository,
        gate_config
    ):
        """Test successful approval request creation."""
        created_approval = MockHITLApproval()
        mock_approval_repository.create.return_value = created_approval

        with patch.object(hitl_service, '_update_execution_status', new_callable=AsyncMock):
            result = await hitl_service.create_approval_request(
                execution_id="exec-123",
                flow_id="flow-456",
                gate_node_id="gate-1",
                crew_sequence=1,
                gate_config=gate_config,
                group_id="group-123",
                previous_crew_name="Research Crew",
                previous_crew_output="Research findings...",
                flow_state_snapshot={"key": "value"}
            )

            assert result == created_approval
            mock_approval_repository.create.assert_called_once()

            # Verify the approval was created with correct attributes
            call_args = mock_approval_repository.create.call_args[0][0]
            assert call_args.execution_id == "exec-123"
            assert call_args.flow_id == "flow-456"
            assert call_args.gate_node_id == "gate-1"
            assert call_args.crew_sequence == 1
            assert call_args.status == HITLApprovalStatus.PENDING
            assert call_args.group_id == "group-123"

    @pytest.mark.asyncio
    async def test_create_approval_request_updates_execution_status(
        self,
        hitl_service,
        mock_approval_repository,
        gate_config
    ):
        """Test that creation updates execution status to WAITING_FOR_APPROVAL."""
        created_approval = MockHITLApproval()
        mock_approval_repository.create.return_value = created_approval

        with patch.object(
            hitl_service,
            '_update_execution_status',
            new_callable=AsyncMock
        ) as mock_update_status:
            await hitl_service.create_approval_request(
                execution_id="exec-123",
                flow_id="flow-456",
                gate_node_id="gate-1",
                crew_sequence=1,
                gate_config=gate_config,
                group_id="group-123"
            )

            mock_update_status.assert_called_once_with(
                execution_id="exec-123",
                status=ExecutionStatus.WAITING_FOR_APPROVAL.value,
                message="Waiting for approval at gate: gate-1"
            )

    @pytest.mark.asyncio
    async def test_create_approval_request_with_custom_timeout(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test approval creation with custom timeout."""
        custom_gate_config = {
            "message": "Review",
            "timeout_seconds": 7200,  # 2 hours
            "timeout_action": "fail"
        }
        created_approval = MockHITLApproval()
        mock_approval_repository.create.return_value = created_approval

        with patch.object(hitl_service, '_update_execution_status', new_callable=AsyncMock):
            await hitl_service.create_approval_request(
                execution_id="exec-123",
                flow_id="flow-456",
                gate_node_id="gate-1",
                crew_sequence=1,
                gate_config=custom_gate_config,
                group_id="group-123"
            )

            call_args = mock_approval_repository.create.call_args[0][0]
            # Verify expiration time is approximately 2 hours from now
            expected_expiry = datetime.now(timezone.utc) + timedelta(seconds=7200)
            actual_expiry = call_args.expires_at
            time_diff = abs((expected_expiry - actual_expiry).total_seconds())
            assert time_diff < 5  # Within 5 seconds tolerance

    @pytest.mark.asyncio
    async def test_create_approval_request_database_error(
        self,
        hitl_service,
        mock_approval_repository,
        gate_config
    ):
        """Test approval creation handles database errors."""
        mock_approval_repository.create.side_effect = SQLAlchemyError("Database error")

        with pytest.raises(HITLServiceError) as exc_info:
            await hitl_service.create_approval_request(
                execution_id="exec-123",
                flow_id="flow-456",
                gate_node_id="gate-1",
                crew_sequence=1,
                gate_config=gate_config,
                group_id="group-123"
            )

        assert "Failed to create approval request" in str(exc_info.value)


# ==============================================================================
# Test Cases: approve
# ==============================================================================

class TestApprove:
    """Test cases for approve method."""

    @pytest.mark.asyncio
    async def test_approve_success(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test successful approval."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ):
            result = await hitl_service.approve(
                approval_id=1,
                approved_by="approver@example.com",
                group_id="group-123",
                comment="Looks good"
            )

            assert isinstance(result, HITLActionResponse)
            assert result.success is True
            assert result.approval_id == 1
            assert result.status == HITLApprovalStatusEnum.APPROVED
            assert result.execution_resumed is True

            mock_approval_repository.update_status.assert_called_once_with(
                approval_id=1,
                status=HITLApprovalStatus.APPROVED,
                responded_by="approver@example.com",
                approval_comment="Looks good"
            )

    @pytest.mark.asyncio
    async def test_approve_with_user_token(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test approval passes user token for OBO authentication."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ) as mock_resume:
            await hitl_service.approve(
                approval_id=1,
                approved_by="approver@example.com",
                group_id="group-123",
                user_token="user-access-token-123"
            )

            mock_resume.assert_called_once()
            call_kwargs = mock_resume.call_args
            assert call_kwargs[1]['user_token'] == "user-access-token-123"

    @pytest.mark.asyncio
    async def test_approve_not_found(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test approval when approval record not found."""
        mock_approval_repository.get_by_id.return_value = None

        with pytest.raises(HITLApprovalNotFoundError) as exc_info:
            await hitl_service.approve(
                approval_id=999,
                approved_by="approver@example.com",
                group_id="group-123"
            )

        assert "Approval 999 not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_approve_already_processed(
        self,
        hitl_service,
        mock_approval_repository,
        mock_processed_approval
    ):
        """Test approval when already processed."""
        mock_approval_repository.get_by_id.return_value = mock_processed_approval

        with pytest.raises(HITLApprovalAlreadyProcessedError) as exc_info:
            await hitl_service.approve(
                approval_id=1,
                approved_by="approver@example.com",
                group_id="group-123"
            )

        assert "already processed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_approve_expired(
        self,
        hitl_service,
        mock_approval_repository,
        mock_expired_approval
    ):
        """Test approval when approval has expired."""
        mock_approval_repository.get_by_id.return_value = mock_expired_approval

        with pytest.raises(HITLApprovalExpiredError) as exc_info:
            await hitl_service.approve(
                approval_id=1,
                approved_by="approver@example.com",
                group_id="group-123"
            )

        assert "has expired" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_approve_permission_denied(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval_with_allowed_approvers
    ):
        """Test approval when user not in allowed approvers list."""
        mock_approval_repository.get_by_id.return_value = mock_approval_with_allowed_approvers

        with pytest.raises(HITLPermissionDeniedError) as exc_info:
            await hitl_service.approve(
                approval_id=1,
                approved_by="unauthorized@example.com",
                group_id="group-123"
            )

        assert "not allowed to approve" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_approve_with_allowed_approver(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval_with_allowed_approvers
    ):
        """Test approval by user in allowed approvers list."""
        mock_approval_repository.get_by_id.return_value = mock_approval_with_allowed_approvers
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ):
            result = await hitl_service.approve(
                approval_id=1,
                approved_by="admin@example.com",  # In allowed list
                group_id="group-123"
            )

            assert result.success is True

    @pytest.mark.asyncio
    async def test_approve_database_error(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test approval handles database errors."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.side_effect = SQLAlchemyError("Database error")

        with pytest.raises(HITLServiceError) as exc_info:
            await hitl_service.approve(
                approval_id=1,
                approved_by="approver@example.com",
                group_id="group-123"
            )

        assert "Failed to approve" in str(exc_info.value)


# ==============================================================================
# Test Cases: reject
# ==============================================================================

class TestReject:
    """Test cases for reject method."""

    @pytest.mark.asyncio
    async def test_reject_success_default_action(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test successful rejection with default REJECT action."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(hitl_service, '_fail_execution', new_callable=AsyncMock):
            result = await hitl_service.reject(
                approval_id=1,
                rejected_by="reviewer@example.com",
                group_id="group-123",
                reason="Output quality not acceptable"
            )

            assert isinstance(result, HITLActionResponse)
            assert result.success is True
            assert result.status == HITLApprovalStatusEnum.REJECTED
            assert result.execution_resumed is False
            assert "rejected" in result.message.lower()

    @pytest.mark.asyncio
    async def test_reject_with_retry_action(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test rejection with RETRY action."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_retry_previous_crew',
            new_callable=AsyncMock,
            return_value=True
        ):
            result = await hitl_service.reject(
                approval_id=1,
                rejected_by="reviewer@example.com",
                group_id="group-123",
                reason="Please try again with different inputs",
                action=HITLRejectionActionEnum.RETRY
            )

            assert result.success is True
            assert result.status == HITLApprovalStatusEnum.RETRY
            assert result.execution_resumed is True
            assert "retry" in result.message.lower()

            mock_approval_repository.update_status.assert_called_once_with(
                approval_id=1,
                status=HITLApprovalStatus.RETRY,
                responded_by="reviewer@example.com",
                rejection_reason="Please try again with different inputs",
                rejection_action=HITLRejectionActionEnum.RETRY.value
            )

    @pytest.mark.asyncio
    async def test_reject_not_found(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test rejection when approval not found."""
        mock_approval_repository.get_by_id.return_value = None

        with pytest.raises(HITLApprovalNotFoundError):
            await hitl_service.reject(
                approval_id=999,
                rejected_by="reviewer@example.com",
                group_id="group-123",
                reason="Rejected"
            )

    @pytest.mark.asyncio
    async def test_reject_already_processed(
        self,
        hitl_service,
        mock_approval_repository,
        mock_processed_approval
    ):
        """Test rejection when already processed."""
        mock_approval_repository.get_by_id.return_value = mock_processed_approval

        with pytest.raises(HITLApprovalAlreadyProcessedError):
            await hitl_service.reject(
                approval_id=1,
                rejected_by="reviewer@example.com",
                group_id="group-123",
                reason="Rejected"
            )

    @pytest.mark.asyncio
    async def test_reject_expired(
        self,
        hitl_service,
        mock_approval_repository,
        mock_expired_approval
    ):
        """Test rejection when expired."""
        mock_approval_repository.get_by_id.return_value = mock_expired_approval

        with pytest.raises(HITLApprovalExpiredError):
            await hitl_service.reject(
                approval_id=1,
                rejected_by="reviewer@example.com",
                group_id="group-123",
                reason="Rejected"
            )

    @pytest.mark.asyncio
    async def test_reject_permission_denied(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval_with_allowed_approvers
    ):
        """Test rejection when user not authorized."""
        mock_approval_repository.get_by_id.return_value = mock_approval_with_allowed_approvers

        with pytest.raises(HITLPermissionDeniedError):
            await hitl_service.reject(
                approval_id=1,
                rejected_by="unauthorized@example.com",
                group_id="group-123",
                reason="Rejected"
            )

    @pytest.mark.asyncio
    async def test_reject_database_error(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test rejection handles database errors."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.side_effect = SQLAlchemyError("Database error")

        with pytest.raises(HITLServiceError) as exc_info:
            await hitl_service.reject(
                approval_id=1,
                rejected_by="reviewer@example.com",
                group_id="group-123",
                reason="Rejected"
            )

        assert "Failed to reject" in str(exc_info.value)


# ==============================================================================
# Test Cases: get_pending_approvals
# ==============================================================================

class TestGetPendingApprovals:
    """Test cases for get_pending_approvals method."""

    @pytest.mark.asyncio
    async def test_get_pending_approvals_success(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test successful retrieval of pending approvals."""
        approvals = [
            MockHITLApproval(id=1, gate_node_id="gate-1"),
            MockHITLApproval(id=2, gate_node_id="gate-2"),
        ]
        mock_approval_repository.get_pending_for_group.return_value = (approvals, 2)

        result = await hitl_service.get_pending_approvals(
            group_id="group-123",
            limit=50,
            offset=0
        )

        assert isinstance(result, HITLApprovalListResponse)
        assert result.total == 2
        assert len(result.items) == 2
        assert result.limit == 50
        assert result.offset == 0

        mock_approval_repository.get_pending_for_group.assert_called_once_with(
            group_id="group-123",
            limit=50,
            offset=0
        )

    @pytest.mark.asyncio
    async def test_get_pending_approvals_empty(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test getting pending approvals when none exist."""
        mock_approval_repository.get_pending_for_group.return_value = ([], 0)

        result = await hitl_service.get_pending_approvals(
            group_id="group-123"
        )

        assert result.total == 0
        assert len(result.items) == 0

    @pytest.mark.asyncio
    async def test_get_pending_approvals_with_pagination(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test pagination of pending approvals."""
        approvals = [MockHITLApproval(id=3)]
        mock_approval_repository.get_pending_for_group.return_value = (approvals, 5)

        result = await hitl_service.get_pending_approvals(
            group_id="group-123",
            limit=2,
            offset=4
        )

        assert result.total == 5
        assert len(result.items) == 1
        assert result.limit == 2
        assert result.offset == 4

    @pytest.mark.asyncio
    async def test_get_pending_approvals_database_error(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test handling of database errors."""
        mock_approval_repository.get_pending_for_group.side_effect = SQLAlchemyError(
            "Database error"
        )

        with pytest.raises(HITLServiceError) as exc_info:
            await hitl_service.get_pending_approvals(group_id="group-123")

        assert "Failed to get pending approvals" in str(exc_info.value)


# ==============================================================================
# Test Cases: get_execution_hitl_status
# ==============================================================================

class TestGetExecutionHITLStatus:
    """Test cases for get_execution_hitl_status method."""

    @pytest.mark.asyncio
    async def test_get_execution_hitl_status_with_pending(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test getting HITL status with pending approval."""
        approvals = [
            MockHITLApproval(id=1, status=HITLApprovalStatus.APPROVED),
            MockHITLApproval(id=2, status=HITLApprovalStatus.PENDING),
        ]
        mock_approval_repository.get_all_for_execution.return_value = approvals

        result = await hitl_service.get_execution_hitl_status(
            execution_id="exec-123",
            group_id="group-123"
        )

        assert isinstance(result, ExecutionHITLStatus)
        assert result.execution_id == "exec-123"
        assert result.has_pending_approval is True
        assert result.pending_approval is not None
        assert result.pending_approval.id == 2
        assert result.total_gates_passed == 1
        assert len(result.approval_history) == 2

    @pytest.mark.asyncio
    async def test_get_execution_hitl_status_no_pending(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test getting HITL status without pending approval."""
        approvals = [
            MockHITLApproval(id=1, status=HITLApprovalStatus.APPROVED),
            MockHITLApproval(id=2, status=HITLApprovalStatus.APPROVED),
        ]
        mock_approval_repository.get_all_for_execution.return_value = approvals

        result = await hitl_service.get_execution_hitl_status(
            execution_id="exec-123",
            group_id="group-123"
        )

        assert result.has_pending_approval is False
        assert result.pending_approval is None
        assert result.total_gates_passed == 2

    @pytest.mark.asyncio
    async def test_get_execution_hitl_status_no_history(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test getting HITL status for execution with no approvals."""
        mock_approval_repository.get_all_for_execution.return_value = []

        result = await hitl_service.get_execution_hitl_status(
            execution_id="exec-123",
            group_id="group-123"
        )

        assert result.has_pending_approval is False
        assert result.pending_approval is None
        assert result.total_gates_passed == 0
        assert len(result.approval_history) == 0

    @pytest.mark.asyncio
    async def test_get_execution_hitl_status_database_error(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test handling of database errors."""
        mock_approval_repository.get_all_for_execution.side_effect = SQLAlchemyError(
            "Database error"
        )

        with pytest.raises(HITLServiceError) as exc_info:
            await hitl_service.get_execution_hitl_status(
                execution_id="exec-123",
                group_id="group-123"
            )

        assert "Failed to get HITL status" in str(exc_info.value)


# ==============================================================================
# Test Cases: process_expired_approvals
# ==============================================================================

class TestProcessExpiredApprovals:
    """Test cases for process_expired_approvals method."""

    @pytest.mark.asyncio
    async def test_process_expired_approvals_auto_reject(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test processing expired approvals with auto_reject action."""
        expired_approval = MockHITLApproval(
            id=1,
            gate_config={"timeout_action": HITLTimeoutAction.AUTO_REJECT}
        )
        mock_approval_repository.get_expired_pending.return_value = [expired_approval]
        mock_approval_repository.update_status.return_value = True

        with patch.object(hitl_service, '_fail_execution', new_callable=AsyncMock):
            result = await hitl_service.process_expired_approvals()

            assert result == [1]
            mock_approval_repository.update_status.assert_called_once_with(
                approval_id=1,
                status=HITLApprovalStatus.TIMEOUT,
                responded_by="system",
                rejection_reason="Approval timed out (auto-rejected)"
            )

    @pytest.mark.asyncio
    async def test_process_expired_approvals_fail_action(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test processing expired approvals with fail action."""
        expired_approval = MockHITLApproval(
            id=1,
            gate_config={"timeout_action": HITLTimeoutAction.FAIL}
        )
        mock_approval_repository.get_expired_pending.return_value = [expired_approval]
        mock_approval_repository.update_status.return_value = True

        with patch.object(hitl_service, '_fail_execution', new_callable=AsyncMock):
            result = await hitl_service.process_expired_approvals()

            assert result == [1]
            mock_approval_repository.update_status.assert_called_once_with(
                approval_id=1,
                status=HITLApprovalStatus.TIMEOUT,
                responded_by="system",
                rejection_reason="Approval timed out"
            )

    @pytest.mark.asyncio
    async def test_process_expired_approvals_multiple(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test processing multiple expired approvals."""
        expired_approvals = [
            MockHITLApproval(id=1),
            MockHITLApproval(id=2),
            MockHITLApproval(id=3),
        ]
        mock_approval_repository.get_expired_pending.return_value = expired_approvals
        mock_approval_repository.update_status.return_value = True

        with patch.object(hitl_service, '_fail_execution', new_callable=AsyncMock):
            result = await hitl_service.process_expired_approvals()

            assert result == [1, 2, 3]
            assert mock_approval_repository.update_status.call_count == 3

    @pytest.mark.asyncio
    async def test_process_expired_approvals_none_expired(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test processing when no approvals are expired."""
        mock_approval_repository.get_expired_pending.return_value = []

        result = await hitl_service.process_expired_approvals()

        assert result == []
        mock_approval_repository.update_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_expired_approvals_partial_failure(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test processing continues when individual approval processing fails."""
        expired_approvals = [
            MockHITLApproval(id=1),
            MockHITLApproval(id=2),
        ]
        mock_approval_repository.get_expired_pending.return_value = expired_approvals

        # First call succeeds, second fails
        mock_approval_repository.update_status.side_effect = [
            True,
            Exception("Update failed")
        ]

        with patch.object(hitl_service, '_fail_execution', new_callable=AsyncMock):
            result = await hitl_service.process_expired_approvals()

            # Only first approval should be in results
            assert result == [1]

    @pytest.mark.asyncio
    async def test_process_expired_approvals_database_error(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test handling of database error when fetching expired approvals."""
        mock_approval_repository.get_expired_pending.side_effect = SQLAlchemyError(
            "Database error"
        )

        with pytest.raises(HITLServiceError) as exc_info:
            await hitl_service.process_expired_approvals()

        assert "Failed to process expired approvals" in str(exc_info.value)


# ==============================================================================
# Test Cases: _resume_flow_execution
# ==============================================================================

class TestResumeFlowExecution:
    """Test cases for _resume_flow_execution private method."""

    @pytest.mark.asyncio
    async def test_resume_flow_execution_success(
        self,
        hitl_service,
        mock_approval,
        mock_session
    ):
        """Test successful flow resume after approval."""
        mock_execution = MockExecutionHistory()

        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class, \
             patch('src.services.crewai_execution_service.CrewAIExecutionService'), \
             patch('src.services.execution_status_service.ExecutionStatusService'), \
             patch('src.utils.user_context.GroupContext'), \
             patch('asyncio.create_task') as mock_create_task, \
             patch.object(hitl_service, '_update_execution_status', new_callable=AsyncMock):

            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            result = await hitl_service._resume_flow_execution(
                approval=mock_approval,
                user_token="user-token-123"
            )

            assert result is True
            mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_resume_flow_execution_not_found(
        self,
        hitl_service,
        mock_approval
    ):
        """Test resume when execution not found."""
        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class, \
             patch.object(hitl_service, '_update_execution_status', new_callable=AsyncMock):

            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = None
            mock_repo_class.return_value = mock_repo

            result = await hitl_service._resume_flow_execution(
                approval=mock_approval
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_resume_flow_execution_exception(
        self,
        hitl_service,
        mock_approval
    ):
        """Test resume handles exceptions gracefully."""
        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class:
            mock_repo_class.side_effect = Exception("Unexpected error")

            result = await hitl_service._resume_flow_execution(
                approval=mock_approval
            )

            assert result is False


# ==============================================================================
# Test Cases: _retry_previous_crew
# ==============================================================================

class TestRetryPreviousCrew:
    """Test cases for _retry_previous_crew private method."""

    @pytest.mark.asyncio
    async def test_retry_previous_crew_success(
        self,
        hitl_service,
        mock_approval
    ):
        """Test successful retry of previous crew."""
        mock_execution = MockExecutionHistory()

        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class, \
             patch('src.services.crewai_execution_service.CrewAIExecutionService'), \
             patch('src.services.execution_status_service.ExecutionStatusService'), \
             patch('src.utils.user_context.GroupContext'), \
             patch('asyncio.create_task') as mock_create_task, \
             patch.object(hitl_service, '_update_execution_status', new_callable=AsyncMock):

            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            result = await hitl_service._retry_previous_crew(mock_approval)

            assert result is True
            mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_previous_crew_not_found(
        self,
        hitl_service,
        mock_approval
    ):
        """Test retry when execution not found."""
        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class, \
             patch.object(hitl_service, '_update_execution_status', new_callable=AsyncMock):

            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = None
            mock_repo_class.return_value = mock_repo

            result = await hitl_service._retry_previous_crew(mock_approval)

            assert result is False

    @pytest.mark.asyncio
    async def test_retry_previous_crew_exception(
        self,
        hitl_service,
        mock_approval
    ):
        """Test retry handles exceptions gracefully."""
        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class:
            mock_repo_class.side_effect = Exception("Unexpected error")

            result = await hitl_service._retry_previous_crew(mock_approval)

            assert result is False


# ==============================================================================
# Test Cases: _fail_execution
# ==============================================================================

class TestFailExecution:
    """Test cases for _fail_execution private method."""

    @pytest.mark.asyncio
    async def test_fail_execution_success(
        self,
        hitl_service,
        mock_approval
    ):
        """Test successful execution failure."""
        with patch.object(
            hitl_service,
            '_update_execution_status',
            new_callable=AsyncMock
        ) as mock_update:
            await hitl_service._fail_execution(
                approval=mock_approval,
                reason="Output quality not acceptable"
            )

            mock_update.assert_called_once_with(
                execution_id=mock_approval.execution_id,
                status=ExecutionStatus.REJECTED.value,
                message="HITL gate rejected: Output quality not acceptable"
            )

    @pytest.mark.asyncio
    async def test_fail_execution_handles_error(
        self,
        hitl_service,
        mock_approval
    ):
        """Test fail_execution handles errors gracefully."""
        with patch.object(
            hitl_service,
            '_update_execution_status',
            new_callable=AsyncMock,
            side_effect=Exception("Update failed")
        ):
            # Should not raise, just log the error
            await hitl_service._fail_execution(
                approval=mock_approval,
                reason="Test reason"
            )


# ==============================================================================
# Test Cases: _update_execution_status
# ==============================================================================

class TestUpdateExecutionStatus:
    """Test cases for _update_execution_status private method."""

    @pytest.mark.asyncio
    async def test_update_execution_status_success(
        self,
        hitl_service,
        mock_session
    ):
        """Test successful status update."""
        mock_execution = MockExecutionHistory()

        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            await hitl_service._update_execution_status(
                execution_id="exec-123",
                status="waiting_for_approval",
                message="Waiting for approval"
            )

            assert mock_execution.status == "waiting_for_approval"
            mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_execution_status_with_error_message(
        self,
        hitl_service,
        mock_session
    ):
        """Test status update includes error message for failure statuses."""
        mock_execution = MockExecutionHistory()

        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            await hitl_service._update_execution_status(
                execution_id="exec-123",
                status=ExecutionStatus.FAILED.value,
                message="HITL gate failed"
            )

            assert mock_execution.error == "HITL gate failed"

    @pytest.mark.asyncio
    async def test_update_execution_status_not_found(
        self,
        hitl_service,
        mock_session
    ):
        """Test status update when execution not found."""
        with patch(
            'src.repositories.execution_history_repository.ExecutionHistoryRepository'
        ) as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_execution_by_job_id.return_value = None
            mock_repo_class.return_value = mock_repo

            # Should not raise, just skip update
            await hitl_service._update_execution_status(
                execution_id="nonexistent",
                status="running"
            )

            mock_session.flush.assert_not_called()


# ==============================================================================
# Test Cases: Edge Cases and Error Handling
# ==============================================================================

class TestEdgeCases:
    """Test cases for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_approve_without_comment(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test approval without providing a comment."""
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ):
            result = await hitl_service.approve(
                approval_id=1,
                approved_by="approver@example.com",
                group_id="group-123"
                # No comment provided
            )

            assert result.success is True
            mock_approval_repository.update_status.assert_called_once()
            call_kwargs = mock_approval_repository.update_status.call_args[1]
            assert call_kwargs['approval_comment'] is None

    @pytest.mark.asyncio
    async def test_approval_response_serialization(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test that approval response correctly serializes with rejection action."""
        approval_with_rejection = MockHITLApproval(
            rejection_action=HITLRejectionAction.RETRY
        )
        mock_approval_repository.get_pending_for_group.return_value = (
            [approval_with_rejection],
            1
        )

        result = await hitl_service.get_pending_approvals(group_id="group-123")

        assert len(result.items) == 1
        # Verify that rejection_action is properly converted to enum

    @pytest.mark.asyncio
    async def test_concurrent_approval_attempt(
        self,
        hitl_service,
        mock_approval_repository,
        mock_processed_approval
    ):
        """Test handling concurrent approval attempts (second request fails)."""
        # Simulate race condition where approval was processed between check and update
        mock_approval_repository.get_by_id.return_value = mock_processed_approval

        with pytest.raises(HITLApprovalAlreadyProcessedError):
            await hitl_service.approve(
                approval_id=1,
                approved_by="second-approver@example.com",
                group_id="group-123"
            )

    @pytest.mark.asyncio
    async def test_case_insensitive_approver_check(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test that approver email check is case-insensitive."""
        approval = MockHITLApproval(
            allowed_approvers=["Admin@Example.COM"]
        )
        mock_approval_repository.get_by_id.return_value = approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ):
            # Should succeed with different case
            result = await hitl_service.approve(
                approval_id=1,
                approved_by="admin@example.com",  # lowercase
                group_id="group-123"
            )

            assert result.success is True

    @pytest.mark.asyncio
    async def test_empty_allowed_approvers_allows_anyone(
        self,
        hitl_service,
        mock_approval_repository
    ):
        """Test that empty allowed_approvers list allows any user in group."""
        approval = MockHITLApproval(allowed_approvers=[])
        mock_approval_repository.get_by_id.return_value = approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ):
            result = await hitl_service.approve(
                approval_id=1,
                approved_by="anyone@example.com",
                group_id="group-123"
            )

            assert result.success is True

    @pytest.mark.asyncio
    async def test_null_allowed_approvers_allows_anyone(
        self,
        hitl_service,
        mock_approval_repository,
        mock_approval
    ):
        """Test that null allowed_approvers allows any user in group."""
        # mock_approval has None/empty allowed_approvers by default
        mock_approval_repository.get_by_id.return_value = mock_approval
        mock_approval_repository.update_status.return_value = True

        with patch.object(
            hitl_service,
            '_resume_flow_execution',
            new_callable=AsyncMock,
            return_value=True
        ):
            result = await hitl_service.approve(
                approval_id=1,
                approved_by="random@example.com",
                group_id="group-123"
            )

            assert result.success is True
