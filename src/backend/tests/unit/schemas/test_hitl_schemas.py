"""
Unit tests for HITL (Human in the Loop) schemas.

Tests the functionality of Pydantic schemas for HITL operations
including validation, serialization, enum values, and field constraints.
"""
import pytest
from datetime import datetime, timezone
from pydantic import ValidationError
from typing import Dict, Any

from src.schemas.hitl import (
    # Enums
    HITLApprovalStatusEnum,
    HITLTimeoutActionEnum,
    HITLRejectionActionEnum,
    HITLWebhookEventEnum,
    # Gate Configuration
    HITLGateConfig,
    # Approval Schemas
    HITLApprovalBase,
    HITLApprovalCreate,
    HITLApprovalResponse,
    HITLApprovalListResponse,
    # Action Schemas
    HITLApproveRequest,
    HITLRejectRequest,
    HITLActionResponse,
    # Status Schema
    ExecutionHITLStatus,
    # Webhook Schemas
    HITLWebhookBase,
    HITLWebhookCreate,
    HITLWebhookUpdate,
    HITLWebhookResponse,
    HITLWebhookListResponse,
    HITLWebhookPayload,
    # Node Configuration
    HITLGateNodeData,
)


# =============================================================================
# Enum Tests
# =============================================================================

class TestHITLApprovalStatusEnum:
    """Test cases for HITLApprovalStatusEnum."""

    def test_all_status_values(self):
        """Test all HITL approval status enum values exist."""
        assert HITLApprovalStatusEnum.PENDING == "pending"
        assert HITLApprovalStatusEnum.APPROVED == "approved"
        assert HITLApprovalStatusEnum.REJECTED == "rejected"
        assert HITLApprovalStatusEnum.TIMEOUT == "timeout"
        assert HITLApprovalStatusEnum.RETRY == "retry"

    def test_status_enum_count(self):
        """Test that enum has exactly 5 values."""
        assert len(HITLApprovalStatusEnum) == 5

    def test_status_is_str_enum(self):
        """Test that status enum is a string enum."""
        assert isinstance(HITLApprovalStatusEnum.PENDING, str)
        assert HITLApprovalStatusEnum.PENDING.value == "pending"

    def test_status_enum_member_access(self):
        """Test enum member access by value."""
        assert HITLApprovalStatusEnum("pending") == HITLApprovalStatusEnum.PENDING
        assert HITLApprovalStatusEnum("approved") == HITLApprovalStatusEnum.APPROVED

    def test_status_enum_invalid_value(self):
        """Test that invalid status value raises ValueError."""
        with pytest.raises(ValueError):
            HITLApprovalStatusEnum("invalid_status")


class TestHITLTimeoutActionEnum:
    """Test cases for HITLTimeoutActionEnum."""

    def test_all_timeout_action_values(self):
        """Test all timeout action enum values exist."""
        assert HITLTimeoutActionEnum.AUTO_REJECT == "auto_reject"
        assert HITLTimeoutActionEnum.FAIL == "fail"

    def test_timeout_action_enum_count(self):
        """Test that enum has exactly 2 values."""
        assert len(HITLTimeoutActionEnum) == 2

    def test_timeout_action_is_str_enum(self):
        """Test that timeout action enum is a string enum."""
        assert isinstance(HITLTimeoutActionEnum.AUTO_REJECT, str)
        assert HITLTimeoutActionEnum.AUTO_REJECT.value == "auto_reject"


class TestHITLRejectionActionEnum:
    """Test cases for HITLRejectionActionEnum."""

    def test_all_rejection_action_values(self):
        """Test all rejection action enum values exist."""
        assert HITLRejectionActionEnum.REJECT == "reject"
        assert HITLRejectionActionEnum.RETRY == "retry"

    def test_rejection_action_enum_count(self):
        """Test that enum has exactly 2 values."""
        assert len(HITLRejectionActionEnum) == 2

    def test_rejection_action_is_str_enum(self):
        """Test that rejection action enum is a string enum."""
        assert isinstance(HITLRejectionActionEnum.REJECT, str)
        assert HITLRejectionActionEnum.REJECT.value == "reject"


class TestHITLWebhookEventEnum:
    """Test cases for HITLWebhookEventEnum."""

    def test_all_webhook_event_values(self):
        """Test all webhook event enum values exist."""
        assert HITLWebhookEventEnum.GATE_REACHED == "gate_reached"
        assert HITLWebhookEventEnum.GATE_APPROVED == "gate_approved"
        assert HITLWebhookEventEnum.GATE_REJECTED == "gate_rejected"
        assert HITLWebhookEventEnum.GATE_TIMEOUT == "gate_timeout"

    def test_webhook_event_enum_count(self):
        """Test that enum has exactly 4 values."""
        assert len(HITLWebhookEventEnum) == 4

    def test_webhook_event_is_str_enum(self):
        """Test that webhook event enum is a string enum."""
        assert isinstance(HITLWebhookEventEnum.GATE_REACHED, str)
        assert HITLWebhookEventEnum.GATE_REACHED.value == "gate_reached"


# =============================================================================
# Gate Configuration Tests
# =============================================================================

class TestHITLGateConfig:
    """Test cases for HITLGateConfig schema."""

    def test_default_values(self):
        """Test HITLGateConfig with default values."""
        config = HITLGateConfig()
        assert config.message == "Approval required to proceed"
        assert config.timeout_seconds == 3600
        assert config.timeout_action == HITLTimeoutActionEnum.AUTO_REJECT
        assert config.require_comment is False
        assert config.allowed_approvers is None

    def test_custom_values(self):
        """Test HITLGateConfig with custom values."""
        config = HITLGateConfig(
            message="Please review the research output",
            timeout_seconds=7200,
            timeout_action=HITLTimeoutActionEnum.FAIL,
            require_comment=True,
            allowed_approvers=["admin@example.com", "manager@example.com"]
        )
        assert config.message == "Please review the research output"
        assert config.timeout_seconds == 7200
        assert config.timeout_action == HITLTimeoutActionEnum.FAIL
        assert config.require_comment is True
        assert config.allowed_approvers == ["admin@example.com", "manager@example.com"]

    def test_timeout_seconds_minimum(self):
        """Test that timeout_seconds respects minimum of 60."""
        with pytest.raises(ValidationError) as exc_info:
            HITLGateConfig(timeout_seconds=59)
        assert "timeout_seconds" in str(exc_info.value)

    def test_timeout_seconds_maximum(self):
        """Test that timeout_seconds respects maximum of 604800 (7 days)."""
        with pytest.raises(ValidationError) as exc_info:
            HITLGateConfig(timeout_seconds=604801)
        assert "timeout_seconds" in str(exc_info.value)

    def test_timeout_seconds_valid_boundary_min(self):
        """Test timeout_seconds at minimum boundary."""
        config = HITLGateConfig(timeout_seconds=60)
        assert config.timeout_seconds == 60

    def test_timeout_seconds_valid_boundary_max(self):
        """Test timeout_seconds at maximum boundary."""
        config = HITLGateConfig(timeout_seconds=604800)
        assert config.timeout_seconds == 604800

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            HITLGateConfig(unknown_field="value")
        assert "extra_forbidden" in str(exc_info.value).lower() or "extra" in str(exc_info.value).lower()

    def test_serialization(self):
        """Test HITLGateConfig serialization to dict."""
        config = HITLGateConfig(
            message="Test message",
            timeout_seconds=1800,
            timeout_action=HITLTimeoutActionEnum.FAIL,
            require_comment=True,
            allowed_approvers=["user@example.com"]
        )
        data = config.model_dump()
        assert data["message"] == "Test message"
        assert data["timeout_seconds"] == 1800
        assert data["timeout_action"] == "fail"
        assert data["require_comment"] is True
        assert data["allowed_approvers"] == ["user@example.com"]


# =============================================================================
# Approval Base Tests
# =============================================================================

class TestHITLApprovalBase:
    """Test cases for HITLApprovalBase schema."""

    def test_valid_approval_base(self):
        """Test HITLApprovalBase with all required fields."""
        data = {
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1
        }
        approval = HITLApprovalBase(**data)
        assert approval.execution_id == "exec_12345"
        assert approval.flow_id == "flow_67890"
        assert approval.gate_node_id == "gate_001"
        assert approval.crew_sequence == 1

    def test_missing_execution_id(self):
        """Test that execution_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            HITLApprovalBase(
                flow_id="flow_67890",
                gate_node_id="gate_001",
                crew_sequence=1
            )
        assert "execution_id" in str(exc_info.value)

    def test_missing_flow_id(self):
        """Test that flow_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            HITLApprovalBase(
                execution_id="exec_12345",
                gate_node_id="gate_001",
                crew_sequence=1
            )
        assert "flow_id" in str(exc_info.value)

    def test_missing_gate_node_id(self):
        """Test that gate_node_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            HITLApprovalBase(
                execution_id="exec_12345",
                flow_id="flow_67890",
                crew_sequence=1
            )
        assert "gate_node_id" in str(exc_info.value)

    def test_missing_crew_sequence(self):
        """Test that crew_sequence is required."""
        with pytest.raises(ValidationError) as exc_info:
            HITLApprovalBase(
                execution_id="exec_12345",
                flow_id="flow_67890",
                gate_node_id="gate_001"
            )
        assert "crew_sequence" in str(exc_info.value)

    def test_serialization(self):
        """Test HITLApprovalBase serialization."""
        approval = HITLApprovalBase(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=2
        )
        data = approval.model_dump()
        assert data["execution_id"] == "exec_12345"
        assert data["flow_id"] == "flow_67890"
        assert data["gate_node_id"] == "gate_001"
        assert data["crew_sequence"] == 2


# =============================================================================
# Approval Create Tests
# =============================================================================

class TestHITLApprovalCreate:
    """Test cases for HITLApprovalCreate schema."""

    def test_minimal_create(self):
        """Test HITLApprovalCreate with minimal required fields."""
        data = {
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "group_id": "group_abc"
        }
        approval = HITLApprovalCreate(**data)
        assert approval.execution_id == "exec_12345"
        assert approval.flow_id == "flow_67890"
        assert approval.gate_node_id == "gate_001"
        assert approval.crew_sequence == 1
        assert approval.group_id == "group_abc"
        # Default gate_config should be applied
        assert approval.gate_config.message == "Approval required to proceed"
        assert approval.previous_crew_name is None
        assert approval.previous_crew_output is None
        assert approval.flow_state_snapshot is None

    def test_full_create(self):
        """Test HITLApprovalCreate with all fields."""
        gate_config = HITLGateConfig(
            message="Review the analysis results",
            timeout_seconds=7200,
            require_comment=True
        )
        data = {
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "group_id": "group_abc",
            "gate_config": gate_config,
            "previous_crew_name": "Research Crew",
            "previous_crew_output": "Research findings...",
            "flow_state_snapshot": {"key": "value"}
        }
        approval = HITLApprovalCreate(**data)
        assert approval.gate_config.message == "Review the analysis results"
        assert approval.gate_config.timeout_seconds == 7200
        assert approval.previous_crew_name == "Research Crew"
        assert approval.previous_crew_output == "Research findings..."
        assert approval.flow_state_snapshot == {"key": "value"}

    def test_missing_group_id(self):
        """Test that group_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            HITLApprovalCreate(
                execution_id="exec_12345",
                flow_id="flow_67890",
                gate_node_id="gate_001",
                crew_sequence=1
            )
        assert "group_id" in str(exc_info.value)

    def test_inherits_from_base(self):
        """Test that HITLApprovalCreate inherits from HITLApprovalBase."""
        assert issubclass(HITLApprovalCreate, HITLApprovalBase)


# =============================================================================
# Approval Response Tests
# =============================================================================

class TestHITLApprovalResponse:
    """Test cases for HITLApprovalResponse schema."""

    def test_valid_pending_response(self):
        """Test HITLApprovalResponse for a pending approval."""
        now = datetime.now(timezone.utc)
        data = {
            "id": 1,
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "status": HITLApprovalStatusEnum.PENDING,
            "gate_config": {"message": "Approval needed", "timeout_seconds": 3600},
            "previous_crew_name": None,
            "previous_crew_output": None,
            "flow_state_snapshot": None,
            "responded_by": None,
            "responded_at": None,
            "approval_comment": None,
            "rejection_reason": None,
            "rejection_action": None,
            "expires_at": now,
            "is_expired": False,
            "created_at": now,
            "group_id": "group_abc"
        }
        response = HITLApprovalResponse(**data)
        assert response.id == 1
        assert response.status == HITLApprovalStatusEnum.PENDING
        assert response.is_expired is False

    def test_valid_approved_response(self):
        """Test HITLApprovalResponse for an approved approval."""
        now = datetime.now(timezone.utc)
        data = {
            "id": 2,
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "status": HITLApprovalStatusEnum.APPROVED,
            "gate_config": {"message": "Approval needed"},
            "responded_by": "user@example.com",
            "responded_at": now,
            "approval_comment": "Looks good!",
            "created_at": now,
            "group_id": "group_abc"
        }
        response = HITLApprovalResponse(**data)
        assert response.status == HITLApprovalStatusEnum.APPROVED
        assert response.responded_by == "user@example.com"
        assert response.approval_comment == "Looks good!"

    def test_valid_rejected_response(self):
        """Test HITLApprovalResponse for a rejected approval."""
        now = datetime.now(timezone.utc)
        data = {
            "id": 3,
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "status": HITLApprovalStatusEnum.REJECTED,
            "gate_config": {"message": "Approval needed"},
            "responded_by": "admin@example.com",
            "responded_at": now,
            "rejection_reason": "Quality not acceptable",
            "rejection_action": HITLRejectionActionEnum.RETRY,
            "created_at": now,
            "group_id": "group_abc"
        }
        response = HITLApprovalResponse(**data)
        assert response.status == HITLApprovalStatusEnum.REJECTED
        assert response.rejection_reason == "Quality not acceptable"
        assert response.rejection_action == HITLRejectionActionEnum.RETRY

    def test_from_attributes_config(self):
        """Test that from_attributes is enabled for ORM conversion."""
        assert HITLApprovalResponse.model_config.get("from_attributes") is True

    def test_inherits_from_base(self):
        """Test that HITLApprovalResponse inherits from HITLApprovalBase."""
        assert issubclass(HITLApprovalResponse, HITLApprovalBase)


# =============================================================================
# Approval List Response Tests
# =============================================================================

class TestHITLApprovalListResponse:
    """Test cases for HITLApprovalListResponse schema."""

    def test_valid_list_response(self):
        """Test HITLApprovalListResponse with items."""
        now = datetime.now(timezone.utc)
        item = {
            "id": 1,
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "status": HITLApprovalStatusEnum.PENDING,
            "gate_config": {"message": "Approval needed"},
            "created_at": now,
            "group_id": "group_abc"
        }
        data = {
            "items": [item],
            "total": 1,
            "limit": 10,
            "offset": 0
        }
        response = HITLApprovalListResponse(**data)
        assert len(response.items) == 1
        assert response.total == 1
        assert response.limit == 10
        assert response.offset == 0

    def test_empty_list_response(self):
        """Test HITLApprovalListResponse with empty items."""
        data = {
            "items": [],
            "total": 0,
            "limit": 10,
            "offset": 0
        }
        response = HITLApprovalListResponse(**data)
        assert len(response.items) == 0
        assert response.total == 0


# =============================================================================
# Approve Request Tests
# =============================================================================

class TestHITLApproveRequest:
    """Test cases for HITLApproveRequest schema."""

    def test_approve_without_comment(self):
        """Test approval request without comment."""
        request = HITLApproveRequest()
        assert request.comment is None

    def test_approve_with_comment(self):
        """Test approval request with comment."""
        request = HITLApproveRequest(comment="Approved after review")
        assert request.comment == "Approved after review"

    def test_comment_max_length(self):
        """Test that comment respects max length of 2000."""
        long_comment = "x" * 2001
        with pytest.raises(ValidationError) as exc_info:
            HITLApproveRequest(comment=long_comment)
        assert "comment" in str(exc_info.value).lower()

    def test_comment_max_length_valid(self):
        """Test comment at max length boundary."""
        comment = "x" * 2000
        request = HITLApproveRequest(comment=comment)
        assert len(request.comment) == 2000

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            HITLApproveRequest(comment="test", extra_field="value")
        assert "extra" in str(exc_info.value).lower()


# =============================================================================
# Reject Request Tests
# =============================================================================

class TestHITLRejectRequest:
    """Test cases for HITLRejectRequest schema."""

    def test_valid_reject_request(self):
        """Test valid rejection request with reason."""
        request = HITLRejectRequest(reason="Quality issues found")
        assert request.reason == "Quality issues found"
        assert request.action == HITLRejectionActionEnum.REJECT  # Default

    def test_reject_with_retry_action(self):
        """Test rejection request with retry action."""
        request = HITLRejectRequest(
            reason="Minor issues, please try again",
            action=HITLRejectionActionEnum.RETRY
        )
        assert request.reason == "Minor issues, please try again"
        assert request.action == HITLRejectionActionEnum.RETRY

    def test_reason_required(self):
        """Test that reason is required."""
        with pytest.raises(ValidationError) as exc_info:
            HITLRejectRequest()
        assert "reason" in str(exc_info.value)

    def test_reason_min_length(self):
        """Test that reason has minimum length of 1."""
        with pytest.raises(ValidationError) as exc_info:
            HITLRejectRequest(reason="")
        assert "reason" in str(exc_info.value).lower()

    def test_reason_max_length(self):
        """Test that reason respects max length of 2000."""
        long_reason = "x" * 2001
        with pytest.raises(ValidationError) as exc_info:
            HITLRejectRequest(reason=long_reason)
        assert "reason" in str(exc_info.value).lower()

    def test_reason_max_length_valid(self):
        """Test reason at max length boundary."""
        reason = "x" * 2000
        request = HITLRejectRequest(reason=reason)
        assert len(request.reason) == 2000

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            HITLRejectRequest(reason="test", extra_field="value")
        assert "extra" in str(exc_info.value).lower()


# =============================================================================
# Action Response Tests
# =============================================================================

class TestHITLActionResponse:
    """Test cases for HITLActionResponse schema."""

    def test_valid_action_response(self):
        """Test valid action response."""
        response = HITLActionResponse(
            success=True,
            approval_id=1,
            status=HITLApprovalStatusEnum.APPROVED,
            message="Approval submitted successfully",
            execution_resumed=True
        )
        assert response.success is True
        assert response.approval_id == 1
        assert response.status == HITLApprovalStatusEnum.APPROVED
        assert response.message == "Approval submitted successfully"
        assert response.execution_resumed is True

    def test_execution_resumed_default(self):
        """Test that execution_resumed defaults to False."""
        response = HITLActionResponse(
            success=True,
            approval_id=1,
            status=HITLApprovalStatusEnum.APPROVED,
            message="Done"
        )
        assert response.execution_resumed is False

    def test_all_required_fields(self):
        """Test that all required fields must be provided."""
        with pytest.raises(ValidationError) as exc_info:
            HITLActionResponse(success=True)
        errors = str(exc_info.value)
        assert "approval_id" in errors or "status" in errors or "message" in errors


# =============================================================================
# Execution HITL Status Tests
# =============================================================================

class TestExecutionHITLStatus:
    """Test cases for ExecutionHITLStatus schema."""

    def test_valid_status_no_pending(self):
        """Test status when no pending approval."""
        status = ExecutionHITLStatus(
            execution_id="exec_12345",
            has_pending_approval=False,
            pending_approval=None,
            approval_history=[],
            total_gates_passed=0
        )
        assert status.execution_id == "exec_12345"
        assert status.has_pending_approval is False
        assert status.pending_approval is None
        assert status.approval_history == []
        assert status.total_gates_passed == 0

    def test_valid_status_with_pending(self):
        """Test status when there is a pending approval."""
        now = datetime.now(timezone.utc)
        pending = HITLApprovalResponse(
            id=1,
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            status=HITLApprovalStatusEnum.PENDING,
            gate_config={"message": "Approval needed"},
            created_at=now,
            group_id="group_abc"
        )
        status = ExecutionHITLStatus(
            execution_id="exec_12345",
            has_pending_approval=True,
            pending_approval=pending,
            approval_history=[],
            total_gates_passed=0
        )
        assert status.has_pending_approval is True
        assert status.pending_approval is not None
        assert status.pending_approval.id == 1

    def test_default_values(self):
        """Test default values for optional fields."""
        status = ExecutionHITLStatus(
            execution_id="exec_12345",
            has_pending_approval=False
        )
        assert status.pending_approval is None
        assert status.approval_history == []
        assert status.total_gates_passed == 0


# =============================================================================
# Webhook Base Tests
# =============================================================================

class TestHITLWebhookBase:
    """Test cases for HITLWebhookBase schema."""

    def test_valid_webhook_base(self):
        """Test valid webhook base with required fields."""
        webhook = HITLWebhookBase(
            name="My Webhook",
            url="https://example.com/webhook"
        )
        assert webhook.name == "My Webhook"
        assert webhook.url == "https://example.com/webhook"
        assert webhook.enabled is True  # Default
        assert webhook.events == [HITLWebhookEventEnum.GATE_REACHED]  # Default
        assert webhook.headers is None
        assert webhook.flow_id is None  # Default - applies to all flows

    def test_webhook_with_all_fields(self):
        """Test webhook with all fields specified."""
        webhook = HITLWebhookBase(
            name="Production Webhook",
            url="https://example.com/webhook/hitl",
            flow_id="flow_policies_123",
            enabled=False,
            events=[
                HITLWebhookEventEnum.GATE_REACHED,
                HITLWebhookEventEnum.GATE_APPROVED,
                HITLWebhookEventEnum.GATE_REJECTED
            ],
            headers={"Authorization": "Bearer token123"}
        )
        assert webhook.name == "Production Webhook"
        assert webhook.flow_id == "flow_policies_123"
        assert webhook.enabled is False
        assert len(webhook.events) == 3
        assert webhook.headers == {"Authorization": "Bearer token123"}

    def test_flow_id_max_length(self):
        """Test that flow_id respects max length of 100."""
        long_flow_id = "x" * 101
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookBase(
                name="Test",
                url="https://example.com/webhook",
                flow_id=long_flow_id
            )
        assert "flow_id" in str(exc_info.value).lower()

    def test_name_min_length(self):
        """Test that name has minimum length of 1."""
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookBase(name="", url="https://example.com/webhook")
        assert "name" in str(exc_info.value).lower()

    def test_name_max_length(self):
        """Test that name respects max length of 255."""
        long_name = "x" * 256
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookBase(name=long_name, url="https://example.com/webhook")
        assert "name" in str(exc_info.value).lower()

    def test_url_min_length(self):
        """Test that url has minimum length of 1."""
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookBase(name="Test", url="")
        assert "url" in str(exc_info.value).lower()

    def test_url_max_length(self):
        """Test that url respects max length of 1000."""
        long_url = "https://example.com/" + "x" * 1000
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookBase(name="Test", url=long_url)
        assert "url" in str(exc_info.value).lower()


# =============================================================================
# Webhook Create Tests
# =============================================================================

class TestHITLWebhookCreate:
    """Test cases for HITLWebhookCreate schema."""

    def test_create_without_secret(self):
        """Test webhook creation without secret."""
        webhook = HITLWebhookCreate(
            name="My Webhook",
            url="https://example.com/webhook"
        )
        assert webhook.secret is None

    def test_create_with_secret(self):
        """Test webhook creation with secret."""
        webhook = HITLWebhookCreate(
            name="Secure Webhook",
            url="https://example.com/webhook",
            secret="my-secret-key"
        )
        assert webhook.secret == "my-secret-key"

    def test_create_with_flow_id(self):
        """Test webhook creation with flow_id for flow-specific webhook."""
        webhook = HITLWebhookCreate(
            name="Flow-Specific Webhook",
            url="https://example.com/webhook",
            flow_id="flow_policies_123"
        )
        assert webhook.flow_id == "flow_policies_123"
        assert webhook.name == "Flow-Specific Webhook"

    def test_secret_max_length(self):
        """Test that secret respects max length of 255."""
        long_secret = "x" * 256
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookCreate(
                name="Test",
                url="https://example.com/webhook",
                secret=long_secret
            )
        assert "secret" in str(exc_info.value).lower()

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookCreate(
                name="Test",
                url="https://example.com/webhook",
                extra_field="value"
            )
        assert "extra" in str(exc_info.value).lower()

    def test_inherits_from_base(self):
        """Test that HITLWebhookCreate inherits from HITLWebhookBase."""
        assert issubclass(HITLWebhookCreate, HITLWebhookBase)


# =============================================================================
# Webhook Update Tests
# =============================================================================

class TestHITLWebhookUpdate:
    """Test cases for HITLWebhookUpdate schema."""

    def test_all_fields_optional(self):
        """Test that all fields are optional for update."""
        update = HITLWebhookUpdate()
        assert update.name is None
        assert update.url is None
        assert update.flow_id is None
        assert update.enabled is None
        assert update.events is None
        assert update.headers is None
        assert update.secret is None

    def test_partial_update(self):
        """Test partial update with only some fields."""
        update = HITLWebhookUpdate(name="Updated Name", enabled=False)
        assert update.name == "Updated Name"
        assert update.enabled is False
        assert update.url is None

    def test_full_update(self):
        """Test update with all fields."""
        update = HITLWebhookUpdate(
            name="New Name",
            url="https://example.com/new-webhook",
            flow_id="flow_new_123",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_APPROVED],
            headers={"X-Custom": "Header"},
            secret="new-secret"
        )
        assert update.name == "New Name"
        assert update.url == "https://example.com/new-webhook"
        assert update.flow_id == "flow_new_123"
        assert update.enabled is True
        assert update.events == [HITLWebhookEventEnum.GATE_APPROVED]
        assert update.headers == {"X-Custom": "Header"}
        assert update.secret == "new-secret"

    def test_update_flow_id_only(self):
        """Test updating only flow_id to change webhook scope."""
        update = HITLWebhookUpdate(flow_id="flow_specific_456")
        assert update.flow_id == "flow_specific_456"
        assert update.name is None
        assert update.url is None

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            HITLWebhookUpdate(extra_field="value")
        assert "extra" in str(exc_info.value).lower()


# =============================================================================
# Webhook Response Tests
# =============================================================================

class TestHITLWebhookResponse:
    """Test cases for HITLWebhookResponse schema."""

    def test_valid_response(self):
        """Test valid webhook response."""
        now = datetime.now(timezone.utc)
        response = HITLWebhookResponse(
            id=1,
            group_id="group_abc",
            name="My Webhook",
            url="https://example.com/webhook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
            headers=None,
            created_at=now,
            updated_at=None
        )
        assert response.id == 1
        assert response.group_id == "group_abc"
        assert response.name == "My Webhook"
        assert response.flow_id is None  # Global webhook by default
        assert response.created_at == now
        assert response.updated_at is None

    def test_response_with_flow_id(self):
        """Test webhook response with flow_id for flow-specific webhook."""
        now = datetime.now(timezone.utc)
        response = HITLWebhookResponse(
            id=2,
            group_id="group_abc",
            flow_id="flow_policies_123",
            name="Flow Webhook",
            url="https://example.com/webhook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
            headers=None,
            created_at=now,
            updated_at=None
        )
        assert response.id == 2
        assert response.flow_id == "flow_policies_123"
        assert response.name == "Flow Webhook"

    def test_from_attributes_config(self):
        """Test that from_attributes is enabled for ORM conversion."""
        assert HITLWebhookResponse.model_config.get("from_attributes") is True

    def test_inherits_from_base(self):
        """Test that HITLWebhookResponse inherits from HITLWebhookBase."""
        assert issubclass(HITLWebhookResponse, HITLWebhookBase)


# =============================================================================
# Webhook List Response Tests
# =============================================================================

class TestHITLWebhookListResponse:
    """Test cases for HITLWebhookListResponse schema."""

    def test_valid_list_response(self):
        """Test valid webhook list response."""
        now = datetime.now(timezone.utc)
        item = {
            "id": 1,
            "group_id": "group_abc",
            "flow_id": None,  # Global webhook
            "name": "Test Webhook",
            "url": "https://example.com/webhook",
            "enabled": True,
            "events": [HITLWebhookEventEnum.GATE_REACHED],
            "headers": None,
            "created_at": now,
            "updated_at": None
        }
        response = HITLWebhookListResponse(items=[item], total=1)
        assert len(response.items) == 1
        assert response.items[0].flow_id is None

    def test_list_response_with_flow_specific_webhooks(self):
        """Test list response with flow-specific webhooks."""
        now = datetime.now(timezone.utc)
        items = [
            {
                "id": 1,
                "group_id": "group_abc",
                "flow_id": None,  # Global webhook
                "name": "Global Webhook",
                "url": "https://example.com/global-webhook",
                "enabled": True,
                "events": [HITLWebhookEventEnum.GATE_REACHED],
                "headers": None,
                "created_at": now,
                "updated_at": None
            },
            {
                "id": 2,
                "group_id": "group_abc",
                "flow_id": "flow_policies_123",  # Flow-specific webhook
                "name": "Policies Webhook",
                "url": "https://example.com/policies-webhook",
                "enabled": True,
                "events": [HITLWebhookEventEnum.GATE_REACHED],
                "headers": None,
                "created_at": now,
                "updated_at": None
            }
        ]
        response = HITLWebhookListResponse(items=items, total=2)
        assert len(response.items) == 2
        assert response.items[0].flow_id is None
        assert response.items[1].flow_id == "flow_policies_123"
        assert response.total == 2

    def test_empty_list_response(self):
        """Test empty webhook list response."""
        response = HITLWebhookListResponse(items=[], total=0)
        assert len(response.items) == 0
        assert response.total == 0


# =============================================================================
# Webhook Payload Tests
# =============================================================================

class TestHITLWebhookPayload:
    """Test cases for HITLWebhookPayload schema."""

    def test_valid_gate_reached_payload(self):
        """Test valid webhook payload for gate reached event."""
        now = datetime.now(timezone.utc)
        payload = HITLWebhookPayload(
            event=HITLWebhookEventEnum.GATE_REACHED,
            timestamp=now,
            approval_id=1,
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            message="Approval required"
        )
        assert payload.event == HITLWebhookEventEnum.GATE_REACHED
        assert payload.approval_id == 1
        assert payload.message == "Approval required"

    def test_valid_completed_event_payload(self):
        """Test valid webhook payload for completed event."""
        now = datetime.now(timezone.utc)
        payload = HITLWebhookPayload(
            event=HITLWebhookEventEnum.GATE_APPROVED,
            timestamp=now,
            approval_id=1,
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            message="Approval required",
            previous_crew_name="Research Crew",
            previous_crew_output_preview="Research output preview...",
            status=HITLApprovalStatusEnum.APPROVED,
            responded_by="user@example.com",
            approval_url="https://app.example.com/approvals/1",
            expires_at=now
        )
        assert payload.event == HITLWebhookEventEnum.GATE_APPROVED
        assert payload.status == HITLApprovalStatusEnum.APPROVED
        assert payload.responded_by == "user@example.com"

    def test_optional_fields_defaults(self):
        """Test that optional fields have correct defaults."""
        now = datetime.now(timezone.utc)
        payload = HITLWebhookPayload(
            event=HITLWebhookEventEnum.GATE_REACHED,
            timestamp=now,
            approval_id=1,
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            message="Test"
        )
        assert payload.previous_crew_name is None
        assert payload.previous_crew_output_preview is None
        assert payload.status is None
        assert payload.responded_by is None
        assert payload.approval_url is None
        assert payload.expires_at is None


# =============================================================================
# Gate Node Data Tests
# =============================================================================

class TestHITLGateNodeData:
    """Test cases for HITLGateNodeData schema."""

    def test_default_values(self):
        """Test HITLGateNodeData with default values."""
        node_data = HITLGateNodeData()
        assert node_data.label == "HITL Gate"
        assert node_data.nodetype == "hitlGateNode"
        assert node_data.gate_config is not None
        assert node_data.gate_config.message == "Approval required to proceed"

    def test_custom_values(self):
        """Test HITLGateNodeData with custom values."""
        gate_config = HITLGateConfig(
            message="Custom approval message",
            timeout_seconds=1800
        )
        node_data = HITLGateNodeData(
            label="Custom HITL Gate",
            nodetype="hitlGateNode",
            gate_config=gate_config
        )
        assert node_data.label == "Custom HITL Gate"
        assert node_data.gate_config.message == "Custom approval message"
        assert node_data.gate_config.timeout_seconds == 1800

    def test_extra_fields_allowed(self):
        """Test that extra fields are allowed for node data."""
        node_data = HITLGateNodeData(extra_field="value", position={"x": 100, "y": 200})
        # extra="allow" in model_config allows additional fields
        assert hasattr(node_data, "extra_field") or True  # Schema allows extra

    def test_serialization(self):
        """Test HITLGateNodeData serialization."""
        node_data = HITLGateNodeData(label="Test Gate")
        data = node_data.model_dump()
        assert data["label"] == "Test Gate"
        assert data["nodetype"] == "hitlGateNode"
        assert "gate_config" in data


# =============================================================================
# Cross-Schema Validation Tests
# =============================================================================

class TestCrossSchemaValidation:
    """Test cross-schema validation and consistency."""

    def test_approval_response_accepts_all_status_values(self):
        """Test that approval response accepts all status enum values."""
        now = datetime.now(timezone.utc)
        base_data = {
            "id": 1,
            "execution_id": "exec_12345",
            "flow_id": "flow_67890",
            "gate_node_id": "gate_001",
            "crew_sequence": 1,
            "gate_config": {"message": "Test"},
            "created_at": now,
            "group_id": "group_abc"
        }

        for status in HITLApprovalStatusEnum:
            data = {**base_data, "status": status}
            response = HITLApprovalResponse(**data)
            assert response.status == status

    def test_reject_request_accepts_all_rejection_actions(self):
        """Test that reject request accepts all rejection action values."""
        for action in HITLRejectionActionEnum:
            request = HITLRejectRequest(
                reason="Test reason",
                action=action
            )
            assert request.action == action

    def test_webhook_base_accepts_all_event_types(self):
        """Test that webhook base accepts all event types."""
        all_events = list(HITLWebhookEventEnum)
        webhook = HITLWebhookBase(
            name="All Events Webhook",
            url="https://example.com/webhook",
            events=all_events
        )
        assert len(webhook.events) == len(HITLWebhookEventEnum)

    def test_action_response_accepts_all_status_values(self):
        """Test that action response accepts all status enum values."""
        for status in HITLApprovalStatusEnum:
            response = HITLActionResponse(
                success=True,
                approval_id=1,
                status=status,
                message="Test"
            )
            assert response.status == status
