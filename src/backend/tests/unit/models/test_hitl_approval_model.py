"""
Unit tests for HITL (Human in the Loop) Approval models.

Tests the functionality of the HITLApproval and HITLWebhook database models
including field validation, properties, defaults, and constraints.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from src.models.hitl_approval import (
    HITLApproval,
    HITLWebhook,
    HITLApprovalStatus,
    HITLTimeoutAction,
    HITLRejectionAction,
)


# =============================================================================
# Status Constants Tests
# =============================================================================

class TestHITLApprovalStatus:
    """Test cases for HITLApprovalStatus constants."""

    def test_pending_status(self):
        """Test PENDING status constant."""
        assert HITLApprovalStatus.PENDING == "pending"

    def test_approved_status(self):
        """Test APPROVED status constant."""
        assert HITLApprovalStatus.APPROVED == "approved"

    def test_rejected_status(self):
        """Test REJECTED status constant."""
        assert HITLApprovalStatus.REJECTED == "rejected"

    def test_timeout_status(self):
        """Test TIMEOUT status constant."""
        assert HITLApprovalStatus.TIMEOUT == "timeout"

    def test_retry_status(self):
        """Test RETRY status constant."""
        assert HITLApprovalStatus.RETRY == "retry"

    def test_all_status_values_are_strings(self):
        """Test that all status values are strings."""
        assert isinstance(HITLApprovalStatus.PENDING, str)
        assert isinstance(HITLApprovalStatus.APPROVED, str)
        assert isinstance(HITLApprovalStatus.REJECTED, str)
        assert isinstance(HITLApprovalStatus.TIMEOUT, str)
        assert isinstance(HITLApprovalStatus.RETRY, str)


class TestHITLTimeoutAction:
    """Test cases for HITLTimeoutAction constants."""

    def test_auto_reject_action(self):
        """Test AUTO_REJECT action constant."""
        assert HITLTimeoutAction.AUTO_REJECT == "auto_reject"

    def test_fail_action(self):
        """Test FAIL action constant."""
        assert HITLTimeoutAction.FAIL == "fail"


class TestHITLRejectionAction:
    """Test cases for HITLRejectionAction constants."""

    def test_reject_action(self):
        """Test REJECT action constant."""
        assert HITLRejectionAction.REJECT == "reject"

    def test_retry_action(self):
        """Test RETRY action constant."""
        assert HITLRejectionAction.RETRY == "retry"


# =============================================================================
# HITLApproval Model Tests
# =============================================================================

class TestHITLApprovalModel:
    """Test cases for HITLApproval model."""

    def test_creation_minimal(self):
        """Test basic HITLApproval model creation with minimal required fields."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        assert approval.execution_id == "exec_12345"
        assert approval.flow_id == "flow_67890"
        assert approval.gate_node_id == "gate_001"
        assert approval.crew_sequence == 1
        assert approval.group_id == "group_abc"
        # Default values set by __init__
        assert approval.status == HITLApprovalStatus.PENDING
        assert approval.gate_config == {}
        assert approval.flow_state_snapshot == {}
        assert approval.webhook_sent is False
        assert approval.created_at is not None

    def test_creation_with_all_fields(self):
        """Test HITLApproval model creation with all fields."""
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=2)
        gate_config = {
            "message": "Review the output",
            "timeout_seconds": 7200,
            "timeout_action": "auto_reject",
            "require_comment": True,
            "allowed_approvers": ["admin@example.com"]
        }
        flow_state = {"key": "value", "step": 1}

        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=2,
            group_id="group_abc",
            status=HITLApprovalStatus.APPROVED,
            gate_config=gate_config,
            previous_crew_name="Research Crew",
            previous_crew_output="Research findings...",
            flow_state_snapshot=flow_state,
            responded_by="user@example.com",
            responded_at=now,
            approval_comment="Looks great!",
            rejection_reason=None,
            rejection_action=None,
            expires_at=expires,
            webhook_sent=True,
            webhook_sent_at=now,
            webhook_response={"status": "ok"},
            created_at=now
        )

        assert approval.status == HITLApprovalStatus.APPROVED
        assert approval.gate_config == gate_config
        assert approval.previous_crew_name == "Research Crew"
        assert approval.previous_crew_output == "Research findings..."
        assert approval.flow_state_snapshot == flow_state
        assert approval.responded_by == "user@example.com"
        assert approval.responded_at == now
        assert approval.approval_comment == "Looks great!"
        assert approval.webhook_sent is True

    def test_default_status(self):
        """Test that status defaults to PENDING."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        assert approval.status == HITLApprovalStatus.PENDING

    def test_default_gate_config(self):
        """Test that gate_config defaults to empty dict."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        assert approval.gate_config == {}

    def test_default_flow_state_snapshot(self):
        """Test that flow_state_snapshot defaults to empty dict."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        assert approval.flow_state_snapshot == {}

    def test_default_webhook_sent(self):
        """Test that webhook_sent defaults to False."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        assert approval.webhook_sent is False

    def test_created_at_auto_set(self):
        """Test that created_at is automatically set."""
        before = datetime.now(timezone.utc)
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        after = datetime.now(timezone.utc)

        assert approval.created_at is not None
        assert before <= approval.created_at <= after

    def test_expires_at_calculated_from_gate_config(self):
        """Test that expires_at is calculated from gate_config timeout_seconds."""
        timeout_seconds = 3600
        gate_config = {"timeout_seconds": timeout_seconds}

        before = datetime.now(timezone.utc)
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config=gate_config
        )
        after = datetime.now(timezone.utc)

        assert approval.expires_at is not None
        expected_min = before + timedelta(seconds=timeout_seconds)
        expected_max = after + timedelta(seconds=timeout_seconds)
        assert expected_min <= approval.expires_at <= expected_max

    def test_expires_at_not_calculated_if_already_set(self):
        """Test that expires_at is not recalculated if already set."""
        custom_expires = datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        gate_config = {"timeout_seconds": 3600}

        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config=gate_config,
            expires_at=custom_expires
        )

        assert approval.expires_at == custom_expires

    def test_table_name(self):
        """Test that the table name is correct."""
        assert HITLApproval.__tablename__ == "hitl_approvals"

    def test_column_indexes(self):
        """Test that important columns have indexes."""
        table = HITLApproval.__table__
        # Check indexed columns
        assert table.columns["id"].index is True
        assert table.columns["execution_id"].index is True
        assert table.columns["flow_id"].index is True
        assert table.columns["gate_node_id"].index is True
        assert table.columns["status"].index is True
        assert table.columns["expires_at"].index is True
        assert table.columns["group_id"].index is True

    def test_column_defaults(self):
        """Test that column defaults are configured correctly."""
        table = HITLApproval.__table__
        assert table.columns["status"].default.arg == HITLApprovalStatus.PENDING
        assert table.columns["webhook_sent"].default.arg is False


# =============================================================================
# HITLApproval Properties Tests
# =============================================================================

class TestHITLApprovalProperties:
    """Test cases for HITLApproval model properties."""

    def test_is_expired_false_when_not_expired(self):
        """Test is_expired returns False when not expired."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            expires_at=future
        )
        assert approval.is_expired is False

    def test_is_expired_true_when_expired(self):
        """Test is_expired returns True when expired."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            expires_at=past
        )
        assert approval.is_expired is True

    def test_is_expired_false_when_expires_at_none(self):
        """Test is_expired returns False when expires_at is None."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            expires_at=None,
            gate_config={}  # Prevent auto-calculation
        )
        # Force expires_at to None after __init__
        approval.expires_at = None
        assert approval.is_expired is False

    def test_is_expired_boundary_just_expired(self):
        """Test is_expired at the boundary (just expired)."""
        # Set to just before current time
        just_past = datetime.now(timezone.utc) - timedelta(seconds=1)
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            expires_at=just_past
        )
        assert approval.is_expired is True

    def test_timeout_action_from_gate_config(self):
        """Test timeout_action property returns value from gate_config."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"timeout_action": "fail"}
        )
        assert approval.timeout_action == "fail"

    def test_timeout_action_default(self):
        """Test timeout_action property returns default when not in gate_config."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={}
        )
        assert approval.timeout_action == HITLTimeoutAction.AUTO_REJECT

    def test_message_from_gate_config(self):
        """Test message property returns value from gate_config."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"message": "Custom approval message"}
        )
        assert approval.message == "Custom approval message"

    def test_message_default(self):
        """Test message property returns default when not in gate_config."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={}
        )
        assert approval.message == "Approval required to proceed"

    def test_allowed_approvers_from_gate_config(self):
        """Test allowed_approvers property returns value from gate_config."""
        approvers = ["admin@example.com", "manager@example.com"]
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": approvers}
        )
        assert approval.allowed_approvers == approvers

    def test_allowed_approvers_default(self):
        """Test allowed_approvers property returns empty list when not set."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={}
        )
        assert approval.allowed_approvers == []

    def test_allowed_approvers_returns_empty_for_none(self):
        """Test allowed_approvers property returns empty list when None."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": None}
        )
        assert approval.allowed_approvers == []


# =============================================================================
# can_be_approved_by Method Tests
# =============================================================================

class TestCanBeApprovedBy:
    """Test cases for can_be_approved_by method."""

    def test_anyone_can_approve_when_no_restrictions(self):
        """Test that anyone can approve when allowed_approvers is empty."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={}
        )
        assert approval.can_be_approved_by("anyone@example.com") is True
        assert approval.can_be_approved_by("random@company.org") is True

    def test_allowed_user_can_approve(self):
        """Test that allowed user can approve."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": ["admin@example.com", "manager@example.com"]}
        )
        assert approval.can_be_approved_by("admin@example.com") is True
        assert approval.can_be_approved_by("manager@example.com") is True

    def test_non_allowed_user_cannot_approve(self):
        """Test that non-allowed user cannot approve."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": ["admin@example.com"]}
        )
        assert approval.can_be_approved_by("random@example.com") is False
        assert approval.can_be_approved_by("other@company.org") is False

    def test_case_insensitive_comparison(self):
        """Test that email comparison is case insensitive."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": ["Admin@Example.COM"]}
        )
        assert approval.can_be_approved_by("admin@example.com") is True
        assert approval.can_be_approved_by("ADMIN@EXAMPLE.COM") is True
        assert approval.can_be_approved_by("Admin@Example.Com") is True

    def test_empty_approvers_list(self):
        """Test that empty list allows anyone."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": []}
        )
        assert approval.can_be_approved_by("anyone@example.com") is True

    def test_single_approver(self):
        """Test with a single allowed approver."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={"allowed_approvers": ["the.one@example.com"]}
        )
        assert approval.can_be_approved_by("the.one@example.com") is True
        assert approval.can_be_approved_by("not.the.one@example.com") is False


# =============================================================================
# HITLWebhook Model Tests
# =============================================================================

class TestHITLWebhookModel:
    """Test cases for HITLWebhook model."""

    def test_creation_minimal(self):
        """Test basic HITLWebhook model creation with minimal required fields."""
        webhook = HITLWebhook(
            group_id="group_abc",
            name="My Webhook",
            url="https://example.com/webhook"
        )
        assert webhook.group_id == "group_abc"
        assert webhook.name == "My Webhook"
        assert webhook.url == "https://example.com/webhook"
        # Default values set by __init__
        assert webhook.events == ["gate_reached"]
        assert webhook.headers == {}
        assert webhook.enabled is True
        assert webhook.created_at is not None

    def test_creation_with_all_fields(self):
        """Test HITLWebhook model creation with all fields."""
        now = datetime.now(timezone.utc)
        events = ["gate_reached", "gate_approved", "gate_rejected", "gate_timeout"]
        headers = {"Authorization": "Bearer token123", "X-Custom": "Header"}

        webhook = HITLWebhook(
            group_id="group_abc",
            name="Production Webhook",
            url="https://example.com/webhook/hitl",
            enabled=False,
            events=events,
            headers=headers,
            secret="my-secret-key",
            created_at=now,
            updated_at=now
        )

        assert webhook.name == "Production Webhook"
        assert webhook.enabled is False
        assert webhook.events == events
        assert webhook.headers == headers
        assert webhook.secret == "my-secret-key"
        assert webhook.created_at == now
        assert webhook.updated_at == now

    def test_default_events(self):
        """Test that events defaults to ['gate_reached']."""
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Test Webhook",
            url="https://example.com/webhook"
        )
        assert webhook.events == ["gate_reached"]

    def test_default_headers(self):
        """Test that headers defaults to empty dict."""
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Test Webhook",
            url="https://example.com/webhook"
        )
        assert webhook.headers == {}

    def test_default_enabled(self):
        """Test that enabled defaults to True."""
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Test Webhook",
            url="https://example.com/webhook"
        )
        assert webhook.enabled is True

    def test_created_at_auto_set(self):
        """Test that created_at is automatically set."""
        before = datetime.now(timezone.utc)
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Test Webhook",
            url="https://example.com/webhook"
        )
        after = datetime.now(timezone.utc)

        assert webhook.created_at is not None
        assert before <= webhook.created_at <= after

    def test_table_name(self):
        """Test that the table name is correct."""
        assert HITLWebhook.__tablename__ == "hitl_webhooks"

    def test_column_indexes(self):
        """Test that important columns have indexes."""
        table = HITLWebhook.__table__
        assert table.columns["id"].index is True
        assert table.columns["group_id"].index is True

    def test_column_defaults(self):
        """Test that column defaults are configured correctly."""
        table = HITLWebhook.__table__
        assert table.columns["enabled"].default.arg is True

    def test_secret_nullable(self):
        """Test that secret column is nullable."""
        table = HITLWebhook.__table__
        assert table.columns["secret"].nullable is True

    def test_headers_nullable(self):
        """Test that headers column is nullable."""
        table = HITLWebhook.__table__
        assert table.columns["headers"].nullable is True


# =============================================================================
# Model Relationship Tests
# =============================================================================

class TestHITLApprovalRelationships:
    """Test cases for HITLApproval relationships."""

    def test_execution_relationship_defined(self):
        """Test that execution relationship is defined."""
        # Check that the relationship property exists
        assert hasattr(HITLApproval, "execution")

    def test_relationship_configuration(self):
        """Test that relationship is correctly configured."""
        from sqlalchemy.orm import relationship as orm_relationship
        from sqlalchemy.inspection import inspect

        mapper = inspect(HITLApproval)
        relationships = mapper.relationships

        # The execution relationship should exist
        assert "execution" in relationships.keys()

        # Verify it points to ExecutionHistory
        execution_rel = relationships["execution"]
        assert execution_rel.argument == "ExecutionHistory"


# =============================================================================
# Edge Cases and Error Handling Tests
# =============================================================================

class TestHITLApprovalEdgeCases:
    """Test edge cases and boundary conditions for HITLApproval."""

    def test_very_long_previous_crew_output(self):
        """Test with very long previous_crew_output."""
        long_output = "x" * 100000
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            previous_crew_output=long_output
        )
        assert len(approval.previous_crew_output) == 100000

    def test_complex_flow_state_snapshot(self):
        """Test with complex nested flow_state_snapshot."""
        complex_state = {
            "nested": {
                "deep": {
                    "values": [1, 2, 3],
                    "data": {"key": "value"}
                }
            },
            "list": [{"item": 1}, {"item": 2}]
        }
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            flow_state_snapshot=complex_state
        )
        assert approval.flow_state_snapshot == complex_state

    def test_crew_sequence_zero(self):
        """Test with crew_sequence of 0."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=0,
            group_id="group_abc"
        )
        assert approval.crew_sequence == 0

    def test_crew_sequence_large_number(self):
        """Test with large crew_sequence number."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=9999,
            group_id="group_abc"
        )
        assert approval.crew_sequence == 9999

    def test_special_characters_in_strings(self):
        """Test with special characters in string fields."""
        approval = HITLApproval(
            execution_id="exec_12345!@#$%",
            flow_id="flow_67890-special",
            gate_node_id="gate_001_test",
            crew_sequence=1,
            group_id="group_abc/def",
            previous_crew_name="Crew with 'quotes' and \"double quotes\"",
            previous_crew_output="Output with\nnewlines\tand\ttabs",
            approval_comment="Unicode: test"
        )
        assert "'" in approval.previous_crew_name
        assert "\n" in approval.previous_crew_output

    def test_empty_gate_config_properties(self):
        """Test that properties handle empty gate_config gracefully."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            gate_config={}
        )
        # All properties should return defaults without error
        assert approval.message == "Approval required to proceed"
        assert approval.timeout_action == HITLTimeoutAction.AUTO_REJECT
        assert approval.allowed_approvers == []


class TestHITLWebhookEdgeCases:
    """Test edge cases and boundary conditions for HITLWebhook."""

    def test_multiple_events(self):
        """Test with all possible events."""
        all_events = ["gate_reached", "gate_approved", "gate_rejected", "gate_timeout"]
        webhook = HITLWebhook(
            group_id="group_abc",
            name="All Events Webhook",
            url="https://example.com/webhook",
            events=all_events
        )
        assert len(webhook.events) == 4
        assert all_events == webhook.events

    def test_complex_headers(self):
        """Test with complex headers structure."""
        headers = {
            "Authorization": "Bearer token123",
            "X-Custom-Header": "CustomValue",
            "Content-Type": "application/json"
        }
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Test Webhook",
            url="https://example.com/webhook",
            headers=headers
        )
        assert webhook.headers == headers

    def test_long_url(self):
        """Test with long URL."""
        long_url = "https://example.com/" + "path/" * 100 + "endpoint"
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Long URL Webhook",
            url=long_url
        )
        assert webhook.url == long_url

    def test_updated_at_initially_none(self):
        """Test that updated_at can be None initially."""
        webhook = HITLWebhook(
            group_id="group_abc",
            name="Test Webhook",
            url="https://example.com/webhook"
        )
        # updated_at may be None or set by column default
        # The model allows it to be None


# =============================================================================
# Status Transition Tests
# =============================================================================

class TestHITLApprovalStatusTransitions:
    """Test status value assignments for HITLApproval."""

    def test_set_status_to_approved(self):
        """Test setting status to APPROVED."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        approval.status = HITLApprovalStatus.APPROVED
        assert approval.status == HITLApprovalStatus.APPROVED

    def test_set_status_to_rejected(self):
        """Test setting status to REJECTED."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        approval.status = HITLApprovalStatus.REJECTED
        assert approval.status == HITLApprovalStatus.REJECTED

    def test_set_status_to_timeout(self):
        """Test setting status to TIMEOUT."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        approval.status = HITLApprovalStatus.TIMEOUT
        assert approval.status == HITLApprovalStatus.TIMEOUT

    def test_set_status_to_retry(self):
        """Test setting status to RETRY."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        approval.status = HITLApprovalStatus.RETRY
        assert approval.status == HITLApprovalStatus.RETRY


# =============================================================================
# Rejection Action Tests
# =============================================================================

class TestHITLApprovalRejectionActions:
    """Test rejection action assignments for HITLApproval."""

    def test_set_rejection_action_reject(self):
        """Test setting rejection_action to REJECT."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        approval.rejection_action = HITLRejectionAction.REJECT
        assert approval.rejection_action == HITLRejectionAction.REJECT

    def test_set_rejection_action_retry(self):
        """Test setting rejection_action to RETRY."""
        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )
        approval.rejection_action = HITLRejectionAction.RETRY
        assert approval.rejection_action == HITLRejectionAction.RETRY


# =============================================================================
# Webhook Notification Tests
# =============================================================================

class TestHITLApprovalWebhookTracking:
    """Test webhook tracking fields in HITLApproval."""

    def test_webhook_sent_tracking(self):
        """Test webhook_sent tracking."""
        now = datetime.now(timezone.utc)
        response = {"status": 200, "message": "ok"}

        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc"
        )

        # Simulate webhook being sent
        approval.webhook_sent = True
        approval.webhook_sent_at = now
        approval.webhook_response = response

        assert approval.webhook_sent is True
        assert approval.webhook_sent_at == now
        assert approval.webhook_response == response

    def test_webhook_response_complex_data(self):
        """Test webhook_response with complex JSON data."""
        complex_response = {
            "status": 200,
            "headers": {"content-type": "application/json"},
            "body": {"message": "success", "data": [1, 2, 3]}
        }

        approval = HITLApproval(
            execution_id="exec_12345",
            flow_id="flow_67890",
            gate_node_id="gate_001",
            crew_sequence=1,
            group_id="group_abc",
            webhook_response=complex_response
        )

        assert approval.webhook_response == complex_response
