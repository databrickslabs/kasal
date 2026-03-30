"""
HITL (Human in the Loop) Schemas.

This module defines Pydantic schemas for HITL API requests and responses.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


# =============================================================================
# Enums
# =============================================================================

class HITLApprovalStatusEnum(str, Enum):
    """HITL approval status values."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    RETRY = "retry"


class HITLTimeoutActionEnum(str, Enum):
    """Actions to take when an HITL approval times out."""
    AUTO_REJECT = "auto_reject"
    FAIL = "fail"


class HITLRejectionActionEnum(str, Enum):
    """Actions available when an HITL gate is rejected."""
    REJECT = "reject"
    RETRY = "retry"


class HITLWebhookEventEnum(str, Enum):
    """HITL webhook event types."""
    GATE_REACHED = "gate_reached"
    GATE_APPROVED = "gate_approved"
    GATE_REJECTED = "gate_rejected"
    GATE_TIMEOUT = "gate_timeout"


# =============================================================================
# Gate Configuration Schemas
# =============================================================================

class HITLGateConfig(BaseModel):
    """Configuration for an HITL gate node in a flow."""
    message: str = Field(
        default="Approval required to proceed",
        description="Message displayed to the approver"
    )
    timeout_seconds: int = Field(
        default=3600,
        ge=60,
        le=604800,  # Max 7 days
        description="Seconds before timeout action is triggered"
    )
    timeout_action: HITLTimeoutActionEnum = Field(
        default=HITLTimeoutActionEnum.AUTO_REJECT,
        description="Action to take when approval times out"
    )
    require_comment: bool = Field(
        default=False,
        description="Whether approver must provide a comment"
    )
    allowed_approvers: Optional[List[str]] = Field(
        default=None,
        description="List of emails allowed to approve. If null, anyone in group can approve"
    )

    model_config = ConfigDict(extra="forbid")


# =============================================================================
# Approval Request/Response Schemas
# =============================================================================

class HITLApprovalBase(BaseModel):
    """Base schema for HITL approval."""
    execution_id: str = Field(..., description="Job ID of the paused execution")
    flow_id: str = Field(..., description="ID of the flow being executed")
    gate_node_id: str = Field(..., description="ID of the HITL gate node")
    crew_sequence: int = Field(..., description="Sequence number of completed crew before gate")


class HITLApprovalCreate(HITLApprovalBase):
    """Schema for creating an HITL approval request."""
    gate_config: HITLGateConfig = Field(
        default_factory=HITLGateConfig,
        description="Gate configuration"
    )
    previous_crew_name: Optional[str] = Field(
        default=None,
        description="Name of the crew that completed before this gate"
    )
    previous_crew_output: Optional[str] = Field(
        default=None,
        description="Output from the previous crew for review"
    )
    flow_state_snapshot: Optional[Dict[str, Any]] = Field(
        default=None,
        description="State of the flow at the gate point"
    )
    group_id: str = Field(..., description="Group ID for multi-tenant isolation")


class HITLApprovalResponse(HITLApprovalBase):
    """Schema for HITL approval response."""
    id: int = Field(..., description="Approval ID")
    status: HITLApprovalStatusEnum = Field(..., description="Current approval status")
    gate_config: Dict[str, Any] = Field(..., description="Gate configuration")
    previous_crew_name: Optional[str] = Field(None, description="Name of previous crew")
    previous_crew_output: Optional[str] = Field(None, description="Output to review")
    flow_state_snapshot: Optional[Dict[str, Any]] = Field(None, description="Flow state at gate")

    # Response fields
    responded_by: Optional[str] = Field(None, description="Email of responder")
    responded_at: Optional[datetime] = Field(None, description="When response was submitted")
    approval_comment: Optional[str] = Field(None, description="Comment with approval")
    rejection_reason: Optional[str] = Field(None, description="Reason for rejection")
    rejection_action: Optional[HITLRejectionActionEnum] = Field(None, description="Action on rejection")

    # Timeout
    expires_at: Optional[datetime] = Field(None, description="When approval expires")
    is_expired: bool = Field(False, description="Whether approval has expired")

    # Audit
    created_at: datetime = Field(..., description="When approval was created")
    group_id: str = Field(..., description="Group ID")

    model_config = ConfigDict(from_attributes=True)


class HITLApprovalListResponse(BaseModel):
    """Schema for list of HITL approvals."""
    items: List[HITLApprovalResponse] = Field(..., description="List of approvals")
    total: int = Field(..., description="Total count")
    limit: int = Field(..., description="Page size")
    offset: int = Field(..., description="Page offset")


# =============================================================================
# Approve/Reject Request Schemas
# =============================================================================

class HITLApproveRequest(BaseModel):
    """Schema for approving an HITL gate."""
    comment: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Optional comment with approval"
    )

    model_config = ConfigDict(extra="forbid")


class HITLRejectRequest(BaseModel):
    """Schema for rejecting an HITL gate."""
    reason: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Reason for rejection"
    )
    action: HITLRejectionActionEnum = Field(
        default=HITLRejectionActionEnum.REJECT,
        description="Action to take: reject (fail flow) or retry (re-run previous crew)"
    )

    model_config = ConfigDict(extra="forbid")


class HITLActionResponse(BaseModel):
    """Schema for approval/rejection action response."""
    success: bool = Field(..., description="Whether action was successful")
    approval_id: int = Field(..., description="ID of the approval")
    status: HITLApprovalStatusEnum = Field(..., description="New status")
    message: str = Field(..., description="Human-readable result message")
    execution_resumed: bool = Field(
        default=False,
        description="Whether flow execution was resumed"
    )


# =============================================================================
# Execution HITL Status Schema
# =============================================================================

class ExecutionHITLStatus(BaseModel):
    """Schema for HITL status of an execution."""
    execution_id: str = Field(..., description="Job ID of the execution")
    has_pending_approval: bool = Field(..., description="Whether there's a pending approval")
    pending_approval: Optional[HITLApprovalResponse] = Field(
        None,
        description="Current pending approval if any"
    )
    approval_history: List[HITLApprovalResponse] = Field(
        default_factory=list,
        description="History of all approvals for this execution"
    )
    total_gates_passed: int = Field(
        default=0,
        description="Number of HITL gates passed"
    )


# =============================================================================
# Webhook Schemas
# =============================================================================

class HITLWebhookBase(BaseModel):
    """Base schema for HITL webhook."""
    name: str = Field(..., min_length=1, max_length=255, description="Webhook name")
    url: str = Field(..., min_length=1, max_length=1000, description="Webhook URL")
    flow_id: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Optional flow ID to scope webhook to specific flow. If null, applies to all flows in group"
    )
    enabled: bool = Field(default=True, description="Whether webhook is active")
    events: List[HITLWebhookEventEnum] = Field(
        default=[HITLWebhookEventEnum.GATE_REACHED],
        description="Events to trigger webhook"
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom headers to send with webhook"
    )


class HITLWebhookCreate(HITLWebhookBase):
    """Schema for creating an HITL webhook."""
    secret: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Secret for signature verification"
    )

    model_config = ConfigDict(extra="forbid")


class HITLWebhookUpdate(BaseModel):
    """Schema for updating an HITL webhook."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    url: Optional[str] = Field(None, min_length=1, max_length=1000)
    flow_id: Optional[str] = Field(None, max_length=100, description="Flow ID to scope webhook to")
    enabled: Optional[bool] = None
    events: Optional[List[HITLWebhookEventEnum]] = None
    headers: Optional[Dict[str, str]] = None
    secret: Optional[str] = Field(None, max_length=255)

    model_config = ConfigDict(extra="forbid")


class HITLWebhookResponse(HITLWebhookBase):
    """Schema for HITL webhook response."""
    id: int = Field(..., description="Webhook ID")
    group_id: str = Field(..., description="Group ID")
    created_at: datetime = Field(..., description="When webhook was created")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    # Note: secret is not included in response for security

    model_config = ConfigDict(from_attributes=True)


class HITLWebhookListResponse(BaseModel):
    """Schema for list of HITL webhooks."""
    items: List[HITLWebhookResponse] = Field(..., description="List of webhooks")
    total: int = Field(..., description="Total count")


# =============================================================================
# Webhook Payload Schemas (for outgoing webhooks)
# =============================================================================

class HITLWebhookPayload(BaseModel):
    """Payload sent to webhook endpoints."""
    event: HITLWebhookEventEnum = Field(..., description="Event type")
    timestamp: datetime = Field(..., description="When event occurred")

    # Approval details
    approval_id: int = Field(..., description="ID of the approval")
    execution_id: str = Field(..., description="Job ID of the execution")
    flow_id: str = Field(..., description="ID of the flow")
    gate_node_id: str = Field(..., description="ID of the gate node")

    # Context
    message: str = Field(..., description="Gate message")
    previous_crew_name: Optional[str] = Field(None, description="Name of previous crew")
    previous_crew_output_preview: Optional[str] = Field(
        None,
        description="Preview of previous crew output (first 500 chars)"
    )

    # Approval status (for completed events)
    status: Optional[HITLApprovalStatusEnum] = Field(None, description="Approval status")
    responded_by: Optional[str] = Field(None, description="Who responded")

    # Links
    approval_url: Optional[str] = Field(
        None,
        description="Direct URL to approval page"
    )
    expires_at: Optional[datetime] = Field(None, description="When approval expires")


# =============================================================================
# Flow Node Configuration Schema (for frontend)
# =============================================================================

class HITLGateNodeData(BaseModel):
    """Schema for HITL gate node data in flow editor."""
    label: str = Field(default="HITL Gate", description="Node label")
    nodetype: str = Field(default="hitlGateNode", description="Node type identifier")
    gate_config: HITLGateConfig = Field(
        default_factory=HITLGateConfig,
        description="Gate configuration"
    )

    model_config = ConfigDict(extra="allow")
