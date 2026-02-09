"""
Human in the Loop (HITL) Approval Model.

This module defines the database model for tracking HITL approval requests
that pause flow execution and wait for human decision.
"""

from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.orm import relationship

from src.db.base import Base


class HITLApprovalStatus:
    """Status constants for HITL approvals."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    RETRY = "retry"


class HITLTimeoutAction:
    """Actions to take when an HITL approval times out."""
    AUTO_REJECT = "auto_reject"
    FAIL = "fail"


class HITLRejectionAction:
    """Actions available when an HITL gate is rejected."""
    REJECT = "reject"  # Fail the flow
    RETRY = "retry"    # Retry the previous crew


class HITLApproval(Base):
    """Database model for Human in the Loop approval requests.

    This model represents an approval gate in a flow execution where human
    intervention is required before the flow can proceed to the next step.

    When a flow execution reaches an HITL gate node, an HITLApproval record
    is created and the execution pauses. The flow resumes only when a human
    approves or rejects the gate.

    Attributes:
        id: Primary key identifier
        execution_id: Foreign key to the paused execution (job_id)
        flow_id: ID of the flow being executed
        gate_node_id: ID of the HITL gate node in the flow
        crew_sequence: Sequence number of the crew that completed before this gate

        Status Fields:
        status: Current status (pending, approved, rejected, timeout, retry)

        Gate Configuration:
        gate_config: JSON configuration for the gate
            - message: Display message for approver
            - timeout_seconds: Seconds before auto-action (default: 3600)
            - timeout_action: Action on timeout (auto_reject, fail)
            - require_comment: Whether approver must provide comment
            - allowed_approvers: List of emails who can approve (null = anyone in group)

        Context for Approver:
        previous_crew_name: Name of the crew that completed before this gate
        previous_crew_output: Output from the previous crew for review
        flow_state_snapshot: State of the flow at the gate point

        Response Fields:
        responded_by: Email of user who responded
        responded_at: Timestamp of response
        approval_comment: Comment provided with approval
        rejection_reason: Reason provided with rejection
        rejection_action: What to do on rejection (reject or retry)

        Timeout Fields:
        expires_at: When this approval expires

        Webhook Fields:
        webhook_sent: Whether webhook notification was sent
        webhook_sent_at: When webhook was sent
        webhook_response: Response from webhook endpoint

        Audit Fields:
        created_at: When the approval request was created
        group_id: Group ID for multi-tenant isolation

    Relationships:
        execution: Parent ExecutionHistory record

    Example:
        >>> approval = HITLApproval(
        ...     execution_id="exec_123",
        ...     flow_id="flow_456",
        ...     gate_node_id="gate_1",
        ...     crew_sequence=1,
        ...     gate_config={
        ...         "message": "Review research results before proceeding",
        ...         "timeout_seconds": 3600,
        ...         "timeout_action": "auto_reject"
        ...     },
        ...     previous_crew_output="Research findings...",
        ...     group_id="acme_corp"
        ... )
    """

    __tablename__ = "hitl_approvals"

    id = Column(Integer, primary_key=True, index=True)

    # Execution reference
    # CASCADE delete: When execution is deleted, delete all associated HITL approvals
    execution_id = Column(String, ForeignKey("executionhistory.job_id", ondelete="CASCADE"), nullable=False, index=True)
    flow_id = Column(String, nullable=False, index=True)
    gate_node_id = Column(String, nullable=False, index=True)
    crew_sequence = Column(Integer, nullable=False)  # Which crew completed before this gate

    # Status
    status = Column(String(50), nullable=False, default=HITLApprovalStatus.PENDING, index=True)

    # Gate configuration
    gate_config = Column(JSON, nullable=False, default=dict)
    # Structure:
    # {
    #     "message": "Review output before proceeding",
    #     "timeout_seconds": 3600,
    #     "timeout_action": "auto_reject" | "fail",
    #     "require_comment": false,
    #     "allowed_approvers": ["user@example.com"] | null
    # }

    # Context for approver
    previous_crew_name = Column(String(255), nullable=True)
    previous_crew_output = Column(Text, nullable=True)  # Output to review
    flow_state_snapshot = Column(JSON, nullable=True, default=dict)  # State at gate

    # Response
    responded_by = Column(String(255), nullable=True)
    responded_at = Column(DateTime(timezone=True), nullable=True)
    approval_comment = Column(Text, nullable=True)
    rejection_reason = Column(Text, nullable=True)
    rejection_action = Column(String(50), nullable=True)  # 'reject' or 'retry'

    # Timeout
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Webhook notification tracking
    webhook_sent = Column(Boolean, default=False, nullable=False)
    webhook_sent_at = Column(DateTime(timezone=True), nullable=True)
    webhook_response = Column(JSON, nullable=True)

    # Audit
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    group_id = Column(String(100), nullable=False, index=True)

    # Relationships
    execution = relationship(
        "ExecutionHistory",
        back_populates="hitl_approvals",
        foreign_keys=[execution_id],
        primaryjoin="HITLApproval.execution_id == ExecutionHistory.job_id"
    )

    def __init__(self, **kwargs):
        super(HITLApproval, self).__init__(**kwargs)

        # Set defaults
        if self.status is None:
            self.status = HITLApprovalStatus.PENDING
        if self.gate_config is None:
            self.gate_config = {}
        if self.flow_state_snapshot is None:
            self.flow_state_snapshot = {}
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.webhook_sent is None:
            self.webhook_sent = False

        # Calculate expiration based on gate_config
        if self.expires_at is None and self.gate_config:
            timeout_seconds = self.gate_config.get("timeout_seconds", 3600)
            if timeout_seconds:
                self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)

    @property
    def is_expired(self) -> bool:
        """Check if this approval has expired."""
        if self.expires_at is None:
            return False
        now_utc = datetime.now(timezone.utc)
        expires = self.expires_at
        # SQLite strips timezone info, so expires_at may be naive on retrieval.
        # Treat naive datetimes as UTC to avoid comparison errors.
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return now_utc > expires

    @property
    def timeout_action(self) -> str:
        """Get the configured timeout action."""
        return self.gate_config.get("timeout_action", HITLTimeoutAction.AUTO_REJECT)

    @property
    def message(self) -> str:
        """Get the display message for approvers."""
        return self.gate_config.get("message", "Approval required to proceed")

    @property
    def allowed_approvers(self) -> list:
        """Get list of allowed approvers (empty = anyone in group)."""
        return self.gate_config.get("allowed_approvers") or []

    def can_be_approved_by(self, user_email: str) -> bool:
        """Check if a user is allowed to approve this gate."""
        if not self.allowed_approvers:
            return True  # Anyone in group can approve
        return user_email.lower() in [email.lower() for email in self.allowed_approvers]


class HITLWebhook(Base):
    """Database model for HITL webhook configurations.

    This model stores webhook URLs that should be notified when HITL gates
    are reached during flow execution.

    Attributes:
        id: Primary key identifier
        group_id: Group this webhook belongs to
        flow_id: Optional flow ID to scope webhook to specific flow (null = all flows)
        name: Human-readable name for the webhook
        url: Webhook URL to call
        enabled: Whether the webhook is active
        events: List of events to trigger the webhook
        headers: Custom headers to send with webhook
        created_at: When the webhook was created
        updated_at: Last update timestamp
    """

    __tablename__ = "hitl_webhooks"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(String(100), nullable=False, index=True)

    # Optional: scope webhook to a specific flow (null = applies to all flows in group)
    flow_id = Column(String(100), nullable=True, index=True)

    name = Column(String(255), nullable=False)
    url = Column(String(1000), nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)

    # Events to trigger webhook
    # ["gate_reached", "gate_approved", "gate_rejected", "gate_timeout"]
    events = Column(JSON, default=lambda: ["gate_reached"], nullable=False)

    # Custom headers (e.g., for authentication)
    headers = Column(JSON, default=dict, nullable=True)

    # Secret for webhook signature verification
    secret = Column(String(255), nullable=True)

    # Audit
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    def __init__(self, **kwargs):
        super(HITLWebhook, self).__init__(**kwargs)
        if self.events is None:
            self.events = ["gate_reached"]
        if self.headers is None:
            self.headers = {}
        if self.enabled is None:
            self.enabled = True
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
