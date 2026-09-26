from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


def generate_billing_id():
    """Generate a unique billing record ID."""
    return str(uuid4())


class LLMUsageBilling(Base):
    """
    LLM Usage Billing model for tracking costs and usage metrics per execution.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "llm_usage_billing"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, default=generate_billing_id, index=True
    )

    # Execution context
    execution_id: Mapped[str] = mapped_column(
        String, ForeignKey("executionhistory.job_id"), nullable=False, index=True
    )
    execution_type: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )  # 'crew', 'agent', 'task', 'flow'
    execution_name: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Name of the crew/agent/task

    # Model information
    model_name: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )  # e.g., 'gpt-4', 'claude-3-sonnet'
    model_provider: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )  # e.g., 'openai', 'anthropic'

    # Token usage
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=True)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=True)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=True)

    # Cost information
    cost_usd: Mapped[Decimal] = mapped_column(
        Numeric(precision=10, scale=6), default=0.000000, nullable=True
    )  # Cost in USD with 6 decimal precision
    cost_per_prompt_token: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(precision=10, scale=8), nullable=True
    )  # Cost per prompt token
    cost_per_completion_token: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(precision=10, scale=8), nullable=True
    )  # Cost per completion token

    # Performance metrics
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )  # Request duration in milliseconds
    request_count: Mapped[int] = mapped_column(
        Integer, default=1, nullable=True
    )  # Number of API requests (for batching)

    # Status and error tracking
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="success"
    )  # 'success', 'error', 'timeout'
    error_message: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Multi-group fields for billing isolation
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    user_email: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )  # User who triggered the execution

    # Timestamps
    usage_date: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True, nullable=True
    )  # When the usage occurred
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    # Additional metadata
    billing_metadata: Mapped[Any] = mapped_column(
        JSON, default=dict, nullable=True
    )  # Additional billing metadata (tags, project info, etc.)

    # Create composite indexes for common queries
    __table_args__ = (
        Index("idx_billing_group_date", "group_id", "usage_date"),
        Index("idx_billing_user_date", "user_email", "usage_date"),
        Index("idx_billing_execution_model", "execution_id", "model_name"),
        Index("idx_billing_provider_date", "model_provider", "usage_date"),
    )


class BillingPeriod(Base):
    """
    Billing Period model for tracking billing cycles and aggregated costs.
    """

    __tablename__ = "billing_periods"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, default=generate_billing_id, index=True
    )

    # Period information
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    period_type: Mapped[str] = mapped_column(
        String, nullable=False, default="monthly"
    )  # 'daily', 'weekly', 'monthly', 'custom'

    # Group isolation
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )

    # Aggregated metrics
    total_cost_usd: Mapped[Decimal] = mapped_column(
        Numeric(precision=10, scale=2), default=0.00, nullable=True
    )
    total_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=True)
    total_prompt_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=True)
    total_completion_tokens: Mapped[int] = mapped_column(
        Integer, default=0, nullable=True
    )
    total_requests: Mapped[int] = mapped_column(Integer, default=0, nullable=True)

    # Model breakdown (JSON with costs per model)
    model_breakdown: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)

    # Status
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="active"
    )  # 'active', 'closed', 'invoiced'

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Create indexes for billing queries
    __table_args__ = (
        Index("idx_period_group_dates", "group_id", "period_start", "period_end"),
        Index("idx_period_status_date", "status", "period_start"),
    )


class BillingAlert(Base):
    """
    Billing Alert model for cost threshold notifications.
    """

    __tablename__ = "billing_alerts"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, default=generate_billing_id, index=True
    )

    # Alert configuration
    alert_name: Mapped[str] = mapped_column(String, nullable=False)
    alert_type: Mapped[str] = mapped_column(
        String, nullable=False, default="cost_threshold"
    )  # 'cost_threshold', 'token_threshold', 'usage_spike'
    threshold_value: Mapped[Decimal] = mapped_column(
        Numeric(precision=10, scale=2), nullable=False
    )
    threshold_period: Mapped[str] = mapped_column(
        String, nullable=False, default="monthly"
    )  # 'daily', 'weekly', 'monthly'

    # Target (group/user/global)
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )
    user_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Specific user alert

    # Alert state
    is_active: Mapped[str] = mapped_column(String, nullable=False, default="true")
    current_value: Mapped[Decimal] = mapped_column(
        Numeric(precision=10, scale=2), default=0.00, nullable=True
    )
    last_triggered: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    notification_emails: Mapped[Any] = mapped_column(
        JSON, default=list, nullable=True
    )  # List of emails to notify

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    # Metadata
    alert_metadata: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)
