from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey, JSON, Index
from sqlalchemy.orm import relationship
from uuid import uuid4

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
    
    id = Column(String, primary_key=True, default=generate_billing_id, index=True)
    
    # Execution context
    execution_id = Column(String, ForeignKey("executionhistory.job_id"), nullable=False, index=True)
    execution_type = Column(String, nullable=False, index=True)  # 'crew', 'agent', 'task', 'flow'
    execution_name = Column(String, nullable=True)  # Name of the crew/agent/task
    
    # Model information
    model_name = Column(String, nullable=False, index=True)  # e.g., 'gpt-4', 'claude-3-sonnet'
    model_provider = Column(String, nullable=False, index=True)  # e.g., 'openai', 'anthropic'
    
    # Token usage
    prompt_tokens = Column(Integer, default=0)
    completion_tokens = Column(Integer, default=0)
    total_tokens = Column(Integer, default=0)
    
    # Cost information
    cost_usd = Column(Numeric(precision=10, scale=6), default=0.000000)  # Cost in USD with 6 decimal precision
    cost_per_prompt_token = Column(Numeric(precision=10, scale=8), nullable=True)  # Cost per prompt token
    cost_per_completion_token = Column(Numeric(precision=10, scale=8), nullable=True)  # Cost per completion token
    
    # Performance metrics
    duration_ms = Column(Integer, nullable=True)  # Request duration in milliseconds
    request_count = Column(Integer, default=1)  # Number of API requests (for batching)
    
    # Status and error tracking
    status = Column(String, nullable=False, default="success")  # 'success', 'error', 'timeout'
    error_message = Column(String, nullable=True)
    
    # Multi-group fields for billing isolation
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    tenant_id = Column(String(100), index=True, nullable=True)  # Legacy compatibility
    user_email = Column(String(255), index=True, nullable=True)  # User who triggered the execution
    
    # Timestamps
    usage_date = Column(DateTime, default=datetime.utcnow, index=True)  # When the usage occurred
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Additional metadata
    billing_metadata = Column(JSON, default=dict)  # Additional billing metadata (tags, project info, etc.)
    
    # Create composite indexes for common queries
    __table_args__ = (
        Index('idx_billing_group_date', 'group_id', 'usage_date'),
        Index('idx_billing_user_date', 'user_email', 'usage_date'),
        Index('idx_billing_execution_model', 'execution_id', 'model_name'),
        Index('idx_billing_provider_date', 'model_provider', 'usage_date'),
    )


class BillingPeriod(Base):
    """
    Billing Period model for tracking billing cycles and aggregated costs.
    """
    
    __tablename__ = "billing_periods"
    
    id = Column(String, primary_key=True, default=generate_billing_id, index=True)
    
    # Period information
    period_start = Column(DateTime, nullable=False, index=True)
    period_end = Column(DateTime, nullable=False, index=True)
    period_type = Column(String, nullable=False, default="monthly")  # 'daily', 'weekly', 'monthly', 'custom'
    
    # Group/tenant isolation
    group_id = Column(String(100), index=True, nullable=True)
    tenant_id = Column(String(100), index=True, nullable=True)  # Legacy compatibility
    
    # Aggregated metrics
    total_cost_usd = Column(Numeric(precision=10, scale=2), default=0.00)
    total_tokens = Column(Integer, default=0)
    total_prompt_tokens = Column(Integer, default=0)
    total_completion_tokens = Column(Integer, default=0)
    total_requests = Column(Integer, default=0)
    
    # Model breakdown (JSON with costs per model)
    model_breakdown = Column(JSON, default=dict)
    
    # Status
    status = Column(String, nullable=False, default="active")  # 'active', 'closed', 'invoiced'
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)
    
    # Create indexes for billing queries
    __table_args__ = (
        Index('idx_period_group_dates', 'group_id', 'period_start', 'period_end'),
        Index('idx_period_status_date', 'status', 'period_start'),
    )


class BillingAlert(Base):
    """
    Billing Alert model for cost threshold notifications.
    """
    
    __tablename__ = "billing_alerts"
    
    id = Column(String, primary_key=True, default=generate_billing_id, index=True)
    
    # Alert configuration
    alert_name = Column(String, nullable=False)
    alert_type = Column(String, nullable=False, default="cost_threshold")  # 'cost_threshold', 'token_threshold', 'usage_spike'
    threshold_value = Column(Numeric(precision=10, scale=2), nullable=False)
    threshold_period = Column(String, nullable=False, default="monthly")  # 'daily', 'weekly', 'monthly'
    
    # Target (group/user/global)
    group_id = Column(String(100), index=True, nullable=True)
    tenant_id = Column(String(100), index=True, nullable=True)  # Legacy compatibility
    user_email = Column(String(255), nullable=True)  # Specific user alert
    
    # Alert state
    is_active = Column(String, nullable=False, default="true")
    current_value = Column(Numeric(precision=10, scale=2), default=0.00)
    last_triggered = Column(DateTime, nullable=True)
    notification_emails = Column(JSON, default=list)  # List of emails to notify
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Metadata
    alert_metadata = Column(JSON, default=dict)