from datetime import datetime, timezone
from typing import Dict, Any, Optional
from sqlalchemy import Column, Integer, String, JSON, ForeignKey, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.db.base import Base


class FlowExecution(Base):
    """
    Model representing a flow execution record.
    Enhanced with group isolation for multi-tenant deployments.
    """
    __tablename__ = "flow_executions"

    id = Column(Integer, primary_key=True)
    flow_id = Column(UUID(as_uuid=True), nullable=True)  # Optional reference to saved flow, no FK constraint
    job_id = Column(String, nullable=False, unique=True)
    status = Column(String, nullable=False, default="pending")
    config = Column(JSON, default=dict)
    result = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)
    run_name = Column(String, nullable=True)  # Descriptive name for the execution

    # Multi-tenant isolation
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation for multi-tenancy

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    def __init__(self, **kwargs):
        super(FlowExecution, self).__init__(**kwargs)
        if self.config is None:
            self.config = {}


class FlowNodeExecution(Base):
    """
    Model representing a flow node execution record.
    Enhanced with group isolation for multi-tenant deployments.
    """
    __tablename__ = "flow_node_executions"

    id = Column(Integer, primary_key=True)
    flow_execution_id = Column(Integer, ForeignKey("flow_executions.id"), nullable=False)
    node_id = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    agent_id = Column(Integer, nullable=True)
    task_id = Column(Integer, nullable=True)
    result = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)

    # Multi-tenant isolation
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation for multi-tenancy

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time for consistency
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Use timezone-naive UTC time for consistency
    completed_at = Column(DateTime, nullable=True)