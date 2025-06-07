"""
Models for execution logs.

This module defines models for storing execution log data.
"""

from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.orm import relationship

from src.db.base import Base


class ExecutionLog(Base):
    """
    ExecutionLog model for storing logs of executions.
    
    This is a dedicated model for execution logs with appropriately named fields.
    Enhanced with tenant isolation for multi-tenant deployments.
    """
    
    __tablename__ = "execution_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String, index=True, nullable=False)
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    
    # Multi-tenant fields
    tenant_id = Column(String(100), index=True, nullable=True)  # Tenant isolation
    tenant_email = Column(String(255), index=True, nullable=True)  # User email for audit
    
    # Create indexes for faster queries including tenant filtering
    __table_args__ = (
        Index('idx_execution_logs_exec_id_timestamp', 'execution_id', 'timestamp'),
        Index('idx_execution_logs_tenant_timestamp', 'tenant_id', 'timestamp'),
        Index('idx_execution_logs_tenant_exec_id', 'tenant_id', 'execution_id'),
    ) 