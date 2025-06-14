from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from src.db.base import Base


class ExecutionTrace(Base):
    """
    ExecutionTrace model for tracking agent/task execution.
    Enhanced with tenant isolation for multi-tenant deployments.
    """
    
    __tablename__ = "execution_trace"
    
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey('executionhistory.id'))
    job_id = Column(String, ForeignKey('executionhistory.job_id'), index=True)
    event_source = Column(String, nullable=False)  # was agent_name
    event_context = Column(String, nullable=False)  # was task_name
    event_type = Column(String, nullable=False, index=True)  # now required
    output = Column(JSON)
    trace_metadata = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Group fields (formerly multi-tenant)
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    group_email = Column(String(255), index=True, nullable=True)  # User email for audit
    
    # Relationship with ExecutionHistory - Use specific foreign keys to resolve ambiguity
    run = relationship("ExecutionHistory", back_populates="execution_traces", foreign_keys=[run_id])
    run_by_job_id = relationship("ExecutionHistory", foreign_keys=[job_id], overlaps="execution_traces_by_job_id") 