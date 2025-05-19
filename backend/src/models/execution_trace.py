from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from src.db.base import Base


class ExecutionTrace(Base):
    """
    ExecutionTrace model for tracking agent/task execution.
    """
    
    __tablename__ = "execution_trace"
    
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey('executionhistory.id'))
    job_id = Column(String, ForeignKey('executionhistory.job_id'), index=True)
    agent_name = Column(String, nullable=False)
    task_name = Column(String, nullable=False)
    event_type = Column(String, nullable=True, index=True)
    output = Column(JSON)
    trace_metadata = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship with ExecutionHistory - Use specific foreign keys to resolve ambiguity
    run = relationship("ExecutionHistory", back_populates="execution_traces", foreign_keys=[run_id])
    run_by_job_id = relationship("ExecutionHistory", foreign_keys=[job_id], overlaps="execution_traces_by_job_id") 