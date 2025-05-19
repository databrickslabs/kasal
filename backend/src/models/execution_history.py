from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from uuid import uuid4

from src.db.base import Base


def generate_job_id():
    """
    Generate a unique job ID.
    
    Returns:
        str: A unique job ID
    """
    return str(uuid4())


class ExecutionHistory(Base):
    """
    Run model representing an execution of a job/workflow.
    """
    
    __tablename__ = "executionhistory"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String, primary_key=False, unique=True, default=generate_job_id, index=True)
    status = Column(String, nullable=False, default="pending")
    inputs = Column(JSON, default=dict)
    result = Column(JSON)
    error = Column(String)
    planning = Column(Boolean, default=False)
    trigger_type = Column(String, default="api")
    created_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    run_name = Column(String)
    completed_at = Column(DateTime)
    
    # Relationships
    task_statuses = relationship("TaskStatus", back_populates="execution_history", 
                                foreign_keys="TaskStatus.job_id", 
                                primaryjoin="ExecutionHistory.job_id == TaskStatus.job_id")
    error_traces = relationship("ErrorTrace", back_populates="execution_history", 
                               foreign_keys="ErrorTrace.run_id",
                               primaryjoin="ExecutionHistory.id == ErrorTrace.run_id")
    
    # New relationship with ExecutionTrace
    execution_traces = relationship("ExecutionTrace", back_populates="run", 
                                   foreign_keys="ExecutionTrace.run_id",
                                   primaryjoin="ExecutionHistory.id == ExecutionTrace.run_id")
    execution_traces_by_job_id = relationship("ExecutionTrace", 
                                             foreign_keys="ExecutionTrace.job_id",
                                             primaryjoin="ExecutionHistory.job_id == ExecutionTrace.job_id")


class TaskStatus(Base):
    """
    TaskStatus model for tracking the status of tasks within a run.
    """
    
    __tablename__ = "taskstatus"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String, ForeignKey("executionhistory.job_id"), index=True)
    task_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)  # 'running', 'completed', or 'failed'
    agent_name = Column(String, nullable=True)  # Store the name of the agent handling this task
    started_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    completed_at = Column(DateTime, nullable=True)
    
    # Relationship to the run
    execution_history = relationship("ExecutionHistory", back_populates="task_statuses")


class ErrorTrace(Base):
    """
    ErrorTrace model for detailed error tracking within a run.
    """
    
    __tablename__ = "errortrace"
    
    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(Integer, ForeignKey("executionhistory.id"), index=True)
    task_key = Column(String, nullable=False, index=True)
    error_type = Column(String, nullable=False)
    error_message = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    error_metadata = Column(JSON, default=dict)
    
    # Relationship to the run
    execution_history = relationship("ExecutionHistory", back_populates="error_traces") 