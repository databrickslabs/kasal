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
    """Database model for tracking AI agent execution history and state.
    
    This model represents a complete execution lifecycle of a CrewAI job or workflow,
    tracking status, results, errors, and supporting multi-tenant isolation through
    group-based data segregation.
    
    The model supports comprehensive execution tracking including:
    - Execution lifecycle management (pending, running, completed, failed)
    - Graceful stop functionality with partial results
    - Multi-tenant isolation via group IDs
    - Relationship tracking to tasks and error traces
    
    Attributes:
        id: Primary key identifier
        job_id: Unique execution identifier (UUID)
        status: Current execution status
        inputs: JSON input parameters for the execution
        result: JSON output/results from the execution
        error: Error message if execution failed
        planning: Boolean flag for planning mode execution
        trigger_type: How execution was triggered (api, schedule, etc.)
        created_at: Timestamp when execution started
        completed_at: Timestamp when execution completed
        run_name: Human-readable name for the execution
        
        Stop Execution Fields:
        stopped_at: Timestamp when stop was requested
        stop_reason: Reason for stopping (user_requested, timeout, etc.)
        stop_requested_by: User who requested the stop
        partial_results: Results captured before stopping
        is_stopping: Flag indicating execution is being stopped
        
        Multi-tenant Fields:
        group_id: Group identifier for data isolation
        group_email: User email for audit trail
    
    Relationships:
        task_statuses: Related TaskStatus records for this execution
        error_traces: Related ErrorTrace records for debugging
        execution_traces: Detailed execution trace logs
    
    Example:
        >>> execution = ExecutionHistory(
        ...     job_id="exec_123",
        ...     status="running",
        ...     inputs={"task": "analyze"},
        ...     group_id="acme_corp"
        ... )
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
    
    # Stop execution fields
    stopped_at = Column(DateTime, nullable=True)  # When the execution was stopped
    stop_reason = Column(String, nullable=True)  # Reason for stopping (user requested, timeout, etc.)
    stop_requested_by = Column(String(255), nullable=True)  # User who requested the stop
    partial_results = Column(JSON, nullable=True)  # Store partial results before stopping
    is_stopping = Column(Boolean, default=False, nullable=False)  # Flag to indicate execution is in stopping state
    
    # MLflow integration fields
    mlflow_trace_id = Column(String, nullable=True, index=True)  # MLflow trace ID for evaluation linking
    mlflow_experiment_name = Column(String, nullable=True)  # MLflow experiment name for reference
    mlflow_evaluation_run_id = Column(String, nullable=True, index=True)  # MLflow evaluation run ID

    # Multi-group fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    group_email = Column(String(255), index=True, nullable=True)  # User email for audit
    
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
    
    def __init__(self, **kwargs):
        super(ExecutionHistory, self).__init__(**kwargs)
        if self.job_id is None:
            self.job_id = generate_job_id()
        if self.status is None:
            self.status = "pending"
        if self.inputs is None:
            self.inputs = {}
        if self.planning is None:
            self.planning = False
        if self.trigger_type is None:
            self.trigger_type = "api"
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class TaskStatus(Base):
    """Database model for tracking individual task status within an execution.
    
    This model tracks the lifecycle and status of individual tasks that are part
    of a larger execution. Each task represents a discrete unit of work performed
    by an agent within the CrewAI system.
    
    Attributes:
        id: Primary key identifier
        job_id: Foreign key linking to ExecutionHistory
        task_id: Unique identifier for the task
        status: Current task status (running, completed, failed)
        agent_name: Name of the agent handling this task
        started_at: Timestamp when task execution began
        completed_at: Timestamp when task completed (if applicable)
    
    Relationships:
        execution_history: Parent ExecutionHistory record
    
    Example:
        >>> task = TaskStatus(
        ...     job_id="exec_123",
        ...     task_id="task_456",
        ...     status="running",
        ...     agent_name="Research Agent"
        ... )
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
    
    def __init__(self, **kwargs):
        super(TaskStatus, self).__init__(**kwargs)
        if self.started_at is None:
            self.started_at = datetime.utcnow()


class ErrorTrace(Base):
    """Database model for detailed error tracking and debugging.
    
    This model captures comprehensive error information when tasks or executions
    fail, providing detailed debugging data for troubleshooting AI agent failures.
    Each error trace is linked to a specific execution and task for traceability.
    
    Attributes:
        id: Primary key identifier
        run_id: Foreign key linking to ExecutionHistory
        task_key: Identifier of the task that generated the error
        error_type: Classification of the error (e.g., ValidationError, TimeoutError)
        error_message: Human-readable error description
        timestamp: When the error occurred (timezone-aware)
        error_metadata: JSON field for additional error context (stack trace, etc.)
    
    Relationships:
        execution_history: Parent ExecutionHistory record
    
    Example:
        >>> error = ErrorTrace(
        ...     run_id=1,
        ...     task_key="task_456",
        ...     error_type="ValidationError",
        ...     error_message="Invalid input format",
        ...     error_metadata={"line": 42, "file": "agent.py"}
        ... )
    
    Note:
        Uses timezone-aware timestamps for consistent time tracking across
        different deployment regions.
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
    
    def __init__(self, **kwargs):
        super(ErrorTrace, self).__init__(**kwargs)
        if self.error_metadata is None:
            self.error_metadata = {}
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc) 