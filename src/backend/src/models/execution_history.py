import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base


def generate_job_id() -> str:
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
        planning: LEGACY historical flag. Kasal removed the CrewAI-style prose
            planner, so this is never populated from a request any more and stays
            at its False default; it is kept so pre-removal rows still read back.
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

    __table_args__ = (
        # Composite for the most-polled endpoint (executions list):
        # WHERE group_id IN (...) ORDER BY created_at DESC.
        Index("idx_executionhistory_group_created", "group_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_id: Mapped[str] = mapped_column(
        String,
        primary_key=False,
        unique=True,
        default=generate_job_id,
        index=True,
        nullable=True,
    )
    # status/created_at are indexed: the trace broadcaster scans status IN
    # ('RUNNING', ...) every second and the executions list orders by
    # created_at DESC on the most-polled endpoint. (Existing deployed DBs get
    # these via the _ensure_hot_polling_indexes self-heal.)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="pending", index=True
    )
    inputs: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(String)
    # LEGACY: kept so pre-planner-removal rows still read back. Never written from
    # a request any more (the planner is gone), so new rows use the False default.
    planning: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)
    trigger_type: Mapped[str] = mapped_column(String, default="api", nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True, nullable=True
    )  # Use timezone-naive UTC time
    run_name: Mapped[Optional[str]] = mapped_column(String)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Stop execution fields
    stopped_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )  # When the execution was stopped
    stop_reason: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Reason for stopping (user requested, timeout, etc.)
    stop_requested_by: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # User who requested the stop
    partial_results: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Store partial results before stopping
    is_stopping: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )  # Flag to indicate execution is in stopping state

    # MLflow integration fields
    mlflow_trace_id: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, index=True
    )  # MLflow trace ID for evaluation linking
    mlflow_experiment_name: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # MLflow experiment name for reference
    mlflow_evaluation_run_id: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, index=True
    )  # MLflow evaluation run ID

    # Multi-group fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    group_email: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )  # User email for audit

    # Execution type and flow fields (consolidated from flow_executions table)
    execution_type: Mapped[str] = mapped_column(
        String(20), default="crew", index=True, nullable=True
    )  # 'crew' or 'flow'
    flow_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True, index=True
    )  # Optional reference to saved flow
    # The saved crew this run was built from, when it had one.
    #
    # The crew equivalent of ``flow_id``, and it exists for the same reason
    # resume needs one: without a link back to a definition, a resume can only
    # replay the frozen ``inputs`` snapshot, so a task edited after the run was
    # invisible to it. An ad-hoc run from an unsaved canvas has no row to point
    # at and leaves this null, which is what makes the snapshot the fallback
    # rather than the source.
    crew_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True, index=True
    )

    # Checkpoint/Persistence fields for CrewAI Flow state management
    flow_uuid: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )  # CrewAI's state.id for @persist
    checkpoint_status: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, default=None
    )  # 'active', 'resumed', 'expired', None
    checkpoint_method: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Last checkpointed method name
    # The execution this one was resumed FROM, if any.
    #
    # Resuming creates a new execution rather than re-running the old record in
    # place. The failed run stays FAILED — the audit trail is append-only, each
    # attempt's token cost is attributable to itself, and a resumed run's traces
    # and logs (both keyed by job_id) no longer interleave with the crashed
    # attempt's under a single id.
    #
    # Deliberately NOT a ForeignKey: purging an old run must not cascade away
    # the successful resume that replaced it, and execution_history is already
    # reached by job_id from several tables without FK constraints.
    resumed_from_execution_id: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, index=True
    )
    # A SHARED JSON bag, not the checkpoint's private space. Keys in use:
    #   "checkpoint"     — the unified resume record (crew tasks OR flow crews);
    #                      shape and versioning owned by
    #                      services/execution/checkpointing/schema.py
    #   "edited_config"  — HITL edits replayed by flow_methods.py
    #   "ucmv_yaml_edits" — user-edited metric-view YAML
    # Anything writing here must MERGE, never replace the column wholesale.
    checkpoint_data: Mapped[Any] = mapped_column(JSON, nullable=True, default=None)

    # Which agent runtime ran this — "kasal" or "crewai". Resolved ONCE when the
    # row is created and read back from here forever after, rather than
    # re-reading the operator setting: a run that started before a switch must
    # not finish on the other engine, and a resume has to continue on whatever
    # produced the checkpoint it is resuming from.
    #
    # NULL on every row written before the engine layer existed, and read as
    # "kasal" — which is what those runs actually used.
    harness: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, index=True
    )

    # Relationships
    task_statuses = relationship(
        "TaskStatus",
        back_populates="execution_history",
        foreign_keys="TaskStatus.job_id",
        primaryjoin="ExecutionHistory.job_id == TaskStatus.job_id",
    )
    error_traces = relationship(
        "ErrorTrace",
        back_populates="execution_history",
        foreign_keys="ErrorTrace.run_id",
        primaryjoin="ExecutionHistory.id == ErrorTrace.run_id",
    )

    # New relationship with ExecutionTrace
    execution_traces = relationship(
        "ExecutionTrace",
        back_populates="run",
        foreign_keys="ExecutionTrace.run_id",
        primaryjoin="ExecutionHistory.id == ExecutionTrace.run_id",
    )
    execution_traces_by_job_id = relationship(
        "ExecutionTrace",
        foreign_keys="ExecutionTrace.job_id",
        primaryjoin="ExecutionHistory.job_id == ExecutionTrace.job_id",
    )

    # HITL (Human in the Loop) approval relationships
    hitl_approvals = relationship(
        "HITLApproval",
        back_populates="execution",
        foreign_keys="HITLApproval.execution_id",
        primaryjoin="ExecutionHistory.job_id == HITLApproval.execution_id",
    )

    def __init__(self, **kwargs: Any) -> None:
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
        if self.execution_type is None:
            self.execution_type = "crew"
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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_id: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("executionhistory.job_id"), index=True
    )
    task_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    status: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'running', 'completed', or 'failed'
    agent_name: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Store the name of the agent handling this task
    started_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationship to the run
    execution_history = relationship("ExecutionHistory", back_populates="task_statuses")

    def __init__(self, **kwargs: Any) -> None:
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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("executionhistory.id"), index=True
    )
    task_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    error_type: Mapped[str] = mapped_column(String, nullable=False)
    error_message: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=True,
    )
    error_metadata: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)

    # Relationship to the run
    execution_history = relationship("ExecutionHistory", back_populates="error_traces")

    def __init__(self, **kwargs: Any) -> None:
        super(ErrorTrace, self).__init__(**kwargs)
        if self.error_metadata is None:
            self.error_metadata = {}
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
