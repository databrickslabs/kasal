from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base


class ExecutionTrace(Base):
    """
    ExecutionTrace model for tracking agent/task execution.
    Enhanced with tenant isolation for multi-tenant deployments.
    """

    __tablename__ = "execution_trace"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # run_id/created_at are indexed: run-scoped reads/deletes filter on run_id
    # and ordered trace reads sort on created_at — both polled during live runs
    # on one of the fastest-growing tables. (Existing deployed DBs get these via
    # the _ensure_hot_polling_indexes self-heal; create_all won't ALTER.)
    run_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("executionhistory.id"), index=True
    )
    job_id: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("executionhistory.job_id"), index=True
    )
    event_source: Mapped[str] = mapped_column(String, nullable=False)  # was agent_name
    event_context: Mapped[str] = mapped_column(String, nullable=False)  # was task_name
    event_type: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )  # now required
    output: Mapped[Any] = mapped_column(JSON, nullable=True)
    trace_metadata: Mapped[Any] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True, nullable=True
    )

    # OTel span hierarchy columns
    span_id: Mapped[Optional[str]] = mapped_column(
        String(32), nullable=True, index=True
    )
    trace_id: Mapped[Optional[str]] = mapped_column(
        String(32), nullable=True, index=True
    )
    parent_span_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # OTel-native fields
    span_name: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True
    )  # Raw OTel span name (e.g. "kasal.task.execute")
    status_code: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )  # OTel status: "OK", "ERROR", "UNSET"
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )  # Span duration in milliseconds

    # Group fields (formerly multi-tenant)
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    group_email: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )  # User email for audit

    # Relationship with ExecutionHistory - Use specific foreign keys to resolve ambiguity
    run = relationship(
        "ExecutionHistory", back_populates="execution_traces", foreign_keys=[run_id]
    )
    run_by_job_id = relationship(
        "ExecutionHistory", foreign_keys=[job_id], overlaps="execution_traces_by_job_id"
    )
