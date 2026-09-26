import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base
from src.utils.model_config import DEFAULT_ENGINE_MODEL


class Schedule(Base):
    """
    Schedule model for recurring job execution based on cron expressions.
    Supports both crew and flow executions.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    cron_expression: Mapped[str] = mapped_column(
        String, nullable=False
    )  # Cron expression for schedule timing

    # Crew execution fields (nullable for flow executions)
    agents_yaml: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Store agents configuration (for crew executions)
    tasks_yaml: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Store tasks configuration (for crew executions)

    # Flow execution fields
    execution_type: Mapped[str] = mapped_column(
        String(20), default="crew", nullable=False
    )  # 'crew' or 'flow'
    flow_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )  # Reference to saved flow (for flow executions)
    nodes: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Flow nodes configuration (for ad-hoc flow executions)
    edges: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Flow edges configuration (for ad-hoc flow executions)
    flow_config: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Flow-specific configuration

    # Common fields
    inputs: Mapped[Any] = mapped_column(
        JSON, default=dict, nullable=True
    )  # Additional inputs for the job
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Whether the schedule is active
    # NOTE: `model` is the model the scheduled RUN uses (not a planner model — the
    # CrewAI-style planner was removed along with the schedules.planning column).
    model: Mapped[str] = mapped_column(
        String, default=DEFAULT_ENGINE_MODEL, nullable=True
    )
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )  # Last time the schedule was executed
    next_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )  # Next scheduled run time
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time

    # Group isolation fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        Index("ix_schedule_group_id", "group_id"),
        Index("ix_schedule_created_by_email", "created_by_email"),
        Index("ix_schedule_execution_type", "execution_type"),
        Index("ix_schedule_flow_id", "flow_id"),
    )

    def __init__(self, **kwargs: Any) -> None:
        super(Schedule, self).__init__(**kwargs)
        if self.inputs is None:
            self.inputs = {}
        if self.is_active is None:
            self.is_active = True
        if self.model is None:
            self.model = DEFAULT_ENGINE_MODEL
        if self.execution_type is None:
            self.execution_type = "crew"
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
