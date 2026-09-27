import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class FlowExecution(Base):
    """
    Model representing a flow execution record.
    Enhanced with group isolation for multi-tenant deployments.
    """

    __tablename__ = "flow_executions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    flow_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )  # Optional reference to saved flow, no FK constraint
    job_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    config: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    run_name: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Descriptive name for the execution

    # Multi-tenant isolation
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation for multi-tenancy

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __init__(self, **kwargs: Any) -> None:
        super(FlowExecution, self).__init__(**kwargs)
        if self.config is None:
            self.config = {}


class FlowNodeExecution(Base):
    """
    Model representing a flow node execution record.
    Enhanced with group isolation for multi-tenant deployments.
    """

    __tablename__ = "flow_node_executions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    flow_execution_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("flow_executions.id"), nullable=False
    )
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    agent_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    task_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Multi-tenant isolation
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation for multi-tenancy

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time for consistency
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time for consistency
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
