import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class Crew(Base):
    """
    SQLAlchemy model for crews.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "crews"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4
    )
    name: Mapped[Optional[str]] = mapped_column(String, index=True)
    agent_ids: Mapped[Any] = mapped_column(JSON, default=lambda: [], nullable=True)
    task_ids: Mapped[Any] = mapped_column(JSON, default=lambda: [], nullable=True)
    nodes: Mapped[Any] = mapped_column(JSON, nullable=True)
    edges: Mapped[Any] = mapped_column(JSON, nullable=True)

    # Crew execution configuration
    process: Mapped[str] = mapped_column(
        String(50), default="sequential", nullable=True
    )  # sequential | hierarchical | parallel
    reasoning: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Enable the model's native reasoning budget
    reasoning_llm: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # LLM for reasoning
    reasoning_config: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # {"reasoning_effort": "low"|"medium"|"high"}
    manager_llm: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # LLM for hierarchical manager
    tool_configs: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Crew-level tool configurations (MCP servers, etc.)
    memory: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Enable memory
    verbose: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Verbose output
    max_rpm: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Max requests per minute (can be int or None)

    # Multi-group fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Creator email for audit

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __init__(self, **kwargs):
        super(Crew, self).__init__(**kwargs)
        if self.agent_ids is None:
            self.agent_ids = []
        if self.task_ids is None:
            self.task_ids = []
        if self.nodes is None:
            self.nodes = []
        if self.edges is None:
            self.edges = []


# Keeping Plan class for backward compatibility, but making it use the same table
class Plan(Crew):
    """
    Plan class (alias for Crew) for backward compatibility.
    """

    pass
