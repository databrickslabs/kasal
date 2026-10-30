import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class Flow(Base):
    """
    Flow model representing a workflow definition with nodes and edges.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "flows"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    crew_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("crews.id", ondelete="CASCADE"), nullable=True
    )
    nodes: Mapped[Any] = mapped_column(JSON, default=list, nullable=True)
    edges: Mapped[Any] = mapped_column(JSON, default=list, nullable=True)
    flow_config: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)

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

    def __init__(self, **kwargs: Any) -> None:
        super(Flow, self).__init__(**kwargs)
        if self.id is None:
            self.id = uuid.uuid4()
        if self.nodes is None:
            self.nodes = []
        if self.edges is None:
            self.edges = []
        if self.flow_config is None:
            self.flow_config = {"actions": []}
        elif isinstance(self.flow_config, dict) and "actions" not in self.flow_config:
            self.flow_config["actions"] = []
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
