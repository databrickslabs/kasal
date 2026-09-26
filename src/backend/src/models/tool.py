from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class Tool(Base):
    """
    SQLAlchemy model for tools.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "tools"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    icon: Mapped[str] = mapped_column(String, nullable=False)
    config: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)

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
        super(Tool, self).__init__(**kwargs)
        if self.config is None:
            self.config = {}
        if self.enabled is None:
            self.enabled = True
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
