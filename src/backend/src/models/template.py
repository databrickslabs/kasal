from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class PromptTemplate(Base):
    """
    PromptTemplate model for storing reusable prompt templates.
    Enhanced with group isolation for multi-group deployments.
    """

    __table_args__ = (
        UniqueConstraint("name", "group_id", name="uq_prompttemplate_name_group"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Allow same name across groups; uniqueness enforced by composite constraint above
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    template: Mapped[str] = mapped_column(
        Text, nullable=False
    )  # The actual prompt template text
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)

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
    )  # Use timezone-naive UTC time
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time

    def __init__(self, **kwargs):
        super(PromptTemplate, self).__init__(**kwargs)
        if self.is_active is None:
            self.is_active = True
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()


# Backward compatibility alias
Template = PromptTemplate
