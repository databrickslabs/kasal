from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, UniqueConstraint

from src.db.base import Base


class PromptTemplate(Base):
    """
    PromptTemplate model for storing reusable prompt templates.
    Enhanced with group isolation for multi-group deployments.
    """
    __table_args__ = (
        UniqueConstraint('name', 'group_id', name='uq_prompttemplate_name_group'),
    )

    id = Column(Integer, primary_key=True)
    # Allow same name across groups; uniqueness enforced by composite constraint above
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    template = Column(Text, nullable=False)  # The actual prompt template text
    is_active = Column(Boolean, default=True)

    # Multi-group fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Use timezone-naive UTC time

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