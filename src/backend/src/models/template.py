from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime

from src.db.base import Base


class PromptTemplate(Base):
    """
    PromptTemplate model for storing reusable prompt templates.
    Enhanced with group isolation for multi-group deployments.
    """
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)  # e.g., 'generate_agent', 'generate_task', etc.
    description = Column(String, nullable=True)
    template = Column(Text, nullable=False)  # The actual prompt template text
    is_active = Column(Boolean, default=True)
    
    # Multi-group fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    tenant_id = Column(String(100), index=True, nullable=True)  # Legacy compatibility (will be removed)
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Use timezone-naive UTC time 