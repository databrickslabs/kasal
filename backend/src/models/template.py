from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime

from src.db.base import Base


class PromptTemplate(Base):
    """
    PromptTemplate model for storing reusable prompt templates.
    """
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)  # e.g., 'generate_agent', 'generate_task', etc.
    description = Column(String, nullable=True)
    template = Column(Text, nullable=False)  # The actual prompt template text
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # Use timezone-naive UTC time 