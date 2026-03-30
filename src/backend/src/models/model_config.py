from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime

from src.db.base import Base


class ModelConfig(Base):
    """
    ModelConfig model for storing LLM configurations.
    Enhanced with group isolation for multi-tenant deployments.
    """
    
    id = Column(Integer, primary_key=True)
    key = Column(String, nullable=False)  # Removed unique=True to allow same key for different groups
    name = Column(String, nullable=False)
    provider = Column(String)
    temperature = Column(Float)
    context_window = Column(Integer)
    max_output_tokens = Column(Integer)
    extended_thinking = Column(Boolean, default=False)
    enabled = Column(Boolean, default=True)
    
    # Multi-tenant fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 