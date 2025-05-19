from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime

from src.db.base import Base


class ModelConfig(Base):
    """
    ModelConfig model for storing LLM configurations.
    """
    
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    provider = Column(String)
    temperature = Column(Float)
    context_window = Column(Integer)
    max_output_tokens = Column(Integer)
    extended_thinking = Column(Boolean, default=False)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 