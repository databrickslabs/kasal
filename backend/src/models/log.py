from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, DateTime

from src.db.base import Base


class LLMLog(Base):
    """
    LLMLog model for tracking LLM interactions and usage.
    """
    
    id = Column(Integer, primary_key=True)
    endpoint = Column(String, nullable=False)  # e.g., 'generate-crew', 'generate-agent'
    prompt = Column(String, nullable=False)    # The input prompt
    response = Column(String, nullable=False)   # The LLM response
    model = Column(String, nullable=False)     # e.g., 'gpt-4'
    tokens_used = Column(Integer)              # Total tokens used
    duration_ms = Column(Integer)              # Time taken in milliseconds
    status = Column(String, nullable=False)    # 'success' or 'error'
    error_message = Column(String)             # Error message if any
    created_at = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    extra_data = Column(JSON)                  # Any additional metadata 