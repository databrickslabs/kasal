from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime

from src.db.base import Base


class ApiKey(Base):
    """
    ApiKey model for storing API authentication keys.
    """
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True, index=True)
    encrypted_value = Column(String, nullable=False)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 