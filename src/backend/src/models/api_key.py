from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime

from src.db.base import Base


class ApiKey(Base):
    """
    ApiKey model for storing API authentication keys with multi-tenant support.
    """
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)  # Removed unique=True to allow same name across groups
    encrypted_value = Column(String, nullable=False)
    description = Column(String, nullable=True)
    
    # Multi-tenant fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), index=True, nullable=True)  # Creator email for audit
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Ensure unique name per group (composite unique constraint would be added in migration)
    __table_args__ = (
        {'extend_existing': True}
    ) 