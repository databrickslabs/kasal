from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer

from src.db.base import Base


class InitializationStatus(Base):
    """
    InitializationStatus model to track database initialization state.
    """

    id = Column(Integer, primary_key=True)
    is_initialized = Column(Boolean, default=False)
    initialized_at = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
