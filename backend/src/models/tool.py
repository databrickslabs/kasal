from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, JSON, DateTime

from src.db.base import Base


class Tool(Base):
    """
    SQLAlchemy model for tools.
    """
    
    __tablename__ = "tools"
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=False)
    icon = Column(String, nullable=False)
    config = Column(JSON, default=dict)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        super(Tool, self).__init__(**kwargs)
        if self.config is None:
            self.config = {} 