from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, UniqueConstraint

from src.db.base import Base


class EngineConfig(Base):
    """
    EngineConfig model for storing execution engine configurations.
    """
    
    id = Column(Integer, primary_key=True)
    engine_name = Column(String, nullable=False)  # e.g., 'crewai'
    engine_type = Column(String, nullable=False)  # e.g., 'workflow', 'ai', 'processing'
    config_key = Column(String, nullable=False)  # e.g., 'flow_enabled'
    config_value = Column(String, nullable=False)  # JSON string or simple value
    enabled = Column(Boolean, default=True)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Ensure unique combination of engine_name and config_key
    __table_args__ = (UniqueConstraint('engine_name', 'config_key', name='_engine_config_uc'),) 