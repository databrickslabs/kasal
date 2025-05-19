"""
Models for execution logs.

This module defines models for storing execution log data.
"""

from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.orm import relationship

from src.db.base import Base


class ExecutionLog(Base):
    """
    ExecutionLog model for storing logs of executions.
    
    This is a dedicated model for execution logs with appropriately named fields.
    """
    
    __tablename__ = "execution_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String, index=True, nullable=False)
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)  # Use timezone-naive UTC time
    
    # Create a composite index for faster queries
    __table_args__ = (
        Index('idx_execution_logs_exec_id_timestamp', 'execution_id', 'timestamp'),
    ) 