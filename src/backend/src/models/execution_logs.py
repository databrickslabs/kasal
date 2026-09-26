"""
Models for execution logs.

This module defines models for storing execution log data.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class ExecutionLog(Base):
    """
    ExecutionLog model for storing logs of executions.

    This is a dedicated model for execution logs with appropriately named fields.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "execution_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    execution_id: Mapped[str] = mapped_column(String, index=True, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time

    # Multi-group fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    group_email: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )  # User email for audit

    # Create indexes for faster queries including group filtering
    __table_args__ = (
        Index("idx_execution_logs_exec_id_timestamp", "execution_id", "timestamp"),
        Index("idx_execution_logs_group_timestamp", "group_id", "timestamp"),
        Index("idx_execution_logs_group_exec_id", "group_id", "execution_id"),
    )

    def __init__(self, **kwargs):
        super(ExecutionLog, self).__init__(**kwargs)
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
