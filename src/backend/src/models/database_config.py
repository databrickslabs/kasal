"""
Database configuration models for storing Lakebase and other database settings.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.db.base import Base


class LakebaseConfig(Base):
    """Model for storing Lakebase configuration."""

    __tablename__ = "database_configs"

    key: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    value: Mapped[Any] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=True
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<DatabaseConfig(key='{self.key}')>"
