"""
Database configuration models for storing Lakebase and other database settings.
"""
from sqlalchemy import Column, String, JSON, DateTime
from sqlalchemy.sql import func
from src.db.base import Base


class LakebaseConfig(Base):
    """Model for storing Lakebase configuration."""

    __tablename__ = "database_configs"

    key = Column(String, primary_key=True, index=True)
    value = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<DatabaseConfig(key='{self.key}')>"