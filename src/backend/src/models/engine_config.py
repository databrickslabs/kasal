from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class EngineConfig(Base):
    """
    EngineConfig model for storing execution engine configurations.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    engine_name: Mapped[str] = mapped_column(String, nullable=False)  # e.g., 'kasal'
    engine_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g., 'workflow', 'ai', 'processing'
    config_key: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g., 'flow_enabled'
    config_value: Mapped[str] = mapped_column(
        String, nullable=False
    )  # JSON string or simple value
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    # Ensure unique combination of engine_name and config_key
    __table_args__ = (
        UniqueConstraint("engine_name", "config_key", name="_engine_config_uc"),
    )
