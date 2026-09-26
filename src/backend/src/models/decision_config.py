"""Workspace opt-in for Jev; credentials never enter execution payloads."""

from sqlalchemy import Boolean, ForeignKey, String, false
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class DecisionConfig(Base):
    __tablename__ = "decision_config"

    group_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True
    )
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
