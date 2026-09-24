"""Workspace opt-in for Jev; credentials never enter execution payloads."""

from sqlalchemy import Boolean, Column, ForeignKey, String, false

from src.db.base import Base


class DecisionConfig(Base):
    __tablename__ = "decision_config"

    group_id = Column(
        String(100), ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True
    )
    enabled = Column(Boolean, nullable=False, default=False, server_default=false())
