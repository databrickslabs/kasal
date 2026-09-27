from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base


class GroupTool(Base):
    """
    Mapping table for making a global Tool available within a specific group (workspace).

    Notes
    - A Tool with group_id = NULL is considered a global catalog tool
    - GroupTool rows represent explicit opt-in by a group to use that tool
    - "enabled" here means enabled within the group (independent of global availability)
    """

    __tablename__ = "group_tools"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Parent tool from global catalog (tools.id)
    tool_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tools.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Group (workspace) this mapping applies to
    group_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # Whether this tool is enabled for this group
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Group-scoped configuration/credentials (subset of global config, where allowed)
    config: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)

    # Optional operational status for credentials/connection checks
    credentials_status: Mapped[str] = mapped_column(
        String(50), default="unknown", nullable=False
    )

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships (optional; avoids heavy joins unless needed)
    tool = relationship("Tool", backref="group_mappings", lazy="joined")

    __table_args__ = (
        UniqueConstraint("tool_id", "group_id", name="uq_group_tools_tool_group"),
        Index("ix_group_tools_group_tool", "group_id", "tool_id"),
    )
