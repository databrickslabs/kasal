from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship

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

    id = Column(Integer, primary_key=True)

    # Parent tool from global catalog (tools.id)
    tool_id = Column(Integer, ForeignKey("tools.id", ondelete="CASCADE"), nullable=False, index=True)

    # Group (workspace) this mapping applies to
    group_id = Column(String(100), nullable=False, index=True)

    # Whether this tool is enabled for this group
    enabled = Column(Boolean, default=False, nullable=False)

    # Group-scoped configuration/credentials (subset of global config, where allowed)
    config = Column(JSON, default=dict)

    # Optional operational status for credentials/connection checks
    credentials_status = Column(String(50), default="unknown", nullable=False)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships (optional; avoids heavy joins unless needed)
    tool = relationship("Tool", backref="group_mappings", lazy="joined")

    __table_args__ = (
        UniqueConstraint("tool_id", "group_id", name="uq_group_tools_tool_group"),
        Index("ix_group_tools_group_tool", "group_id", "tool_id"),
    )

