from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class MCPSettings(Base):
    """
    SQLAlchemy model for MCP (Model Context Protocol) global settings.

    Stores global configuration for the MCP service.
    """

    __tablename__ = "mcp_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    global_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Master switch for all MCP functionality
    individual_enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Allow agent/task-specific MCP selection
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
