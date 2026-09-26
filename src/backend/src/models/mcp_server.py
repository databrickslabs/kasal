from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class MCPServer(Base):
    """
    SQLAlchemy model for MCP (Model Context Protocol) server configurations.

    Stores configuration information for connecting to an MCP server,
    including authentication and connection parameters.
    """

    __tablename__ = "mcp_servers"
    __table_args__ = (
        UniqueConstraint("name", "group_id", name="uq_mcpserver_name_group"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    server_url: Mapped[str] = mapped_column(String, nullable=False)
    encrypted_api_key: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Encrypted API key
    server_type: Mapped[str] = mapped_column(
        String, default="sse", nullable=True
    )  # "sse" or "streamable"
    auth_type: Mapped[str] = mapped_column(
        String, default="api_key", nullable=True
    )  # "api_key" or "databricks_spn"
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)
    global_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Enable across all agents/tasks
    # NEW: Workspace scoping (nullable for base entries)
    group_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=30, nullable=True)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=True)
    model_mapping_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )
    rate_limit: Mapped[int] = mapped_column(
        Integer, default=60, nullable=True
    )  # Requests per minute
    additional_config: Mapped[Any] = mapped_column(
        JSON, default=dict, nullable=True
    )  # Additional configuration parameters
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __init__(self, **kwargs):
        super(MCPServer, self).__init__(**kwargs)
        if self.additional_config is None:
            self.additional_config = {}
