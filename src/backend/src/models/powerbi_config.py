from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class PowerBIConfig(Base):
    """
    PowerBIConfig model for Power BI integration settings with multi-tenant support.
    Stores connection details for Power BI Semantic Model (Dataset) access.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Power BI connection details
    tenant_id: Mapped[str] = mapped_column(String, nullable=False)  # Azure AD Tenant ID
    client_id: Mapped[str] = mapped_column(
        String, nullable=False
    )  # Service Principal Application ID
    encrypted_client_secret: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Encrypted SPN secret
    workspace_id: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Power BI Workspace ID (optional)
    semantic_model_id: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Default semantic model/dataset ID (optional)

    # Service account credentials (alternative auth method)
    encrypted_username: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Encrypted username (e.g., sa_datamesh_powerbi@domain.com)
    encrypted_password: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Encrypted password

    # Authentication method
    auth_method: Mapped[str] = mapped_column(
        String, default="username_password", nullable=False
    )  # 'username_password' or 'device_code'

    # Configuration flags
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Track the currently active configuration
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Enable/disable Power BI integration

    # Multi-tenant fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )  # Creator email for audit

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=True,
    )
