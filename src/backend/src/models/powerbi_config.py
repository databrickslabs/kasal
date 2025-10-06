from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from src.db.base import Base


class PowerBIConfig(Base):
    """
    PowerBIConfig model for Power BI integration settings with multi-tenant support.
    Stores connection details for Power BI Semantic Model (Dataset) access.
    """

    id = Column(Integer, primary_key=True)

    # Power BI connection details
    tenant_id = Column(String, nullable=False)  # Azure AD Tenant ID
    client_id = Column(String, nullable=False)  # Service Principal Application ID
    encrypted_client_secret = Column(String, nullable=True)  # Encrypted SPN secret
    workspace_id = Column(String, nullable=True)  # Power BI Workspace ID (optional)
    semantic_model_id = Column(String, nullable=True)  # Default semantic model/dataset ID (optional)

    # Service account credentials (alternative auth method)
    encrypted_username = Column(String, nullable=True)  # Encrypted username (e.g., sa_datamesh_powerbi@domain.com)
    encrypted_password = Column(String, nullable=True)  # Encrypted password

    # Configuration flags
    is_active = Column(Boolean, default=True)  # Track the currently active configuration
    is_enabled = Column(Boolean, default=True)  # Enable/disable Power BI integration

    # Multi-tenant fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), index=True, nullable=True)  # Creator email for audit

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
