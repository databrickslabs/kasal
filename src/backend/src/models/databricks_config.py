from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from src.db.base import Base


class DatabricksConfig(Base):
    """
    DatabricksConfig model for Databricks integration settings with multi-tenant support.
    """
    
    id = Column(Integer, primary_key=True)
    workspace_url = Column(String, nullable=True, default="")  # Make nullable with empty string default
    warehouse_id = Column(String, nullable=False)
    catalog = Column(String, nullable=False)
    schema = Column(String, nullable=False)
    secret_scope = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)  # To track the currently active configuration
    is_enabled = Column(Boolean, default=True)  # To enable/disable Databricks integration
    apps_enabled = Column(Boolean, default=False)  # To enable/disable Databricks apps
    encrypted_personal_access_token = Column(String, nullable=True)  # Encrypted personal access token for apps
    
    # Multi-tenant fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), index=True, nullable=True)  # Creator email for audit
    
    # Volume configuration fields
    volume_enabled = Column(Boolean, default=False)  # Enable/disable volume uploads for all tasks
    volume_path = Column(String, nullable=True)  # Default volume path (e.g., catalog.schema.volume)
    volume_file_format = Column(String, nullable=True, default="json")  # Default file format
    volume_create_date_dirs = Column(Boolean, default=True)  # Create date-based directories
    
    # Knowledge source volume configuration fields
    knowledge_volume_enabled = Column(Boolean, default=False)  # Enable/disable knowledge volume
    knowledge_volume_path = Column(String, nullable=True)  # Knowledge volume path (e.g., catalog.schema.knowledge)
    knowledge_chunk_size = Column(Integer, default=1000)  # Chunk size for knowledge processing
    knowledge_chunk_overlap = Column(Integer, default=200)  # Chunk overlap for context preservation
    
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc)) 