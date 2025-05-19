from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from src.db.base import Base


class DatabricksConfig(Base):
    """
    DatabricksConfig model for Databricks integration settings.
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
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc)) 