from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from src.db.base import Base


class MCPSettings(Base):
    """
    SQLAlchemy model for MCP (Model Context Protocol) global settings.
    
    Stores global configuration for the MCP service.
    """
    
    __tablename__ = "mcp_settings"
    
    id = Column(Integer, primary_key=True)
    global_enabled = Column(Boolean, default=False)  # Master switch for all MCP functionality
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 