from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, Float, JSON, DateTime

from src.db.base import Base


class MCPServer(Base):
    """
    SQLAlchemy model for MCP (Model Context Protocol) server configurations.
    
    Stores configuration information for connecting to an MCP server,
    including authentication and connection parameters.
    """
    
    __tablename__ = "mcp_servers"
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    server_url = Column(String, nullable=False)
    encrypted_api_key = Column(String, nullable=True)  # Encrypted API key
    server_type = Column(String, default="sse")  # "sse" or "stdio"
    enabled = Column(Boolean, default=False)
    timeout_seconds = Column(Integer, default=30)
    max_retries = Column(Integer, default=3)
    model_mapping_enabled = Column(Boolean, default=False)
    rate_limit = Column(Integer, default=60)  # Requests per minute
    command = Column(String, nullable=True)  # Command for stdio server type
    args = Column(JSON, default=list)  # Arguments for stdio server type
    additional_config = Column(JSON, default=dict)  # Additional configuration parameters
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        super(MCPServer, self).__init__(**kwargs)
        if self.additional_config is None:
            self.additional_config = {}
        if self.args is None:
            self.args = [] 