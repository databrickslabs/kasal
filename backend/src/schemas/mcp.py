from typing import Dict, Any, Optional, List, ClassVar
from datetime import datetime
from pydantic import BaseModel, Field


class MCPServerBase(BaseModel):
    """Base MCP Server schema with common attributes"""
    name: str = Field(..., description="Name of the MCP server")
    server_url: str = Field(..., description="URL of the MCP server")
    server_type: str = Field(default="sse", description="Type of MCP server (sse or stdio)")
    enabled: bool = Field(default=False, description="Whether the server is enabled")
    timeout_seconds: int = Field(default=30, description="Timeout in seconds for server requests")
    max_retries: int = Field(default=3, description="Maximum number of retry attempts")
    model_mapping_enabled: bool = Field(default=False, description="Enable model name mapping")
    rate_limit: int = Field(default=60, description="Maximum requests per minute")
    command: Optional[str] = Field(default=None, description="Command for stdio server type")
    args: Optional[List[str]] = Field(default=None, description="Arguments for stdio server type")
    additional_config: Optional[Dict[str, Any]] = Field(default=None, description="Additional configuration parameters")


class MCPServerCreate(MCPServerBase):
    """Schema for creating a new MCP server"""
    api_key: str = Field(..., description="API key for authentication (will be encrypted)")


class MCPServerUpdate(BaseModel):
    """Schema for updating an existing MCP server"""
    name: Optional[str] = Field(default=None, description="Name of the MCP server")
    server_url: Optional[str] = Field(default=None, description="URL of the MCP server")
    api_key: Optional[str] = Field(default=None, description="API key for authentication (will be encrypted)")
    server_type: Optional[str] = Field(default=None, description="Type of MCP server (sse or stdio)")
    enabled: Optional[bool] = Field(default=None, description="Whether the server is enabled")
    timeout_seconds: Optional[int] = Field(default=None, description="Timeout in seconds for server requests")
    max_retries: Optional[int] = Field(default=None, description="Maximum number of retry attempts")
    model_mapping_enabled: Optional[bool] = Field(default=None, description="Enable model name mapping")
    rate_limit: Optional[int] = Field(default=None, description="Maximum requests per minute")
    command: Optional[str] = Field(default=None, description="Command for stdio server type")
    args: Optional[List[str]] = Field(default=None, description="Arguments for stdio server type")
    additional_config: Optional[Dict[str, Any]] = Field(default=None, description="Additional configuration parameters")


class MCPServerResponse(MCPServerBase):
    """Schema for MCP server responses"""
    id: int = Field(..., description="Unique identifier for the MCP server")
    api_key: str = Field("", description="Decrypted API key (only present in specific scenarios)")
    created_at: datetime = Field(..., description="Timestamp when the server was created")
    updated_at: datetime = Field(..., description="Timestamp when the server was last updated")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    }


class MCPServerListResponse(BaseModel):
    """Schema for list of MCP servers response"""
    servers: List[MCPServerResponse] = Field(..., description="List of MCP servers")
    count: int = Field(..., description="Total number of MCP servers")


class MCPToggleResponse(BaseModel):
    """Schema for toggle enabled response"""
    message: str = Field(..., description="Success message")
    enabled: bool = Field(..., description="Current enabled state")


class MCPTestConnectionRequest(BaseModel):
    """Schema for testing MCP server connection"""
    server_url: str = Field(..., description="URL of the MCP server")
    api_key: str = Field(..., description="API key for authentication")
    server_type: str = Field(default="sse", description="Type of MCP server (sse or stdio)")
    timeout_seconds: int = Field(default=30, description="Timeout in seconds")


class MCPTestConnectionResponse(BaseModel):
    """Schema for MCP server connection test response"""
    success: bool = Field(..., description="Whether the connection test was successful")
    message: str = Field(..., description="Details about the connection test")


class MCPSettingsBase(BaseModel):
    """Base MCP Settings schema"""
    global_enabled: bool = Field(default=False, description="Master switch for all MCP functionality")


class MCPSettingsUpdate(MCPSettingsBase):
    """Schema for updating MCP settings"""
    pass


class MCPSettingsResponse(MCPSettingsBase):
    """Schema for MCP settings response"""
    id: int = Field(..., description="Unique identifier for the settings")
    created_at: datetime = Field(..., description="Timestamp when the settings were created")
    updated_at: datetime = Field(..., description="Timestamp when the settings were last updated")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    } 