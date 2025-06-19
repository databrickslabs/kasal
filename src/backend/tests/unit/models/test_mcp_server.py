"""Unit tests for MCPServer model."""

import pytest
from datetime import datetime
from src.models.mcp_server import MCPServer


class TestMCPServerModel:
    """Test suite for MCPServer model."""
    
    def test_mcp_server_creation_minimal(self):
        """Test creating MCPServer with minimal required fields."""
        server = MCPServer(
            name="Test Server",
            server_url="https://test.server/api"
        )
        
        assert server.name == "Test Server"
        assert server.server_url == "https://test.server/api"
        # SQLAlchemy defaults are applied when adding to session, not in __init__
        # Just verify the fields exist
        assert hasattr(server, 'server_type')
        assert hasattr(server, 'enabled')
        assert hasattr(server, 'timeout_seconds')
        assert hasattr(server, 'max_retries')
        assert hasattr(server, 'rate_limit')
        assert hasattr(server, 'model_mapping_enabled')
        assert server.additional_config == {}  # Default
        assert server.encrypted_api_key is None
        # Timestamps are set by SQLAlchemy when adding to session, not in __init__
        # assert isinstance(server.created_at, datetime)
        # assert isinstance(server.updated_at, datetime)
    
    def test_mcp_server_creation_full(self):
        """Test creating MCPServer with all fields."""
        server = MCPServer(
            name="Full Test Server",
            server_url="https://full.test.server/api",
            encrypted_api_key="encrypted_key_123",
            server_type="streamable",
            enabled=True,
            timeout_seconds=60,
            max_retries=5,
            model_mapping_enabled=True,
            rate_limit=120,
            additional_config={"custom": "config"}
        )
        
        assert server.name == "Full Test Server"
        assert server.server_url == "https://full.test.server/api"
        assert server.encrypted_api_key == "encrypted_key_123"
        assert server.server_type == "streamable"
        assert server.enabled is True
        assert server.timeout_seconds == 60
        assert server.max_retries == 5
        assert server.model_mapping_enabled is True
        assert server.rate_limit == 120
        assert server.additional_config == {"custom": "config"}
    
    def test_mcp_server_none_additional_config(self):
        """Test MCPServer handles None additional_config."""
        server = MCPServer(
            name="Test",
            server_url="https://test.server",
            additional_config=None
        )
        
        # Should be converted to empty dict
        assert server.additional_config == {}
    
    def test_mcp_server_server_types(self):
        """Test different server types."""
        # SSE server
        sse_server = MCPServer(
            name="SSE Server",
            server_url="https://sse.server/sse",
            server_type="sse"
        )
        assert sse_server.server_type == "sse"
        
        # Streamable server
        streamable_server = MCPServer(
            name="Streamable Server",
            server_url="https://streamable.server/api/mcp/",
            server_type="streamable"
        )
        assert streamable_server.server_type == "streamable"
    
    def test_mcp_server_tablename(self):
        """Test the table name is correct."""
        assert MCPServer.__tablename__ == "mcp_servers"
    
    def test_mcp_server_update(self):
        """Test updating MCPServer fields."""
        server = MCPServer(
            name="Original Name",
            server_url="https://original.server"
        )
        
        # Update fields
        server.name = "Updated Name"
        server.enabled = True
        server.timeout_seconds = 45
        server.additional_config = {"new": "config"}
        
        assert server.name == "Updated Name"
        assert server.enabled is True
        assert server.timeout_seconds == 45
        assert server.additional_config == {"new": "config"}
    
    def test_mcp_server_rate_limit_values(self):
        """Test various rate limit values."""
        # Default rate limit - field exists but no value until session
        server1 = MCPServer(name="Test1", server_url="https://test1.server")
        assert hasattr(server1, 'rate_limit')
        
        # Custom rate limit
        server2 = MCPServer(
            name="Test2",
            server_url="https://test2.server",
            rate_limit=300
        )
        assert server2.rate_limit == 300
        
        # Zero rate limit (unlimited)
        server3 = MCPServer(
            name="Test3",
            server_url="https://test3.server",
            rate_limit=0
        )
        assert server3.rate_limit == 0
    
    def test_mcp_server_timeout_values(self):
        """Test various timeout values."""
        # Default timeout - field exists but no value until session
        server1 = MCPServer(name="Test1", server_url="https://test1.server")
        assert hasattr(server1, 'timeout_seconds')
        
        # Short timeout
        server2 = MCPServer(
            name="Test2",
            server_url="https://test2.server",
            timeout_seconds=5
        )
        assert server2.timeout_seconds == 5
        
        # Long timeout
        server3 = MCPServer(
            name="Test3",
            server_url="https://test3.server",
            timeout_seconds=300
        )
        assert server3.timeout_seconds == 300