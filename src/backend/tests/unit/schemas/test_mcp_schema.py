"""
Unit tests for MCP (Model Control Protocol) schemas.

Tests the functionality of Pydantic schemas for MCP server operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.mcp import (
    MCPServerBase, MCPServerCreate, MCPServerUpdate, MCPServerResponse,
    MCPServerListResponse, MCPToggleResponse, MCPTestConnectionRequest,
    MCPTestConnectionResponse, MCPSettingsBase, MCPSettingsUpdate,
    MCPSettingsResponse
)


class TestMCPServerBase:
    """Test cases for MCPServerBase schema."""
    
    def test_valid_mcp_server_base_minimal(self):
        """Test MCPServerBase with minimal required fields."""
        data = {
            "name": "test-server",
            "server_url": "https://api.example.com"
        }
        server = MCPServerBase(**data)
        assert server.name == "test-server"
        assert server.server_url == "https://api.example.com"
        assert server.server_type == "sse"  # Default
        assert server.enabled is False  # Default
        assert server.timeout_seconds == 30  # Default
        assert server.max_retries == 3  # Default
        assert server.model_mapping_enabled is False  # Default
        assert server.rate_limit == 60  # Default
        assert server.command is None  # Default
        assert server.args is None  # Default
        assert server.additional_config is None  # Default

    def test_valid_mcp_server_base_full(self):
        """Test MCPServerBase with all fields specified."""
        data = {
            "name": "full-server",
            "server_url": "https://full.example.com",
            "server_type": "stdio",
            "enabled": True,
            "timeout_seconds": 60,
            "max_retries": 5,
            "model_mapping_enabled": True,
            "rate_limit": 120,
            "command": "/usr/bin/python",
            "args": ["-m", "server"],
            "additional_config": {"debug": True, "log_level": "INFO"}
        }
        server = MCPServerBase(**data)
        assert server.name == "full-server"
        assert server.server_url == "https://full.example.com"
        assert server.server_type == "stdio"
        assert server.enabled is True
        assert server.timeout_seconds == 60
        assert server.max_retries == 5
        assert server.model_mapping_enabled is True
        assert server.rate_limit == 120
        assert server.command == "/usr/bin/python"
        assert server.args == ["-m", "server"]
        assert server.additional_config == {"debug": True, "log_level": "INFO"}

    def test_mcp_server_base_missing_required_fields(self):
        """Test MCPServerBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            MCPServerBase(name="test-server")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "server_url" in missing_fields

    def test_mcp_server_base_boolean_conversions(self):
        """Test MCPServerBase boolean field conversions."""
        data = {
            "name": "bool-server",
            "server_url": "https://bool.example.com",
            "enabled": "true",
            "model_mapping_enabled": 1
        }
        server = MCPServerBase(**data)
        assert server.enabled is True
        assert server.model_mapping_enabled is True

    def test_mcp_server_base_integer_validations(self):
        """Test MCPServerBase integer field validations."""
        data = {
            "name": "int-server",
            "server_url": "https://int.example.com",
            "timeout_seconds": "45",  # String that can be converted
            "max_retries": 3.0,  # Float that can be converted
            "rate_limit": "90"
        }
        server = MCPServerBase(**data)
        assert server.timeout_seconds == 45
        assert server.max_retries == 3
        assert server.rate_limit == 90
        assert isinstance(server.timeout_seconds, int)
        assert isinstance(server.max_retries, int)
        assert isinstance(server.rate_limit, int)


class TestMCPServerCreate:
    """Test cases for MCPServerCreate schema."""
    
    def test_mcp_server_create_inheritance(self):
        """Test that MCPServerCreate inherits from MCPServerBase."""
        data = {
            "name": "create-server",
            "server_url": "https://create.example.com",
            "api_key": "secret-api-key"
        }
        create_server = MCPServerCreate(**data)
        
        # Should have all base class attributes
        assert hasattr(create_server, 'name')
        assert hasattr(create_server, 'server_url')
        assert hasattr(create_server, 'server_type')
        assert hasattr(create_server, 'enabled')
        assert hasattr(create_server, 'api_key')
        
        # Should behave like base class
        assert create_server.name == "create-server"
        assert create_server.server_url == "https://create.example.com"
        assert create_server.server_type == "sse"  # Default
        assert create_server.api_key == "secret-api-key"

    def test_mcp_server_create_missing_api_key(self):
        """Test MCPServerCreate validation with missing api_key."""
        data = {
            "name": "create-server",
            "server_url": "https://create.example.com"
        }
        with pytest.raises(ValidationError) as exc_info:
            MCPServerCreate(**data)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "api_key" in missing_fields

    def test_mcp_server_create_with_custom_values(self):
        """Test MCPServerCreate with custom values."""
        data = {
            "name": "custom-server",
            "server_url": "https://custom.example.com",
            "api_key": "custom-api-key",
            "server_type": "stdio",
            "enabled": True,
            "timeout_seconds": 90,
            "command": "/opt/python/bin/python"
        }
        create_server = MCPServerCreate(**data)
        assert create_server.name == "custom-server"
        assert create_server.api_key == "custom-api-key"
        assert create_server.server_type == "stdio"
        assert create_server.enabled is True
        assert create_server.timeout_seconds == 90
        assert create_server.command == "/opt/python/bin/python"


class TestMCPServerUpdate:
    """Test cases for MCPServerUpdate schema."""
    
    def test_mcp_server_update_all_optional(self):
        """Test that all MCPServerUpdate fields are optional."""
        update = MCPServerUpdate()
        assert update.name is None
        assert update.server_url is None
        assert update.api_key is None
        assert update.server_type is None
        assert update.enabled is None
        assert update.timeout_seconds is None
        assert update.max_retries is None
        assert update.model_mapping_enabled is None
        assert update.rate_limit is None
        assert update.command is None
        assert update.args is None
        assert update.additional_config is None

    def test_mcp_server_update_partial(self):
        """Test MCPServerUpdate with partial fields."""
        update_data = {
            "name": "updated-server",
            "enabled": True,
            "timeout_seconds": 120
        }
        update = MCPServerUpdate(**update_data)
        assert update.name == "updated-server"
        assert update.enabled is True
        assert update.timeout_seconds == 120
        assert update.server_url is None
        assert update.api_key is None

    def test_mcp_server_update_full(self):
        """Test MCPServerUpdate with all fields."""
        update_data = {
            "name": "fully-updated-server",
            "server_url": "https://updated.example.com",
            "api_key": "updated-api-key",
            "server_type": "stdio",
            "enabled": False,
            "timeout_seconds": 180,
            "max_retries": 10,
            "model_mapping_enabled": True,
            "rate_limit": 240,
            "command": "/updated/python",
            "args": ["-m", "updated_server"],
            "additional_config": {"updated": True}
        }
        update = MCPServerUpdate(**update_data)
        assert update.name == "fully-updated-server"
        assert update.server_url == "https://updated.example.com"
        assert update.api_key == "updated-api-key"
        assert update.server_type == "stdio"
        assert update.enabled is False
        assert update.timeout_seconds == 180
        assert update.max_retries == 10
        assert update.model_mapping_enabled is True
        assert update.rate_limit == 240
        assert update.command == "/updated/python"
        assert update.args == ["-m", "updated_server"]
        assert update.additional_config == {"updated": True}


class TestMCPServerResponse:
    """Test cases for MCPServerResponse schema."""
    
    def test_valid_mcp_server_response(self):
        """Test MCPServerResponse with all required fields."""
        now = datetime.now()
        data = {
            "id": 123,
            "name": "response-server",
            "server_url": "https://response.example.com",
            "api_key": "decrypted-key",
            "created_at": now,
            "updated_at": now
        }
        response = MCPServerResponse(**data)
        assert response.id == 123
        assert response.name == "response-server"
        assert response.server_url == "https://response.example.com"
        assert response.api_key == "decrypted-key"
        assert response.created_at == now
        assert response.updated_at == now
        
        # Should inherit all base class defaults
        assert response.server_type == "sse"
        assert response.enabled is False
        assert response.timeout_seconds == 30

    def test_mcp_server_response_config(self):
        """Test MCPServerResponse model config."""
        assert hasattr(MCPServerResponse, 'model_config')
        assert MCPServerResponse.model_config["from_attributes"] is True

    def test_mcp_server_response_missing_fields(self):
        """Test MCPServerResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MCPServerResponse(
                name="test-server",
                server_url="https://test.example.com"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields

    def test_mcp_server_response_datetime_conversion(self):
        """Test MCPServerResponse with datetime string conversion."""
        data = {
            "id": 456,
            "name": "datetime-server",
            "server_url": "https://datetime.example.com",
            "api_key": "",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:00:00"
        }
        response = MCPServerResponse(**data)
        assert response.id == 456
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)


class TestMCPServerListResponse:
    """Test cases for MCPServerListResponse schema."""
    
    def test_valid_mcp_server_list_response(self):
        """Test MCPServerListResponse with all fields."""
        now = datetime.now()
        servers = [
            MCPServerResponse(
                id=1,
                name="server-1",
                server_url="https://server1.example.com",
                api_key="key-1",
                created_at=now,
                updated_at=now
            ),
            MCPServerResponse(
                id=2,
                name="server-2",
                server_url="https://server2.example.com",
                api_key="key-2",
                created_at=now,
                updated_at=now
            )
        ]
        
        data = {
            "servers": servers,
            "count": 2
        }
        list_response = MCPServerListResponse(**data)
        
        assert len(list_response.servers) == 2
        assert list_response.count == 2
        assert list_response.servers[0].name == "server-1"
        assert list_response.servers[1].name == "server-2"

    def test_empty_server_list(self):
        """Test MCPServerListResponse with empty server list."""
        data = {
            "servers": [],
            "count": 0
        }
        list_response = MCPServerListResponse(**data)
        assert len(list_response.servers) == 0
        assert list_response.count == 0


class TestMCPToggleResponse:
    """Test cases for MCPToggleResponse schema."""
    
    def test_valid_mcp_toggle_response(self):
        """Test MCPToggleResponse with all fields."""
        data = {
            "message": "Server enabled successfully",
            "enabled": True
        }
        toggle_response = MCPToggleResponse(**data)
        assert toggle_response.message == "Server enabled successfully"
        assert toggle_response.enabled is True

    def test_mcp_toggle_response_disabled(self):
        """Test MCPToggleResponse for disabled state."""
        data = {
            "message": "Server disabled successfully",
            "enabled": False
        }
        toggle_response = MCPToggleResponse(**data)
        assert toggle_response.message == "Server disabled successfully"
        assert toggle_response.enabled is False


class TestMCPTestConnectionRequest:
    """Test cases for MCPTestConnectionRequest schema."""
    
    def test_valid_mcp_test_connection_request(self):
        """Test MCPTestConnectionRequest with all fields."""
        data = {
            "server_url": "https://test.example.com",
            "api_key": "test-api-key",
            "server_type": "stdio",
            "timeout_seconds": 45
        }
        request = MCPTestConnectionRequest(**data)
        assert request.server_url == "https://test.example.com"
        assert request.api_key == "test-api-key"
        assert request.server_type == "stdio"
        assert request.timeout_seconds == 45

    def test_mcp_test_connection_request_defaults(self):
        """Test MCPTestConnectionRequest with default values."""
        data = {
            "server_url": "https://default.example.com",
            "api_key": "default-key"
        }
        request = MCPTestConnectionRequest(**data)
        assert request.server_url == "https://default.example.com"
        assert request.api_key == "default-key"
        assert request.server_type == "sse"  # Default
        assert request.timeout_seconds == 30  # Default

    def test_mcp_test_connection_request_missing_fields(self):
        """Test MCPTestConnectionRequest validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MCPTestConnectionRequest(server_url="https://test.example.com")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "api_key" in missing_fields


class TestMCPTestConnectionResponse:
    """Test cases for MCPTestConnectionResponse schema."""
    
    def test_valid_mcp_test_connection_response_success(self):
        """Test MCPTestConnectionResponse for successful connection."""
        data = {
            "success": True,
            "message": "Connection successful"
        }
        response = MCPTestConnectionResponse(**data)
        assert response.success is True
        assert response.message == "Connection successful"

    def test_valid_mcp_test_connection_response_failure(self):
        """Test MCPTestConnectionResponse for failed connection."""
        data = {
            "success": False,
            "message": "Connection failed: timeout"
        }
        response = MCPTestConnectionResponse(**data)
        assert response.success is False
        assert response.message == "Connection failed: timeout"

    def test_mcp_test_connection_response_missing_fields(self):
        """Test MCPTestConnectionResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MCPTestConnectionResponse(success=True)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields


class TestMCPSettingsBase:
    """Test cases for MCPSettingsBase schema."""
    
    def test_valid_mcp_settings_base(self):
        """Test MCPSettingsBase with all fields."""
        data = {
            "global_enabled": True
        }
        settings = MCPSettingsBase(**data)
        assert settings.global_enabled is True

    def test_mcp_settings_base_default(self):
        """Test MCPSettingsBase with default values."""
        settings = MCPSettingsBase()
        assert settings.global_enabled is False  # Default

    def test_mcp_settings_base_boolean_conversion(self):
        """Test MCPSettingsBase boolean conversion."""
        settings = MCPSettingsBase(global_enabled="true")
        assert settings.global_enabled is True


class TestMCPSettingsUpdate:
    """Test cases for MCPSettingsUpdate schema."""
    
    def test_mcp_settings_update_inheritance(self):
        """Test that MCPSettingsUpdate inherits from MCPSettingsBase."""
        data = {
            "global_enabled": True
        }
        update = MCPSettingsUpdate(**data)
        
        assert hasattr(update, 'global_enabled')
        assert update.global_enabled is True

    def test_mcp_settings_update_empty(self):
        """Test MCPSettingsUpdate with no fields."""
        update = MCPSettingsUpdate()
        assert update.global_enabled is False  # Default from base


class TestMCPSettingsResponse:
    """Test cases for MCPSettingsResponse schema."""
    
    def test_valid_mcp_settings_response(self):
        """Test MCPSettingsResponse with all required fields."""
        now = datetime.now()
        data = {
            "id": 1,
            "global_enabled": True,
            "created_at": now,
            "updated_at": now
        }
        response = MCPSettingsResponse(**data)
        assert response.id == 1
        assert response.global_enabled is True
        assert response.created_at == now
        assert response.updated_at == now

    def test_mcp_settings_response_config(self):
        """Test MCPSettingsResponse model config."""
        assert hasattr(MCPSettingsResponse, 'model_config')
        assert MCPSettingsResponse.model_config["from_attributes"] is True

    def test_mcp_settings_response_missing_fields(self):
        """Test MCPSettingsResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MCPSettingsResponse(global_enabled=True)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields


class TestSchemaIntegration:
    """Integration tests for MCP schema interactions."""
    
    def test_mcp_server_workflow(self):
        """Test complete MCP server workflow."""
        # Create server
        create_data = {
            "name": "workflow-server",
            "server_url": "https://workflow.example.com",
            "api_key": "workflow-key",
            "server_type": "sse",
            "enabled": False
        }
        create_schema = MCPServerCreate(**create_data)
        
        # Update server
        update_data = {
            "enabled": True,
            "timeout_seconds": 60,
            "rate_limit": 120
        }
        update_schema = MCPServerUpdate(**update_data)
        
        # Test connection
        test_request = MCPTestConnectionRequest(
            server_url=create_schema.server_url,
            api_key=create_schema.api_key,
            server_type=create_schema.server_type
        )
        
        test_response = MCPTestConnectionResponse(
            success=True,
            message="Connection test successful"
        )
        
        # Simulate database entity
        now = datetime.now()
        db_data = {
            "id": 1,
            "name": create_schema.name,
            "server_url": create_schema.server_url,
            "api_key": "decrypted-workflow-key",
            "server_type": create_schema.server_type,
            "enabled": update_data["enabled"],
            "timeout_seconds": update_data["timeout_seconds"],
            "rate_limit": update_data["rate_limit"],
            "created_at": now,
            "updated_at": now
        }
        server_response = MCPServerResponse(**db_data)
        
        # Toggle response
        toggle_response = MCPToggleResponse(
            message="Server enabled successfully",
            enabled=True
        )
        
        # Verify the complete workflow
        assert create_schema.name == "workflow-server"
        assert create_schema.enabled is False
        assert update_schema.enabled is True
        assert test_request.server_url == "https://workflow.example.com"
        assert test_response.success is True
        assert server_response.id == 1
        assert server_response.enabled is True
        assert server_response.timeout_seconds == 60
        assert toggle_response.enabled is True

    def test_mcp_settings_workflow(self):
        """Test MCP settings workflow."""
        # Create settings update
        settings_update = MCPSettingsUpdate(global_enabled=True)
        
        # Simulate database entity
        now = datetime.now()
        settings_response = MCPSettingsResponse(
            id=1,
            global_enabled=settings_update.global_enabled,
            created_at=now,
            updated_at=now
        )
        
        # Verify workflow
        assert settings_update.global_enabled is True
        assert settings_response.id == 1
        assert settings_response.global_enabled is True
        assert settings_response.created_at == now

    def test_mcp_server_configuration_scenarios(self):
        """Test different MCP server configuration scenarios."""
        # SSE server configuration
        sse_server = MCPServerCreate(
            name="sse-server",
            server_url="https://sse.example.com",
            api_key="sse-key",
            server_type="sse",
            timeout_seconds=30,
            rate_limit=60
        )
        assert sse_server.server_type == "sse"
        assert sse_server.command is None
        assert sse_server.args is None
        
        # STDIO server configuration
        stdio_server = MCPServerCreate(
            name="stdio-server",
            server_url="https://stdio.example.com",
            api_key="stdio-key",
            server_type="stdio",
            command="/usr/bin/python3",
            args=["-m", "mcp_server"],
            additional_config={"env": {"PATH": "/usr/bin"}}
        )
        assert stdio_server.server_type == "stdio"
        assert stdio_server.command == "/usr/bin/python3"
        assert stdio_server.args == ["-m", "mcp_server"]
        assert stdio_server.additional_config == {"env": {"PATH": "/usr/bin"}}
        
        # High-performance server configuration
        high_perf_server = MCPServerCreate(
            name="high-perf-server",
            server_url="https://highperf.example.com",
            api_key="highperf-key",
            timeout_seconds=120,
            max_retries=10,
            rate_limit=300,
            model_mapping_enabled=True,
            additional_config={"cache_enabled": True, "batch_size": 100}
        )
        assert high_perf_server.timeout_seconds == 120
        assert high_perf_server.max_retries == 10
        assert high_perf_server.rate_limit == 300
        assert high_perf_server.model_mapping_enabled is True