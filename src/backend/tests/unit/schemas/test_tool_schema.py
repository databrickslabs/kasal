"""
Unit tests for tool schemas.

Tests the functionality of Pydantic schemas for tool operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.tool import (
    ToolBase, ToolCreate, ToolUpdate, ToolResponse, ToolListResponse, ToggleResponse
)


class TestToolBase:
    """Test cases for ToolBase schema."""
    
    def test_valid_tool_base_minimal(self):
        """Test ToolBase with minimal required fields."""
        tool_data = {
            "title": "Test Tool",
            "description": "A tool for testing",
            "icon": "test-icon"
        }
        tool = ToolBase(**tool_data)
        assert tool.title == "Test Tool"
        assert tool.description == "A tool for testing"
        assert tool.icon == "test-icon"
        assert tool.config == {}
        assert tool.enabled is True
    
    def test_valid_tool_base_full(self):
        """Test ToolBase with all fields specified."""
        tool_data = {
            "title": "Advanced Analytics Tool",
            "description": "Provides advanced data analytics capabilities",
            "icon": "analytics-icon",
            "config": {
                "timeout": 300,
                "max_rows": 10000,
                "cache_enabled": True,
                "api_endpoint": "https://api.example.com"
            },
            "enabled": False
        }
        tool = ToolBase(**tool_data)
        assert tool.title == "Advanced Analytics Tool"
        assert tool.description == "Provides advanced data analytics capabilities"
        assert tool.icon == "analytics-icon"
        assert tool.config == {
            "timeout": 300,
            "max_rows": 10000,
            "cache_enabled": True,
            "api_endpoint": "https://api.example.com"
        }
        assert tool.enabled is False
    
    def test_tool_base_missing_required_fields(self):
        """Test ToolBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ToolBase(title="Test Tool")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "description" in missing_fields
        assert "icon" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ToolBase(description="Test description", icon="test-icon")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "title" in missing_fields
    
    def test_tool_base_empty_strings(self):
        """Test ToolBase with empty strings for required fields."""
        tool_data = {
            "title": "",
            "description": "",
            "icon": ""
        }
        tool = ToolBase(**tool_data)
        assert tool.title == ""
        assert tool.description == ""
        assert tool.icon == ""
    
    def test_tool_base_complex_config(self):
        """Test ToolBase with complex configuration."""
        complex_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "credentials": {
                    "username": "admin",
                    "password_env": "DB_PASSWORD"
                }
            },
            "features": ["analytics", "reporting", "export"],
            "limits": {
                "max_connections": 10,
                "timeout": 30.5,
                "retry_attempts": 3
            },
            "metadata": {
                "version": "1.2.3",
                "author": "Tool Developer",
                "license": "MIT"
            }
        }
        tool_data = {
            "title": "Database Tool",
            "description": "Database connectivity tool",
            "icon": "database",
            "config": complex_config
        }
        tool = ToolBase(**tool_data)
        assert tool.config == complex_config
        assert tool.config["database"]["port"] == 5432
        assert tool.config["features"] == ["analytics", "reporting", "export"]
        assert tool.config["limits"]["timeout"] == 30.5
    
    def test_tool_base_boolean_conversion(self):
        """Test ToolBase boolean field conversion."""
        tool_data = {
            "title": "Bool Test Tool",
            "description": "Testing boolean conversion",
            "icon": "bool-icon",
            "enabled": "true"
        }
        tool = ToolBase(**tool_data)
        assert tool.enabled is True
        
        tool_data["enabled"] = 0
        tool = ToolBase(**tool_data)
        assert tool.enabled is False
        
        tool_data["enabled"] = 1
        tool = ToolBase(**tool_data)
        assert tool.enabled is True


class TestToolCreate:
    """Test cases for ToolCreate schema."""
    
    def test_tool_create_inheritance(self):
        """Test that ToolCreate inherits from ToolBase."""
        tool_data = {
            "title": "Create Test Tool",
            "description": "Tool for testing creation",
            "icon": "create-icon"
        }
        tool = ToolCreate(**tool_data)
        
        # Should have all base class attributes
        assert hasattr(tool, 'title')
        assert hasattr(tool, 'description')
        assert hasattr(tool, 'icon')
        assert hasattr(tool, 'config')
        assert hasattr(tool, 'enabled')
        
        # Should behave like base class
        assert tool.title == "Create Test Tool"
        assert tool.description == "Tool for testing creation"
        assert tool.icon == "create-icon"
        assert tool.config == {}
        assert tool.enabled is True
    
    def test_tool_create_with_config(self):
        """Test ToolCreate with configuration."""
        tool_data = {
            "title": "Configured Tool",
            "description": "Tool with initial configuration",
            "icon": "config-icon",
            "config": {
                "api_key": "test-key",
                "endpoint": "https://api.test.com",
                "rate_limit": 100
            },
            "enabled": False
        }
        tool = ToolCreate(**tool_data)
        assert tool.title == "Configured Tool"
        assert tool.config["api_key"] == "test-key"
        assert tool.config["rate_limit"] == 100
        assert tool.enabled is False


class TestToolUpdate:
    """Test cases for ToolUpdate schema."""
    
    def test_tool_update_all_optional(self):
        """Test that all ToolUpdate fields are optional."""
        update = ToolUpdate()
        assert update.title is None
        assert update.description is None
        assert update.icon is None
        assert update.config is None
        assert update.enabled is None
    
    def test_tool_update_partial(self):
        """Test ToolUpdate with partial fields."""
        update_data = {
            "title": "Updated Tool Title",
            "enabled": False
        }
        update = ToolUpdate(**update_data)
        assert update.title == "Updated Tool Title"
        assert update.enabled is False
        assert update.description is None
        assert update.icon is None
        assert update.config is None
    
    def test_tool_update_full(self):
        """Test ToolUpdate with all fields."""
        update_data = {
            "title": "Fully Updated Tool",
            "description": "Updated description",
            "icon": "updated-icon",
            "config": {
                "new_setting": "value",
                "updated_timeout": 600
            },
            "enabled": True
        }
        update = ToolUpdate(**update_data)
        assert update.title == "Fully Updated Tool"
        assert update.description == "Updated description"
        assert update.icon == "updated-icon"
        assert update.config == {
            "new_setting": "value",
            "updated_timeout": 600
        }
        assert update.enabled is True
    
    def test_tool_update_config_replacement(self):
        """Test ToolUpdate config replacement scenarios."""
        # Replace entire config
        update_data = {
            "config": {"completely": "new", "config": True}
        }
        update = ToolUpdate(**update_data)
        assert update.config == {"completely": "new", "config": True}
        
        # Set config to empty dict
        update_data = {"config": {}}
        update = ToolUpdate(**update_data)
        assert update.config == {}
        
        # Set config to None (explicit removal)
        update_data = {"config": None}
        update = ToolUpdate(**update_data)
        assert update.config is None
    
    def test_tool_update_empty_strings(self):
        """Test ToolUpdate with empty strings."""
        update_data = {
            "title": "",
            "description": "",
            "icon": ""
        }
        update = ToolUpdate(**update_data)
        assert update.title == ""
        assert update.description == ""
        assert update.icon == ""


class TestToolResponse:
    """Test cases for ToolResponse schema."""
    
    def test_valid_tool_response(self):
        """Test ToolResponse with all required fields."""
        now = datetime.now()
        response_data = {
            "id": 1,
            "title": "Response Tool",
            "description": "Tool response test",
            "icon": "response-icon",
            "config": {"setting": "value"},
            "enabled": True,
            "created_at": now,
            "updated_at": now
        }
        response = ToolResponse(**response_data)
        assert response.id == 1
        assert response.title == "Response Tool"
        assert response.description == "Tool response test"
        assert response.icon == "response-icon"
        assert response.config == {"setting": "value"}
        assert response.enabled is True
        assert response.created_at == now
        assert response.updated_at == now
    
    def test_tool_response_inheritance(self):
        """Test that ToolResponse inherits from ToolBase."""
        now = datetime.now()
        response_data = {
            "id": 2,
            "title": "Inherited Response Tool",
            "description": "Tool response inheritance test",
            "icon": "inherit-icon",
            "created_at": now,
            "updated_at": now
        }
        response = ToolResponse(**response_data)
        
        # Should have all base class attributes
        assert hasattr(response, 'title')
        assert hasattr(response, 'description')
        assert hasattr(response, 'icon')
        assert hasattr(response, 'config')
        assert hasattr(response, 'enabled')
        
        # Should have response-specific attributes
        assert hasattr(response, 'id')
        assert hasattr(response, 'created_at')
        assert hasattr(response, 'updated_at')
        
        # Should behave like base class with defaults
        assert response.title == "Inherited Response Tool"
        assert response.config == {}  # Default from base
        assert response.enabled is True  # Default from base
    
    def test_tool_response_model_config(self):
        """Test ToolResponse model configuration."""
        assert hasattr(ToolResponse, 'model_config')
        assert ToolResponse.model_config['from_attributes'] is True
    
    def test_tool_response_missing_fields(self):
        """Test ToolResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ToolResponse(
                title="Test Tool",
                description="Test description",
                icon="test-icon"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields
    
    def test_tool_response_datetime_conversion(self):
        """Test ToolResponse with datetime string conversion."""
        response_data = {
            "id": 3,
            "title": "DateTime Tool",
            "description": "Tool with datetime strings",
            "icon": "datetime-icon",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:30:00"
        }
        response = ToolResponse(**response_data)
        assert response.id == 3
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)
    
    def test_tool_response_id_types(self):
        """Test ToolResponse with different ID types."""
        now = datetime.now()
        
        # Integer ID
        response_data = {
            "id": 42,
            "title": "Integer ID Tool",
            "description": "Tool with integer ID",
            "icon": "int-icon",
            "created_at": now,
            "updated_at": now
        }
        response = ToolResponse(**response_data)
        assert response.id == 42
        assert isinstance(response.id, int)
        
        # String that can be converted to int
        response_data["id"] = "123"
        response = ToolResponse(**response_data)
        assert response.id == 123
        assert isinstance(response.id, int)


class TestToolListResponse:
    """Test cases for ToolListResponse schema."""
    
    def test_valid_tool_list_response(self):
        """Test ToolListResponse with tools."""
        now = datetime.now()
        tools = [
            ToolResponse(
                id=1,
                title="Tool 1",
                description="First tool",
                icon="icon1",
                created_at=now,
                updated_at=now
            ),
            ToolResponse(
                id=2,
                title="Tool 2",
                description="Second tool",
                icon="icon2",
                enabled=False,
                created_at=now,
                updated_at=now
            )
        ]
        
        list_response_data = {
            "tools": tools,
            "count": 2
        }
        list_response = ToolListResponse(**list_response_data)
        assert len(list_response.tools) == 2
        assert list_response.count == 2
        assert list_response.tools[0].id == 1
        assert list_response.tools[1].id == 2
        assert list_response.tools[0].enabled is True
        assert list_response.tools[1].enabled is False
    
    def test_tool_list_response_empty(self):
        """Test ToolListResponse with empty tool list."""
        list_response_data = {
            "tools": [],
            "count": 0
        }
        list_response = ToolListResponse(**list_response_data)
        assert len(list_response.tools) == 0
        assert list_response.count == 0
    
    def test_tool_list_response_count_mismatch(self):
        """Test ToolListResponse with mismatched count and list length."""
        now = datetime.now()
        tools = [
            ToolResponse(
                id=1,
                title="Single Tool",
                description="Only tool in list",
                icon="single-icon",
                created_at=now,
                updated_at=now
            )
        ]
        
        # Count represents total available, not just current page
        list_response_data = {
            "tools": tools,
            "count": 100
        }
        list_response = ToolListResponse(**list_response_data)
        assert len(list_response.tools) == 1
        assert list_response.count == 100
    
    def test_tool_list_response_missing_fields(self):
        """Test ToolListResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ToolListResponse(tools=[])
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ToolListResponse(count=0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "tools" in missing_fields
    
    def test_tool_list_response_with_dicts(self):
        """Test ToolListResponse creation with tool dicts."""
        now = datetime.now()
        list_response_data = {
            "tools": [
                {
                    "id": 1,
                    "title": "Dict Tool",
                    "description": "Tool from dict",
                    "icon": "dict-icon",
                    "config": {"dict_setting": True},
                    "enabled": True,
                    "created_at": now,
                    "updated_at": now
                }
            ],
            "count": 1
        }
        list_response = ToolListResponse(**list_response_data)
        assert len(list_response.tools) == 1
        assert isinstance(list_response.tools[0], ToolResponse)
        assert list_response.tools[0].title == "Dict Tool"
        assert list_response.tools[0].config == {"dict_setting": True}


class TestToggleResponse:
    """Test cases for ToggleResponse schema."""
    
    def test_valid_toggle_response(self):
        """Test ToggleResponse with valid data."""
        response_data = {
            "message": "Tool enabled successfully",
            "enabled": True
        }
        response = ToggleResponse(**response_data)
        assert response.message == "Tool enabled successfully"
        assert response.enabled is True
    
    def test_toggle_response_disabled(self):
        """Test ToggleResponse when disabling tool."""
        response_data = {
            "message": "Tool disabled successfully",
            "enabled": False
        }
        response = ToggleResponse(**response_data)
        assert response.message == "Tool disabled successfully"
        assert response.enabled is False
    
    def test_toggle_response_missing_fields(self):
        """Test ToggleResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ToggleResponse(message="Test message")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "enabled" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ToggleResponse(enabled=True)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields
    
    def test_toggle_response_boolean_conversion(self):
        """Test ToggleResponse boolean field conversion."""
        response_data = {
            "message": "Tool toggled",
            "enabled": "true"
        }
        response = ToggleResponse(**response_data)
        assert response.enabled is True
        
        response_data["enabled"] = 0
        response = ToggleResponse(**response_data)
        assert response.enabled is False
    
    def test_toggle_response_empty_message(self):
        """Test ToggleResponse with empty message."""
        response_data = {
            "message": "",
            "enabled": True
        }
        response = ToggleResponse(**response_data)
        assert response.message == ""
        assert response.enabled is True


class TestSchemaIntegration:
    """Integration tests for tool schema interactions."""
    
    def test_tool_lifecycle_workflow(self):
        """Test complete tool lifecycle workflow."""
        # Create tool
        create_data = {
            "title": "Lifecycle Tool",
            "description": "Testing complete lifecycle",
            "icon": "lifecycle-icon",
            "config": {
                "initial_setting": "value",
                "timeout": 300
            },
            "enabled": True
        }
        create_schema = ToolCreate(**create_data)
        
        # Update tool
        update_data = {
            "title": "Updated Lifecycle Tool",
            "config": {
                "initial_setting": "updated_value",
                "timeout": 600,
                "new_setting": "added"
            },
            "enabled": False
        }
        update_schema = ToolUpdate(**update_data)
        
        # Database entity (simulating what would come from database)
        now = datetime.now()
        response_data = {
            "id": 1,
            "title": update_data["title"],  # Updated title
            "description": create_schema.description,  # Original description
            "icon": create_schema.icon,  # Original icon
            "config": update_data["config"],  # Updated config
            "enabled": update_data["enabled"],  # Updated enabled state
            "created_at": now,
            "updated_at": now
        }
        tool_response = ToolResponse(**response_data)
        
        # Toggle response
        toggle_response = ToggleResponse(
            message="Tool enabled successfully",
            enabled=True
        )
        
        # Verify the complete workflow
        assert create_schema.title == "Lifecycle Tool"
        assert create_schema.config["initial_setting"] == "value"
        assert update_schema.title == "Updated Lifecycle Tool"
        assert update_schema.config["new_setting"] == "added"
        assert tool_response.id == 1
        assert tool_response.title == "Updated Lifecycle Tool"  # From update
        assert tool_response.description == "Testing complete lifecycle"  # From creation
        assert tool_response.config["initial_setting"] == "updated_value"  # From update
        assert tool_response.enabled is False  # From update
        assert toggle_response.enabled is True
        assert "successfully" in toggle_response.message
    
    def test_tool_configuration_scenarios(self):
        """Test different tool configuration scenarios."""
        # Simple tool with minimal config
        simple_tool = ToolCreate(
            title="Simple Tool",
            description="A simple tool",
            icon="simple"
        )
        assert simple_tool.config == {}
        assert simple_tool.enabled is True
        
        # Tool with API configuration
        api_tool = ToolCreate(
            title="API Tool",
            description="Tool for API interactions",
            icon="api",
            config={
                "base_url": "https://api.example.com",
                "api_key": "secret-key",
                "rate_limit": 100,
                "timeout": 30,
                "retry_attempts": 3,
                "headers": {
                    "User-Agent": "Kasal-Tool/1.0",
                    "Accept": "application/json"
                }
            }
        )
        assert api_tool.config["base_url"] == "https://api.example.com"
        assert api_tool.config["headers"]["User-Agent"] == "Kasal-Tool/1.0"
        
        # Tool with database configuration
        db_tool = ToolCreate(
            title="Database Tool",
            description="Database connectivity tool",
            icon="database",
            config={
                "driver": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "kasal",
                "pool_size": 10,
                "ssl_required": True,
                "connection_params": {
                    "connect_timeout": 30,
                    "command_timeout": 300
                }
            },
            enabled=False  # Disabled by default for security
        )
        assert db_tool.config["driver"] == "postgresql"
        assert db_tool.config["connection_params"]["connect_timeout"] == 30
        assert db_tool.enabled is False
    
    def test_tool_list_scenarios(self):
        """Test different tool list scenarios."""
        now = datetime.now()
        
        # Mixed enabled/disabled tools
        tools = [
            ToolResponse(
                id=1, title="Active Tool", description="Enabled tool",
                icon="active", enabled=True, created_at=now, updated_at=now
            ),
            ToolResponse(
                id=2, title="Inactive Tool", description="Disabled tool",
                icon="inactive", enabled=False, created_at=now, updated_at=now
            ),
            ToolResponse(
                id=3, title="Complex Tool", description="Tool with complex config",
                icon="complex", config={"setting1": "value1", "nested": {"key": "value"}},
                enabled=True, created_at=now, updated_at=now
            )
        ]
        
        tool_list = ToolListResponse(tools=tools, count=3)
        
        # Verify tool states
        active_tools = [t for t in tool_list.tools if t.enabled]
        inactive_tools = [t for t in tool_list.tools if not t.enabled]
        
        assert len(active_tools) == 2
        assert len(inactive_tools) == 1
        assert active_tools[0].title == "Active Tool"
        assert inactive_tools[0].title == "Inactive Tool"
        assert tool_list.tools[2].config["nested"]["key"] == "value"
    
    def test_tool_update_scenarios(self):
        """Test different tool update scenarios."""
        # Partial update - only title
        title_update = ToolUpdate(title="New Title")
        assert title_update.title == "New Title"
        assert title_update.description is None
        assert title_update.config is None
        
        # Config-only update
        config_update = ToolUpdate(
            config={
                "new_timeout": 900,
                "feature_enabled": True,
                "api_version": "v2"
            }
        )
        assert config_update.config["new_timeout"] == 900
        assert config_update.title is None
        
        # Enable/disable update
        enable_update = ToolUpdate(enabled=True)
        disable_update = ToolUpdate(enabled=False)
        assert enable_update.enabled is True
        assert disable_update.enabled is False
        
        # Complete update
        full_update = ToolUpdate(
            title="Completely Updated Tool",
            description="All fields updated",
            icon="new-icon",
            config={"everything": "new"},
            enabled=True
        )
        assert full_update.title == "Completely Updated Tool"
        assert full_update.config == {"everything": "new"}