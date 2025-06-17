"""
Unit tests for databricks_config schemas.

Tests the functionality of Pydantic schemas for Databricks configuration
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.databricks_config import (
    DatabricksConfigBase, DatabricksConfigCreate, DatabricksConfigUpdate,
    DatabricksConfigInDB, DatabricksConfigResponse, DatabricksTokenStatus
)


class TestDatabricksConfigBase:
    """Test cases for DatabricksConfigBase schema."""
    
    def test_valid_databricks_config_base_defaults(self):
        """Test DatabricksConfigBase with default values."""
        config = DatabricksConfigBase()
        assert config.workspace_url == ""
        assert config.warehouse_id == ""
        assert config.catalog == ""
        assert config.db_schema == ""
        assert config.secret_scope == ""
        assert config.enabled is True
        assert config.apps_enabled is False
    
    def test_valid_databricks_config_base_full(self):
        """Test DatabricksConfigBase with all fields specified."""
        config_data = {
            "workspace_url": "https://test.cloud.databricks.com",
            "warehouse_id": "abc123def456",
            "catalog": "main",
            "schema": "default",  # Using alias
            "secret_scope": "kasal-secrets",
            "enabled": True,
            "apps_enabled": False
        }
        config = DatabricksConfigBase(**config_data)
        assert config.workspace_url == "https://test.cloud.databricks.com"
        assert config.warehouse_id == "abc123def456"
        assert config.catalog == "main"
        assert config.db_schema == "default"
        assert config.secret_scope == "kasal-secrets"
        assert config.enabled is True
        assert config.apps_enabled is False
    
    def test_databricks_config_base_schema_alias(self):
        """Test DatabricksConfigBase with schema field alias."""
        # Using the alias 'schema'
        config_data = {"schema": "test_schema"}
        config = DatabricksConfigBase(**config_data)
        assert config.db_schema == "test_schema"
        
        # The alias takes precedence, so the actual field name gets default value
        config_data = {"schema": "alias_schema", "db_schema": "direct_schema"}
        config = DatabricksConfigBase(**config_data)
        assert config.db_schema == "alias_schema"  # Alias wins
    
    def test_databricks_config_base_boolean_conversions(self):
        """Test DatabricksConfigBase boolean field conversions."""
        config_data = {
            "enabled": "true",
            "apps_enabled": 1
        }
        config = DatabricksConfigBase(**config_data)
        assert config.enabled is True
        assert config.apps_enabled is True
        
        config_data = {
            "enabled": "false",
            "apps_enabled": 0
        }
        config = DatabricksConfigBase(**config_data)
        assert config.enabled is False
        assert config.apps_enabled is False
    
    def test_databricks_config_base_url_formats(self):
        """Test DatabricksConfigBase with various URL formats."""
        url_formats = [
            "https://test.cloud.databricks.com",
            "https://test.cloud.databricks.com/",
            "https://company.cloud.databricks.com",
            "https://adb-123456789.12.azuredatabricks.net",
            "https://dbc-12345678-abcd.cloud.databricks.com"
        ]
        
        for url in url_formats:
            config_data = {"workspace_url": url}
            config = DatabricksConfigBase(**config_data)
            assert config.workspace_url == url


class TestDatabricksConfigCreate:
    """Test cases for DatabricksConfigCreate schema."""
    
    def test_databricks_config_create_disabled(self):
        """Test DatabricksConfigCreate when disabled."""
        config_data = {"enabled": False}
        config = DatabricksConfigCreate(**config_data)
        assert config.enabled is False
        # Should not raise validation errors when disabled
    
    def test_databricks_config_create_apps_enabled(self):
        """Test DatabricksConfigCreate when apps are enabled."""
        config_data = {
            "enabled": True,
            "apps_enabled": True
        }
        config = DatabricksConfigCreate(**config_data)
        assert config.enabled is True
        assert config.apps_enabled is True
        # Should not raise validation errors when apps are enabled
    
    def test_databricks_config_create_valid_full(self):
        """Test DatabricksConfigCreate with all required fields when enabled."""
        config_data = {
            "workspace_url": "https://test.cloud.databricks.com",
            "warehouse_id": "abc123def456",
            "catalog": "main",
            "schema": "default",
            "secret_scope": "kasal-secrets",
            "enabled": True,
            "apps_enabled": False
        }
        config = DatabricksConfigCreate(**config_data)
        assert config.workspace_url == "https://test.cloud.databricks.com"
        assert config.warehouse_id == "abc123def456"
        assert config.catalog == "main"
        assert config.db_schema == "default"
        assert config.secret_scope == "kasal-secrets"
        assert config.enabled is True
        assert config.apps_enabled is False
    
    def test_databricks_config_create_validation_enabled_missing_fields(self):
        """Test DatabricksConfigCreate validation when enabled but missing required fields."""
        # Missing all required fields
        config_data = {
            "enabled": True,
            "apps_enabled": False
        }
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfigCreate(**config_data)
        
        error_message = str(exc_info.value)
        assert "Invalid configuration" in error_message
        assert "warehouse_id" in error_message
        assert "catalog" in error_message
        assert "db_schema" in error_message
        assert "secret_scope" in error_message
    
    def test_databricks_config_create_validation_partial_missing_fields(self):
        """Test DatabricksConfigCreate validation with some missing fields."""
        config_data = {
            "warehouse_id": "abc123",
            "catalog": "main",
            # Missing db_schema and secret_scope
            "enabled": True,
            "apps_enabled": False
        }
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfigCreate(**config_data)
        
        # Check that the error mentions missing fields
        error_message = str(exc_info.value)
        assert "db_schema" in error_message
        assert "secret_scope" in error_message
    
    def test_databricks_config_create_validation_empty_strings(self):
        """Test DatabricksConfigCreate validation with empty string fields."""
        config_data = {
            "warehouse_id": "",
            "catalog": "main",
            "schema": "",
            "secret_scope": "secrets",
            "enabled": True,
            "apps_enabled": False
        }
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfigCreate(**config_data)
        
        error_message = str(exc_info.value)
        assert "warehouse_id" in error_message
        assert "db_schema" in error_message
    
    def test_databricks_config_create_required_fields_property(self):
        """Test the required_fields property."""
        # When disabled
        config = DatabricksConfigCreate(enabled=False)
        required = config.required_fields
        assert required == []
        
        # When apps enabled
        config = DatabricksConfigCreate(enabled=True, apps_enabled=True)
        required = config.required_fields
        assert required == []
        
        # When enabled and apps disabled - provide valid data to avoid validation error
        config = DatabricksConfigCreate(
            enabled=True, 
            apps_enabled=False,
            warehouse_id="test-wh",
            catalog="test",
            schema="test",
            secret_scope="test-scope"
        )
        required = config.required_fields
        assert set(required) == {"warehouse_id", "catalog", "db_schema", "secret_scope"}


class TestDatabricksConfigUpdate:
    """Test cases for DatabricksConfigUpdate schema."""
    
    def test_databricks_config_update_all_optional(self):
        """Test that all DatabricksConfigUpdate fields are optional."""
        update = DatabricksConfigUpdate()
        assert update.workspace_url is None
        assert update.warehouse_id is None
        assert update.catalog is None
        assert update.db_schema is None
        assert update.secret_scope is None
        assert update.enabled is None
        assert update.apps_enabled is None
    
    def test_databricks_config_update_partial(self):
        """Test DatabricksConfigUpdate with partial fields."""
        update_data = {
            "warehouse_id": "new-warehouse-123",
            "enabled": False
        }
        update = DatabricksConfigUpdate(**update_data)
        assert update.warehouse_id == "new-warehouse-123"
        assert update.enabled is False
        assert update.workspace_url is None
        assert update.catalog is None
    
    def test_databricks_config_update_full(self):
        """Test DatabricksConfigUpdate with all fields."""
        update_data = {
            "workspace_url": "https://new.cloud.databricks.com",
            "warehouse_id": "new-warehouse-456",
            "catalog": "new_catalog",
            "schema": "new_schema",
            "secret_scope": "new-secrets",
            "enabled": False,
            "apps_enabled": True
        }
        update = DatabricksConfigUpdate(**update_data)
        assert update.workspace_url == "https://new.cloud.databricks.com"
        assert update.warehouse_id == "new-warehouse-456"
        assert update.catalog == "new_catalog"
        assert update.db_schema == "new_schema"
        assert update.secret_scope == "new-secrets"
        assert update.enabled is False
        assert update.apps_enabled is True
    
    def test_databricks_config_update_schema_alias(self):
        """Test DatabricksConfigUpdate with schema field alias."""
        update_data = {"schema": "updated_schema"}
        update = DatabricksConfigUpdate(**update_data)
        assert update.db_schema == "updated_schema"


class TestDatabricksConfigInDB:
    """Test cases for DatabricksConfigInDB schema."""
    
    def test_valid_databricks_config_in_db(self):
        """Test DatabricksConfigInDB with all required fields."""
        now = datetime.now()
        config_data = {
            "id": 1,
            "workspace_url": "https://db.cloud.databricks.com",
            "warehouse_id": "wh123",
            "catalog": "main",
            "schema": "default",
            "secret_scope": "secrets",
            "enabled": True,
            "apps_enabled": False,
            "is_active": True,
            "created_at": now,
            "updated_at": now
        }
        config = DatabricksConfigInDB(**config_data)
        assert config.id == 1
        assert config.workspace_url == "https://db.cloud.databricks.com"
        assert config.warehouse_id == "wh123"
        assert config.catalog == "main"
        assert config.db_schema == "default"
        assert config.secret_scope == "secrets"
        assert config.enabled is True
        assert config.apps_enabled is False
        assert config.is_active is True
        assert config.created_at == now
        assert config.updated_at == now
    
    def test_databricks_config_in_db_model_config(self):
        """Test DatabricksConfigInDB model configuration."""
        assert hasattr(DatabricksConfigInDB, 'model_config')
        assert DatabricksConfigInDB.model_config['from_attributes'] is True
        assert DatabricksConfigInDB.model_config['populate_by_name'] is True
    
    def test_databricks_config_in_db_missing_fields(self):
        """Test DatabricksConfigInDB validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksConfigInDB(
                workspace_url="https://test.com",
                warehouse_id="wh123"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "is_active" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields
    
    def test_databricks_config_in_db_datetime_conversion(self):
        """Test DatabricksConfigInDB with datetime string conversion."""
        config_data = {
            "id": 2,
            "is_active": True,
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:30:00"
        }
        config = DatabricksConfigInDB(**config_data)
        assert config.id == 2
        assert isinstance(config.created_at, datetime)
        assert isinstance(config.updated_at, datetime)


class TestDatabricksConfigResponse:
    """Test cases for DatabricksConfigResponse schema."""
    
    def test_databricks_config_response_inheritance(self):
        """Test that DatabricksConfigResponse inherits from DatabricksConfigBase."""
        config_data = {
            "workspace_url": "https://response.cloud.databricks.com",
            "warehouse_id": "response-wh",
            "catalog": "response_catalog",
            "schema": "response_schema",
            "secret_scope": "response-secrets",
            "enabled": True,
            "apps_enabled": True
        }
        response = DatabricksConfigResponse(**config_data)
        
        # Should have all base class attributes
        assert hasattr(response, 'workspace_url')
        assert hasattr(response, 'warehouse_id')
        assert hasattr(response, 'catalog')
        assert hasattr(response, 'db_schema')
        assert hasattr(response, 'secret_scope')
        assert hasattr(response, 'enabled')
        assert hasattr(response, 'apps_enabled')
        
        # Should behave like base class
        assert response.workspace_url == "https://response.cloud.databricks.com"
        assert response.warehouse_id == "response-wh"
        assert response.catalog == "response_catalog"
        assert response.db_schema == "response_schema"
        assert response.secret_scope == "response-secrets"
        assert response.enabled is True
        assert response.apps_enabled is True
    
    def test_databricks_config_response_minimal(self):
        """Test DatabricksConfigResponse with minimal data."""
        response = DatabricksConfigResponse()
        assert response.workspace_url == ""
        assert response.warehouse_id == ""
        assert response.catalog == ""
        assert response.db_schema == ""
        assert response.secret_scope == ""
        assert response.enabled is True
        assert response.apps_enabled is False


class TestDatabricksTokenStatus:
    """Test cases for DatabricksTokenStatus schema."""
    
    def test_valid_databricks_token_status(self):
        """Test DatabricksTokenStatus with valid data."""
        status_data = {
            "personal_token_required": True,
            "message": "Personal access token is required for Databricks access"
        }
        status = DatabricksTokenStatus(**status_data)
        assert status.personal_token_required is True
        assert status.message == "Personal access token is required for Databricks access"
    
    def test_databricks_token_status_not_required(self):
        """Test DatabricksTokenStatus when token is not required."""
        status_data = {
            "personal_token_required": False,
            "message": "Databricks workspace is configured and accessible"
        }
        status = DatabricksTokenStatus(**status_data)
        assert status.personal_token_required is False
        assert status.message == "Databricks workspace is configured and accessible"
    
    def test_databricks_token_status_missing_fields(self):
        """Test DatabricksTokenStatus validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksTokenStatus(personal_token_required=True)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            DatabricksTokenStatus(message="Test message")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "personal_token_required" in missing_fields
    
    def test_databricks_token_status_boolean_conversion(self):
        """Test DatabricksTokenStatus boolean field conversion."""
        status_data = {
            "personal_token_required": "true",
            "message": "Test message"
        }
        status = DatabricksTokenStatus(**status_data)
        assert status.personal_token_required is True
        
        status_data = {
            "personal_token_required": 0,
            "message": "Test message"
        }
        status = DatabricksTokenStatus(**status_data)
        assert status.personal_token_required is False


class TestSchemaIntegration:
    """Integration tests for Databricks configuration schema interactions."""
    
    def test_databricks_config_lifecycle_workflow(self):
        """Test complete Databricks configuration lifecycle."""
        # Create configuration (disabled initially)
        create_data = {
            "enabled": False,
            "apps_enabled": False
        }
        create_schema = DatabricksConfigCreate(**create_data)
        
        # Update to enable with full configuration
        update_data = {
            "workspace_url": "https://company.cloud.databricks.com",
            "warehouse_id": "abc123def456",
            "catalog": "production",
            "schema": "main",
            "secret_scope": "prod-secrets",
            "enabled": True,
            "apps_enabled": False
        }
        update_schema = DatabricksConfigUpdate(**update_data)
        
        # Database entity (simulating what would come from database)
        now = datetime.now()
        db_data = {
            "id": 1,
            "workspace_url": update_data["workspace_url"],
            "warehouse_id": update_data["warehouse_id"],
            "catalog": update_data["catalog"],
            "schema": update_data["schema"],
            "secret_scope": update_data["secret_scope"],
            "enabled": update_data["enabled"],
            "apps_enabled": update_data["apps_enabled"],
            "is_active": True,
            "created_at": now,
            "updated_at": now
        }
        db_config = DatabricksConfigInDB(**db_data)
        
        # Response schema
        response_data = {
            "workspace_url": db_config.workspace_url,
            "warehouse_id": db_config.warehouse_id,
            "catalog": db_config.catalog,
            "schema": db_config.db_schema,
            "secret_scope": db_config.secret_scope,
            "enabled": db_config.enabled,
            "apps_enabled": db_config.apps_enabled
        }
        response_config = DatabricksConfigResponse(**response_data)
        
        # Verify the complete workflow
        assert create_schema.enabled is False
        assert update_schema.enabled is True
        assert db_config.id == 1
        assert db_config.enabled is True
        assert response_config.workspace_url == "https://company.cloud.databricks.com"
        assert response_config.enabled is True
    
    def test_databricks_validation_scenarios(self):
        """Test various Databricks configuration validation scenarios."""
        # Valid enabled configuration
        valid_enabled = DatabricksConfigCreate(
            workspace_url="https://test.cloud.databricks.com",
            warehouse_id="wh123",
            catalog="test",
            schema="default",
            secret_scope="test-secrets",
            enabled=True,
            apps_enabled=False
        )
        assert valid_enabled.enabled is True
        
        # Valid disabled configuration (no validation required)
        valid_disabled = DatabricksConfigCreate(enabled=False)
        assert valid_disabled.enabled is False
        
        # Valid apps-enabled configuration (no warehouse validation required)
        valid_apps = DatabricksConfigCreate(
            enabled=True,
            apps_enabled=True
        )
        assert valid_apps.apps_enabled is True
        
        # Invalid enabled configuration (missing required fields)
        with pytest.raises(ValueError):
            DatabricksConfigCreate(
                workspace_url="https://test.com",
                enabled=True,
                apps_enabled=False
                # Missing warehouse_id, catalog, schema, secret_scope
            )
    
    def test_databricks_token_status_scenarios(self):
        """Test different Databricks token status scenarios."""
        # Token required scenario
        token_required = DatabricksTokenStatus(
            personal_token_required=True,
            message="Please configure your Databricks personal access token"
        )
        assert token_required.personal_token_required is True
        assert "personal access token" in token_required.message.lower()
        
        # Token not required scenario
        token_not_required = DatabricksTokenStatus(
            personal_token_required=False,
            message="Databricks is properly configured and accessible"
        )
        assert token_not_required.personal_token_required is False
        assert "configured" in token_not_required.message.lower()
        
        # Error scenario
        token_error = DatabricksTokenStatus(
            personal_token_required=True,
            message="Failed to connect to Databricks workspace. Please check your configuration."
        )
        assert token_error.personal_token_required is True
        assert "failed" in token_error.message.lower()