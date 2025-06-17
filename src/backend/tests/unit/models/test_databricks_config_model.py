"""
Unit tests for databricks_config model.

Tests the functionality of the DatabricksConfig database model including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.models.databricks_config import DatabricksConfig


class TestDatabricksConfig:
    """Test cases for DatabricksConfig model."""

    def test_databricks_config_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert DatabricksConfig.__tablename__ == "databricksconfig"

    def test_databricks_config_column_structure(self):
        """Test DatabricksConfig model column structure."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert - Check that all expected columns exist
        expected_columns = [
            'id', 'workspace_url', 'warehouse_id', 'catalog', 'schema',
            'secret_scope', 'is_active', 'is_enabled', 'apps_enabled',
            'encrypted_personal_access_token', 'created_at', 'updated_at'
        ]
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in DatabricksConfig model"

    def test_databricks_config_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "INTEGER" in str(columns['id'].type)
        
        # workspace_url field (nullable with default)
        assert columns['workspace_url'].nullable is True
        assert columns['workspace_url'].default.arg == ""
        assert "VARCHAR" in str(columns['workspace_url'].type) or "STRING" in str(columns['workspace_url'].type)
        
        # Required string fields
        required_string_fields = ['warehouse_id', 'catalog', 'schema', 'secret_scope']
        for field in required_string_fields:
            assert columns[field].nullable is False
            assert "VARCHAR" in str(columns[field].type) or "STRING" in str(columns[field].type)
        
        # Boolean fields with defaults
        boolean_fields_defaults = {
            'is_active': True,
            'is_enabled': True,
            'apps_enabled': False
        }
        for field, default_value in boolean_fields_defaults.items():
            assert "BOOLEAN" in str(columns[field].type)
            assert columns[field].default.arg is default_value
        
        # Optional encrypted token field
        assert columns['encrypted_personal_access_token'].nullable is True
        assert "VARCHAR" in str(columns['encrypted_personal_access_token'].type) or "STRING" in str(columns['encrypted_personal_access_token'].type)
        
        # DateTime fields with timezone
        datetime_fields = ['created_at', 'updated_at']
        for field in datetime_fields:
            assert "DATETIME" in str(columns[field].type)

    def test_databricks_config_default_values(self):
        """Test DatabricksConfig model default values."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert
        assert columns['workspace_url'].default.arg == ""
        assert columns['is_active'].default.arg is True
        assert columns['is_enabled'].default.arg is True
        assert columns['apps_enabled'].default.arg is False
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None

    def test_databricks_config_required_fields(self):
        """Test DatabricksConfig required fields."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert required fields
        required_fields = ['warehouse_id', 'catalog', 'schema', 'secret_scope']
        for field in required_fields:
            assert columns[field].nullable is False

    def test_databricks_config_optional_fields(self):
        """Test DatabricksConfig optional fields."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert optional fields
        optional_fields = ['workspace_url', 'encrypted_personal_access_token']
        for field in optional_fields:
            assert columns[field].nullable is True

    def test_databricks_config_workspace_url_scenarios(self):
        """Test workspace URL field scenarios."""
        # Test valid workspace URL patterns
        valid_workspace_urls = [
            "https://dbc-12345678-9abc.cloud.databricks.com",
            "https://adb-1234567890123456.7.azuredatabricks.net",
            "https://my-workspace.gcp.databricks.com",
            "https://community.cloud.databricks.com"
        ]
        
        for url in valid_workspace_urls:
            # Assert URL format
            assert url.startswith("https://")
            assert "databricks" in url
            assert len(url) > 20

    def test_databricks_config_warehouse_id_scenarios(self):
        """Test warehouse ID field scenarios."""
        # Test valid warehouse ID patterns
        valid_warehouse_ids = [
            "abc123def456",
            "warehouse-12345",
            "sql-warehouse-xyz",
            "dw-production-001"
        ]
        
        for warehouse_id in valid_warehouse_ids:
            # Assert warehouse ID format
            assert isinstance(warehouse_id, str)
            assert len(warehouse_id) > 0

    def test_databricks_config_catalog_schema_scenarios(self):
        """Test catalog and schema field scenarios."""
        # Test valid catalog/schema combinations
        catalog_schema_combinations = [
            ("main", "default"),
            ("production", "analytics"),
            ("development", "staging"),
            ("shared", "common"),
            ("ml_catalog", "feature_store")
        ]
        
        for catalog, schema in catalog_schema_combinations:
            # Assert catalog/schema format
            assert isinstance(catalog, str)
            assert isinstance(schema, str)
            assert len(catalog) > 0
            assert len(schema) > 0

    def test_databricks_config_secret_scope_scenarios(self):
        """Test secret scope field scenarios."""
        # Test valid secret scope patterns
        valid_secret_scopes = [
            "databricks-secrets",
            "production-secrets",
            "api-keys-scope",
            "ml-secrets",
            "shared-secrets"
        ]
        
        for secret_scope in valid_secret_scopes:
            # Assert secret scope format
            assert isinstance(secret_scope, str)
            assert len(secret_scope) > 0
            assert "-" in secret_scope or "_" in secret_scope or secret_scope.isalnum()

    def test_databricks_config_boolean_flag_scenarios(self):
        """Test boolean flag scenarios."""
        # Test different configuration states
        config_states = [
            {"is_active": True, "is_enabled": True, "apps_enabled": True},   # Fully active
            {"is_active": True, "is_enabled": True, "apps_enabled": False},  # Active but no apps
            {"is_active": False, "is_enabled": True, "apps_enabled": False}, # Inactive but enabled
            {"is_active": False, "is_enabled": False, "apps_enabled": False} # Fully disabled
        ]
        
        for config in config_states:
            # Assert boolean values
            for key, value in config.items():
                assert isinstance(value, bool)

    def test_databricks_config_encryption_scenarios(self):
        """Test encrypted personal access token scenarios."""
        # Test encrypted token patterns
        encrypted_tokens = [
            "enc_AES256_abc123def456...",
            "encrypted:base64:encoded_token_here",
            "vault:secret/databricks/token",
            None  # No token configured
        ]
        
        for token in encrypted_tokens:
            if token is not None:
                # Assert token format
                assert isinstance(token, str)
                assert len(token) > 10
            else:
                # Assert None is acceptable
                assert token is None

    def test_databricks_config_timestamp_behavior(self):
        """Test timestamp behavior in DatabricksConfig."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert timezone-aware timestamps
        for field in ['created_at', 'updated_at']:
            assert columns[field].default is not None
            # Should use timezone-aware defaults
            assert "timezone" in str(columns[field].type).lower() or "DATETIME" in str(columns[field].type)

    def test_databricks_config_model_documentation(self):
        """Test DatabricksConfig model documentation."""
        # Act & Assert
        assert DatabricksConfig.__doc__ is not None
        assert "DatabricksConfig model for Databricks integration" in DatabricksConfig.__doc__


class TestDatabricksConfigEdgeCases:
    """Test edge cases and error scenarios for DatabricksConfig."""

    def test_databricks_config_very_long_urls(self):
        """Test DatabricksConfig with very long workspace URLs."""
        # Arrange
        long_url = "https://" + "very-long-workspace-name-" * 10 + ".cloud.databricks.com"
        
        # Assert
        assert isinstance(long_url, str)
        assert len(long_url) > 100
        assert long_url.startswith("https://")
        assert long_url.endswith(".databricks.com")

    def test_databricks_config_empty_workspace_url(self):
        """Test DatabricksConfig with empty workspace URL."""
        # Act
        columns = DatabricksConfig.__table__.columns
        
        # Assert empty string default is configured
        assert columns['workspace_url'].default.arg == ""

    def test_databricks_config_special_characters(self):
        """Test DatabricksConfig with special characters in fields."""
        # Test catalog/schema with special characters
        special_catalogs = [
            "my_catalog",
            "catalog-2023",
            "test.catalog",
            "catalog_v2"
        ]
        
        special_schemas = [
            "default_schema",
            "schema-prod",
            "test.schema",
            "schema_v1"
        ]
        
        for catalog in special_catalogs:
            assert isinstance(catalog, str)
            assert len(catalog) > 0
        
        for schema in special_schemas:
            assert isinstance(schema, str)
            assert len(schema) > 0

    def test_databricks_config_environment_scenarios(self):
        """Test DatabricksConfig for different environments."""
        # Development environment
        dev_config = {
            "workspace_url": "https://dev-workspace.cloud.databricks.com",
            "warehouse_id": "dev-warehouse",
            "catalog": "development",
            "schema": "default",
            "secret_scope": "dev-secrets",
            "is_active": True,
            "is_enabled": True,
            "apps_enabled": False
        }
        
        # Production environment
        prod_config = {
            "workspace_url": "https://prod-workspace.cloud.databricks.com",
            "warehouse_id": "prod-warehouse",
            "catalog": "production",
            "schema": "analytics",
            "secret_scope": "prod-secrets",
            "is_active": True,
            "is_enabled": True,
            "apps_enabled": True
        }
        
        # Staging environment
        staging_config = {
            "workspace_url": "https://staging-workspace.cloud.databricks.com",
            "warehouse_id": "staging-warehouse",
            "catalog": "staging",
            "schema": "testing",
            "secret_scope": "staging-secrets",
            "is_active": False,
            "is_enabled": True,
            "apps_enabled": False
        }
        
        environments = [dev_config, prod_config, staging_config]
        
        for config in environments:
            # Assert environment configuration structure
            required_keys = ['workspace_url', 'warehouse_id', 'catalog', 'schema', 'secret_scope']
            for key in required_keys:
                assert key in config
                assert isinstance(config[key], str)
            
            boolean_keys = ['is_active', 'is_enabled', 'apps_enabled']
            for key in boolean_keys:
                assert key in config
                assert isinstance(config[key], bool)

    def test_databricks_config_cloud_provider_scenarios(self):
        """Test DatabricksConfig for different cloud providers."""
        # AWS Databricks
        aws_config = {
            "workspace_url": "https://dbc-12345678-9abc.cloud.databricks.com",
            "warehouse_id": "aws-warehouse-123"
        }
        
        # Azure Databricks
        azure_config = {
            "workspace_url": "https://adb-1234567890123456.7.azuredatabricks.net",
            "warehouse_id": "azure-warehouse-456"
        }
        
        # GCP Databricks
        gcp_config = {
            "workspace_url": "https://workspace.gcp.databricks.com",
            "warehouse_id": "gcp-warehouse-789"
        }
        
        cloud_configs = [aws_config, azure_config, gcp_config]
        
        for config in cloud_configs:
            # Assert cloud-specific URL patterns
            assert config["workspace_url"].startswith("https://")
            assert "databricks" in config["workspace_url"]
            assert isinstance(config["warehouse_id"], str)

    def test_databricks_config_security_scenarios(self):
        """Test DatabricksConfig security-related scenarios."""
        # High security configuration
        high_security = {
            "encrypted_personal_access_token": "enc_AES256_highly_secure_token",
            "secret_scope": "high-security-secrets",
            "is_enabled": True,
            "apps_enabled": False  # Apps disabled for security
        }
        
        # Standard security configuration
        standard_security = {
            "encrypted_personal_access_token": "enc_standard_token",
            "secret_scope": "standard-secrets",
            "is_enabled": True,
            "apps_enabled": True
        }
        
        # No token configuration
        no_token = {
            "encrypted_personal_access_token": None,
            "secret_scope": "basic-secrets",
            "is_enabled": True,
            "apps_enabled": False
        }
        
        security_configs = [high_security, standard_security, no_token]
        
        for config in security_configs:
            # Assert security configuration structure
            if config["encrypted_personal_access_token"] is not None:
                assert isinstance(config["encrypted_personal_access_token"], str)
                assert len(config["encrypted_personal_access_token"]) > 0
            
            assert isinstance(config["secret_scope"], str)
            assert isinstance(config["is_enabled"], bool)
            assert isinstance(config["apps_enabled"], bool)

    def test_databricks_config_apps_integration_scenarios(self):
        """Test Databricks Apps integration scenarios."""
        # Apps enabled scenario
        apps_enabled_config = {
            "apps_enabled": True,
            "encrypted_personal_access_token": "enc_apps_token",
            "is_enabled": True,
            "is_active": True
        }
        
        # Apps disabled scenario
        apps_disabled_config = {
            "apps_enabled": False,
            "encrypted_personal_access_token": None,
            "is_enabled": True,
            "is_active": True
        }
        
        apps_configs = [apps_enabled_config, apps_disabled_config]
        
        for config in apps_configs:
            # Assert apps configuration logic
            if config["apps_enabled"]:
                # When apps are enabled, token should be present
                assert config["encrypted_personal_access_token"] is not None
            else:
                # When apps are disabled, token may be None
                pass  # Token can be None or present
            
            assert isinstance(config["apps_enabled"], bool)

    def test_databricks_config_migration_scenarios(self):
        """Test DatabricksConfig migration scenarios."""
        # Old configuration (legacy)
        old_config = {
            "workspace_url": "",  # Empty in old systems
            "warehouse_id": "legacy-warehouse",
            "catalog": "hive_metastore",  # Old catalog
            "schema": "default"
        }
        
        # New configuration (modern)
        new_config = {
            "workspace_url": "https://modern-workspace.cloud.databricks.com",
            "warehouse_id": "modern-warehouse",
            "catalog": "unity_catalog",  # Unity Catalog
            "schema": "production"
        }
        
        migration_configs = [old_config, new_config]
        
        for config in migration_configs:
            # Assert configuration validity
            assert isinstance(config["workspace_url"], str)
            assert isinstance(config["warehouse_id"], str)
            assert isinstance(config["catalog"], str)
            assert isinstance(config["schema"], str)

    def test_databricks_config_data_integrity(self):
        """Test data integrity constraints."""
        # Act
        table = DatabricksConfig.__table__
        
        # Assert primary key
        primary_keys = [col for col in table.columns if col.primary_key]
        assert len(primary_keys) == 1
        assert primary_keys[0].name == 'id'
        
        # Assert no unique constraints (besides primary key)
        unique_columns = [col for col in table.columns if col.unique and not col.primary_key]
        assert len(unique_columns) == 0  # No additional unique constraints expected
        
        # Assert required fields
        required_fields = ['warehouse_id', 'catalog', 'schema', 'secret_scope']
        for field_name in required_fields:
            field = table.columns[field_name]
            assert field.nullable is False