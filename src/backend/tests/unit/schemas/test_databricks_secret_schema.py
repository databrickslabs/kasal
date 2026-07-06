"""
Unit tests for databricks_secret schemas.

Tests the functionality of Pydantic schemas for Databricks secret management
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError

from src.schemas.databricks_secret import (
    SecretBase, SecretCreate, SecretUpdate, SecretResponse, DatabricksTokenRequest
)


class TestSecretBase:
    """Test cases for SecretBase schema."""
    
    def test_valid_secret_base_minimal(self):
        """Test SecretBase with minimal required fields."""
        secret_data = {"name": "test_secret"}
        secret = SecretBase(**secret_data)
        assert secret.name == "test_secret"
        assert secret.description == ""
    
    def test_valid_secret_base_with_description(self):
        """Test SecretBase with description."""
        secret_data = {
            "name": "database_password",
            "description": "Password for production database"
        }
        secret = SecretBase(**secret_data)
        assert secret.name == "database_password"
        assert secret.description == "Password for production database"
    
    def test_secret_base_missing_name(self):
        """Test SecretBase validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            SecretBase(description="Missing name")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_secret_base_empty_name(self):
        """Test SecretBase with empty name."""
        secret_data = {"name": ""}
        secret = SecretBase(**secret_data)
        assert secret.name == ""
        assert secret.description == ""
    
    def test_secret_base_none_description(self):
        """Test SecretBase with None description."""
        secret_data = {"name": "test_secret", "description": None}
        secret = SecretBase(**secret_data)
        assert secret.name == "test_secret"
        assert secret.description is None
    
    def test_secret_base_long_name_and_description(self):
        """Test SecretBase with long name and description."""
        long_name = "a" * 1000
        long_description = "b" * 2000
        secret_data = {
            "name": long_name,
            "description": long_description
        }
        secret = SecretBase(**secret_data)
        assert secret.name == long_name
        assert secret.description == long_description
    
    def test_secret_base_special_characters(self):
        """Test SecretBase with special characters."""
        secret_data = {
            "name": "api_key_v2.1_prod-env",
            "description": "API key for v2.1 production environment (high-security)"
        }
        secret = SecretBase(**secret_data)
        assert secret.name == "api_key_v2.1_prod-env"
        assert secret.description == "API key for v2.1 production environment (high-security)"


class TestSecretCreate:
    """Test cases for SecretCreate schema."""
    
    def test_valid_secret_create_minimal(self):
        """Test SecretCreate with minimal required fields."""
        create_data = {
            "name": "new_secret",
            "value": "secret_value_123"
        }
        create_secret = SecretCreate(**create_data)
        assert create_secret.name == "new_secret"
        assert create_secret.value == "secret_value_123"
        assert create_secret.description == ""
    
    def test_valid_secret_create_full(self):
        """Test SecretCreate with all fields."""
        create_data = {
            "name": "database_connection",
            "value": "postgresql://user" ":pass@host:5432/db",
            "description": "Connection string for main database"
        }
        create_secret = SecretCreate(**create_data)
        assert create_secret.name == "database_connection"
        assert create_secret.value == "postgresql://user" ":pass@host:5432/db"
        assert create_secret.description == "Connection string for main database"
    
    def test_secret_create_inheritance(self):
        """Test that SecretCreate inherits from SecretBase."""
        create_data = {
            "name": "inherited_secret",
            "value": "inherited_value",
            "description": "Testing inheritance"
        }
        create_secret = SecretCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_secret, 'name')
        assert hasattr(create_secret, 'description')
        
        # Should have create-specific attributes
        assert hasattr(create_secret, 'value')
        
        # Should behave like base class
        assert create_secret.name == "inherited_secret"
        assert create_secret.description == "Testing inheritance"
        assert create_secret.value == "inherited_value"
    
    def test_secret_create_missing_value(self):
        """Test SecretCreate validation with missing value."""
        with pytest.raises(ValidationError) as exc_info:
            SecretCreate(name="test_secret")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "value" in missing_fields
    
    def test_secret_create_missing_name(self):
        """Test SecretCreate validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            SecretCreate(value="secret_value")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_secret_create_empty_value(self):
        """Test SecretCreate with empty value."""
        create_data = {
            "name": "empty_secret",
            "value": ""
        }
        create_secret = SecretCreate(**create_data)
        assert create_secret.name == "empty_secret"
        assert create_secret.value == ""
    
    def test_secret_create_sensitive_values(self):
        """Test SecretCreate with various sensitive value types."""
        sensitive_values = [
            "password123",
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",  # gitleaks:allow
            "sk-1234567890abcdef1234567890abcdef",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:MySecret-a1b2c3",
            "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC..."  # gitleaks:allow
        ]
        
        for i, value in enumerate(sensitive_values):
            create_data = {
                "name": f"sensitive_secret_{i}",
                "value": value,
                "description": f"Test sensitive value {i}"
            }
            create_secret = SecretCreate(**create_data)
            assert create_secret.value == value
            assert create_secret.name == f"sensitive_secret_{i}"


class TestSecretUpdate:
    """Test cases for SecretUpdate schema."""
    
    def test_valid_secret_update_minimal(self):
        """Test SecretUpdate with minimal required fields."""
        update_data = {"value": "updated_secret_value"}
        update_secret = SecretUpdate(**update_data)
        assert update_secret.value == "updated_secret_value"
        assert update_secret.description == ""
    
    def test_valid_secret_update_full(self):
        """Test SecretUpdate with all fields."""
        update_data = {
            "value": "new_connection_string",
            "description": "Updated connection string with new credentials"
        }
        update_secret = SecretUpdate(**update_data)
        assert update_secret.value == "new_connection_string"
        assert update_secret.description == "Updated connection string with new credentials"
    
    def test_secret_update_missing_value(self):
        """Test SecretUpdate validation with missing value."""
        with pytest.raises(ValidationError) as exc_info:
            SecretUpdate(description="Only description")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "value" in missing_fields
    
    def test_secret_update_empty_value(self):
        """Test SecretUpdate with empty value."""
        update_data = {"value": ""}
        update_secret = SecretUpdate(**update_data)
        assert update_secret.value == ""
        assert update_secret.description == ""
    
    def test_secret_update_none_description(self):
        """Test SecretUpdate with None description."""
        update_data = {
            "value": "test_value",
            "description": None
        }
        update_secret = SecretUpdate(**update_data)
        assert update_secret.value == "test_value"
        assert update_secret.description is None
    
    def test_secret_update_value_change_scenarios(self):
        """Test SecretUpdate with different value change scenarios."""
        scenarios = [
            {
                "name": "password_rotation",
                "old_value": "old_password_123",
                "new_value": "new_password_456",
                "description": "Rotated password for security"
            },
            {
                "name": "token_refresh",
                "old_value": "expired_token_abc",
                "new_value": "fresh_token_xyz",
                "description": "Refreshed API token"
            },
            {
                "name": "key_upgrade",
                "old_value": "rsa_key_2048",
                "new_value": "rsa_key_4096",
                "description": "Upgraded to stronger encryption key"
            }
        ]
        
        for scenario in scenarios:
            update_data = {
                "value": scenario["new_value"],
                "description": scenario["description"]
            }
            update_secret = SecretUpdate(**update_data)
            assert update_secret.value == scenario["new_value"]
            assert update_secret.description == scenario["description"]


class TestSecretResponse:
    """Test cases for SecretResponse schema."""
    
    def test_valid_secret_response_minimal(self):
        """Test SecretResponse with all required fields."""
        response_data = {
            "id": 1,
            "name": "test_secret",
            "value": "secret_value",
            "description": "Test secret",
            "scope": "default"
        }
        response = SecretResponse(**response_data)
        assert response.id == 1
        assert response.name == "test_secret"
        assert response.value == "secret_value"
        assert response.description == "Test secret"
        assert response.scope == "default"
        assert response.source == "databricks"  # Default value
    
    def test_valid_secret_response_full(self):
        """Test SecretResponse with all fields including source."""
        response_data = {
            "id": 42,
            "name": "production_db_password",
            "value": "super_secure_password_123",
            "description": "Production database password",
            "scope": "production",
            "source": "sqlite"
        }
        response = SecretResponse(**response_data)
        assert response.id == 42
        assert response.name == "production_db_password"
        assert response.value == "super_secure_password_123"
        assert response.description == "Production database password"
        assert response.scope == "production"
        assert response.source == "sqlite"
    
    def test_secret_response_missing_required_fields(self):
        """Test SecretResponse validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            SecretResponse(id=1, name="test")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        required_fields = {"value", "description", "scope"}
        assert required_fields.intersection(set(missing_fields)) == required_fields
    
    def test_secret_response_id_conversion(self):
        """Test SecretResponse with different ID types."""
        response_data = {
            "id": "123",  # String that can be converted to int
            "name": "converted_id_secret",
            "value": "test_value",
            "description": "Test ID conversion",
            "scope": "test"
        }
        response = SecretResponse(**response_data)
        assert response.id == 123
        assert isinstance(response.id, int)
    
    def test_secret_response_source_values(self):
        """Test SecretResponse with different source values."""
        sources = ["databricks", "sqlite", "azure", "aws", "gcp"]
        
        for i, source in enumerate(sources):
            response_data = {
                "id": i + 1,
                "name": f"secret_{source}",
                "value": f"value_{source}",
                "description": f"Secret from {source}",
                "scope": "test",
                "source": source
            }
            response = SecretResponse(**response_data)
            assert response.source == source
            assert response.name == f"secret_{source}"
    
    def test_secret_response_scope_scenarios(self):
        """Test SecretResponse with different scope scenarios."""
        scopes = ["default", "production", "staging", "development", "test", "user-specific"]
        
        for i, scope in enumerate(scopes):
            response_data = {
                "id": i + 1,
                "name": f"secret_for_{scope}",
                "value": f"value_for_{scope}",
                "description": f"Secret for {scope} environment",
                "scope": scope
            }
            response = SecretResponse(**response_data)
            assert response.scope == scope
            assert response.description == f"Secret for {scope} environment"
    
    def test_secret_response_empty_strings(self):
        """Test SecretResponse with empty strings."""
        response_data = {
            "id": 1,
            "name": "",
            "value": "",
            "description": "",
            "scope": ""
        }
        response = SecretResponse(**response_data)
        assert response.name == ""
        assert response.value == ""
        assert response.description == ""
        assert response.scope == ""


class TestDatabricksTokenRequest:
    """Test cases for DatabricksTokenRequest schema."""
    
    def test_valid_databricks_token_request(self):
        """Test DatabricksTokenRequest with valid data."""
        request_data = {
            "workspace_url": "https://dbc-12345-abcde.cloud.databricks.com",
            "token": "fake_dapi_token_for_testing"  # gitleaks:allow
        }
        request = DatabricksTokenRequest(**request_data)
        assert request.workspace_url == "https://dbc-12345-abcde.cloud.databricks.com"
        assert request.token == "fake_dapi_token_for_testing"  # gitleaks:allow
    
    def test_databricks_token_request_missing_fields(self):
        """Test DatabricksTokenRequest validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksTokenRequest(workspace_url="https://example.databricks.com")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "token" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            DatabricksTokenRequest(token="test_token")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "workspace_url" in missing_fields
    
    def test_databricks_token_request_url_formats(self):
        """Test DatabricksTokenRequest with various URL formats."""
        url_formats = [
            "https://dbc-12345-abcde.cloud.databricks.com",
            "https://company.cloud.databricks.com", 
            "https://workspace.databricks.com",
            "https://adb-1234567890123456.7.azuredatabricks.net",
            "https://databricks-workspace.amazonaws.com"
        ]
        
        for url in url_formats:
            request_data = {
                "workspace_url": url,
                "token": "test_token_123"
            }
            request = DatabricksTokenRequest(**request_data)
            assert request.workspace_url == url
            assert request.token == "test_token_123"
    
    def test_databricks_token_request_token_formats(self):
        """Test DatabricksTokenRequest with various token formats."""
        token_formats = [
            "fake_dapi_token_for_testing",  # gitleaks:allow
            "dapi-1a2b3c4d-5e6f-7890-abcd-ef1234567890",
            "personal_access_token_xyz",
            "service_principal_token_abc",
        ]
        
        for token in token_formats:
            request_data = {
                "workspace_url": "https://test.databricks.com",
                "token": token
            }
            request = DatabricksTokenRequest(**request_data)
            assert request.token == token
            assert request.workspace_url == "https://test.databricks.com"
    
    def test_databricks_token_request_empty_values(self):
        """Test DatabricksTokenRequest with empty values."""
        request_data = {
            "workspace_url": "",
            "token": ""
        }
        request = DatabricksTokenRequest(**request_data)
        assert request.workspace_url == ""
        assert request.token == ""


class TestSchemaIntegration:
    """Integration tests for databricks_secret schema interactions."""
    
    def test_secret_lifecycle_workflow(self):
        """Test complete secret lifecycle workflow."""
        # Create secret
        create_data = {
            "name": "api_key",
            "value": "sk-1234567890abcdef",
            "description": "API key for external service"
        }
        create_request = SecretCreate(**create_data)
        
        # Update secret
        update_data = {
            "value": "sk-newkey567890abcdef",
            "description": "Updated API key after rotation"
        }
        update_request = SecretUpdate(**update_data)
        
        # Secret response (simulating what would come from database)
        response_data = {
            "id": 1,
            "name": create_request.name,  # Original name
            "value": update_data["value"],  # Updated value
            "description": update_data["description"],  # Updated description
            "scope": "production",
            "source": "databricks"
        }
        secret_response = SecretResponse(**response_data)
        
        # Verify workflow
        assert create_request.name == "api_key"
        assert create_request.value == "sk-1234567890abcdef"
        assert update_request.value == "sk-newkey567890abcdef"
        assert secret_response.id == 1
        assert secret_response.name == "api_key"  # From creation
        assert secret_response.value == "sk-newkey567890abcdef"  # From update
        assert secret_response.description == "Updated API key after rotation"  # From update
        assert secret_response.scope == "production"
        assert secret_response.source == "databricks"
    
    def test_databricks_integration_workflow(self):
        """Test Databricks integration workflow."""
        # Token request for authentication
        token_request = DatabricksTokenRequest(
            workspace_url="https://company.cloud.databricks.com",
            token="fake_dapi_token_for_testing"  # gitleaks:allow
        )
        
        # Create secret in Databricks
        secret_create = SecretCreate(
            name="database_password",
            value="super_secure_db_password",
            description="Production database password"
        )
        
        # Response from Databricks secret creation
        databricks_response = SecretResponse(
            id=100,
            name=secret_create.name,
            value=secret_create.value,
            description=secret_create.description,
            scope="production",
            source="databricks"
        )
        
        # Verify integration workflow
        assert token_request.workspace_url == "https://company.cloud.databricks.com"
        assert secret_create.name == "database_password"
        assert databricks_response.source == "databricks"
        assert databricks_response.scope == "production"
        assert databricks_response.name == secret_create.name
        assert databricks_response.value == secret_create.value
    
    def test_multi_source_secret_management(self):
        """Test managing secrets from multiple sources."""
        # Databricks secret
        databricks_secret = SecretResponse(
            id=1,
            name="databricks_api_key",
            value="dapi_key_123",
            description="Databricks API key",
            scope="default",
            source="databricks"
        )
        
        # SQLite secret
        sqlite_secret = SecretResponse(
            id=2,
            name="local_db_password",
            value="local_password_456",
            description="Local database password",
            scope="development",
            source="sqlite"
        )
        
        # Cloud secret
        cloud_secret = SecretResponse(
            id=3,
            name="cloud_storage_key",
            value="cloud_key_789",
            description="Cloud storage access key",
            scope="production",
            source="aws"
        )
        
        secrets = [databricks_secret, sqlite_secret, cloud_secret]
        
        # Verify different sources
        sources = [secret.source for secret in secrets]
        assert "databricks" in sources
        assert "sqlite" in sources
        assert "aws" in sources
        
        # Verify scopes
        scopes = [secret.scope for secret in secrets]
        assert "default" in scopes
        assert "development" in scopes
        assert "production" in scopes
        
        # Group by source
        by_source = {}
        for secret in secrets:
            if secret.source not in by_source:
                by_source[secret.source] = []
            by_source[secret.source].append(secret)
        
        assert len(by_source["databricks"]) == 1
        assert len(by_source["sqlite"]) == 1
        assert len(by_source["aws"]) == 1
    
    def test_secret_security_scenarios(self):
        """Test various security scenarios for secrets."""
        # High-security production secret
        prod_secret = SecretCreate(
            name="prod_master_key",
            value="-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...",
            description="Master encryption key for production environment"
        )
        
        # Service account token
        service_token = SecretCreate(
            name="service_account_token",
            value="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",  # gitleaks:allow
            description="JWT token for service-to-service authentication"
        )
        
        # Database connection string
        db_connection = SecretCreate(
            name="database_connection",
            value="postgresql://username" ":password@hostname:5432/database?sslmode=require",
            description="Secure database connection string"
        )
        
        # API key with rotation
        api_key_update = SecretUpdate(
            value="rotated_api_key_v2_secure",
            description="API key rotated for security compliance"
        )
        
        # Verify security-related secrets
        assert "BEGIN PRIVATE KEY" in prod_secret.value
        assert prod_secret.description.startswith("Master encryption key")
        assert service_token.value.startswith("eyJ")  # JWT format
        assert "postgresql://" in db_connection.value
        assert "sslmode=require" in db_connection.value
        assert "rotated" in api_key_update.description
        assert "secure" in api_key_update.value