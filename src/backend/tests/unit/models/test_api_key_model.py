"""
Unit tests for api_key model.

Tests the functionality of the ApiKey database model including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.models.api_key import ApiKey


class TestApiKey:
    """Test cases for ApiKey model."""

    def test_api_key_creation(self):
        """Test basic ApiKey model creation."""
        # Arrange
        name = "openai_api_key"
        encrypted_value = "encrypted_sk-1234567890abcdef"
        description = "OpenAI API key for GPT-4 access"
        
        # Act
        api_key = ApiKey(
            name=name,
            encrypted_value=encrypted_value,
            description=description
        )
        
        # Assert
        assert api_key.name == name
        assert api_key.encrypted_value == encrypted_value
        assert api_key.description == description

    def test_api_key_minimal_creation(self):
        """Test ApiKey model creation with minimal required fields."""
        # Arrange
        name = "minimal_key"
        encrypted_value = "encrypted_key_value"
        
        # Act
        api_key = ApiKey(
            name=name,
            encrypted_value=encrypted_value
        )
        
        # Assert
        assert api_key.name == name
        assert api_key.encrypted_value == encrypted_value
        assert api_key.description is None

    def test_api_key_with_timestamps(self):
        """Test ApiKey model with custom timestamps."""
        # Arrange
        name = "timestamp_key"
        encrypted_value = "encrypted_timestamp_value"
        created_at = datetime.utcnow()
        updated_at = datetime.utcnow()
        
        # Act
        api_key = ApiKey(
            name=name,
            encrypted_value=encrypted_value,
            created_at=created_at,
            updated_at=updated_at
        )
        
        # Assert
        assert api_key.created_at == created_at
        assert api_key.updated_at == updated_at

    def test_api_key_different_providers(self):
        """Test ApiKey model for different API providers."""
        # OpenAI API Key
        openai_key = ApiKey(
            name="openai_production",
            encrypted_value="encrypted_sk-openai123",
            description="OpenAI API key for production environment"
        )
        
        # Anthropic API Key
        anthropic_key = ApiKey(
            name="anthropic_claude",
            encrypted_value="encrypted_sk-ant-api03",
            description="Anthropic Claude API key"
        )
        
        # Google API Key
        google_key = ApiKey(
            name="google_palm",
            encrypted_value="encrypted_AIza123",
            description="Google PaLM API key"
        )
        
        # Custom API Key
        custom_key = ApiKey(
            name="custom_llm_service",
            encrypted_value="encrypted_custom_token",
            description="Custom LLM service API key"
        )
        
        # Assert
        assert openai_key.name == "openai_production"
        assert "OpenAI" in openai_key.description
        
        assert anthropic_key.name == "anthropic_claude"
        assert "Anthropic" in anthropic_key.description
        
        assert google_key.name == "google_palm"
        assert "Google" in google_key.description
        
        assert custom_key.name == "custom_llm_service"
        assert "Custom" in custom_key.description

    def test_api_key_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert ApiKey.__tablename__ == "apikey"

    def test_api_key_primary_key(self):
        """Test that primary key is correctly configured."""
        # Act
        api_key = ApiKey(
            name="pk_test",
            encrypted_value="encrypted_value"
        )
        
        # Assert
        id_column = ApiKey.__table__.columns['id']
        assert id_column.primary_key is True
        assert id_column.index is True
        assert "INTEGER" in str(id_column.type)

    def test_api_key_unique_constraints(self):
        """Test that name has unique constraint."""
        # Act
        columns = ApiKey.__table__.columns
        
        # Assert
        assert columns['name'].unique is True
        assert columns['name'].index is True
        assert columns['name'].nullable is False

    def test_api_key_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = ApiKey.__table__.columns
        
        # Assert
        assert columns['id'].index is True
        assert columns['name'].index is True

    def test_api_key_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = ApiKey.__table__.columns
        
        # Assert
        # Primary key
        assert "INTEGER" in str(columns['id'].type)
        assert columns['id'].primary_key is True
        
        # String columns
        assert "VARCHAR" in str(columns['name'].type) or "STRING" in str(columns['name'].type)
        assert "VARCHAR" in str(columns['encrypted_value'].type) or "STRING" in str(columns['encrypted_value'].type)
        assert "VARCHAR" in str(columns['description'].type) or "STRING" in str(columns['description'].type)
        
        # DateTime columns
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)
        
        # Nullable constraints
        assert columns['name'].nullable is False
        assert columns['encrypted_value'].nullable is False
        assert columns['description'].nullable is True

    def test_api_key_timestamp_defaults(self):
        """Test timestamp column defaults."""
        # Act
        columns = ApiKey.__table__.columns
        
        # Assert
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None

    def test_api_key_repr(self):
        """Test string representation of ApiKey model."""
        # Arrange
        api_key = ApiKey(
            name="repr_test",
            encrypted_value="encrypted_value"
        )
        
        # Act
        repr_str = repr(api_key)
        
        # Assert
        assert "ApiKey" in repr_str

    def test_api_key_with_long_description(self):
        """Test ApiKey with long description."""
        # Arrange
        name = "long_desc_key"
        encrypted_value = "encrypted_long_desc_value"
        long_description = """
        This is a very long description for an API key that contains
        detailed information about its purpose, usage restrictions,
        environment (production/staging/development), rotation schedule,
        access permissions, security considerations, and any other
        relevant metadata that might be useful for API key management.
        
        Key Details:
        - Environment: Production
        - Access Level: Full
        - Rotation: Monthly
        - Owner: DevOps Team
        - Last Rotated: 2023-01-15
        - Expires: 2024-01-15
        """
        
        # Act
        api_key = ApiKey(
            name=name,
            encrypted_value=encrypted_value,
            description=long_description
        )
        
        # Assert
        assert api_key.description == long_description
        assert len(api_key.description) > 500
        assert "Production" in api_key.description
        assert "DevOps Team" in api_key.description

    def test_api_key_encryption_patterns(self):
        """Test ApiKey with different encryption patterns."""
        # Base64 encoded key
        base64_key = ApiKey(
            name="base64_encoded",
            encrypted_value="ZW5jcnlwdGVkX2Jhc2U2NF9rZXk=",
            description="Base64 encoded encrypted key"
        )
        
        # Hex encoded key
        hex_key = ApiKey(
            name="hex_encoded",
            encrypted_value="656e6372797074656448657856616c7565",
            description="Hex encoded encrypted key"
        )
        
        # JWT-like structure
        jwt_like_key = ApiKey(
            name="jwt_structure",
            encrypted_value="encrypted.header.payload.signature",
            description="JWT-like structured encrypted key"
        )
        
        # Custom format
        custom_key = ApiKey(
            name="custom_format",
            encrypted_value="ENC[AES256]:IV[abcd1234]:DATA[encrypted_content]",
            description="Custom encryption format"
        )
        
        # Assert
        assert base64_key.encrypted_value.endswith("=")
        assert len(hex_key.encrypted_value) % 2 == 0  # Hex strings have even length
        assert jwt_like_key.encrypted_value.count(".") == 3
        assert custom_key.encrypted_value.startswith("ENC[")

    def test_api_key_environment_variations(self):
        """Test ApiKey for different environments."""
        # Development environment keys
        dev_openai = ApiKey(
            name="openai_dev",
            encrypted_value="encrypted_dev_openai_key",
            description="OpenAI API key for development environment"
        )
        
        # Staging environment keys
        staging_anthropic = ApiKey(
            name="anthropic_staging",
            encrypted_value="encrypted_staging_anthropic_key",
            description="Anthropic API key for staging environment"
        )
        
        # Production environment keys
        prod_google = ApiKey(
            name="google_prod",
            encrypted_value="encrypted_prod_google_key",
            description="Google API key for production environment"
        )
        
        # Assert
        assert "dev" in dev_openai.name
        assert "development" in dev_openai.description
        
        assert "staging" in staging_anthropic.name
        assert "staging" in staging_anthropic.description
        
        assert "prod" in prod_google.name
        assert "production" in prod_google.description

    def test_api_key_special_characters_in_name(self):
        """Test ApiKey with special characters in name."""
        # Underscore
        underscore_key = ApiKey(
            name="api_key_with_underscores",
            encrypted_value="encrypted_underscore_value"
        )
        
        # Hyphens
        hyphen_key = ApiKey(
            name="api-key-with-hyphens",
            encrypted_value="encrypted_hyphen_value"
        )
        
        # Numbers
        number_key = ApiKey(
            name="api_key_v2_2023",
            encrypted_value="encrypted_number_value"
        )
        
        # Mixed case
        mixed_case_key = ApiKey(
            name="OpenAI_GPT4_ProductionKey",
            encrypted_value="encrypted_mixed_case_value"
        )
        
        # Assert
        assert "_" in underscore_key.name
        assert "-" in hyphen_key.name
        assert "2023" in number_key.name
        assert "OpenAI" in mixed_case_key.name
        assert "GPT4" in mixed_case_key.name

    def test_api_key_empty_description(self):
        """Test ApiKey with empty description."""
        # Act
        api_key = ApiKey(
            name="no_description_key",
            encrypted_value="encrypted_no_desc_value",
            description=""
        )
        
        # Assert
        assert api_key.description == ""

    def test_api_key_none_description(self):
        """Test ApiKey with None description."""
        # Act
        api_key = ApiKey(
            name="none_description_key",
            encrypted_value="encrypted_none_desc_value",
            description=None
        )
        
        # Assert
        assert api_key.description is None


class TestApiKeyEdgeCases:
    """Test edge cases and error scenarios for ApiKey."""

    def test_api_key_very_long_name(self):
        """Test ApiKey with very long name."""
        # Arrange
        long_name = "very_long_api_key_name_" * 10  # 230 characters
        
        # Act
        api_key = ApiKey(
            name=long_name,
            encrypted_value="encrypted_long_name_value"
        )
        
        # Assert
        assert api_key.name == long_name
        assert len(api_key.name) == 230

    def test_api_key_very_long_encrypted_value(self):
        """Test ApiKey with very long encrypted value."""
        # Arrange
        long_encrypted_value = "encrypted_" + "a" * 1000  # Very long encrypted string
        
        # Act
        api_key = ApiKey(
            name="long_value_key",
            encrypted_value=long_encrypted_value
        )
        
        # Assert
        assert api_key.encrypted_value == long_encrypted_value
        assert len(api_key.encrypted_value) > 1000

    def test_api_key_minimum_required_fields(self):
        """Test ApiKey with only required fields."""
        # Act
        api_key = ApiKey(
            name="minimal",
            encrypted_value="encrypted_minimal"
        )
        
        # Assert
        assert api_key.name == "minimal"
        assert api_key.encrypted_value == "encrypted_minimal"
        assert api_key.description is None

    def test_api_key_common_naming_patterns(self):
        """Test common API key naming patterns."""
        # Environment-based naming
        env_keys = [
            ApiKey(name="openai_dev", encrypted_value="enc1"),
            ApiKey(name="openai_staging", encrypted_value="enc2"),
            ApiKey(name="openai_prod", encrypted_value="enc3")
        ]
        
        # Service-based naming
        service_keys = [
            ApiKey(name="llm_openai", encrypted_value="enc4"),
            ApiKey(name="llm_anthropic", encrypted_value="enc5"),
            ApiKey(name="llm_google", encrypted_value="enc6")
        ]
        
        # Version-based naming
        version_keys = [
            ApiKey(name="api_key_v1", encrypted_value="enc7"),
            ApiKey(name="api_key_v2", encrypted_value="enc8"),
            ApiKey(name="api_key_v3", encrypted_value="enc9")
        ]
        
        # Assert
        assert all("openai" in key.name for key in env_keys)
        assert all("llm" in key.name for key in service_keys)
        assert all("v" in key.name for key in version_keys)

    def test_api_key_timestamp_behavior(self):
        """Test timestamp behavior in ApiKey."""
        # Arrange
        before_creation = datetime.utcnow()
        
        # Act
        api_key = ApiKey(
            name="timestamp_test",
            encrypted_value="encrypted_timestamp_value"
        )
        
        after_creation = datetime.utcnow()
        
        # Assert
        # Note: created_at and updated_at are set by database defaults
        # Here we verify the column configurations
        created_at_column = ApiKey.__table__.columns['created_at']
        updated_at_column = ApiKey.__table__.columns['updated_at']
        
        assert created_at_column.default is not None
        assert updated_at_column.default is not None
        assert updated_at_column.onupdate is not None

    def test_api_key_security_considerations(self):
        """Test API key configurations for security scenarios."""
        # Read-only key
        readonly_key = ApiKey(
            name="readonly_key",
            encrypted_value="encrypted_readonly_value",
            description="Read-only access key with limited permissions"
        )
        
        # Admin key
        admin_key = ApiKey(
            name="admin_key",
            encrypted_value="encrypted_admin_value", 
            description="Administrative key with full permissions - use with caution"
        )
        
        # Temporary key
        temp_key = ApiKey(
            name="temp_key_24h",
            encrypted_value="encrypted_temp_value",
            description="Temporary key valid for 24 hours - auto-expires"
        )
        
        # Service key
        service_key = ApiKey(
            name="service_account_key",
            encrypted_value="encrypted_service_value",
            description="Service account key for automated processes"
        )
        
        # Assert
        assert "readonly" in readonly_key.name
        assert "Read-only" in readonly_key.description
        
        assert "admin" in admin_key.name
        assert "caution" in admin_key.description
        
        assert "temp" in temp_key.name
        assert "24 hours" in temp_key.description
        
        assert "service" in service_key.name
        assert "automated" in service_key.description

    def test_api_key_provider_specific_formats(self):
        """Test API key formats specific to different providers."""
        # OpenAI format (sk-...)
        openai_key = ApiKey(
            name="openai_sk_key",
            encrypted_value="encrypted_sk-1234567890abcdef1234567890abcdef",
            description="OpenAI secret key format"
        )
        
        # Anthropic format (sk-ant-...)
        anthropic_key = ApiKey(
            name="anthropic_ant_key",
            encrypted_value="encrypted_sk-ant-api03-1234567890abcdef",
            description="Anthropic API key format"
        )
        
        # Google format (AIza...)
        google_key = ApiKey(
            name="google_aiza_key",
            encrypted_value="encrypted_AIzaSyDaGmWKa4JsXZ-HjGw1c2k3n4m5v6b7",
            description="Google API key format"
        )
        
        # Azure format
        azure_key = ApiKey(
            name="azure_key",
            encrypted_value="encrypted_1234567890abcdef1234567890abcdef",
            description="Azure API key format"
        )
        
        # Assert
        assert "sk" in openai_key.encrypted_value
        assert "ant" in anthropic_key.encrypted_value
        assert "AIza" in google_key.encrypted_value
        assert len(azure_key.encrypted_value) > 32  # Azure keys are typically long