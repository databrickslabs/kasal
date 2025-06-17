"""
Unit tests for api_key schemas.

Tests the functionality of Pydantic schemas for API key operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError

from src.schemas.api_key import (
    ApiKeyBase, ApiKeyCreate, ApiKeyUpdate, ApiKeyResponse
)


class TestApiKeyBase:
    """Test cases for ApiKeyBase schema."""
    
    def test_valid_api_key_base_minimal(self):
        """Test ApiKeyBase with minimal required fields."""
        api_key_data = {"name": "Test API Key"}
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == "Test API Key"
        assert api_key.description is None
    
    def test_valid_api_key_base_full(self):
        """Test ApiKeyBase with all fields."""
        api_key_data = {
            "name": "Production API Key",
            "description": "API key for production environment"
        }
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == "Production API Key"
        assert api_key.description == "API key for production environment"
    
    def test_api_key_base_missing_name(self):
        """Test ApiKeyBase validation with missing name."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyBase()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_api_key_base_empty_name(self):
        """Test ApiKeyBase validation with empty name."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyBase(name="")
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_short" for error in errors)
    
    def test_api_key_base_whitespace_name(self):
        """Test ApiKeyBase validation with whitespace-only name."""
        # Pydantic allows whitespace in strings with min_length constraint
        # The constraint is on the string length, not trimmed length
        api_key_data = {"name": "   "}
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == "   "
    
    def test_api_key_base_single_char_name(self):
        """Test ApiKeyBase with single character name."""
        api_key_data = {"name": "A"}
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == "A"
    
    def test_api_key_base_long_name(self):
        """Test ApiKeyBase with very long name."""
        long_name = "A" * 1000
        api_key_data = {"name": long_name}
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == long_name
    
    def test_api_key_base_special_characters(self):
        """Test ApiKeyBase with special characters in name."""
        special_names = [
            "API Key #1",
            "test-api-key",
            "api_key_2023",
            "API Key (Production)",
            "测试API密钥",  # Unicode characters
            "🔑 API Key"  # Emoji
        ]
        
        for name in special_names:
            api_key_data = {"name": name}
            api_key = ApiKeyBase(**api_key_data)
            assert api_key.name == name
    
    def test_api_key_base_none_description(self):
        """Test ApiKeyBase with explicit None description."""
        api_key_data = {
            "name": "Test Key",
            "description": None
        }
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == "Test Key"
        assert api_key.description is None
    
    def test_api_key_base_empty_description(self):
        """Test ApiKeyBase with empty description."""
        api_key_data = {
            "name": "Test Key",
            "description": ""
        }
        api_key = ApiKeyBase(**api_key_data)
        assert api_key.name == "Test Key"
        assert api_key.description == ""


class TestApiKeyCreate:
    """Test cases for ApiKeyCreate schema."""
    
    def test_valid_api_key_create_minimal(self):
        """Test ApiKeyCreate with minimal required fields."""
        create_data = {
            "name": "New API Key",
            "value": "sk-1234567890abcdef"
        }
        api_key = ApiKeyCreate(**create_data)
        assert api_key.name == "New API Key"
        assert api_key.value == "sk-1234567890abcdef"
        assert api_key.description is None
    
    def test_valid_api_key_create_full(self):
        """Test ApiKeyCreate with all fields."""
        create_data = {
            "name": "Development API Key",
            "description": "API key for development environment",
            "value": "sk-dev1234567890abcdef"
        }
        api_key = ApiKeyCreate(**create_data)
        assert api_key.name == "Development API Key"
        assert api_key.description == "API key for development environment"
        assert api_key.value == "sk-dev1234567890abcdef"
    
    def test_api_key_create_inheritance(self):
        """Test that ApiKeyCreate inherits from ApiKeyBase."""
        create_data = {
            "name": "Inherited Key",
            "value": "sk-inherited123"
        }
        api_key = ApiKeyCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(api_key, 'name')
        assert hasattr(api_key, 'description')
        assert hasattr(api_key, 'value')
        
        # Should behave like base class
        assert api_key.name == "Inherited Key"
        assert api_key.description is None
        assert api_key.value == "sk-inherited123"
    
    def test_api_key_create_missing_fields(self):
        """Test ApiKeyCreate validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyCreate(name="Test Key")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "value" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyCreate(value="sk-test123")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_api_key_create_empty_value(self):
        """Test ApiKeyCreate validation with empty value."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyCreate(name="Test Key", value="")
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_short" for error in errors)
    
    def test_api_key_create_whitespace_value(self):
        """Test ApiKeyCreate validation with whitespace-only value."""
        # Pydantic allows whitespace in strings with min_length constraint
        api_key = ApiKeyCreate(name="Test Key", value="   ")
        assert api_key.name == "Test Key"
        assert api_key.value == "   "
    
    def test_api_key_create_various_key_formats(self):
        """Test ApiKeyCreate with various API key formats."""
        key_formats = [
            "sk-1234567890abcdef",  # OpenAI style
            "anthropic_key_12345",  # Anthropic style
            "Bearer token123",      # Bearer token
            "AIzaSyDummy_Key_123", # Google API style
            "xapp-1-dummy-key",    # Twitter style
            "ghp_1234567890abcdef", # GitHub style
            "pk_test_1234567890",  # Stripe style
            "rk_live_abcdef123456", # Random format  # gitleaks:allow
            "a",                   # Single character (minimum)
            "x" * 1000            # Very long key
        ]
        
        for key_value in key_formats:
            create_data = {
                "name": f"Test Key for {key_value[:10]}",
                "value": key_value
            }
            api_key = ApiKeyCreate(**create_data)
            assert api_key.value == key_value


class TestApiKeyUpdate:
    """Test cases for ApiKeyUpdate schema."""
    
    def test_valid_api_key_update_minimal(self):
        """Test ApiKeyUpdate with minimal required fields."""
        update_data = {"value": "sk-updated1234567890"}
        update = ApiKeyUpdate(**update_data)
        assert update.value == "sk-updated1234567890"
        assert update.description is None
    
    def test_valid_api_key_update_full(self):
        """Test ApiKeyUpdate with all fields."""
        update_data = {
            "value": "sk-updated1234567890abcdef",
            "description": "Updated API key description"
        }
        update = ApiKeyUpdate(**update_data)
        assert update.value == "sk-updated1234567890abcdef"
        assert update.description == "Updated API key description"
    
    def test_api_key_update_missing_value(self):
        """Test ApiKeyUpdate validation with missing value."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyUpdate()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "value" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyUpdate(description="Only description")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "value" in missing_fields
    
    def test_api_key_update_empty_value(self):
        """Test ApiKeyUpdate validation with empty value."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyUpdate(value="")
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_short" for error in errors)
    
    def test_api_key_update_none_description(self):
        """Test ApiKeyUpdate with explicit None description."""
        update_data = {
            "value": "sk-test123",
            "description": None
        }
        update = ApiKeyUpdate(**update_data)
        assert update.value == "sk-test123"
        assert update.description is None
    
    def test_api_key_update_empty_description(self):
        """Test ApiKeyUpdate with empty description."""
        update_data = {
            "value": "sk-test123",
            "description": ""
        }
        update = ApiKeyUpdate(**update_data)
        assert update.value == "sk-test123"
        assert update.description == ""
    
    def test_api_key_update_description_only_scenarios(self):
        """Test ApiKeyUpdate scenarios focusing on description changes."""
        test_descriptions = [
            "Short desc",
            "A very long description that spans multiple lines and contains detailed information about the API key usage",
            "Description with special chars: !@#$%^&*()",
            "描述包含中文字符",  # Chinese characters
            "🔑 Description with emoji",
            "",  # Empty string
            None  # Null value
        ]
        
        for desc in test_descriptions:
            update_data = {
                "value": "sk-constant123",
                "description": desc
            }
            update = ApiKeyUpdate(**update_data)
            assert update.value == "sk-constant123"
            assert update.description == desc


class TestApiKeyResponse:
    """Test cases for ApiKeyResponse schema."""
    
    def test_valid_api_key_response(self):
        """Test ApiKeyResponse with all required fields."""
        response_data = {
            "id": 1,
            "name": "Response API Key",
            "description": "API key response test",
            "value": "sk-response1234567890"
        }
        response = ApiKeyResponse(**response_data)
        assert response.id == 1
        assert response.name == "Response API Key"
        assert response.description == "API key response test"
        assert response.value == "sk-response1234567890"
    
    def test_api_key_response_inheritance(self):
        """Test that ApiKeyResponse inherits from ApiKeyBase."""
        response_data = {
            "id": 2,
            "name": "Inherited Response Key",
            "value": "sk-inherited-response123"
        }
        response = ApiKeyResponse(**response_data)
        
        # Should have all base class attributes
        assert hasattr(response, 'name')
        assert hasattr(response, 'description')
        assert hasattr(response, 'id')
        assert hasattr(response, 'value')
        
        # Should behave like base class
        assert response.name == "Inherited Response Key"
        assert response.description is None  # Default from base
        assert response.id == 2
        assert response.value == "sk-inherited-response123"
    
    def test_api_key_response_config(self):
        """Test ApiKeyResponse Config class."""
        assert hasattr(ApiKeyResponse, 'model_config')
        assert ApiKeyResponse.model_config.get('from_attributes') is True
    
    def test_api_key_response_missing_fields(self):
        """Test ApiKeyResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyResponse(name="Test Key", value="sk-test123")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyResponse(id=1, name="Test Key")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "value" in missing_fields
    
    def test_api_key_response_id_types(self):
        """Test ApiKeyResponse with different ID types."""
        # Integer ID
        response_data = {
            "id": 42,
            "name": "Integer ID Key",
            "value": "sk-int123"
        }
        response = ApiKeyResponse(**response_data)
        assert response.id == 42
        assert isinstance(response.id, int)
        
        # String that can be converted to int
        response_data = {
            "id": "123",
            "name": "String ID Key",
            "value": "sk-str123"
        }
        response = ApiKeyResponse(**response_data)
        assert response.id == 123
        assert isinstance(response.id, int)
        
        # Float that can be converted to int
        response_data = {
            "id": 99.0,
            "name": "Float ID Key",
            "value": "sk-float123"
        }
        response = ApiKeyResponse(**response_data)
        assert response.id == 99
        assert isinstance(response.id, int)
    
    def test_api_key_response_invalid_id_types(self):
        """Test ApiKeyResponse with invalid ID types."""
        invalid_ids = ["not_a_number", 99.5, None, [], {}]
        
        for invalid_id in invalid_ids:
            with pytest.raises(ValidationError):
                ApiKeyResponse(
                    id=invalid_id,
                    name="Invalid ID Key",
                    value="sk-invalid123"
                )
    
    def test_api_key_response_decrypted_values(self):
        """Test ApiKeyResponse with various decrypted value formats."""
        decrypted_values = [
            "sk-plaintext1234567890",
            "Bearer decrypted_token_123",
            "anthropic_decrypted_key_456",
            "fully_decrypted_api_key_789",
            "🔓 decrypted_key_with_emoji"
        ]
        
        for i, value in enumerate(decrypted_values):
            response_data = {
                "id": i + 1,
                "name": f"Decrypted Key {i + 1}",
                "description": f"Decrypted API key #{i + 1}",
                "value": value
            }
            response = ApiKeyResponse(**response_data)
            assert response.id == i + 1
            assert response.name == f"Decrypted Key {i + 1}"
            assert response.value == value


class TestSchemaIntegration:
    """Integration tests for API key schema interactions."""
    
    def test_api_key_lifecycle_workflow(self):
        """Test complete API key lifecycle workflow."""
        # Create API key
        create_data = {
            "name": "Lifecycle Test Key",
            "description": "Testing complete lifecycle",
            "value": "sk-lifecycle1234567890abcdef"
        }
        create_schema = ApiKeyCreate(**create_data)
        
        # Update API key
        update_data = {
            "value": "sk-updated-lifecycle1234567890abcdef",
            "description": "Updated lifecycle description"
        }
        update_schema = ApiKeyUpdate(**update_data)
        
        # Response (simulating what would come from database)
        response_data = {
            "id": 100,
            "name": create_schema.name,  # Original name
            "description": update_schema.description,  # Updated description
            "value": update_schema.value  # Updated value (decrypted)
        }
        response_schema = ApiKeyResponse(**response_data)
        
        # Verify the complete workflow
        assert create_schema.name == "Lifecycle Test Key"
        assert create_schema.value == "sk-lifecycle1234567890abcdef"
        assert create_schema.description == "Testing complete lifecycle"
        
        assert update_schema.value == "sk-updated-lifecycle1234567890abcdef"
        assert update_schema.description == "Updated lifecycle description"
        
        assert response_schema.id == 100
        assert response_schema.name == "Lifecycle Test Key"  # From creation
        assert response_schema.description == "Updated lifecycle description"  # From update
        assert response_schema.value == "sk-updated-lifecycle1234567890abcdef"  # From update
    
    def test_api_key_validation_scenarios(self):
        """Test various API key validation scenarios."""
        # Valid scenarios
        valid_scenarios = [
            {
                "name": "Production OpenAI",
                "value": "sk-prod1234567890abcdef1234567890abcdef",
                "description": "Production OpenAI API key"
            },
            {
                "name": "Dev Anthropic",
                "value": "anthropic_dev_key_abcdef123456",
                "description": None
            },
            {
                "name": "Test Google",
                "value": "AIzaSyTest_Key_123456789abcdef",
                "description": ""
            }
        ]
        
        for scenario in valid_scenarios:
            # Test creation
            create = ApiKeyCreate(**scenario)
            assert create.name == scenario["name"]
            assert create.value == scenario["value"]
            assert create.description == scenario["description"]
            
            # Test update (without name)
            update_data = {
                "value": scenario["value"],
                "description": scenario["description"]
            }
            update = ApiKeyUpdate(**update_data)
            assert update.value == scenario["value"]
            assert update.description == scenario["description"]
        
        # Invalid scenarios (only empty strings, not whitespace)
        invalid_scenarios = [
            {"name": "", "value": "sk-test123"},  # Empty name
            {"name": "Test", "value": ""},  # Empty value
        ]
        
        for scenario in invalid_scenarios:
            with pytest.raises(ValidationError):
                ApiKeyCreate(**scenario)
            
            # Update should also fail for empty values
            if scenario.get("value") == "":  # Only test empty strings
                with pytest.raises(ValidationError):
                    ApiKeyUpdate(value=scenario["value"])
    
    def test_api_key_edge_cases(self):
        """Test edge cases for API key schemas."""
        # Minimum valid inputs
        min_create = ApiKeyCreate(name="A", value="B")
        assert min_create.name == "A"
        assert min_create.value == "B"
        assert min_create.description is None
        
        min_update = ApiKeyUpdate(value="C")
        assert min_update.value == "C"
        assert min_update.description is None
        
        min_response = ApiKeyResponse(id=1, name="D", value="E")
        assert min_response.id == 1
        assert min_response.name == "D"
        assert min_response.value == "E"
        assert min_response.description is None
        
        # Maximum length scenarios (no explicit max, but test reasonable lengths)
        long_name = "A" * 1000
        long_value = "sk-" + "x" * 1000
        long_description = "Long description: " + "y" * 2000
        
        long_create = ApiKeyCreate(
            name=long_name,
            value=long_value,
            description=long_description
        )
        assert long_create.name == long_name
        assert long_create.value == long_value
        assert long_create.description == long_description
        
        # Unicode and special characters
        unicode_create = ApiKeyCreate(
            name="🔑 API Key 测试",
            value="sk-unicodé_key_123_éñ",
            description="Description with émojis 🚀 and ünïcödë"
        )
        assert "🔑" in unicode_create.name
        assert "éñ" in unicode_create.value
        assert "🚀" in unicode_create.description