"""
Unit tests for schema schemas.

Tests the functionality of Pydantic schemas for schema management operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.schema import (
    SchemaBase, SchemaCreate, SchemaUpdate, SchemaResponse,
    SchemaListResponse
)


class TestSchemaBase:
    """Test cases for SchemaBase schema."""
    
    def test_valid_schema_base_minimal(self):
        """Test SchemaBase with minimal required fields."""
        data = {
            "name": "user_profile",
            "description": "Schema for user profile data",
            "schema_type": "data_model",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "email": {"type": "string", "format": "email"}
                },
                "required": ["name", "email"]
            }
        }
        schema = SchemaBase(**data)
        assert schema.name == "user_profile"
        assert schema.description == "Schema for user profile data"
        assert schema.schema_type == "data_model"
        assert schema.schema_definition["type"] == "object"
        assert len(schema.schema_definition["properties"]) == 2
        assert schema.field_descriptions == {}  # Default
        assert schema.keywords == []  # Default
        assert schema.tools == []  # Default
        assert schema.example_data is None  # Default

    def test_valid_schema_base_full(self):
        """Test SchemaBase with all fields specified."""
        data = {
            "name": "api_response",
            "description": "Schema for API response format",
            "schema_type": "tool_config",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["success", "error"]},
                    "data": {"type": "object"},
                    "message": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"}
                },
                "required": ["status"]
            },
            "field_descriptions": {
                "status": "Response status indicator",
                "data": "Response payload data",
                "message": "Human-readable message",
                "timestamp": "Response generation timestamp"
            },
            "keywords": ["api", "response", "rest", "json"],
            "tools": ["api_client", "response_validator"],
            "example_data": {
                "status": "success",
                "data": {"id": 123, "name": "example"},
                "message": "Request processed successfully",
                "timestamp": "2023-12-01T12:00:00Z"
            }
        }
        schema = SchemaBase(**data)
        assert schema.name == "api_response"
        assert schema.description == "Schema for API response format"
        assert schema.schema_type == "tool_config"
        assert schema.schema_definition["properties"]["status"]["enum"] == ["success", "error"]
        assert len(schema.field_descriptions) == 4
        assert schema.field_descriptions["status"] == "Response status indicator"
        assert schema.keywords == ["api", "response", "rest", "json"]
        assert schema.tools == ["api_client", "response_validator"]
        assert schema.example_data["status"] == "success"
        assert schema.example_data["data"]["id"] == 123

    def test_schema_base_missing_required_fields(self):
        """Test SchemaBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            SchemaBase(name="incomplete_schema")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "description" in missing_fields
        assert "schema_type" in missing_fields
        assert "schema_definition" in missing_fields

    def test_schema_base_empty_collections(self):
        """Test SchemaBase with empty collections."""
        data = {
            "name": "empty_collections",
            "description": "Schema with empty collections",
            "schema_type": "data_model",
            "schema_definition": {"type": "object"},
            "field_descriptions": {},
            "keywords": [],
            "tools": []
        }
        schema = SchemaBase(**data)
        assert schema.field_descriptions == {}
        assert schema.keywords == []
        assert schema.tools == []

    def test_schema_base_complex_schema_definition(self):
        """Test SchemaBase with complex nested schema definition."""
        complex_definition = {
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "profile": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "preferences": {
                                    "type": "object",
                                    "properties": {
                                        "theme": {"type": "string", "enum": ["light", "dark"]},
                                        "notifications": {"type": "boolean"}
                                    }
                                }
                            }
                        }
                    }
                },
                "metadata": {
                    "type": "object",
                    "properties": {
                        "version": {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"}
                    }
                }
            },
            "required": ["user"],
            "additionalProperties": False
        }
        
        data = {
            "name": "complex_nested",
            "description": "Complex nested schema structure",
            "schema_type": "data_model",
            "schema_definition": complex_definition
        }
        schema = SchemaBase(**data)
        assert schema.schema_definition == complex_definition
        assert schema.schema_definition["properties"]["user"]["properties"]["profile"]["properties"]["preferences"]
        assert schema.schema_definition["additionalProperties"] is False


class TestSchemaCreate:
    """Test cases for SchemaCreate schema."""
    
    def test_schema_create_inheritance(self):
        """Test that SchemaCreate inherits from SchemaBase."""
        data = {
            "name": "create_test",
            "description": "Schema for creation testing",
            "schema_type": "data_model",
            "schema_definition": {"type": "string"}
        }
        create_schema = SchemaCreate(**data)
        
        # Should have all base class attributes
        assert hasattr(create_schema, 'name')
        assert hasattr(create_schema, 'description')
        assert hasattr(create_schema, 'schema_type')
        assert hasattr(create_schema, 'schema_definition')
        assert hasattr(create_schema, 'field_descriptions')
        assert hasattr(create_schema, 'keywords')
        assert hasattr(create_schema, 'tools')
        assert hasattr(create_schema, 'example_data')
        
        # Should behave like base class
        assert create_schema.name == "create_test"
        assert create_schema.description == "Schema for creation testing"
        assert create_schema.schema_type == "data_model"
        assert create_schema.schema_definition == {"type": "string"}

    def test_schema_create_with_legacy_field(self):
        """Test SchemaCreate with legacy_schema_json field."""
        data = {
            "name": "legacy_test",
            "description": "Schema with legacy field",
            "schema_type": "tool_config",
            "schema_definition": {},  # Empty
            "legacy_schema_json": {
                "type": "object",
                "properties": {"legacy": {"type": "string"}}
            }
        }
        create_schema = SchemaCreate(**data)
        assert hasattr(create_schema, 'legacy_schema_json')
        assert create_schema.legacy_schema_json["type"] == "object"

    def test_schema_create_legacy_migration(self):
        """Test SchemaCreate legacy field migration via validator."""
        data = {
            "name": "migration_test",
            "description": "Test legacy field migration",
            "schema_type": "data_model",
            "schema_definition": {},  # Empty initially
            "legacy_schema_json": {
                "type": "array",
                "items": {"type": "string"}
            }
        }
        create_schema = SchemaCreate(**data)
        # The validator should migrate legacy_schema_json to schema_definition
        assert create_schema.schema_definition == {
            "type": "array",
            "items": {"type": "string"}
        }

    def test_schema_create_no_legacy_migration_when_definition_exists(self):
        """Test that legacy migration doesn't override existing schema_definition."""
        data = {
            "name": "no_migration_test",
            "description": "Test no migration when definition exists",
            "schema_type": "data_model",
            "schema_definition": {"type": "number"},  # Has value
            "legacy_schema_json": {"type": "string"}  # Should not override
        }
        create_schema = SchemaCreate(**data)
        # Should keep the original schema_definition
        assert create_schema.schema_definition == {"type": "number"}

    def test_schema_create_with_comprehensive_data(self):
        """Test SchemaCreate with comprehensive schema data."""
        data = {
            "name": "comprehensive_schema",
            "description": "Comprehensive schema for testing",
            "schema_type": "data_model",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "minimum": 1},
                    "name": {"type": "string", "minLength": 1, "maxLength": 100},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "uniqueItems": True
                    }
                }
            },
            "field_descriptions": {
                "id": "Unique identifier",
                "name": "Display name",
                "tags": "Associated tags"
            },
            "keywords": ["comprehensive", "testing", "validation"],
            "tools": ["validator", "serializer"],
            "example_data": {
                "id": 1,
                "name": "Test Item",
                "tags": ["test", "example"]
            }
        }
        create_schema = SchemaCreate(**data)
        assert create_schema.name == "comprehensive_schema"
        assert create_schema.schema_definition["properties"]["name"]["maxLength"] == 100
        assert len(create_schema.field_descriptions) == 3
        assert len(create_schema.keywords) == 3
        assert create_schema.example_data["tags"] == ["test", "example"]


class TestSchemaUpdate:
    """Test cases for SchemaUpdate schema."""
    
    def test_schema_update_all_optional(self):
        """Test that all SchemaUpdate fields are optional."""
        update = SchemaUpdate()
        assert update.name is None
        assert update.description is None
        assert update.schema_type is None
        assert update.schema_definition is None
        assert update.field_descriptions is None
        assert update.keywords is None
        assert update.tools is None
        assert update.example_data is None
        assert update.legacy_schema_json is None

    def test_schema_update_partial(self):
        """Test SchemaUpdate with partial fields."""
        update_data = {
            "description": "Updated schema description",
            "keywords": ["updated", "modified"]
        }
        update = SchemaUpdate(**update_data)
        assert update.description == "Updated schema description"
        assert update.keywords == ["updated", "modified"]
        assert update.name is None
        assert update.schema_definition is None

    def test_schema_update_full(self):
        """Test SchemaUpdate with all fields."""
        update_data = {
            "name": "updated_schema",
            "description": "Fully updated schema",
            "schema_type": "tool_config",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "updated_field": {"type": "string"}
                }
            },
            "field_descriptions": {
                "updated_field": "An updated field"
            },
            "keywords": ["updated", "new", "version"],
            "tools": ["new_tool", "updated_tool"],
            "example_data": {
                "updated_field": "example value"
            }
        }
        update = SchemaUpdate(**update_data)
        assert update.name == "updated_schema"
        assert update.description == "Fully updated schema"
        assert update.schema_type == "tool_config"
        assert update.schema_definition["properties"]["updated_field"]["type"] == "string"
        assert update.field_descriptions["updated_field"] == "An updated field"
        assert update.keywords == ["updated", "new", "version"]
        assert update.tools == ["new_tool", "updated_tool"]
        assert update.example_data["updated_field"] == "example value"

    def test_schema_update_none_values(self):
        """Test SchemaUpdate with explicit None values."""
        update_data = {
            "name": None,
            "description": None,
            "schema_definition": None,
            "keywords": None
        }
        update = SchemaUpdate(**update_data)
        assert update.name is None
        assert update.description is None
        assert update.schema_definition is None
        assert update.keywords is None

    def test_schema_update_empty_collections(self):
        """Test SchemaUpdate with empty collections."""
        update_data = {
            "field_descriptions": {},
            "keywords": [],
            "tools": []
        }
        update = SchemaUpdate(**update_data)
        assert update.field_descriptions == {}
        assert update.keywords == []
        assert update.tools == []

    def test_schema_update_with_legacy_field(self):
        """Test SchemaUpdate with legacy_schema_json field."""
        update_data = {
            "description": "Updated with legacy field",
            "legacy_schema_json": {
                "type": "boolean"
            }
        }
        update = SchemaUpdate(**update_data)
        assert update.description == "Updated with legacy field"
        assert update.legacy_schema_json == {"type": "boolean"}


class TestSchemaResponse:
    """Test cases for SchemaResponse schema."""
    
    def test_valid_schema_response(self):
        """Test SchemaResponse with all required fields."""
        now = datetime.now()
        data = {
            "id": 1,
            "name": "response_schema",
            "description": "Schema for response testing",
            "schema_type": "data_model",
            "schema_definition": {
                "type": "object",
                "properties": {"test": {"type": "string"}}
            },
            "created_at": now,
            "updated_at": now
        }
        response = SchemaResponse(**data)
        assert response.id == 1
        assert response.name == "response_schema"
        assert response.description == "Schema for response testing"
        assert response.schema_type == "data_model"
        assert response.schema_definition["type"] == "object"
        assert response.created_at == now
        assert response.updated_at == now
        
        # Should inherit all base class defaults
        assert response.field_descriptions == {}
        assert response.keywords == []
        assert response.tools == []
        assert response.example_data is None

    def test_schema_response_config(self):
        """Test SchemaResponse model config."""
        assert hasattr(SchemaResponse, 'model_config')
        assert SchemaResponse.model_config["from_attributes"] is True

    def test_schema_response_missing_fields(self):
        """Test SchemaResponse validation with missing fields."""
        now = datetime.now()
        with pytest.raises(ValidationError) as exc_info:
            SchemaResponse(
                name="incomplete_schema",
                description="Incomplete schema",
                schema_type="data_model",
                schema_definition={},
                created_at=now,
                updated_at=now
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields

    def test_schema_response_datetime_conversion(self):
        """Test SchemaResponse with datetime string conversion."""
        data = {
            "id": 2,
            "name": "datetime_schema",
            "description": "Schema with datetime conversion",
            "schema_type": "tool_config",
            "schema_definition": {"type": "string"},
            "created_at": "2023-01-01T10:00:00",
            "updated_at": "2023-01-01T11:00:00"
        }
        response = SchemaResponse(**data)
        assert response.id == 2
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)

    def test_schema_response_with_comprehensive_data(self):
        """Test SchemaResponse with comprehensive schema data."""
        now = datetime.now()
        data = {
            "id": 3,
            "name": "comprehensive_response",
            "description": "Comprehensive response schema",
            "schema_type": "data_model",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "entities": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "type": {"type": "string"},
                                "attributes": {"type": "object"}
                            }
                        }
                    }
                }
            },
            "field_descriptions": {
                "entities": "List of entity objects",
                "entities.id": "Entity identifier",
                "entities.type": "Entity type classification"
            },
            "keywords": ["entity", "response", "comprehensive"],
            "tools": ["entity_parser", "validator"],
            "example_data": {
                "entities": [
                    {"id": "1", "type": "person", "attributes": {"name": "John"}},
                    {"id": "2", "type": "organization", "attributes": {"name": "ACME Corp"}}
                ]
            },
            "created_at": now,
            "updated_at": now
        }
        response = SchemaResponse(**data)
        assert response.id == 3
        assert response.name == "comprehensive_response"
        assert len(response.schema_definition["properties"]["entities"]["items"]["properties"]) == 3
        assert len(response.field_descriptions) == 3
        assert "entity_parser" in response.tools
        assert len(response.example_data["entities"]) == 2


class TestSchemaListResponse:
    """Test cases for SchemaListResponse schema."""
    
    def test_valid_schema_list_response(self):
        """Test SchemaListResponse with all fields."""
        now = datetime.now()
        schemas = [
            SchemaResponse(
                id=1,
                name="schema_1",
                description="First schema",
                schema_type="data_model",
                schema_definition={"type": "string"},
                created_at=now,
                updated_at=now
            ),
            SchemaResponse(
                id=2,
                name="schema_2",
                description="Second schema",
                schema_type="tool_config",
                schema_definition={"type": "number"},
                created_at=now,
                updated_at=now
            )
        ]
        
        data = {
            "schemas": schemas,
            "count": 2
        }
        list_response = SchemaListResponse(**data)
        
        assert len(list_response.schemas) == 2
        assert list_response.count == 2
        assert list_response.schemas[0].name == "schema_1"
        assert list_response.schemas[1].name == "schema_2"
        assert list_response.schemas[0].schema_type == "data_model"
        assert list_response.schemas[1].schema_type == "tool_config"

    def test_empty_schema_list_response(self):
        """Test SchemaListResponse with empty schema list."""
        data = {
            "schemas": [],
            "count": 0
        }
        list_response = SchemaListResponse(**data)
        assert len(list_response.schemas) == 0
        assert list_response.count == 0

    def test_schema_list_response_missing_fields(self):
        """Test SchemaListResponse validation with missing fields."""
        now = datetime.now()
        schemas = [
            SchemaResponse(
                id=1,
                name="test_schema",
                description="Test schema",
                schema_type="data_model",
                schema_definition={"type": "string"},
                created_at=now,
                updated_at=now
            )
        ]
        
        with pytest.raises(ValidationError) as exc_info:
            SchemaListResponse(schemas=schemas)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields


class TestSchemaIntegration:
    """Integration tests for schema schema interactions."""
    
    def test_schema_management_workflow(self):
        """Test complete schema management workflow."""
        # Create schema
        create_data = {
            "name": "user_registration",
            "description": "Schema for user registration data",
            "schema_type": "data_model",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "username": {"type": "string", "minLength": 3, "maxLength": 50},
                    "email": {"type": "string", "format": "email"},
                    "password": {"type": "string", "minLength": 8},
                    "age": {"type": "integer", "minimum": 13, "maximum": 120}
                },
                "required": ["username", "email", "password"]
            },
            "field_descriptions": {
                "username": "Unique username for the account",
                "email": "Valid email address for notifications",
                "password": "Secure password (minimum 8 characters)",
                "age": "User's age (optional, must be 13 or older)"
            },
            "keywords": ["user", "registration", "validation", "form"],
            "tools": ["form_validator", "password_checker"]
        }
        create_schema = SchemaCreate(**create_data)
        
        # Update schema
        update_data = {
            "description": "Updated schema for user registration with additional fields",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "username": {"type": "string", "minLength": 3, "maxLength": 50},
                    "email": {"type": "string", "format": "email"},
                    "password": {"type": "string", "minLength": 10},  # Increased minimum
                    "age": {"type": "integer", "minimum": 13, "maximum": 120},
                    "phone": {"type": "string", "pattern": "^\\+?[1-9]\\d{1,14}$"}  # Added field
                },
                "required": ["username", "email", "password"]
            },
            "field_descriptions": {
                "username": "Unique username for the account",
                "email": "Valid email address for notifications",
                "password": "Secure password (minimum 10 characters)",  # Updated
                "age": "User's age (optional, must be 13 or older)",
                "phone": "Optional phone number in international format"  # Added
            },
            "keywords": ["user", "registration", "validation", "form", "phone"],  # Added keyword
            "tools": ["form_validator", "password_checker", "phone_validator"]  # Added tool
        }
        update_schema = SchemaUpdate(**update_data)
        
        # Simulate database entity
        now = datetime.now()
        db_data = {
            "id": 1,
            "name": create_schema.name,
            "description": update_data["description"],
            "schema_type": create_schema.schema_type,
            "schema_definition": update_data["schema_definition"],
            "field_descriptions": update_data["field_descriptions"],
            "keywords": update_data["keywords"],
            "tools": update_data["tools"],
            "example_data": {
                "username": "john_doe",
                "email": "john@example.com",
                "password": "securepass123",
                "age": 25,
                "phone": "+1234567890"
            },
            "created_at": now,
            "updated_at": now
        }
        schema_response = SchemaResponse(**db_data)
        
        # Verify the complete workflow
        assert create_schema.name == "user_registration"
        assert create_schema.schema_definition["properties"]["password"]["minLength"] == 8
        assert len(create_schema.field_descriptions) == 4
        assert len(create_schema.tools) == 2
        
        assert update_schema.description == "Updated schema for user registration with additional fields"
        assert update_schema.schema_definition["properties"]["password"]["minLength"] == 10
        assert "phone" in update_schema.schema_definition["properties"]
        assert "phone_validator" in update_schema.tools
        
        assert schema_response.id == 1
        assert schema_response.name == "user_registration"
        assert schema_response.schema_definition["properties"]["phone"]["pattern"] == "^\\+?[1-9]\\d{1,14}$"
        assert schema_response.example_data["phone"] == "+1234567890"

    def test_schema_type_scenarios(self):
        """Test different schema type scenarios."""
        # Data model schema
        data_model = SchemaCreate(
            name="product_catalog",
            description="Schema for product catalog entries",
            schema_type="data_model",
            schema_definition={
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "price": {"type": "number", "minimum": 0},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "in_stock": {"type": "boolean"}
                }
            },
            keywords=["product", "catalog", "ecommerce"]
        )
        assert data_model.schema_type == "data_model"
        assert data_model.schema_definition["properties"]["price"]["minimum"] == 0
        
        # Tool configuration schema
        tool_config = SchemaCreate(
            name="api_endpoint_config",
            description="Configuration schema for API endpoints",
            schema_type="tool_config",
            schema_definition={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "format": "uri"},
                    "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE"]},
                    "headers": {"type": "object"},
                    "timeout": {"type": "integer", "minimum": 1, "maximum": 300},
                    "retry_count": {"type": "integer", "minimum": 0, "maximum": 5}
                }
            },
            tools=["api_client", "request_builder"],
            keywords=["api", "configuration", "http"]
        )
        assert tool_config.schema_type == "tool_config"
        assert tool_config.schema_definition["properties"]["method"]["enum"] == ["GET", "POST", "PUT", "DELETE"]
        assert "api_client" in tool_config.tools

    def test_schema_validation_scenarios(self):
        """Test different schema validation scenarios."""
        # JSON Schema with complex validation rules
        validation_schema = SchemaCreate(
            name="complex_validation",
            description="Schema with complex validation rules",
            schema_type="data_model",
            schema_definition={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "pattern": "^[A-Za-z\\s]+$",
                        "minLength": 2,
                        "maxLength": 100
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    },
                    "age": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 150
                    },
                    "preferences": {
                        "type": "object",
                        "properties": {
                            "theme": {"type": "string", "enum": ["light", "dark", "auto"]},
                            "language": {"type": "string", "pattern": "^[a-z]{2}(-[A-Z]{2})?$"},
                            "notifications": {
                                "type": "object",
                                "properties": {
                                    "email": {"type": "boolean"},
                                    "push": {"type": "boolean"},
                                    "sms": {"type": "boolean"}
                                },
                                "additionalProperties": False
                            }
                        },
                        "required": ["theme", "language"]
                    }
                },
                "required": ["name", "email"],
                "additionalProperties": False
            },
            field_descriptions={
                "name": "Full name (letters and spaces only)",
                "email": "Valid email address",
                "age": "Age in years (0-150)",
                "preferences.theme": "UI theme preference",
                "preferences.language": "Language code (ISO 639-1 format)",
                "preferences.notifications": "Notification preferences"
            }
        )
        assert validation_schema.name == "complex_validation"
        assert validation_schema.schema_definition["properties"]["name"]["pattern"] == "^[A-Za-z\\s]+$"
        assert validation_schema.schema_definition["properties"]["preferences"]["required"] == ["theme", "language"]
        assert len(validation_schema.field_descriptions) == 6

    def test_schema_list_management(self):
        """Test schema list management workflow."""
        now = datetime.now()
        
        # Create multiple schemas of different types
        schemas = []
        schema_types = ["data_model", "tool_config", "data_model", "tool_config"]
        
        for i, schema_type in enumerate(schema_types):
            schema_data = {
                "id": i + 1,
                "name": f"{schema_type}_schema_{i + 1}",
                "description": f"Schema {i + 1} of type {schema_type}",
                "schema_type": schema_type,
                "schema_definition": {
                    "type": "object" if schema_type == "data_model" else "string"
                },
                "keywords": [schema_type, f"test_{i + 1}"],
                "created_at": now,
                "updated_at": now
            }
            schemas.append(SchemaResponse(**schema_data))
        
        # Create list response
        list_response = SchemaListResponse(
            schemas=schemas,
            count=len(schemas)
        )
        
        # Verify list management
        assert list_response.count == 4
        assert len(list_response.schemas) == 4
        
        # Test filtering by schema type
        data_model_schemas = [s for s in list_response.schemas if s.schema_type == "data_model"]
        tool_config_schemas = [s for s in list_response.schemas if s.schema_type == "tool_config"]
        
        assert len(data_model_schemas) == 2
        assert len(tool_config_schemas) == 2
        assert data_model_schemas[0].name == "data_model_schema_1"
        assert tool_config_schemas[0].name == "tool_config_schema_2"
        
        # Test searching by keywords
        schemas_with_test_1 = [s for s in list_response.schemas if "test_1" in s.keywords]
        assert len(schemas_with_test_1) == 1
        assert schemas_with_test_1[0].name == "data_model_schema_1"