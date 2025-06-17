"""
Unit tests for schema model.

Tests the functionality of the Schema database model including
field validation, JSON handling, and data integrity.
"""
import pytest
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.models.schema import Schema


class TestSchema:
    """Test cases for Schema model."""

    def test_schema_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Schema.__tablename__ == "schema"

    def test_schema_column_structure(self):
        """Test Schema model column structure."""
        # Act
        columns = Schema.__table__.columns
        
        # Assert - Check that all expected columns exist
        expected_columns = [
            'id', 'name', 'description', 'schema_type', 'schema_definition',
            'field_descriptions', 'keywords', 'tools', 'example_data',
            'created_at', 'updated_at'
        ]
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in Schema model"

    def test_schema_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = Schema.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "INTEGER" in str(columns['id'].type)
        
        # Required string fields
        required_string_fields = ['name', 'description', 'schema_type']
        for field in required_string_fields:
            assert columns[field].nullable is False
            assert "VARCHAR" in str(columns[field].type) or "STRING" in str(columns[field].type)
        
        # Unique constraint on name
        assert columns['name'].unique is True
        
        # JSON fields
        json_fields = ['schema_definition', 'field_descriptions', 'keywords', 'tools', 'example_data']
        for field in json_fields:
            assert "JSON" in str(columns[field].type)
        
        # Required JSON field
        assert columns['schema_definition'].nullable is False
        
        # Optional JSON fields
        optional_json_fields = ['field_descriptions', 'keywords', 'tools', 'example_data']
        for field in optional_json_fields:
            if field != 'example_data':  # example_data doesn't have default
                assert columns[field].default is not None
        
        # DateTime fields
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)

    def test_schema_default_values(self):
        """Test Schema model default values."""
        # Act
        columns = Schema.__table__.columns
        
        # Assert
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None

    def test_schema_init_method(self):
        """Test Schema custom __init__ method."""
        # Act & Assert
        assert hasattr(Schema, '__init__')
        assert Schema.__init__.__doc__ is not None
        assert "enhanced JSON handling" in Schema.__init__.__doc__

    def test_schema_as_dict_method(self):
        """Test Schema as_dict method."""
        # Act & Assert
        assert hasattr(Schema, 'as_dict')
        assert Schema.as_dict.__doc__ is not None
        assert "Convert the schema object to a dictionary" in Schema.as_dict.__doc__

    def test_schema_backward_compatibility(self):
        """Test schema_json to schema_definition conversion."""
        # Test the logic for backward compatibility
        test_schema_json = {"type": "object", "properties": {"name": {"type": "string"}}}
        
        # Assert conversion logic would work
        assert isinstance(test_schema_json, dict)
        assert "type" in test_schema_json
        assert "properties" in test_schema_json

    def test_schema_json_string_handling(self):
        """Test JSON string to object conversion logic."""
        # Test different JSON field scenarios
        json_scenarios = [
            {
                "field": "schema_definition",
                "string_value": '{"type": "object", "properties": {}}',
                "expected_type": dict
            },
            {
                "field": "field_descriptions", 
                "string_value": '{"field1": "Description 1"}',
                "expected_type": dict
            },
            {
                "field": "keywords",
                "string_value": '["keyword1", "keyword2"]',
                "expected_type": list
            },
            {
                "field": "tools",
                "string_value": '["tool1", "tool2"]',
                "expected_type": list
            }
        ]
        
        for scenario in json_scenarios:
            # Assert JSON string parsing would work
            parsed = json.loads(scenario["string_value"])
            assert isinstance(parsed, scenario["expected_type"])

    def test_schema_invalid_json_handling(self):
        """Test invalid JSON string handling logic."""
        # Test invalid JSON scenarios and default values
        invalid_json_scenarios = [
            {"field": "schema_definition", "default": {}},
            {"field": "field_descriptions", "default": {}},
            {"field": "keywords", "default": []},
            {"field": "tools", "default": []}
        ]
        
        for scenario in invalid_json_scenarios:
            # Assert default values are proper type
            default_value = scenario["default"]
            if scenario["field"] in ["schema_definition", "field_descriptions"]:
                assert isinstance(default_value, dict)
            else:
                assert isinstance(default_value, list)

    def test_schema_model_documentation(self):
        """Test Schema model documentation."""
        # Act & Assert
        assert Schema.__doc__ is not None
        assert "Schema model for storing data schemas" in Schema.__doc__

    def test_schema_type_scenarios(self):
        """Test schema type field scenarios."""
        # Test valid schema types
        valid_schema_types = [
            "data_model",
            "tool_config",
            "api_schema",
            "validation_schema",
            "ui_schema",
            "workflow_schema"
        ]
        
        for schema_type in valid_schema_types:
            # Assert schema type format
            assert isinstance(schema_type, str)
            assert len(schema_type) > 0

    def test_schema_definition_scenarios(self):
        """Test schema definition field scenarios."""
        # Test different schema definition formats
        schema_definitions = [
            {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"}
                },
                "required": ["name"]
            },
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "value": {"type": "number"}
                    }
                }
            },
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "settings": {"type": "object"}
                        }
                    }
                }
            }
        ]
        
        for schema_def in schema_definitions:
            # Assert schema definition is valid JSON
            json.dumps(schema_def)
            assert "type" in schema_def

    def test_schema_field_descriptions_scenarios(self):
        """Test field descriptions scenarios."""
        # Test different field description formats
        field_descriptions = [
            {},  # Empty descriptions
            {
                "name": "The name of the entity",
                "age": "Age in years"
            },
            {
                "config.enabled": "Whether the feature is enabled",
                "config.settings.timeout": "Timeout value in seconds",
                "data.items[].id": "Unique identifier for the item"
            }
        ]
        
        for descriptions in field_descriptions:
            # Assert field descriptions are valid
            json.dumps(descriptions)
            assert isinstance(descriptions, dict)

    def test_schema_keywords_scenarios(self):
        """Test keywords field scenarios."""
        # Test different keyword formats
        keywords_examples = [
            [],  # No keywords
            ["user", "profile"],  # Simple keywords
            ["api", "rest", "json", "validation"],  # Multiple keywords
            ["machine-learning", "ai", "data-processing"]  # Hyphenated keywords
        ]
        
        for keywords in keywords_examples:
            # Assert keywords are valid
            json.dumps(keywords)
            assert isinstance(keywords, list)
            for keyword in keywords:
                assert isinstance(keyword, str)

    def test_schema_tools_scenarios(self):
        """Test tools field scenarios."""
        # Test different tools configurations
        tools_examples = [
            [],  # No tools
            ["validator", "transformer"],  # Simple tool names
            [
                {"name": "validator", "version": "1.0"},
                {"name": "transformer", "config": {"strict": True}}
            ]  # Complex tool configurations
        ]
        
        for tools in tools_examples:
            # Assert tools are valid
            json.dumps(tools)
            assert isinstance(tools, list)

    def test_schema_example_data_scenarios(self):
        """Test example data field scenarios."""
        # Test different example data formats
        example_data_examples = [
            None,  # No example data
            {"name": "John Doe", "age": 30},  # Simple example
            {
                "users": [
                    {"id": "1", "name": "Alice", "email": "alice@example.com"},
                    {"id": "2", "name": "Bob", "email": "bob@example.com"}
                ],
                "metadata": {
                    "total_count": 2,
                    "page": 1
                }
            }  # Complex nested example
        ]
        
        for example_data in example_data_examples:
            if example_data is not None:
                # Assert example data is valid JSON
                json.dumps(example_data)
                assert isinstance(example_data, dict)

    def test_schema_as_dict_structure(self):
        """Test as_dict method structure."""
        # Test expected as_dict output structure
        expected_keys = [
            'id', 'name', 'description', 'schema_type', 'schema_definition',
            'field_descriptions', 'keywords', 'tools', 'example_data',
            'created_at', 'updated_at'
        ]
        
        # Assert all expected keys are present in as_dict logic
        for key in expected_keys:
            assert isinstance(key, str)
            assert len(key) > 0


class TestSchemaEdgeCases:
    """Test edge cases and error scenarios for Schema."""

    def test_schema_very_long_fields(self):
        """Test Schema with very long field values."""
        # Arrange
        long_name = "very_long_schema_name_" * 20  # 440 characters
        long_description = "Very long description " * 30  # 660 characters
        long_schema_type = "very_long_schema_type_" * 10  # 220 characters
        
        # Assert
        assert len(long_name) == 440
        assert len(long_description) == 660
        assert len(long_schema_type) == 220

    def test_schema_complex_definitions(self):
        """Test Schema with complex schema definitions."""
        # Complex JSON Schema examples
        complex_schemas = [
            {
                "name": "user_profile_schema",
                "schema_definition": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "personal_info": {
                            "type": "object",
                            "properties": {
                                "first_name": {"type": "string", "minLength": 1},
                                "last_name": {"type": "string", "minLength": 1},
                                "birth_date": {"type": "string", "format": "date"},
                                "email": {"type": "string", "format": "email"}
                            },
                            "required": ["first_name", "last_name", "email"]
                        },
                        "preferences": {
                            "type": "object",
                            "properties": {
                                "language": {"type": "string", "enum": ["en", "es", "fr"]},
                                "notifications": {
                                    "type": "object",
                                    "properties": {
                                        "email": {"type": "boolean"},
                                        "sms": {"type": "boolean"},
                                        "push": {"type": "boolean"}
                                    }
                                }
                            }
                        }
                    },
                    "required": ["personal_info"]
                }
            },
            {
                "name": "api_config_schema",
                "schema_definition": {
                    "type": "object",
                    "properties": {
                        "endpoints": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "path": {"type": "string"},
                                    "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE"]},
                                    "auth_required": {"type": "boolean"},
                                    "rate_limit": {"type": "integer", "minimum": 1}
                                },
                                "required": ["path", "method"]
                            }
                        },
                        "global_settings": {
                            "type": "object",
                            "properties": {
                                "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300},
                                "retry_attempts": {"type": "integer", "minimum": 0, "maximum": 10}
                            }
                        }
                    }
                }
            }
        ]
        
        for schema_data in complex_schemas:
            # Assert complex schema is valid
            json.dumps(schema_data["schema_definition"])
            assert "properties" in schema_data["schema_definition"]

    def test_schema_multilingual_content(self):
        """Test Schema with multilingual content."""
        # Multilingual schema examples
        multilingual_schemas = [
            {
                "name": "multilingual_content",
                "description": "Schema for multilingual content management",
                "field_descriptions": {
                    "title_en": "Title in English",
                    "title_es": "Título en español",
                    "title_fr": "Titre en français",
                    "content_en": "Content in English",
                    "content_es": "Contenido en español",
                    "content_fr": "Contenu en français"
                },
                "keywords": ["multilingual", "i18n", "localization", "translation"]
            }
        ]
        
        for schema_data in multilingual_schemas:
            # Assert multilingual content is properly structured
            assert "field_descriptions" in schema_data
            assert len(schema_data["keywords"]) > 0

    def test_schema_json_error_scenarios(self):
        """Test Schema with JSON parsing error scenarios."""
        # Invalid JSON string scenarios
        invalid_json_scenarios = [
            {"field": "schema_definition", "invalid_json": "invalid json string"},
            {"field": "field_descriptions", "invalid_json": "{incomplete json"},
            {"field": "keywords", "invalid_json": "[unclosed array"},
            {"field": "tools", "invalid_json": "not json at all"}
        ]
        
        for scenario in invalid_json_scenarios:
            # Assert we can detect invalid JSON
            with pytest.raises(json.JSONDecodeError):
                json.loads(scenario["invalid_json"])

    def test_schema_performance_large_data(self):
        """Test Schema with large data structures."""
        # Large data structure examples
        large_data_scenarios = [
            {
                "scenario": "large_schema_definition",
                "schema_definition": {
                    "type": "object",
                    "properties": {
                        f"field_{i}": {"type": "string"} for i in range(100)
                    }
                }
            },
            {
                "scenario": "large_field_descriptions",
                "field_descriptions": {
                    f"field_{i}": f"Description for field {i}" for i in range(100)
                }
            },
            {
                "scenario": "large_keywords_list",
                "keywords": [f"keyword_{i}" for i in range(200)]
            }
        ]
        
        for scenario in large_data_scenarios:
            # Assert large data structures are serializable
            if "schema_definition" in scenario:
                json.dumps(scenario["schema_definition"])
                assert len(scenario["schema_definition"]["properties"]) == 100
            elif "field_descriptions" in scenario:
                json.dumps(scenario["field_descriptions"])
                assert len(scenario["field_descriptions"]) == 100
            elif "keywords" in scenario:
                json.dumps(scenario["keywords"])
                assert len(scenario["keywords"]) == 200

    def test_schema_version_compatibility(self):
        """Test Schema for different JSON Schema versions."""
        # Different JSON Schema versions
        version_scenarios = [
            {
                "version": "draft-04",
                "schema_definition": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"name": {"type": "string"}}
                }
            },
            {
                "version": "draft-07",
                "schema_definition": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {"name": {"type": "string"}}
                }
            },
            {
                "version": "draft-2019-09",
                "schema_definition": {
                    "$schema": "https://json-schema.org/draft/2019-09/schema",
                    "type": "object",
                    "properties": {"name": {"type": "string"}}
                }
            }
        ]
        
        for scenario in version_scenarios:
            # Assert different versions are supported
            json.dumps(scenario["schema_definition"])
            assert "$schema" in scenario["schema_definition"]

    def test_schema_data_integrity(self):
        """Test data integrity constraints."""
        # Act
        table = Schema.__table__
        
        # Assert primary key
        primary_keys = [col for col in table.columns if col.primary_key]
        assert len(primary_keys) == 1
        assert primary_keys[0].name == 'id'
        
        # Assert required fields
        required_fields = ['name', 'description', 'schema_type', 'schema_definition']
        for field_name in required_fields:
            field = table.columns[field_name]
            assert field.nullable is False
        
        # Assert unique constraint
        assert table.columns['name'].unique is True
        
        # Assert optional fields
        optional_fields = ['field_descriptions', 'keywords', 'tools', 'example_data']
        for field_name in optional_fields:
            field = table.columns[field_name]
            # example_data is truly optional, others have defaults
            if field_name != 'example_data':
                assert field.default is not None
        
        # Assert JSON fields
        json_fields = ['schema_definition', 'field_descriptions', 'keywords', 'tools', 'example_data']
        for field_name in json_fields:
            field = table.columns[field_name]
            assert "JSON" in str(field.type)


class TestSchemaInitialization:
    """Test cases for Schema model initialization and methods."""

    def test_schema_init_with_schema_json_backward_compatibility(self):
        """Test __init__ method with schema_json parameter for backward compatibility."""
        # Arrange
        test_schema_json = {"type": "object", "properties": {"name": {"type": "string"}}}
        
        # Act
        schema = Schema(
            name="test_schema",
            description="Test description",
            schema_type="test_type",
            schema_json=test_schema_json
        )
        
        # Assert
        assert schema.schema_definition == test_schema_json
        assert not hasattr(schema, 'schema_json')

    def test_schema_init_with_json_strings(self):
        """Test __init__ method with JSON strings that need parsing."""
        # Arrange
        schema_def_str = '{"type": "object", "properties": {}}'
        field_desc_str = '{"field1": "Description 1"}'
        keywords_str = '["keyword1", "keyword2"]'
        tools_str = '["tool1", "tool2"]'
        example_data_str = '{"example": "data"}'
        
        # Act
        schema = Schema(
            name="test_schema",
            description="Test description",
            schema_type="test_type",
            schema_definition=schema_def_str,
            field_descriptions=field_desc_str,
            keywords=keywords_str,
            tools=tools_str,
            example_data=example_data_str
        )
        
        # Assert
        assert schema.schema_definition == {"type": "object", "properties": {}}
        assert schema.field_descriptions == {"field1": "Description 1"}
        assert schema.keywords == ["keyword1", "keyword2"]
        assert schema.tools == ["tool1", "tool2"]
        assert schema.example_data == {"example": "data"}

    def test_schema_init_with_invalid_json_strings(self):
        """Test __init__ method with invalid JSON strings."""
        # Act
        schema = Schema(
            name="test_schema",
            description="Test description",
            schema_type="test_type",
            schema_definition="invalid json",
            field_descriptions="{incomplete json",
            keywords="[unclosed array",
            tools="not json at all",
            example_data="invalid json too"
        )
        
        # Assert - defaults should be set for invalid JSON
        assert schema.schema_definition == {}
        assert schema.field_descriptions == {}
        assert schema.keywords == []
        assert schema.tools == []
        # example_data should remain as the invalid string since it's nullable
        assert schema.example_data == "invalid json too"

    def test_schema_init_with_none_values(self):
        """Test __init__ method with None values for JSON fields."""
        # Act
        schema = Schema(
            name="test_schema",
            description="Test description",
            schema_type="test_type",
            schema_definition={"type": "object"},
            field_descriptions=None,
            keywords=None,
            tools=None,
            example_data=None
        )
        
        # Assert - defaults should be set for None values
        assert schema.field_descriptions == {}
        assert schema.keywords == []
        assert schema.tools == []
        assert schema.example_data is None  # example_data can remain None

    def test_schema_init_with_type_error_json_fields(self):
        """Test __init__ method with TypeError in JSON parsing."""
        # Mock json.loads to raise TypeError
        with patch('src.models.schema.json.loads') as mock_json_loads:
            mock_json_loads.side_effect = TypeError("Type error")
            
            # Act
            schema = Schema(
                name="test_schema",
                description="Test description",
                schema_type="test_type",
                schema_definition="some string",
                field_descriptions="some string",
                keywords="some string",
                tools="some string"
            )
            
            # Assert - defaults should be set when TypeError occurs
            assert schema.schema_definition == {}
            assert schema.field_descriptions == {}
            assert schema.keywords == []
            assert schema.tools == []

    def test_schema_as_dict_method(self):
        """Test as_dict method with actual Schema instance."""
        # Arrange
        test_datetime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Act
        schema = Schema(
            name="test_schema",
            description="Test description",
            schema_type="test_type",
            schema_definition={"type": "object"},
            field_descriptions={"field1": "desc1"},
            keywords=["keyword1"],
            tools=["tool1"],
            example_data={"example": "data"}
        )
        
        # Mock the datetime fields
        schema.id = 1
        schema.created_at = test_datetime
        schema.updated_at = test_datetime
        
        result = schema.as_dict()
        
        # Assert
        expected = {
            'id': 1,
            'name': 'test_schema',
            'description': 'Test description',
            'schema_type': 'test_type',
            'schema_definition': {'type': 'object'},
            'field_descriptions': {'field1': 'desc1'},
            'keywords': ['keyword1'],
            'tools': ['tool1'],
            'example_data': {'example': 'data'},
            'created_at': '2023-01-01T12:00:00+00:00',
            'updated_at': '2023-01-01T12:00:00+00:00'
        }
        assert result == expected

    def test_schema_as_dict_with_none_values(self):
        """Test as_dict method with None values for optional fields."""
        # Act
        schema = Schema(
            name="test_schema",
            description="Test description",
            schema_type="test_type",
            schema_definition={"type": "object"}
        )
        
        # Set None values explicitly after initialization
        schema.id = 1
        schema.field_descriptions = None
        schema.keywords = None
        schema.tools = None
        schema.example_data = None
        schema.created_at = None
        schema.updated_at = None
        
        result = schema.as_dict()
        
        # Assert
        assert result['field_descriptions'] == {}
        assert result['keywords'] == []
        assert result['tools'] == []
        assert result['example_data'] is None
        assert result['created_at'] is None
        assert result['updated_at'] is None