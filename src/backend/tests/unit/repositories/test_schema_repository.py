"""
Unit tests for SchemaRepository.

Tests the functionality of schema repository including
CRUD operations, JSON handling, keyword/tool searching, and error handling.
"""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, func
from sqlalchemy.sql.expression import cast
from sqlalchemy.dialects.postgresql import JSONB

from src.repositories.schema_repository import SchemaRepository
from src.models.schema import Schema


# Mock schema model
class MockSchema:
    def __init__(self, id=1, name="Test Schema", description="Test Description", schema_type="object",
                 schema_definition=None, field_descriptions=None, example_data=None,
                 keywords=None, tools=None, created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.description = description
        self.schema_type = schema_type
        self.schema_definition = schema_definition or {}
        self.field_descriptions = field_descriptions or {}
        self.example_data = example_data
        self.keywords = keywords or []
        self.tools = tools or []
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def schema_repository(mock_async_session):
    """Create a schema repository with async session."""
    return SchemaRepository(session=mock_async_session)


@pytest.fixture
def sample_schemas():
    """Create sample schemas for testing."""
    return [
        MockSchema(id=1, name="User Schema", schema_type="object",
                  keywords=["user", "person"], tools=["api", "validation"]),
        MockSchema(id=2, name="Product Schema", schema_type="object",
                  keywords=["product", "inventory"], tools=["api", "database"]),
        MockSchema(id=3, name="Order Schema", schema_type="array",
                  keywords=["order", "transaction"], tools=["payment", "notification"])
    ]


@pytest.fixture
def sample_schema_data():
    """Create sample schema data for creation."""
    return {
        "name": "new_schema",
        "schema_type": "object",
        "schema_definition": {"type": "object", "properties": {"id": {"type": "integer"}}},
        "field_descriptions": {"id": "Unique identifier"},
        "keywords": ["test", "new"],
        "tools": ["testing", "validation"]
    }


class TestSchemaRepositoryInit:
    """Test cases for SchemaRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = SchemaRepository(session=mock_async_session)
        
        assert repository.model == Schema
        assert repository.session == mock_async_session


class TestSchemaRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, schema_repository, mock_async_session):
        """Test successful schema search by name."""
        schema = MockSchema(name="test_schema")
        mock_result = MockResult([schema])
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_name("test_schema")
        
        assert result == schema
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Schema)))
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, schema_repository, mock_async_session):
        """Test find by name when schema not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_name("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestSchemaRepositoryFindByNameSync:
    """Test cases for find_by_name_sync method."""
    
    def test_find_by_name_sync_with_sync_session(self):
        """Test sync find by name with synchronous session."""
        schema = MockSchema(name="sync_schema")
        mock_result = MockResult([schema])
        
        # Create a non-async mock session for sync behavior
        mock_sync_session = MagicMock()
        mock_sync_session.execute.return_value = mock_result
        
        repository = SchemaRepository(session=mock_sync_session)
        result = repository.find_by_name_sync("sync_schema")
        
        assert result == schema
        mock_sync_session.execute.assert_called_once()
    
    def test_find_by_name_sync_without_execute_attribute(self):
        """Test sync find by name when session doesn't have execute attribute."""
        schema = MockSchema(name="test_schema")
        mock_result = MockResult([schema])
        
        # Create a mock session without execute attribute to trigger else branch
        mock_session = MagicMock()
        # Remove execute attribute to trigger hasattr check to fail
        if hasattr(mock_session, 'execute'):
            delattr(mock_session, 'execute')
        
        repository = SchemaRepository(session=mock_session)
        
        # Add execute back after the hasattr check
        mock_session.execute = MagicMock(return_value=mock_result)
        
        with patch('logging.getLogger') as mock_logger:
            mock_log = MagicMock()
            mock_logger.return_value = mock_log
            
            # Mock hasattr to return False for this specific test
            with patch('builtins.hasattr') as mock_hasattr:
                mock_hasattr.return_value = False
                
                result = repository.find_by_name_sync("test_schema")
                
                # Should have logged the warning
                mock_log.warning.assert_called_once()
                warning_message = mock_log.warning.call_args[0][0]
                assert "find_by_name_sync called with an async session" in warning_message
                assert result == schema


class TestSchemaRepositoryFindByType:
    """Test cases for find_by_type method."""
    
    @pytest.mark.asyncio
    async def test_find_by_type_success(self, schema_repository, mock_async_session, sample_schemas):
        """Test successful schema search by type."""
        object_schemas = [schema for schema in sample_schemas if schema.schema_type == "object"]
        mock_result = MockResult(object_schemas)
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_type("object")
        
        assert len(result) == len(object_schemas)
        assert all(schema.schema_type == "object" for schema in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_type_not_found(self, schema_repository, mock_async_session):
        """Test find by type when no schemas found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_type("nonexistent_type")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestSchemaRepositoryFindByKeyword:
    """Test cases for find_by_keyword method."""
    
    @pytest.mark.asyncio
    async def test_find_by_keyword_success(self, schema_repository, mock_async_session, sample_schemas):
        """Test successful schema search by keyword."""
        user_schemas = [schema for schema in sample_schemas if "user" in schema.keywords]
        mock_result = MockResult(user_schemas)
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_keyword("user")
        
        assert len(result) == len(user_schemas)
        assert all("user" in schema.keywords for schema in result)
        mock_async_session.execute.assert_called_once()
        
        # Verify query uses JSON containment operators
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Schema)))
    
    @pytest.mark.asyncio
    async def test_find_by_keyword_database_error_fallback(self, schema_repository, mock_async_session, sample_schemas):
        """Test find by keyword with database error falls back to application filtering."""
        # First call fails (database query), second call succeeds (fallback query)
        mock_async_session.execute.side_effect = [
            Exception("JSONB not supported"),
            MockResult(sample_schemas)
        ]
        
        with patch('logging.getLogger') as mock_logger:
            mock_log = MagicMock()
            mock_logger.return_value = mock_log
            
            result = await schema_repository.find_by_keyword("user")
            
            # Should fall back and filter application-level
            user_schemas = [schema for schema in sample_schemas if "user" in schema.keywords]
            assert len(result) == len(user_schemas)
            
            # Verify warning was logged
            mock_log.warning.assert_called_once()
            assert "Database JSON query failed" in mock_log.warning.call_args[0][0]
        
        # Verify both database query and fallback query were attempted
        assert mock_async_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_find_by_keyword_empty_keywords(self, schema_repository, mock_async_session):
        """Test find by keyword when schemas have empty keywords."""
        schemas_no_keywords = [
            MockSchema(id=1, keywords=None),
            MockSchema(id=2, keywords=[]),
            MockSchema(id=3, keywords=["other"])
        ]
        
        # Simulate database error to test fallback filtering
        mock_async_session.execute.side_effect = [
            Exception("DB error"),
            MockResult(schemas_no_keywords)
        ]
        
        with patch('logging.getLogger'):
            result = await schema_repository.find_by_keyword("test")
            
            # Should return empty since none have the "test" keyword
            assert result == []


class TestSchemaRepositoryFindByTool:
    """Test cases for find_by_tool method."""
    
    @pytest.mark.asyncio
    async def test_find_by_tool_success(self, schema_repository, mock_async_session, sample_schemas):
        """Test successful schema search by tool."""
        api_schemas = [schema for schema in sample_schemas if "api" in schema.tools]
        mock_result = MockResult(api_schemas)
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_tool("api")
        
        assert len(result) == len(api_schemas)
        assert all("api" in schema.tools for schema in result)
        mock_async_session.execute.assert_called_once()
        
        # Verify query uses JSON containment operators
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Schema)))
    
    @pytest.mark.asyncio
    async def test_find_by_tool_database_error_fallback(self, schema_repository, mock_async_session, sample_schemas):
        """Test find by tool with database error falls back to application filtering."""
        # First call fails (database query), second call succeeds (fallback query)
        mock_async_session.execute.side_effect = [
            Exception("JSON functions not available"),
            MockResult(sample_schemas)
        ]
        
        with patch('logging.getLogger') as mock_logger:
            mock_log = MagicMock()
            mock_logger.return_value = mock_log
            
            result = await schema_repository.find_by_tool("api")
            
            # Should fall back and filter application-level
            api_schemas = [schema for schema in sample_schemas if "api" in schema.tools]
            assert len(result) == len(api_schemas)
            
            # Verify warning was logged
            mock_log.warning.assert_called_once()
        
        # Verify both database query and fallback query were attempted
        assert mock_async_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_find_by_tool_empty_tools(self, schema_repository, mock_async_session):
        """Test find by tool when schemas have empty tools."""
        schemas_no_tools = [
            MockSchema(id=1, tools=None),
            MockSchema(id=2, tools=[]),
            MockSchema(id=3, tools=["other"])
        ]
        
        # Simulate database error to test fallback filtering
        mock_async_session.execute.side_effect = [
            Exception("DB error"),
            MockResult(schemas_no_tools)
        ]
        
        with patch('logging.getLogger'):
            result = await schema_repository.find_by_tool("api")
            
            # Should return empty since none have the "api" tool
            assert result == []


class TestSchemaRepositoryCreate:
    """Test cases for create method with JSON handling."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, schema_repository, mock_async_session, sample_schema_data):
        """Test successful schema creation with JSON sanitization."""
        with patch('src.repositories.schema_repository.Schema') as mock_schema_class:
            created_schema = MockSchema(**sample_schema_data)
            mock_schema_class.return_value = created_schema
            
            with patch.object(schema_repository.__class__.__bases__[0], 'create', return_value=created_schema) as mock_super_create:
                result = await schema_repository.create(sample_schema_data)
                
                assert result == created_schema
                mock_super_create.assert_called_once()
                
                # Verify _sanitize_json_data was called on the data
                called_data = mock_super_create.call_args[0][0]
                assert isinstance(called_data, dict)
    
    @pytest.mark.asyncio
    async def test_create_with_json_string_data(self, schema_repository, mock_async_session):
        """Test schema creation with JSON strings that need parsing."""
        schema_data_with_strings = {
            "name": "json_string_schema",
            "schema_definition": '{"type": "object", "properties": {"id": {"type": "integer"}}}',
            "keywords": '["json", "string"]',
            "tools": '["parser", "validator"]',
            "field_descriptions": '{"id": "Identifier field"}'
        }
        
        with patch('src.repositories.schema_repository.Schema') as mock_schema_class:
            created_schema = MockSchema(name="json_string_schema")
            mock_schema_class.return_value = created_schema
            
            with patch.object(schema_repository.__class__.__bases__[0], 'create', return_value=created_schema) as mock_super_create:
                result = await schema_repository.create(schema_data_with_strings)
                
                assert result == created_schema
                
                # Verify JSON strings were parsed
                called_data = mock_super_create.call_args[0][0]
                assert isinstance(called_data["schema_definition"], dict)
                assert isinstance(called_data["keywords"], list)
                assert isinstance(called_data["tools"], list)
                assert isinstance(called_data["field_descriptions"], dict)
    
    @pytest.mark.asyncio
    async def test_create_with_invalid_json_data(self, schema_repository, mock_async_session):
        """Test schema creation with invalid JSON strings uses defaults."""
        schema_data_invalid_json = {
            "name": "invalid_json_schema",
            "schema_definition": "invalid json{",
            "keywords": "invalid json[",
            "tools": "not json",
            "field_descriptions": "not json object"
        }
        
        with patch('src.repositories.schema_repository.Schema') as mock_schema_class:
            created_schema = MockSchema(name="invalid_json_schema")
            mock_schema_class.return_value = created_schema
            
            with patch.object(schema_repository.__class__.__bases__[0], 'create', return_value=created_schema) as mock_super_create:
                result = await schema_repository.create(schema_data_invalid_json)
                
                assert result == created_schema
                
                # Verify invalid JSON was replaced with defaults
                called_data = mock_super_create.call_args[0][0]
                assert called_data["schema_definition"] == {}  # Default for schema_definition
                assert called_data["keywords"] == []  # Default for keywords
                assert called_data["tools"] == []  # Default for tools
                assert called_data["field_descriptions"] == {}  # Default for field_descriptions


class TestSchemaRepositoryUpdate:
    """Test cases for update method with JSON handling."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, schema_repository, mock_async_session):
        """Test successful schema update with JSON sanitization."""
        update_data = {
            "name": "updated_schema",
            "keywords": ["updated", "modified"]
        }
        
        updated_schema = MockSchema(id=1, name="updated_schema")
        
        with patch.object(schema_repository.__class__.__bases__[0], 'update', return_value=updated_schema) as mock_super_update:
            result = await schema_repository.update(1, update_data)
            
            assert result == updated_schema
            mock_super_update.assert_called_once_with(1, update_data)
    
    @pytest.mark.asyncio
    async def test_update_with_json_string_data(self, schema_repository, mock_async_session):
        """Test schema update with JSON strings that need parsing."""
        update_data = {
            "schema_definition": '{"type": "array", "items": {"type": "string"}}',
            "keywords": '["updated", "json"]'
        }
        
        updated_schema = MockSchema(id=1)
        
        with patch.object(schema_repository.__class__.__bases__[0], 'update', return_value=updated_schema) as mock_super_update:
            result = await schema_repository.update(1, update_data)
            
            assert result == updated_schema
            
            # Verify JSON strings were parsed
            called_data = mock_super_update.call_args[0][1]
            assert isinstance(called_data["schema_definition"], dict)
            assert isinstance(called_data["keywords"], list)
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, schema_repository, mock_async_session):
        """Test update when schema not found."""
        with patch.object(schema_repository.__class__.__bases__[0], 'update', return_value=None) as mock_super_update:
            result = await schema_repository.update(999, {"name": "not_found"})
            
            assert result is None
            mock_super_update.assert_called_once()


class TestSchemaRepositorySanitizeJsonData:
    """Test cases for _sanitize_json_data method."""
    
    def test_sanitize_json_data_valid_json_strings(self, schema_repository):
        """Test sanitization of valid JSON strings."""
        data = {
            "schema_definition": '{"type": "object"}',
            "keywords": '["test", "valid"]',
            "tools": '["tool1", "tool2"]',
            "field_descriptions": '{"field1": "Description 1"}',
            "example_data": '{"example": "value"}'
        }
        
        schema_repository._sanitize_json_data(data)
        
        assert data["schema_definition"] == {"type": "object"}
        assert data["keywords"] == ["test", "valid"]
        assert data["tools"] == ["tool1", "tool2"]
        assert data["field_descriptions"] == {"field1": "Description 1"}
        assert data["example_data"] == {"example": "value"}
    
    def test_sanitize_json_data_invalid_json_strings(self, schema_repository):
        """Test sanitization of invalid JSON strings uses defaults."""
        data = {
            "schema_definition": "invalid{json",
            "keywords": "invalid[json",
            "tools": "not json",
            "field_descriptions": "invalid}json",
            "example_data": "invalid json"
        }
        
        schema_repository._sanitize_json_data(data)
        
        assert data["schema_definition"] == {}
        assert data["keywords"] == []
        assert data["tools"] == []
        assert data["field_descriptions"] == {}
        assert data["example_data"] == "invalid json"  # Left as is for example_data
    
    def test_sanitize_json_data_already_parsed(self, schema_repository):
        """Test sanitization doesn't change already parsed JSON."""
        data = {
            "schema_definition": {"type": "object"},
            "keywords": ["already", "parsed"],
            "tools": ["tool1"],
            "field_descriptions": {"field": "desc"}
        }
        
        original_data = data.copy()
        schema_repository._sanitize_json_data(data)
        
        # Should remain unchanged
        assert data == original_data
    
    def test_sanitize_json_data_missing_fields(self, schema_repository):
        """Test sanitization with missing fields."""
        data = {"name": "test_schema"}
        
        schema_repository._sanitize_json_data(data)
        
        # Should not add missing fields
        assert "schema_definition" not in data
        assert "keywords" not in data
        assert data == {"name": "test_schema"}
    
    def test_sanitize_json_data_none_values(self, schema_repository):
        """Test sanitization with None values."""
        data = {
            "schema_definition": None,
            "keywords": None,
            "tools": None
        }
        
        original_data = data.copy()
        schema_repository._sanitize_json_data(data)
        
        # None values should remain None (not strings)
        assert data == original_data


class TestSchemaRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_name(self, schema_repository, mock_async_session):
        """Test creating schema then finding it by name."""
        schema_data = {
            "name": "integration_schema",
            "schema_type": "object",
            "keywords": ["integration", "test"]
        }
        
        with patch('src.repositories.schema_repository.Schema') as mock_schema_class:
            created_schema = MockSchema(**schema_data)
            mock_schema_class.return_value = created_schema
            
            # Mock create
            with patch.object(schema_repository.__class__.__bases__[0], 'create', return_value=created_schema):
                create_result = await schema_repository.create(schema_data)
                
                # Mock find_by_name
                mock_result = MockResult([created_schema])
                mock_async_session.execute.return_value = mock_result
                
                find_result = await schema_repository.find_by_name("integration_schema")
                
                assert create_result == created_schema
                assert find_result == created_schema
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_keyword(self, schema_repository, mock_async_session):
        """Test creating schema then finding it by keyword."""
        schema_data = {
            "name": "keyword_schema",
            "keywords": ["integration", "test"]
        }
        
        with patch('src.repositories.schema_repository.Schema') as mock_schema_class:
            created_schema = MockSchema(**schema_data)
            mock_schema_class.return_value = created_schema
            
            # Mock create
            with patch.object(schema_repository.__class__.__bases__[0], 'create', return_value=created_schema):
                create_result = await schema_repository.create(schema_data)
                
                # Mock find_by_keyword
                mock_result = MockResult([created_schema])
                mock_async_session.execute.return_value = mock_result
                
                find_result = await schema_repository.find_by_keyword("integration")
                
                assert create_result == created_schema
                assert len(find_result) == 1
                assert find_result[0] == created_schema


class TestSchemaRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_database_error(self, schema_repository, mock_async_session):
        """Test find by name with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await schema_repository.find_by_name("test_schema")
    
    @pytest.mark.asyncio
    async def test_find_by_type_database_error(self, schema_repository, mock_async_session):
        """Test find by type with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await schema_repository.find_by_type("object")
    
    @pytest.mark.asyncio
    async def test_create_with_sanitization_error(self, schema_repository, mock_async_session):
        """Test create with JSON sanitization error."""
        schema_data = {"name": "error_schema"}
        
        with patch.object(schema_repository, '_sanitize_json_data', side_effect=Exception("Sanitization failed")):
            with pytest.raises(Exception, match="Sanitization failed"):
                await schema_repository.create(schema_data)
    
    @pytest.mark.asyncio
    async def test_update_with_sanitization_error(self, schema_repository, mock_async_session):
        """Test update with JSON sanitization error."""
        update_data = {"name": "updated_schema"}
        
        with patch.object(schema_repository, '_sanitize_json_data', side_effect=Exception("Sanitization failed")):
            with pytest.raises(Exception, match="Sanitization failed"):
                await schema_repository.update(1, update_data)


class TestSchemaRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_find_by_keyword_special_characters(self, schema_repository, mock_async_session):
        """Test find by keyword with special characters."""
        special_keywords = ["test@example.com", "user-name", "data_field", "100%"]
        
        for keyword in special_keywords:
            mock_result = MockResult([])
            mock_async_session.execute.return_value = mock_result
            
            result = await schema_repository.find_by_keyword(keyword)
            
            assert result == []
            mock_async_session.execute.assert_called()
    
    @pytest.mark.asyncio
    async def test_find_by_tool_case_sensitivity(self, schema_repository, mock_async_session):
        """Test find by tool case sensitivity."""
        # Test exact case match
        mock_result = MockResult([MockSchema(tools=["API"])])
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_tool("API")
        assert len(result) == 1
        
        # Test different case (should not match unless database is case-insensitive)
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await schema_repository.find_by_tool("api")
        assert len(result) == 0
    
    def test_sanitize_json_data_empty_strings(self, schema_repository):
        """Test sanitization with empty JSON strings."""
        data = {
            "schema_definition": "",
            "keywords": "",
            "tools": ""
        }
        
        schema_repository._sanitize_json_data(data)
        
        # Empty strings should result in defaults
        assert data["schema_definition"] == {}
        assert data["keywords"] == []
        assert data["tools"] == []
    
    def test_sanitize_json_data_numeric_strings(self, schema_repository):
        """Test sanitization with numeric strings that are valid JSON but wrong type."""
        data = {
            "keywords": "123",  # Valid JSON number but wrong type for keywords (should be array)
            "tools": "456",     # Valid JSON number but wrong type for tools (should be array)
            "schema_definition": "789"  # Valid JSON number but should be object
        }
        
        schema_repository._sanitize_json_data(data)
        
        # Numeric strings get parsed as numbers, not arrays/objects, but the method
        # only converts strings, so they remain as the parsed number
        assert data["keywords"] == 123  # json.loads("123") = 123
        assert data["tools"] == 456     # json.loads("456") = 456
        assert data["schema_definition"] == 789  # json.loads("789") = 789
    
    @pytest.mark.asyncio
    async def test_fallback_filtering_edge_cases(self, schema_repository, mock_async_session):
        """Test fallback filtering with edge case data."""
        edge_case_schemas = [
            MockSchema(id=1, keywords=None),  # None keywords
            MockSchema(id=2, keywords="not_a_list"),  # String instead of list
            MockSchema(id=3, keywords=[]),  # Empty list
            MockSchema(id=4, keywords=["valid", "keyword"]),  # Valid list
            MockSchema(id=5, tools={"not": "a_list"}),  # Dict instead of list for tools
        ]
        
        # Force database error to trigger fallback
        mock_async_session.execute.side_effect = [
            Exception("DB error"),
            MockResult(edge_case_schemas)
        ]
        
        with patch('logging.getLogger'):
            result = await schema_repository.find_by_keyword("keyword")
            
            # Should only return schemas with valid keyword lists containing the keyword
            assert len(result) == 1
            assert result[0].id == 4
    
    @pytest.mark.asyncio
    async def test_complex_json_structures(self, schema_repository, mock_async_session):
        """Test with complex nested JSON structures."""
        complex_schema_data = {
            "name": "complex_schema",
            "schema_definition": {
                "type": "object",
                "properties": {
                    "nested": {
                        "type": "object",
                        "properties": {
                            "deep_field": {"type": "string"}
                        }
                    },
                    "array_field": {
                        "type": "array",
                        "items": {"type": "number"}
                    }
                },
                "required": ["nested"]
            },
            "field_descriptions": {
                "nested.deep_field": "A deeply nested field",
                "array_field": "An array of numbers"
            }
        }
        
        with patch('src.repositories.schema_repository.Schema') as mock_schema_class:
            created_schema = MockSchema(**complex_schema_data)
            mock_schema_class.return_value = created_schema
            
            with patch.object(schema_repository.__class__.__bases__[0], 'create', return_value=created_schema) as mock_super_create:
                result = await schema_repository.create(complex_schema_data)
                
                assert result == created_schema
                
                # Verify complex structures were preserved
                called_data = mock_super_create.call_args[0][0]
                assert called_data["schema_definition"]["properties"]["nested"]["type"] == "object"
                assert "required" in called_data["schema_definition"]