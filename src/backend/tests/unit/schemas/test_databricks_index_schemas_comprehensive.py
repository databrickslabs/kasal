"""
Comprehensive unit tests for Databricks Index Schemas.

Tests MemoryType enum and DatabricksIndexSchemas class methods.
"""
import pytest
from typing import Dict, List, Any

from src.schemas.databricks_index_schemas import (
    MemoryType,
    DatabricksIndexSchemas
)


class TestMemoryType:
    """Test MemoryType enumeration."""

    def test_memory_type_enum_values(self):
        """Test MemoryType has expected values."""
        assert MemoryType.SHORT_TERM.value == "short_term"
        assert MemoryType.LONG_TERM.value == "long_term"
        assert MemoryType.ENTITY.value == "entity"
        assert MemoryType.DOCUMENT.value == "document"

    def test_memory_type_enum_all_values(self):
        """Test MemoryType contains all expected values."""
        expected_values = {"short_term", "long_term", "entity", "document"}
        actual_values = {item.value for item in MemoryType}
        
        assert actual_values == expected_values

    def test_memory_type_enum_iteration(self):
        """Test MemoryType can be iterated."""
        enum_values = list(MemoryType)
        
        assert len(enum_values) == 4
        assert MemoryType.SHORT_TERM in enum_values
        assert MemoryType.LONG_TERM in enum_values
        assert MemoryType.ENTITY in enum_values
        assert MemoryType.DOCUMENT in enum_values

    def test_memory_type_enum_string_representation(self):
        """Test MemoryType string representation."""
        assert str(MemoryType.SHORT_TERM) == "MemoryType.SHORT_TERM"
        assert str(MemoryType.LONG_TERM) == "MemoryType.LONG_TERM"
        assert str(MemoryType.ENTITY) == "MemoryType.ENTITY"
        assert str(MemoryType.DOCUMENT) == "MemoryType.DOCUMENT"


class TestDatabricksIndexSchemas:
    """Test DatabricksIndexSchemas class."""

    def test_databricks_index_schemas_has_schema_constants(self):
        """Test DatabricksIndexSchemas has all schema constants."""
        assert hasattr(DatabricksIndexSchemas, 'SHORT_TERM_SCHEMA')
        assert hasattr(DatabricksIndexSchemas, 'LONG_TERM_SCHEMA')
        assert hasattr(DatabricksIndexSchemas, 'ENTITY_SCHEMA')
        assert hasattr(DatabricksIndexSchemas, 'DOCUMENT_SCHEMA')

    def test_databricks_index_schemas_has_search_columns_constants(self):
        """Test DatabricksIndexSchemas has all search columns constants."""
        assert hasattr(DatabricksIndexSchemas, 'SHORT_TERM_SEARCH_COLUMNS')
        assert hasattr(DatabricksIndexSchemas, 'LONG_TERM_SEARCH_COLUMNS')
        assert hasattr(DatabricksIndexSchemas, 'ENTITY_SEARCH_COLUMNS')
        assert hasattr(DatabricksIndexSchemas, 'DOCUMENT_SEARCH_COLUMNS')

    def test_short_term_schema_structure(self):
        """Test SHORT_TERM_SCHEMA has expected structure."""
        schema = DatabricksIndexSchemas.SHORT_TERM_SCHEMA
        
        # Check required fields exist
        required_fields = [
            'id', 'content', 'query_text', 'session_id', 'interaction_sequence',
            'timestamp', 'created_at', 'ttl_hours', 'metadata', 'crew_id',
            'agent_id', 'group_id', 'llm_model', 'tools_used', 'embedding',
            'embedding_model', 'version'
        ]
        
        for field in required_fields:
            assert field in schema
            assert isinstance(schema[field], str)

    def test_long_term_schema_structure(self):
        """Test LONG_TERM_SCHEMA has expected structure."""
        schema = DatabricksIndexSchemas.LONG_TERM_SCHEMA
        
        # Check required fields exist
        required_fields = [
            'id', 'content', 'task_description', 'task_hash', 'quality',
            'importance', 'timestamp', 'last_accessed', 'metadata', 'crew_id',
            'agent_id', 'group_id', 'llm_model', 'tools_used', 'embedding',
            'embedding_model', 'version'
        ]
        
        for field in required_fields:
            assert field in schema
            assert isinstance(schema[field], str)

    def test_entity_schema_structure(self):
        """Test ENTITY_SCHEMA has expected structure."""
        schema = DatabricksIndexSchemas.ENTITY_SCHEMA
        
        # Check required fields exist
        required_fields = [
            'id', 'entity_name', 'entity_type', 'description', 'relationships',
            'timestamp', 'crew_id', 'agent_id', 'group_id', 'llm_model',
            'tools_used', 'embedding', 'embedding_model'
        ]
        
        for field in required_fields:
            assert field in schema
            assert isinstance(schema[field], str)

    def test_document_schema_structure(self):
        """Test DOCUMENT_SCHEMA has expected structure."""
        schema = DatabricksIndexSchemas.DOCUMENT_SCHEMA
        
        # Check required fields exist
        required_fields = [
            'id', 'title', 'content', 'source', 'document_type', 'section',
            'chunk_index', 'chunk_size', 'parent_document_id', 'document_summary',
            'agent_ids', 'created_at', 'updated_at', 'doc_metadata', 'group_id',
            'embedding', 'embedding_model', 'version'
        ]
        
        for field in required_fields:
            assert field in schema
            assert isinstance(schema[field], str)

    def test_search_columns_are_lists(self):
        """Test all search columns constants are lists."""
        assert isinstance(DatabricksIndexSchemas.SHORT_TERM_SEARCH_COLUMNS, list)
        assert isinstance(DatabricksIndexSchemas.LONG_TERM_SEARCH_COLUMNS, list)
        assert isinstance(DatabricksIndexSchemas.ENTITY_SEARCH_COLUMNS, list)
        assert isinstance(DatabricksIndexSchemas.DOCUMENT_SEARCH_COLUMNS, list)

    def test_search_columns_not_empty(self):
        """Test all search columns lists are not empty."""
        assert len(DatabricksIndexSchemas.SHORT_TERM_SEARCH_COLUMNS) > 0
        assert len(DatabricksIndexSchemas.LONG_TERM_SEARCH_COLUMNS) > 0
        assert len(DatabricksIndexSchemas.ENTITY_SEARCH_COLUMNS) > 0
        assert len(DatabricksIndexSchemas.DOCUMENT_SEARCH_COLUMNS) > 0

    def test_get_schema_short_term(self):
        """Test get_schema for short_term memory type."""
        result = DatabricksIndexSchemas.get_schema("short_term")
        
        assert result == DatabricksIndexSchemas.SHORT_TERM_SCHEMA
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_schema_long_term(self):
        """Test get_schema for long_term memory type."""
        result = DatabricksIndexSchemas.get_schema("long_term")
        
        assert result == DatabricksIndexSchemas.LONG_TERM_SCHEMA
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_schema_entity(self):
        """Test get_schema for entity memory type."""
        result = DatabricksIndexSchemas.get_schema("entity")
        
        assert result == DatabricksIndexSchemas.ENTITY_SCHEMA
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_schema_document(self):
        """Test get_schema for document memory type."""
        result = DatabricksIndexSchemas.get_schema("document")
        
        assert result == DatabricksIndexSchemas.DOCUMENT_SCHEMA
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_schema_unknown_type(self):
        """Test get_schema for unknown memory type."""
        result = DatabricksIndexSchemas.get_schema("unknown")
        
        assert result == {}
        assert isinstance(result, dict)

    def test_get_schema_none_type(self):
        """Test get_schema with None memory type."""
        result = DatabricksIndexSchemas.get_schema(None)
        
        assert result == {}
        assert isinstance(result, dict)

    def test_get_search_columns_short_term(self):
        """Test get_search_columns for short_term memory type."""
        result = DatabricksIndexSchemas.get_search_columns("short_term")
        
        assert result == DatabricksIndexSchemas.SHORT_TERM_SEARCH_COLUMNS
        assert isinstance(result, list)
        assert len(result) > 0

    def test_get_search_columns_long_term(self):
        """Test get_search_columns for long_term memory type."""
        result = DatabricksIndexSchemas.get_search_columns("long_term")
        
        assert result == DatabricksIndexSchemas.LONG_TERM_SEARCH_COLUMNS
        assert isinstance(result, list)
        assert len(result) > 0

    def test_get_search_columns_entity(self):
        """Test get_search_columns for entity memory type."""
        result = DatabricksIndexSchemas.get_search_columns("entity")
        
        assert result == DatabricksIndexSchemas.ENTITY_SEARCH_COLUMNS
        assert isinstance(result, list)
        assert len(result) > 0

    def test_get_search_columns_document(self):
        """Test get_search_columns for document memory type."""
        result = DatabricksIndexSchemas.get_search_columns("document")
        
        assert result == DatabricksIndexSchemas.DOCUMENT_SEARCH_COLUMNS
        assert isinstance(result, list)
        assert len(result) > 0

    def test_get_search_columns_unknown_type(self):
        """Test get_search_columns for unknown memory type."""
        result = DatabricksIndexSchemas.get_search_columns("unknown")
        
        assert result == ["id"]
        assert isinstance(result, list)

    def test_get_search_columns_none_type(self):
        """Test get_search_columns with None memory type."""
        result = DatabricksIndexSchemas.get_search_columns(None)
        
        assert result == ["id"]
        assert isinstance(result, list)

    def test_get_column_positions_short_term(self):
        """Test get_column_positions for short_term memory type."""
        result = DatabricksIndexSchemas.get_column_positions("short_term")
        
        expected_columns = DatabricksIndexSchemas.SHORT_TERM_SEARCH_COLUMNS
        assert isinstance(result, dict)
        assert len(result) == len(expected_columns)
        
        for idx, col in enumerate(expected_columns):
            assert col in result
            assert result[col] == idx

    def test_get_column_positions_long_term(self):
        """Test get_column_positions for long_term memory type."""
        result = DatabricksIndexSchemas.get_column_positions("long_term")
        
        expected_columns = DatabricksIndexSchemas.LONG_TERM_SEARCH_COLUMNS
        assert isinstance(result, dict)
        assert len(result) == len(expected_columns)
        
        for idx, col in enumerate(expected_columns):
            assert col in result
            assert result[col] == idx

    def test_get_column_positions_entity(self):
        """Test get_column_positions for entity memory type."""
        result = DatabricksIndexSchemas.get_column_positions("entity")
        
        expected_columns = DatabricksIndexSchemas.ENTITY_SEARCH_COLUMNS
        assert isinstance(result, dict)
        assert len(result) == len(expected_columns)
        
        for idx, col in enumerate(expected_columns):
            assert col in result
            assert result[col] == idx

    def test_get_column_positions_document(self):
        """Test get_column_positions for document memory type."""
        result = DatabricksIndexSchemas.get_column_positions("document")
        
        expected_columns = DatabricksIndexSchemas.DOCUMENT_SEARCH_COLUMNS
        assert isinstance(result, dict)
        assert len(result) == len(expected_columns)
        
        for idx, col in enumerate(expected_columns):
            assert col in result
            assert result[col] == idx

    def test_get_column_positions_unknown_type(self):
        """Test get_column_positions for unknown memory type."""
        result = DatabricksIndexSchemas.get_column_positions("unknown")
        
        assert result == {"id": 0}
        assert isinstance(result, dict)

    def test_parse_search_result_short_term(self):
        """Test parse_search_result for short_term memory type."""
        test_result = ["id1", "content1", "query1", "session1", 1]
        
        result = DatabricksIndexSchemas.parse_search_result("short_term", test_result)
        
        expected_columns = DatabricksIndexSchemas.SHORT_TERM_SEARCH_COLUMNS
        assert isinstance(result, dict)
        
        for idx, value in enumerate(test_result):
            if idx < len(expected_columns):
                column_name = expected_columns[idx]
                assert column_name in result
                assert result[column_name] == value

    def test_parse_search_result_long_term(self):
        """Test parse_search_result for long_term memory type."""
        test_result = ["id1", "content1", "task1", "hash1", 0.8]
        
        result = DatabricksIndexSchemas.parse_search_result("long_term", test_result)
        
        expected_columns = DatabricksIndexSchemas.LONG_TERM_SEARCH_COLUMNS
        assert isinstance(result, dict)
        
        for idx, value in enumerate(test_result):
            if idx < len(expected_columns):
                column_name = expected_columns[idx]
                assert column_name in result
                assert result[column_name] == value

    def test_parse_search_result_entity(self):
        """Test parse_search_result for entity memory type."""
        test_result = ["id1", "entity1", "type1", "description1"]
        
        result = DatabricksIndexSchemas.parse_search_result("entity", test_result)
        
        expected_columns = DatabricksIndexSchemas.ENTITY_SEARCH_COLUMNS
        assert isinstance(result, dict)
        
        for idx, value in enumerate(test_result):
            if idx < len(expected_columns):
                column_name = expected_columns[idx]
                assert column_name in result
                assert result[column_name] == value

    def test_parse_search_result_document(self):
        """Test parse_search_result for document memory type."""
        test_result = ["id1", "title1", "content1", "source1", "pdf"]
        
        result = DatabricksIndexSchemas.parse_search_result("document", test_result)
        
        expected_columns = DatabricksIndexSchemas.DOCUMENT_SEARCH_COLUMNS
        assert isinstance(result, dict)
        
        for idx, value in enumerate(test_result):
            if idx < len(expected_columns):
                column_name = expected_columns[idx]
                assert column_name in result
                assert result[column_name] == value

    def test_parse_search_result_empty_result(self):
        """Test parse_search_result with empty result."""
        result = DatabricksIndexSchemas.parse_search_result("short_term", [])
        
        assert result == {}
        assert isinstance(result, dict)

    def test_parse_search_result_more_values_than_columns(self):
        """Test parse_search_result with more values than columns."""
        # Create a result with more values than columns
        columns = DatabricksIndexSchemas.get_search_columns("entity")
        test_result = ["value"] * (len(columns) + 5)  # 5 extra values
        
        result = DatabricksIndexSchemas.parse_search_result("entity", test_result)
        
        # Should only map values up to the number of columns
        assert len(result) == len(columns)
        for col in columns:
            assert col in result
            assert result[col] == "value"

    def test_parse_search_result_fewer_values_than_columns(self):
        """Test parse_search_result with fewer values than columns."""
        test_result = ["id1", "content1"]  # Only 2 values
        
        result = DatabricksIndexSchemas.parse_search_result("short_term", test_result)
        
        # Should only map the available values
        assert len(result) == 2
        assert result["id"] == "id1"
        assert result["content"] == "content1"
