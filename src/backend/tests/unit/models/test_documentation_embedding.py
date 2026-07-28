"""
Comprehensive unit tests for DocumentationEmbedding model and Vector type.

Tests SQLAlchemy model attributes, Vector type behavior, and database operations.
"""
import pytest
import json
from datetime import datetime
from unittest.mock import Mock, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import sqlite, postgresql

from src.models.documentation_embedding import DocumentationEmbedding, Vector
from src.db.base import Base


class TestVector:
    """Test Vector custom type."""

    def test_vector_init_default_dim(self):
        """Test Vector initialization with default dimension."""
        vector = Vector()
        
        assert vector.dim == 1024

    def test_vector_init_custom_dim(self):
        """Test Vector initialization with custom dimension."""
        vector = Vector(dim=512)
        
        assert vector.dim == 512

    def test_get_col_spec_postgresql(self):
        """Test get_col_spec for PostgreSQL."""
        vector = Vector(dim=768)
        
        # Mock PostgreSQL dialect
        vector.dialect = Mock()
        vector.dialect.__str__ = Mock(return_value="postgresql")
        
        result = vector.get_col_spec()
        
        assert result == "vector(768)"

    def test_get_col_spec_sqlite(self):
        """Test get_col_spec for SQLite."""
        vector = Vector(dim=1024)
        
        # Mock SQLite dialect
        vector.dialect = Mock()
        vector.dialect.__str__ = Mock(return_value="sqlite")
        
        result = vector.get_col_spec()
        
        assert result == "TEXT"

    def test_get_col_spec_no_dialect(self):
        """Test get_col_spec without dialect (defaults to PostgreSQL)."""
        vector = Vector(dim=256)
        
        result = vector.get_col_spec()
        
        assert result == "vector(256)"

    def test_bind_processor_sqlite_list(self):
        """Test bind_processor for SQLite with list input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "sqlite"
        
        processor = vector.bind_processor(dialect)
        test_list = [0.1, 0.2, 0.3, 0.4, 0.5]
        
        result = processor(test_list)
        
        assert result == json.dumps(test_list)
        assert isinstance(result, str)

    def test_bind_processor_sqlite_none(self):
        """Test bind_processor for SQLite with None input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "sqlite"
        
        processor = vector.bind_processor(dialect)
        
        result = processor(None)
        
        assert result is None

    def test_bind_processor_sqlite_string(self):
        """Test bind_processor for SQLite with string input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "sqlite"
        
        processor = vector.bind_processor(dialect)
        test_string = "[0.1,0.2,0.3]"
        
        result = processor(test_string)
        
        assert result == test_string

    def test_bind_processor_postgresql_list(self):
        """Test bind_processor for PostgreSQL with list input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "postgresql"
        
        processor = vector.bind_processor(dialect)
        test_list = [0.1, 0.2, 0.3]
        
        result = processor(test_list)
        
        assert result == "[0.1,0.2,0.3]"

    def test_bind_processor_postgresql_none(self):
        """Test bind_processor for PostgreSQL with None input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "postgresql"
        
        processor = vector.bind_processor(dialect)
        
        result = processor(None)
        
        assert result is None

    def test_bind_processor_postgresql_string(self):
        """Test bind_processor for PostgreSQL with string input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "postgresql"
        
        processor = vector.bind_processor(dialect)
        test_string = "[0.1,0.2,0.3]"
        
        result = processor(test_string)
        
        assert result == test_string

    def test_result_processor_sqlite_json_string(self):
        """Test result_processor for SQLite with JSON string."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "sqlite"
        
        processor = vector.result_processor(dialect, None)
        test_json = json.dumps([0.1, 0.2, 0.3])
        
        result = processor(test_json)
        
        assert result == [0.1, 0.2, 0.3]
        assert isinstance(result, list)

    def test_result_processor_sqlite_invalid_json(self):
        """Test result_processor for SQLite with invalid JSON."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "sqlite"
        
        processor = vector.result_processor(dialect, None)
        invalid_json = "invalid json string"
        
        result = processor(invalid_json)
        
        assert result == invalid_json

    def test_result_processor_sqlite_none(self):
        """Test result_processor for SQLite with None input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "sqlite"
        
        processor = vector.result_processor(dialect, None)
        
        result = processor(None)
        
        assert result is None

    def test_result_processor_postgresql(self):
        """Test result_processor for PostgreSQL."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "postgresql"
        
        processor = vector.result_processor(dialect, None)
        test_value = "[0.1,0.2,0.3]"
        
        result = processor(test_value)
        
        assert result == test_value

    def test_result_processor_postgresql_none(self):
        """Test result_processor for PostgreSQL with None input."""
        vector = Vector()
        dialect = Mock()
        dialect.name = "postgresql"
        
        processor = vector.result_processor(dialect, None)
        
        result = processor(None)
        
        assert result is None


class TestDocumentationEmbedding:
    """Test DocumentationEmbedding model."""

    def test_documentation_embedding_table_name(self):
        """Test DocumentationEmbedding table name."""
        assert DocumentationEmbedding.__tablename__ == "documentation_embeddings"

    def test_documentation_embedding_columns(self):
        """Test DocumentationEmbedding has expected columns."""
        expected_columns = [
            'id', 'source', 'title', 'content', 'embedding', 
            'doc_metadata', 'created_at', 'updated_at'
        ]
        
        actual_columns = list(DocumentationEmbedding.__table__.columns.keys())
        
        for col in expected_columns:
            assert col in actual_columns

    def test_documentation_embedding_primary_key(self):
        """Test DocumentationEmbedding primary key."""
        id_column = DocumentationEmbedding.__table__.columns['id']
        
        assert id_column.primary_key is True
        assert id_column.index is True

    def test_documentation_embedding_indexes(self):
        """Test DocumentationEmbedding indexed columns."""
        source_column = DocumentationEmbedding.__table__.columns['source']
        title_column = DocumentationEmbedding.__table__.columns['title']
        
        assert source_column.index is True
        assert title_column.index is True

    def test_documentation_embedding_nullable_constraints(self):
        """Test DocumentationEmbedding nullable constraints."""
        columns = DocumentationEmbedding.__table__.columns
        
        # Required fields
        assert columns['source'].nullable is False
        assert columns['title'].nullable is False
        assert columns['content'].nullable is False
        assert columns['embedding'].nullable is False
        
        # Optional fields
        assert columns['doc_metadata'].nullable is True

    def test_documentation_embedding_repr(self):
        """Test DocumentationEmbedding __repr__ method."""
        embedding = DocumentationEmbedding()
        embedding.id = 1
        embedding.source = "test_source"
        embedding.title = "Test Title"
        
        result = repr(embedding)
        
        assert result == "DocumentationEmbedding(id=1, source=test_source, title=Test Title)"

    def test_documentation_embedding_repr_none_values(self):
        """Test DocumentationEmbedding __repr__ with None values."""
        embedding = DocumentationEmbedding()
        embedding.id = None
        embedding.source = None
        embedding.title = None
        
        result = repr(embedding)
        
        assert result == "DocumentationEmbedding(id=None, source=None, title=None)"

    def test_documentation_embedding_vector_column_type(self):
        """Test DocumentationEmbedding embedding column uses Vector type."""
        embedding_column = DocumentationEmbedding.__table__.columns['embedding']
        
        assert isinstance(embedding_column.type, Vector)
        assert embedding_column.type.dim == 1024

    def test_documentation_embedding_inherits_base(self):
        """Test DocumentationEmbedding inherits from Base."""
        assert issubclass(DocumentationEmbedding, Base)

    def test_documentation_embedding_datetime_columns(self):
        """Test DocumentationEmbedding datetime columns have server defaults."""
        columns = DocumentationEmbedding.__table__.columns
        
        created_at = columns['created_at']
        updated_at = columns['updated_at']
        
        assert created_at.server_default is not None
        assert updated_at.server_default is not None
        assert updated_at.onupdate is not None

    def test_documentation_embedding_json_column(self):
        """Test DocumentationEmbedding doc_metadata is JSON type."""
        doc_metadata_column = DocumentationEmbedding.__table__.columns['doc_metadata']
        
        # Check if it's a JSON type (implementation may vary)
        assert hasattr(doc_metadata_column.type, 'python_type') or 'JSON' in str(type(doc_metadata_column.type))

    def test_documentation_embedding_text_column(self):
        """Test DocumentationEmbedding content is Text type."""
        content_column = DocumentationEmbedding.__table__.columns['content']
        
        # Check if it's a Text type
        assert 'TEXT' in str(type(content_column.type)).upper()

    def test_documentation_embedding_string_columns(self):
        """Test DocumentationEmbedding string columns."""
        columns = DocumentationEmbedding.__table__.columns
        
        source_column = columns['source']
        title_column = columns['title']
        
        # Check if they are String types
        assert 'STRING' in str(type(source_column.type)).upper() or 'VARCHAR' in str(type(source_column.type)).upper()
        assert 'STRING' in str(type(title_column.type)).upper() or 'VARCHAR' in str(type(title_column.type)).upper()
