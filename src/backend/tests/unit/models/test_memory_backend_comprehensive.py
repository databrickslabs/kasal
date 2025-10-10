"""
Comprehensive unit tests for MemoryBackend model and related utilities.

Tests SQLAlchemy model attributes, enum values, utility functions, and methods.
"""
import pytest
from datetime import datetime
from unittest.mock import patch, Mock
from uuid import UUID

from src.models.memory_backend import (
    MemoryBackend,
    MemoryBackendTypeEnum,
    generate_uuid
)
from src.db.base import Base


class TestGenerateUuid:
    """Test generate_uuid utility function."""

    def test_generate_uuid_returns_string(self):
        """Test generate_uuid returns a string."""
        result = generate_uuid()
        
        assert isinstance(result, str)

    def test_generate_uuid_is_valid_uuid(self):
        """Test generate_uuid returns a valid UUID string."""
        result = generate_uuid()
        
        # Should be able to parse as UUID
        uuid_obj = UUID(result)
        assert str(uuid_obj) == result

    def test_generate_uuid_unique_values(self):
        """Test generate_uuid returns unique values."""
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        assert uuid1 != uuid2

    def test_generate_uuid_format(self):
        """Test generate_uuid returns properly formatted UUID."""
        result = generate_uuid()
        
        # UUID format: 8-4-4-4-12 characters
        parts = result.split('-')
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4
        assert len(parts[2]) == 4
        assert len(parts[3]) == 4
        assert len(parts[4]) == 12


class TestMemoryBackendTypeEnum:
    """Test MemoryBackendTypeEnum enumeration."""

    def test_memory_backend_type_enum_values(self):
        """Test MemoryBackendTypeEnum has expected values."""
        assert MemoryBackendTypeEnum.DEFAULT == "default"
        assert MemoryBackendTypeEnum.DATABRICKS == "databricks"

    def test_memory_backend_type_enum_is_string_enum(self):
        """Test MemoryBackendTypeEnum inherits from str."""
        assert isinstance(MemoryBackendTypeEnum.DEFAULT, str)
        assert isinstance(MemoryBackendTypeEnum.DATABRICKS, str)

    def test_memory_backend_type_enum_all_values(self):
        """Test MemoryBackendTypeEnum contains all expected values."""
        expected_values = {"default", "databricks"}
        actual_values = {item.value for item in MemoryBackendTypeEnum}
        
        assert actual_values == expected_values

    def test_memory_backend_type_enum_iteration(self):
        """Test MemoryBackendTypeEnum can be iterated."""
        enum_values = list(MemoryBackendTypeEnum)
        
        assert len(enum_values) == 2
        assert MemoryBackendTypeEnum.DEFAULT in enum_values
        assert MemoryBackendTypeEnum.DATABRICKS in enum_values


class TestMemoryBackend:
    """Test MemoryBackend model."""

    def test_memory_backend_table_name(self):
        """Test MemoryBackend table name."""
        assert MemoryBackend.__tablename__ == "memory_backends"

    def test_memory_backend_inherits_base(self):
        """Test MemoryBackend inherits from Base."""
        assert issubclass(MemoryBackend, Base)

    def test_memory_backend_columns(self):
        """Test MemoryBackend has expected columns."""
        expected_columns = [
            'id', 'group_id', 'name', 'description', 'backend_type',
            'databricks_config', 'enable_short_term', 'enable_long_term',
            'enable_entity', 'enable_relationship_retrieval', 'custom_config',
            'is_active', 'is_default', 'created_at', 'updated_at'
        ]
        
        actual_columns = list(MemoryBackend.__table__.columns.keys())
        
        for col in expected_columns:
            assert col in actual_columns

    def test_memory_backend_primary_key(self):
        """Test MemoryBackend primary key."""
        id_column = MemoryBackend.__table__.columns['id']
        
        assert id_column.primary_key is True

    def test_memory_backend_id_default(self):
        """Test MemoryBackend id column has generate_uuid default."""
        id_column = MemoryBackend.__table__.columns['id']
        
        assert id_column.default is not None

    def test_memory_backend_group_id_indexed(self):
        """Test MemoryBackend group_id is indexed."""
        group_id_column = MemoryBackend.__table__.columns['group_id']
        
        assert group_id_column.index is True
        assert group_id_column.nullable is False

    def test_memory_backend_nullable_constraints(self):
        """Test MemoryBackend nullable constraints."""
        columns = MemoryBackend.__table__.columns
        
        # Required fields
        assert columns['group_id'].nullable is False
        assert columns['name'].nullable is False
        assert columns['backend_type'].nullable is False
        
        # Optional fields
        assert columns['description'].nullable is True
        assert columns['databricks_config'].nullable is True
        assert columns['custom_config'].nullable is True

    def test_memory_backend_default_values(self):
        """Test MemoryBackend column default values."""
        columns = MemoryBackend.__table__.columns
        
        # Boolean defaults
        assert columns['enable_short_term'].default.arg is True
        assert columns['enable_long_term'].default.arg is True
        assert columns['enable_entity'].default.arg is True
        assert columns['enable_relationship_retrieval'].default.arg is False
        assert columns['is_active'].default.arg is True
        assert columns['is_default'].default.arg is False

    def test_memory_backend_backend_type_enum_default(self):
        """Test MemoryBackend backend_type has enum default."""
        backend_type_column = MemoryBackend.__table__.columns['backend_type']
        
        assert backend_type_column.default.arg == MemoryBackendTypeEnum.DEFAULT

    def test_memory_backend_datetime_defaults(self):
        """Test MemoryBackend datetime columns have defaults."""
        columns = MemoryBackend.__table__.columns
        
        created_at = columns['created_at']
        updated_at = columns['updated_at']
        
        assert created_at.default is not None
        assert updated_at.default is not None
        assert updated_at.onupdate is not None

    def test_memory_backend_to_dict_minimal(self):
        """Test MemoryBackend to_dict with minimal data."""
        backend = MemoryBackend()
        backend.id = "test-id"
        backend.group_id = "test-group"
        backend.name = "Test Backend"
        backend.description = None
        backend.backend_type = MemoryBackendTypeEnum.DEFAULT
        backend.databricks_config = None
        backend.enable_short_term = True
        backend.enable_long_term = True
        backend.enable_entity = True
        backend.enable_relationship_retrieval = False
        backend.custom_config = None
        backend.is_active = True
        backend.is_default = False
        backend.created_at = None
        backend.updated_at = None
        
        result = backend.to_dict()
        
        expected = {
            "id": "test-id",
            "group_id": "test-group",
            "name": "Test Backend",
            "description": None,
            "backend_type": "default",
            "databricks_config": None,
            "enable_short_term": True,
            "enable_long_term": True,
            "enable_entity": True,
            "enable_relationship_retrieval": False,
            "custom_config": None,
            "is_active": True,
            "is_default": False,
            "created_at": None,
            "updated_at": None,
        }
        
        assert result == expected

    def test_memory_backend_to_dict_full(self):
        """Test MemoryBackend to_dict with full data."""
        now = datetime(2023, 1, 1, 12, 0, 0)
        backend = MemoryBackend()
        backend.id = "test-id"
        backend.group_id = "test-group"
        backend.name = "Test Backend"
        backend.description = "Test description"
        backend.backend_type = MemoryBackendTypeEnum.DATABRICKS
        backend.databricks_config = {"endpoint": "test"}
        backend.enable_short_term = False
        backend.enable_long_term = False
        backend.enable_entity = False
        backend.enable_relationship_retrieval = True
        backend.custom_config = {"custom": "value"}
        backend.is_active = False
        backend.is_default = True
        backend.created_at = now
        backend.updated_at = now
        
        result = backend.to_dict()
        
        expected = {
            "id": "test-id",
            "group_id": "test-group",
            "name": "Test Backend",
            "description": "Test description",
            "backend_type": "databricks",
            "databricks_config": {"endpoint": "test"},
            "enable_short_term": False,
            "enable_long_term": False,
            "enable_entity": False,
            "enable_relationship_retrieval": True,
            "custom_config": {"custom": "value"},
            "is_active": False,
            "is_default": True,
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:00:00",
        }
        
        assert result == expected

    def test_memory_backend_to_dict_none_backend_type(self):
        """Test MemoryBackend to_dict with None backend_type."""
        backend = MemoryBackend()
        backend.id = "test-id"
        backend.group_id = "test-group"
        backend.name = "Test Backend"
        backend.backend_type = None
        
        result = backend.to_dict()
        
        assert result["backend_type"] is None

    def test_memory_backend_to_config_dict_default(self):
        """Test MemoryBackend to_config_dict with default backend."""
        backend = MemoryBackend()
        backend.backend_type = MemoryBackendTypeEnum.DEFAULT
        backend.enable_short_term = True
        backend.enable_long_term = False
        backend.enable_entity = True
        backend.enable_relationship_retrieval = False
        backend.databricks_config = None
        backend.custom_config = None
        
        result = backend.to_config_dict()
        
        expected = {
            "backend_type": "default",
            "enable_short_term": True,
            "enable_long_term": False,
            "enable_entity": True,
            "enable_relationship_retrieval": False,
        }
        
        assert result == expected

    def test_memory_backend_to_config_dict_databricks(self):
        """Test MemoryBackend to_config_dict with Databricks backend."""
        databricks_config = {"endpoint": "test", "index": "test-index"}
        backend = MemoryBackend()
        backend.backend_type = MemoryBackendTypeEnum.DATABRICKS
        backend.enable_short_term = True
        backend.enable_long_term = True
        backend.enable_entity = False
        backend.enable_relationship_retrieval = True
        backend.databricks_config = databricks_config
        backend.custom_config = None
        
        result = backend.to_config_dict()
        
        expected = {
            "backend_type": "databricks",
            "enable_short_term": True,
            "enable_long_term": True,
            "enable_entity": False,
            "enable_relationship_retrieval": True,
            "databricks_config": databricks_config,
        }
        
        assert result == expected

    def test_memory_backend_to_config_dict_with_custom_config(self):
        """Test MemoryBackend to_config_dict with custom config."""
        custom_config = {"custom_setting": "value", "another": 42}
        backend = MemoryBackend()
        backend.backend_type = MemoryBackendTypeEnum.DEFAULT
        backend.enable_short_term = True
        backend.enable_long_term = True
        backend.enable_entity = True
        backend.enable_relationship_retrieval = False
        backend.databricks_config = None
        backend.custom_config = custom_config
        
        result = backend.to_config_dict()
        
        expected = {
            "backend_type": "default",
            "enable_short_term": True,
            "enable_long_term": True,
            "enable_entity": True,
            "enable_relationship_retrieval": False,
            "custom_config": custom_config,
        }
        
        assert result == expected

    def test_memory_backend_to_config_dict_databricks_no_config(self):
        """Test MemoryBackend to_config_dict with Databricks but no config."""
        backend = MemoryBackend()
        backend.backend_type = MemoryBackendTypeEnum.DATABRICKS
        backend.enable_short_term = True
        backend.enable_long_term = True
        backend.enable_entity = True
        backend.enable_relationship_retrieval = False
        backend.databricks_config = None
        backend.custom_config = None
        
        result = backend.to_config_dict()
        
        expected = {
            "backend_type": "databricks",
            "enable_short_term": True,
            "enable_long_term": True,
            "enable_entity": True,
            "enable_relationship_retrieval": False,
        }
        
        assert result == expected

    def test_memory_backend_to_config_dict_none_backend_type(self):
        """Test MemoryBackend to_config_dict with None backend_type."""
        backend = MemoryBackend()
        backend.backend_type = None
        backend.enable_short_term = True
        backend.enable_long_term = True
        backend.enable_entity = True
        backend.enable_relationship_retrieval = False
        
        result = backend.to_config_dict()
        
        assert result["backend_type"] == "default"
