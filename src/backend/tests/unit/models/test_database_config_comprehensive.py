"""
Comprehensive unit tests for database configuration models.

Tests the LakebaseConfig SQLAlchemy model.
"""
import pytest
from datetime import datetime
from sqlalchemy import Column, String, JSON, DateTime
from sqlalchemy.sql import func

from src.models.database_config import LakebaseConfig
from src.db.base import Base


class TestLakebaseConfig:
    """Test LakebaseConfig model."""

    def test_lakebase_config_inherits_base(self):
        """Test LakebaseConfig inherits from Base."""
        assert issubclass(LakebaseConfig, Base)

    def test_lakebase_config_tablename(self):
        """Test LakebaseConfig table name."""
        assert LakebaseConfig.__tablename__ == "database_configs"

    def test_lakebase_config_columns_exist(self):
        """Test LakebaseConfig has expected columns."""
        # Check that columns exist
        assert hasattr(LakebaseConfig, 'key')
        assert hasattr(LakebaseConfig, 'value')
        assert hasattr(LakebaseConfig, 'created_at')
        assert hasattr(LakebaseConfig, 'updated_at')

    def test_lakebase_config_key_column_properties(self):
        """Test key column properties."""
        key_column = LakebaseConfig.key
        assert isinstance(key_column.property.columns[0], Column)
        assert isinstance(key_column.property.columns[0].type, String)
        assert key_column.property.columns[0].primary_key is True
        assert key_column.property.columns[0].index is True

    def test_lakebase_config_value_column_properties(self):
        """Test value column properties."""
        value_column = LakebaseConfig.value
        assert isinstance(value_column.property.columns[0], Column)
        assert isinstance(value_column.property.columns[0].type, JSON)
        assert value_column.property.columns[0].nullable is False

    def test_lakebase_config_created_at_column_properties(self):
        """Test created_at column properties."""
        created_at_column = LakebaseConfig.created_at
        assert isinstance(created_at_column.property.columns[0], Column)
        assert isinstance(created_at_column.property.columns[0].type, DateTime)
        assert created_at_column.property.columns[0].server_default is not None

    def test_lakebase_config_updated_at_column_properties(self):
        """Test updated_at column properties."""
        updated_at_column = LakebaseConfig.updated_at
        assert isinstance(updated_at_column.property.columns[0], Column)
        assert isinstance(updated_at_column.property.columns[0].type, DateTime)
        assert updated_at_column.property.columns[0].onupdate is not None

    def test_lakebase_config_initialization(self):
        """Test LakebaseConfig initialization."""
        config = LakebaseConfig()
        
        # Should be able to create instance
        assert isinstance(config, LakebaseConfig)
        assert isinstance(config, Base)

    def test_lakebase_config_initialization_with_values(self):
        """Test LakebaseConfig initialization with values."""
        test_value = {"host": "localhost", "port": 5432}
        config = LakebaseConfig(key="test_key", value=test_value)
        
        assert config.key == "test_key"
        assert config.value == test_value

    def test_lakebase_config_repr(self):
        """Test LakebaseConfig __repr__ method."""
        config = LakebaseConfig(key="test_key")
        
        repr_str = repr(config)
        
        assert repr_str == "<DatabaseConfig(key='test_key')>"

    def test_lakebase_config_repr_with_none_key(self):
        """Test LakebaseConfig __repr__ method with None key."""
        config = LakebaseConfig()
        
        repr_str = repr(config)
        
        assert repr_str == "<DatabaseConfig(key='None')>"

    def test_lakebase_config_repr_with_empty_key(self):
        """Test LakebaseConfig __repr__ method with empty key."""
        config = LakebaseConfig(key="")
        
        repr_str = repr(config)
        
        assert repr_str == "<DatabaseConfig(key='')>"

    def test_lakebase_config_key_assignment(self):
        """Test LakebaseConfig key assignment."""
        config = LakebaseConfig()
        config.key = "new_key"
        
        assert config.key == "new_key"

    def test_lakebase_config_value_assignment(self):
        """Test LakebaseConfig value assignment."""
        config = LakebaseConfig()
        test_value = {"database": "test_db", "schema": "public"}
        config.value = test_value
        
        assert config.value == test_value

    def test_lakebase_config_value_json_types(self):
        """Test LakebaseConfig value accepts various JSON types."""
        config = LakebaseConfig()
        
        # Test dict
        dict_value = {"key": "value"}
        config.value = dict_value
        assert config.value == dict_value
        
        # Test list
        list_value = [1, 2, 3]
        config.value = list_value
        assert config.value == list_value
        
        # Test string
        string_value = "test_string"
        config.value = string_value
        assert config.value == string_value
        
        # Test number
        number_value = 42
        config.value = number_value
        assert config.value == number_value
        
        # Test boolean
        bool_value = True
        config.value = bool_value
        assert config.value == bool_value

    def test_lakebase_config_datetime_columns_timezone_aware(self):
        """Test datetime columns are timezone aware."""
        created_at_column = LakebaseConfig.created_at.property.columns[0]
        updated_at_column = LakebaseConfig.updated_at.property.columns[0]
        
        # Check that timezone is True for both datetime columns
        assert created_at_column.type.timezone is True
        assert updated_at_column.type.timezone is True

    def test_lakebase_config_created_at_server_default(self):
        """Test created_at has server default."""
        created_at_column = LakebaseConfig.created_at.property.columns[0]
        
        # Should have server_default set to func.now()
        assert created_at_column.server_default is not None
        # The server_default should be a function call
        assert hasattr(created_at_column.server_default, 'arg')

    def test_lakebase_config_updated_at_onupdate(self):
        """Test updated_at has onupdate."""
        updated_at_column = LakebaseConfig.updated_at.property.columns[0]
        
        # Should have onupdate set to func.now()
        assert updated_at_column.onupdate is not None
        # The onupdate should be a function call
        assert hasattr(updated_at_column.onupdate, 'arg')

    def test_lakebase_config_complex_json_value(self):
        """Test LakebaseConfig with complex JSON value."""
        complex_value = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "credentials": {
                    "username": "user",
                    "password": "pass"
                }
            },
            "settings": {
                "timeout": 30,
                "retry_count": 3,
                "features": ["feature1", "feature2"]
            },
            "metadata": {
                "created_by": "admin",
                "version": "1.0.0",
                "tags": ["production", "critical"]
            }
        }
        
        config = LakebaseConfig(key="complex_config", value=complex_value)
        
        assert config.key == "complex_config"
        assert config.value == complex_value
        assert config.value["database"]["host"] == "localhost"
        assert config.value["settings"]["features"] == ["feature1", "feature2"]

    def test_lakebase_config_attributes_exist(self):
        """Test all expected attributes exist on the model."""
        config = LakebaseConfig()
        
        # Test that all expected attributes exist
        expected_attributes = ['key', 'value', 'created_at', 'updated_at']
        for attr in expected_attributes:
            assert hasattr(config, attr)

    def test_lakebase_config_is_sqlalchemy_model(self):
        """Test LakebaseConfig is a proper SQLAlchemy model."""
        # Should have __table__ attribute
        assert hasattr(LakebaseConfig, '__table__')
        
        # Should have __mapper__ attribute
        assert hasattr(LakebaseConfig, '__mapper__')
        
        # Should have metadata
        assert hasattr(LakebaseConfig, 'metadata')

    def test_lakebase_config_table_columns(self):
        """Test table has expected columns."""
        table = LakebaseConfig.__table__
        column_names = [col.name for col in table.columns]
        
        expected_columns = ['key', 'value', 'created_at', 'updated_at']
        for col_name in expected_columns:
            assert col_name in column_names

    def test_lakebase_config_primary_key(self):
        """Test primary key configuration."""
        table = LakebaseConfig.__table__
        primary_key_columns = [col.name for col in table.primary_key.columns]
        
        assert primary_key_columns == ['key']

    def test_lakebase_config_indexes(self):
        """Test index configuration."""
        table = LakebaseConfig.__table__
        
        # Check that key column has an index
        key_column = table.columns['key']
        assert key_column.index is True
