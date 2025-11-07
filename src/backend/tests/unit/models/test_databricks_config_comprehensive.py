"""
Comprehensive unit tests for DatabricksConfig SQLAlchemy model.

Tests all aspects of the DatabricksConfig model including table structure and configuration fields.
"""
import pytest
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from src.models.databricks_config import DatabricksConfig
from src.db.base import Base


class TestDatabricksConfig:
    """Test DatabricksConfig model."""

    def test_databricks_config_inherits_base(self):
        """Test DatabricksConfig inherits from Base."""
        assert issubclass(DatabricksConfig, Base)

    def test_databricks_config_tablename(self):
        """Test DatabricksConfig table name."""
        assert DatabricksConfig.__tablename__ == "databricksconfig"

    def test_databricks_config_columns_exist(self):
        """Test DatabricksConfig has expected columns."""
        expected_columns = [
            'id', 'workspace_url', 'warehouse_id', 'catalog', 'schema', 'is_active',
            'is_enabled', 'encrypted_personal_access_token', 'mlflow_enabled',
            'evaluation_enabled', 'evaluation_judge_model', 'group_id', 'created_by_email',
            'volume_enabled', 'volume_path', 'volume_file_format', 'volume_create_date_dirs',
            'knowledge_volume_enabled', 'knowledge_volume_path', 'knowledge_chunk_size',
            'knowledge_chunk_overlap', 'created_at', 'updated_at'
        ]
        
        for column_name in expected_columns:
            assert hasattr(DatabricksConfig, column_name)

    def test_databricks_config_id_column_properties(self):
        """Test id column properties."""
        id_column = DatabricksConfig.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, Integer)
        assert id_column.property.columns[0].primary_key is True

    def test_databricks_config_required_columns_properties(self):
        """Test required column properties."""
        warehouse_id_column = DatabricksConfig.warehouse_id
        assert warehouse_id_column.property.columns[0].nullable is False
        
        catalog_column = DatabricksConfig.catalog
        assert catalog_column.property.columns[0].nullable is False
        
        schema_column = DatabricksConfig.schema
        assert schema_column.property.columns[0].nullable is False

    def test_databricks_config_workspace_url_column_properties(self):
        """Test workspace_url column properties."""
        workspace_url_column = DatabricksConfig.workspace_url
        column = workspace_url_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is True
        assert column.default.arg == ""

    def test_databricks_config_boolean_columns_properties(self):
        """Test boolean column properties."""
        boolean_columns_with_defaults = {
            'is_active': True,
            'is_enabled': True,
            'mlflow_enabled': False,
            'evaluation_enabled': False,
            'volume_enabled': False,
            'volume_create_date_dirs': True,
            'knowledge_volume_enabled': False
        }
        
        for col_name, expected_default in boolean_columns_with_defaults.items():
            column = getattr(DatabricksConfig, col_name).property.columns[0]
            assert isinstance(column, Column)
            assert isinstance(column.type, Boolean)
            assert column.default.arg is expected_default

    def test_databricks_config_string_columns_properties(self):
        """Test string column properties."""
        nullable_string_columns = [
            'encrypted_personal_access_token', 'evaluation_judge_model',
            'volume_path', 'knowledge_volume_path'
        ]
        
        for col_name in nullable_string_columns:
            column = getattr(DatabricksConfig, col_name).property.columns[0]
            assert isinstance(column, Column)
            assert isinstance(column.type, String)
            assert column.nullable is True

    def test_databricks_config_volume_file_format_column_properties(self):
        """Test volume_file_format column properties."""
        volume_file_format_column = DatabricksConfig.volume_file_format
        column = volume_file_format_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is True
        assert column.default.arg == "json"

    def test_databricks_config_integer_columns_properties(self):
        """Test integer column properties."""
        integer_columns_with_defaults = {
            'knowledge_chunk_size': 1000,
            'knowledge_chunk_overlap': 200
        }
        
        for col_name, expected_default in integer_columns_with_defaults.items():
            column = getattr(DatabricksConfig, col_name).property.columns[0]
            assert isinstance(column, Column)
            assert isinstance(column.type, Integer)
            assert column.default.arg == expected_default

    def test_databricks_config_group_columns_properties(self):
        """Test group-related column properties."""
        group_id_column = DatabricksConfig.group_id
        column = group_id_column.property.columns[0]
        assert isinstance(column.type, String)
        assert column.type.length == 100
        assert column.index is True
        assert column.nullable is True

        created_by_email_column = DatabricksConfig.created_by_email
        column = created_by_email_column.property.columns[0]
        assert isinstance(column.type, String)
        assert column.type.length == 255
        assert column.index is True
        assert column.nullable is True

    def test_databricks_config_datetime_columns_properties(self):
        """Test datetime column properties."""
        created_at_column = DatabricksConfig.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.default is not None

        updated_at_column = DatabricksConfig.updated_at
        column = updated_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.default is not None
        assert column.onupdate is not None


class TestDatabricksConfigInitialization:
    """Test DatabricksConfig initialization."""

    def test_databricks_config_minimal_initialization(self):
        """Test DatabricksConfig initialization with minimal required fields."""
        config = DatabricksConfig(
            warehouse_id="warehouse-123",
            catalog="test_catalog",
            schema="test_schema"
        )
        
        assert config.warehouse_id == "warehouse-123"
        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"

    def test_databricks_config_initialization_with_all_fields(self):
        """Test DatabricksConfig initialization with all fields."""
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            warehouse_id="warehouse-123",
            catalog="test_catalog",
            schema="test_schema",
            is_active=False,
            is_enabled=False,
            encrypted_personal_access_token="encrypted_token",
            mlflow_enabled=True,
            evaluation_enabled=True,
            evaluation_judge_model="databricks:/judge-endpoint",
            group_id="group-123",
            created_by_email="test@example.com",
            volume_enabled=True,
            volume_path="catalog.schema.volume",
            volume_file_format="parquet",
            volume_create_date_dirs=False,
            knowledge_volume_enabled=True,
            knowledge_volume_path="catalog.schema.knowledge",
            knowledge_chunk_size=2000,
            knowledge_chunk_overlap=400
        )
        
        assert config.workspace_url == "https://test.databricks.com"
        assert config.warehouse_id == "warehouse-123"
        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"
        assert config.is_active is False
        assert config.is_enabled is False
        assert config.encrypted_personal_access_token == "encrypted_token"
        assert config.mlflow_enabled is True
        assert config.evaluation_enabled is True
        assert config.evaluation_judge_model == "databricks:/judge-endpoint"
        assert config.group_id == "group-123"
        assert config.created_by_email == "test@example.com"
        assert config.volume_enabled is True
        assert config.volume_path == "catalog.schema.volume"
        assert config.volume_file_format == "parquet"
        assert config.volume_create_date_dirs is False
        assert config.knowledge_volume_enabled is True
        assert config.knowledge_volume_path == "catalog.schema.knowledge"
        assert config.knowledge_chunk_size == 2000
        assert config.knowledge_chunk_overlap == 400

    def test_databricks_config_default_values(self):
        """Test DatabricksConfig default values (applied at database level)."""
        config = DatabricksConfig(
            warehouse_id="warehouse-123",
            catalog="test_catalog",
            schema="test_schema"
        )
        
        # These should be None until saved to database (defaults are applied at DB level)
        assert config.workspace_url is None  # Will be "" when saved to DB
        assert config.is_active is None  # Will be True when saved to DB
        assert config.is_enabled is None  # Will be True when saved to DB
        assert config.mlflow_enabled is None  # Will be False when saved to DB
        assert config.evaluation_enabled is None  # Will be False when saved to DB
        assert config.volume_enabled is None  # Will be False when saved to DB
        assert config.volume_file_format is None  # Will be "json" when saved to DB
        assert config.volume_create_date_dirs is None  # Will be True when saved to DB
        assert config.knowledge_volume_enabled is None  # Will be False when saved to DB
        assert config.knowledge_chunk_size is None  # Will be 1000 when saved to DB
        assert config.knowledge_chunk_overlap is None  # Will be 200 when saved to DB

    def test_databricks_config_boolean_values(self):
        """Test DatabricksConfig boolean field handling."""
        config = DatabricksConfig(
            warehouse_id="warehouse-123",
            catalog="test_catalog",
            schema="test_schema",
            is_active=False,
            is_enabled=False,
            mlflow_enabled=True,
            evaluation_enabled=True,
            volume_enabled=True,
            volume_create_date_dirs=False,
            knowledge_volume_enabled=True
        )
        
        assert config.is_active is False
        assert config.is_enabled is False
        assert config.mlflow_enabled is True
        assert config.evaluation_enabled is True
        assert config.volume_enabled is True
        assert config.volume_create_date_dirs is False
        assert config.knowledge_volume_enabled is True

    def test_databricks_config_string_values(self):
        """Test DatabricksConfig string field handling."""
        config = DatabricksConfig(
            warehouse_id="",
            catalog="",
            schema="",
            workspace_url="",
            encrypted_personal_access_token="",
            evaluation_judge_model="",
            group_id="",
            created_by_email="",
            volume_path="",
            volume_file_format="",
            knowledge_volume_path=""
        )
        
        assert config.warehouse_id == ""
        assert config.catalog == ""
        assert config.schema == ""
        assert config.workspace_url == ""
        assert config.encrypted_personal_access_token == ""
        assert config.evaluation_judge_model == ""
        assert config.group_id == ""
        assert config.created_by_email == ""
        assert config.volume_path == ""
        assert config.volume_file_format == ""
        assert config.knowledge_volume_path == ""

    def test_databricks_config_integer_values(self):
        """Test DatabricksConfig integer field handling."""
        config = DatabricksConfig(
            warehouse_id="warehouse-123",
            catalog="test_catalog",
            schema="test_schema",
            knowledge_chunk_size=0,
            knowledge_chunk_overlap=0
        )
        
        assert config.knowledge_chunk_size == 0
        assert config.knowledge_chunk_overlap == 0

    def test_databricks_config_large_integer_values(self):
        """Test DatabricksConfig with large integer values."""
        config = DatabricksConfig(
            warehouse_id="warehouse-123",
            catalog="test_catalog",
            schema="test_schema",
            knowledge_chunk_size=10000,
            knowledge_chunk_overlap=5000
        )
        
        assert config.knowledge_chunk_size == 10000
        assert config.knowledge_chunk_overlap == 5000


class TestDatabricksConfigTableStructure:
    """Test DatabricksConfig table structure and metadata."""

    def test_databricks_config_table_exists(self):
        """Test DatabricksConfig table exists in metadata."""
        assert hasattr(DatabricksConfig, '__table__')
        assert DatabricksConfig.__table__.name == "databricksconfig"

    def test_databricks_config_primary_key(self):
        """Test DatabricksConfig primary key."""
        table = DatabricksConfig.__table__
        primary_key_columns = [col.name for col in table.primary_key.columns]
        assert primary_key_columns == ['id']

    def test_databricks_config_indexes(self):
        """Test DatabricksConfig indexes."""
        table = DatabricksConfig.__table__
        indexed_columns = []
        
        for column in table.columns:
            if column.index:
                indexed_columns.append(column.name)
        
        assert 'group_id' in indexed_columns
        assert 'created_by_email' in indexed_columns

    def test_databricks_config_nullable_columns(self):
        """Test DatabricksConfig nullable column configuration."""
        table = DatabricksConfig.__table__
        
        # Non-nullable columns
        non_nullable = ['id', 'warehouse_id', 'catalog', 'schema']
        for col_name in non_nullable:
            column = table.columns[col_name]
            assert not column.nullable, f"Column {col_name} should not be nullable"
        
        # Nullable columns
        nullable = [
            'workspace_url', 'encrypted_personal_access_token', 'evaluation_judge_model',
            'group_id', 'created_by_email', 'volume_path', 'volume_file_format',
            'knowledge_volume_path'
        ]
        for col_name in nullable:
            column = table.columns[col_name]
            assert column.nullable, f"Column {col_name} should be nullable"

    def test_databricks_config_column_defaults(self):
        """Test DatabricksConfig column default values."""
        table = DatabricksConfig.__table__
        
        # Columns with defaults
        columns_with_defaults = [
            'workspace_url', 'is_active', 'is_enabled', 'mlflow_enabled',
            'evaluation_enabled', 'volume_enabled', 'volume_file_format',
            'volume_create_date_dirs', 'knowledge_volume_enabled',
            'knowledge_chunk_size', 'knowledge_chunk_overlap',
            'created_at', 'updated_at'
        ]
        
        for col_name in columns_with_defaults:
            column = table.columns[col_name]
            assert column.default is not None, f"Column {col_name} should have a default"

    def test_databricks_config_sqlalchemy_attributes(self):
        """Test DatabricksConfig has required SQLAlchemy attributes."""
        assert hasattr(DatabricksConfig, '__mapper__')
        assert hasattr(DatabricksConfig, 'metadata')
        assert hasattr(DatabricksConfig, '__table__')
        assert hasattr(DatabricksConfig, '__tablename__')

    def test_databricks_config_column_types(self):
        """Test DatabricksConfig column types are correct."""
        table = DatabricksConfig.__table__
        
        type_mapping = {
            'id': Integer,
            'workspace_url': String,
            'warehouse_id': String,
            'catalog': String,
            'schema': String,
            'is_active': Boolean,
            'is_enabled': Boolean,
            'encrypted_personal_access_token': String,
            'mlflow_enabled': Boolean,
            'evaluation_enabled': Boolean,
            'evaluation_judge_model': String,
            'group_id': String,
            'created_by_email': String,
            'volume_enabled': Boolean,
            'volume_path': String,
            'volume_file_format': String,
            'volume_create_date_dirs': Boolean,
            'knowledge_volume_enabled': Boolean,
            'knowledge_volume_path': String,
            'knowledge_chunk_size': Integer,
            'knowledge_chunk_overlap': Integer,
            'created_at': DateTime,
            'updated_at': DateTime
        }
        
        for col_name, expected_type in type_mapping.items():
            column = table.columns[col_name]
            assert isinstance(column.type, expected_type), f"Column {col_name} should be {expected_type}"

    def test_databricks_config_timezone_aware_datetime_columns(self):
        """Test timezone-aware datetime columns."""
        datetime_columns = ['created_at', 'updated_at']
        
        for col_name in datetime_columns:
            column = DatabricksConfig.__table__.columns[col_name]
            if isinstance(column.type, DateTime):
                assert column.type.timezone is True, f"Column {col_name} should be timezone-aware"
