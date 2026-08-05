"""
Unit tests for ModelConfig model class.

Tests the SQLAlchemy model attributes, defaults, table name, and column definitions.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String

from src.db.base import Base
from src.models.model_config import ModelConfig


class TestModelConfigModel:
    """Test cases for ModelConfig SQLAlchemy model."""

    def test_model_inherits_from_base(self):
        """Test that ModelConfig inherits from the declarative Base."""
        assert issubclass(ModelConfig, Base)

    def test_table_name(self):
        """Test that the auto-generated table name is lowercase class name."""
        assert ModelConfig.__tablename__ == "modelconfig"

    def test_primary_key_column(self):
        """Test that id column exists and is the primary key."""
        id_col = ModelConfig.__table__.columns["id"]
        assert id_col.primary_key is True

    def test_key_column(self):
        """Test key column properties."""
        key_col = ModelConfig.__table__.columns["key"]
        assert key_col.nullable is False

    def test_name_column(self):
        """Test name column properties."""
        name_col = ModelConfig.__table__.columns["name"]
        assert name_col.nullable is False

    def test_provider_column_exists(self):
        """Test provider column exists."""
        assert "provider" in ModelConfig.__table__.columns

    def test_temperature_column_exists(self):
        """Test temperature column exists."""
        assert "temperature" in ModelConfig.__table__.columns

    def test_context_window_column_exists(self):
        """Test context_window column exists."""
        assert "context_window" in ModelConfig.__table__.columns

    def test_max_output_tokens_column_exists(self):
        """Test max_output_tokens column exists."""
        assert "max_output_tokens" in ModelConfig.__table__.columns

    def test_extended_thinking_column_default(self):
        """Test that extended_thinking defaults to False."""
        col = ModelConfig.__table__.columns["extended_thinking"]
        assert col.default is not None
        assert col.default.arg is False

    def test_enabled_column_default(self):
        """Test that enabled defaults to True."""
        col = ModelConfig.__table__.columns["enabled"]
        assert col.default is not None
        assert col.default.arg is True

    def test_group_id_column(self):
        """Test group_id column for multi-tenant support."""
        col = ModelConfig.__table__.columns["group_id"]
        assert col.nullable is True
        assert col.index is True

    def test_created_by_email_column(self):
        """Test created_by_email column for audit."""
        col = ModelConfig.__table__.columns["created_by_email"]
        assert col.nullable is True

    def test_created_at_column_exists(self):
        """Test created_at column exists."""
        assert "created_at" in ModelConfig.__table__.columns

    def test_updated_at_column_exists(self):
        """Test updated_at column exists."""
        assert "updated_at" in ModelConfig.__table__.columns

    def test_all_expected_columns_present(self):
        """Test that all expected columns are present on the model."""
        expected_columns = [
            "id",
            "key",
            "name",
            "provider",
            "temperature",
            "context_window",
            "max_output_tokens",
            # Declared sampling parameters, and the names this endpoint refuses.
            # Before these, the catalogue could express exactly two knobs, so the
            # transport's top_p / frequency_penalty / presence_penalty / stop
            # fields were forwarded on every request and set by nothing.
            "params",
            "unsupported_params",
            "extended_thinking",
            # Thinking depth. Which of the two applies is the MODEL's property,
            # decided by core.llm.model_capabilities — a budget belongs to Claude
            # 4.1-4.6, an effort level to 4.7+/5/Fable and the GPT-5/Gemini
            # families, and sending the wrong one is a 400. Both are stored so a
            # model swap cannot invalidate a saved row.
            "thinking_budget_tokens",
            "reasoning_effort",
            "enabled",
            "group_id",
            "created_by_email",
            "created_at",
            "updated_at",
        ]
        actual_columns = [c.name for c in ModelConfig.__table__.columns]
        for col_name in expected_columns:
            assert col_name in actual_columns, f"Missing column: {col_name}"

    def test_no_unexpected_columns(self):
        """Test that there are no unexpected extra columns."""
        expected_columns = {
            "id",
            "key",
            "name",
            "provider",
            "temperature",
            "context_window",
            "max_output_tokens",
            # Declared sampling parameters, and the names this endpoint refuses.
            # Before these, the catalogue could express exactly two knobs, so the
            # transport's top_p / frequency_penalty / presence_penalty / stop
            # fields were forwarded on every request and set by nothing.
            "params",
            "unsupported_params",
            "extended_thinking",
            # Thinking depth. Which of the two applies is the MODEL's property,
            # decided by core.llm.model_capabilities — a budget belongs to Claude
            # 4.1-4.6, an effort level to 4.7+/5/Fable and the GPT-5/Gemini
            # families, and sending the wrong one is a 400. Both are stored so a
            # model swap cannot invalidate a saved row.
            "thinking_budget_tokens",
            "reasoning_effort",
            "enabled",
            "group_id",
            "created_by_email",
            "created_at",
            "updated_at",
        }
        actual_columns = {c.name for c in ModelConfig.__table__.columns}
        assert actual_columns == expected_columns


class TestModelConfig:
    """Test ModelConfig model."""

    def test_model_config_inherits_base(self):
        """Test ModelConfig inherits from Base."""
        assert issubclass(ModelConfig, Base)

    def test_model_config_tablename(self):
        """Test ModelConfig table name."""
        assert ModelConfig.__tablename__ == "modelconfig"

    def test_model_config_columns_exist(self):
        """Test ModelConfig has expected columns."""
        expected_columns = [
            "id",
            "key",
            "name",
            "provider",
            "temperature",
            "context_window",
            "max_output_tokens",
            # Declared sampling parameters, and the names this endpoint refuses.
            # Before these, the catalogue could express exactly two knobs, so the
            # transport's top_p / frequency_penalty / presence_penalty / stop
            # fields were forwarded on every request and set by nothing.
            "params",
            "unsupported_params",
            "extended_thinking",
            # Thinking depth. Which of the two applies is the MODEL's property,
            # decided by core.llm.model_capabilities — a budget belongs to Claude
            # 4.1-4.6, an effort level to 4.7+/5/Fable and the GPT-5/Gemini
            # families, and sending the wrong one is a 400. Both are stored so a
            # model swap cannot invalidate a saved row.
            "thinking_budget_tokens",
            "reasoning_effort",
            "enabled",
            "group_id",
            "created_by_email",
            "created_at",
            "updated_at",
        ]

        for column_name in expected_columns:
            assert hasattr(ModelConfig, column_name)

    def test_model_config_id_column_properties(self):
        """Test id column properties."""
        id_column = ModelConfig.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, Integer)
        assert id_column.property.columns[0].primary_key is True

    def test_model_config_key_column_properties(self):
        """Test key column properties."""
        key_column = ModelConfig.key
        column = key_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False

    def test_model_config_name_column_properties(self):
        """Test name column properties."""
        name_column = ModelConfig.name
        column = name_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False

    def test_model_config_provider_column_properties(self):
        """Test provider column properties."""
        provider_column = ModelConfig.provider
        column = provider_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is True  # Default nullable

    def test_model_config_temperature_column_properties(self):
        """Test temperature column properties."""
        temperature_column = ModelConfig.temperature
        column = temperature_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Float)
        assert column.nullable is True  # Default nullable

    def test_model_config_context_window_column_properties(self):
        """Test context_window column properties."""
        context_window_column = ModelConfig.context_window
        column = context_window_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Integer)
        assert column.nullable is True  # Default nullable

    def test_model_config_max_output_tokens_column_properties(self):
        """Test max_output_tokens column properties."""
        max_output_tokens_column = ModelConfig.max_output_tokens
        column = max_output_tokens_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Integer)
        assert column.nullable is True  # Default nullable

    def test_model_config_extended_thinking_column_properties(self):
        """Test extended_thinking column properties."""
        extended_thinking_column = ModelConfig.extended_thinking
        column = extended_thinking_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Boolean)
        assert column.default.arg is False

    def test_model_config_enabled_column_properties(self):
        """Test enabled column properties."""
        enabled_column = ModelConfig.enabled
        column = enabled_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Boolean)
        assert column.default.arg is True

    def test_model_config_group_id_column_properties(self):
        """Test group_id column properties."""
        group_id_column = ModelConfig.group_id
        column = group_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.type.length == 100
        assert column.index is True
        assert column.nullable is True

    def test_model_config_created_by_email_column_properties(self):
        """Test created_by_email column properties."""
        created_by_email_column = ModelConfig.created_by_email
        column = created_by_email_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.type.length == 255
        assert column.nullable is True

    def test_model_config_created_at_column_properties(self):
        """Test created_at column properties."""
        created_at_column = ModelConfig.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None

    def test_model_config_updated_at_column_properties(self):
        """Test updated_at column properties."""
        updated_at_column = ModelConfig.updated_at
        column = updated_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.default is not None
        assert column.onupdate is not None

    def test_model_config_initialization(self):
        """Test ModelConfig initialization."""
        config = ModelConfig()

        assert isinstance(config, ModelConfig)
        assert isinstance(config, Base)

    def test_model_config_initialization_with_required_values(self):
        """Test ModelConfig initialization with required values."""
        config = ModelConfig(key="test-model-key", name="Test Model")

        assert config.key == "test-model-key"
        assert config.name == "Test Model"

    def test_model_config_initialization_with_all_values(self):
        """Test ModelConfig initialization with all values."""
        config = ModelConfig(
            key="gpt-4",
            name="GPT-4",
            provider="openai",
            temperature=0.7,
            context_window=8192,
            max_output_tokens=4096,
            extended_thinking=True,
            enabled=True,
            group_id="test-group",
            created_by_email="test@example.com",
        )

        assert config.key == "gpt-4"
        assert config.name == "GPT-4"
        assert config.provider == "openai"
        assert config.temperature == 0.7
        assert config.context_window == 8192
        assert config.max_output_tokens == 4096
        assert config.extended_thinking is True
        assert config.enabled is True
        assert config.group_id == "test-group"
        assert config.created_by_email == "test@example.com"

    def test_model_config_default_values(self):
        """Test ModelConfig default values (applied at database level)."""
        config = ModelConfig(key="test-key", name="Test Model")

        # These should be None until saved to database (defaults are applied at DB level)
        assert config.extended_thinking is None  # Will be False when saved to DB
        assert config.enabled is None  # Will be True when saved to DB

        # These should be None (nullable)
        assert config.provider is None
        assert config.temperature is None
        assert config.context_window is None
        assert config.max_output_tokens is None
        assert config.group_id is None
        assert config.created_by_email is None

    def test_model_config_boolean_values(self):
        """Test ModelConfig boolean field handling."""
        config = ModelConfig(
            key="test-key", name="Test Model", extended_thinking=False, enabled=False
        )

        assert config.extended_thinking is False
        assert config.enabled is False

    def test_model_config_numeric_values(self):
        """Test ModelConfig numeric field handling."""
        config = ModelConfig(
            key="test-key",
            name="Test Model",
            temperature=0.0,
            context_window=0,
            max_output_tokens=0,
        )

        assert config.temperature == 0.0
        assert config.context_window == 0
        assert config.max_output_tokens == 0

    def test_model_config_string_values(self):
        """Test ModelConfig string field handling."""
        config = ModelConfig(
            key="", name="", provider="", group_id="", created_by_email=""
        )

        assert config.key == ""
        assert config.name == ""
        assert config.provider == ""
        assert config.group_id == ""
        assert config.created_by_email == ""

    def test_model_config_long_string_values(self):
        """Test ModelConfig with long string values."""
        long_group_id = "a" * 100  # Maximum length
        long_email = "a" * 255  # Maximum length

        config = ModelConfig(
            key="test-key",
            name="Test Model",
            group_id=long_group_id,
            created_by_email=long_email,
        )

        assert config.group_id == long_group_id
        assert config.created_by_email == long_email

    def test_model_config_temperature_range(self):
        """Test ModelConfig temperature with various values."""
        # Test different temperature values
        temperatures = [0.0, 0.5, 1.0, 1.5, 2.0]

        for temp in temperatures:
            config = ModelConfig(key="test-key", name="Test Model", temperature=temp)
            assert config.temperature == temp

    def test_model_config_context_window_values(self):
        """Test ModelConfig context_window with various values."""
        context_windows = [1024, 2048, 4096, 8192, 16384, 32768]

        for window in context_windows:
            config = ModelConfig(
                key="test-key", name="Test Model", context_window=window
            )
            assert config.context_window == window

    def test_model_config_max_output_tokens_values(self):
        """Test ModelConfig max_output_tokens with various values."""
        max_tokens = [256, 512, 1024, 2048, 4096]

        for tokens in max_tokens:
            config = ModelConfig(
                key="test-key", name="Test Model", max_output_tokens=tokens
            )
            assert config.max_output_tokens == tokens

    def test_model_config_provider_values(self):
        """Test ModelConfig provider with various values."""
        providers = ["openai", "anthropic", "databricks", "azure", "google"]

        for provider in providers:
            config = ModelConfig(key="test-key", name="Test Model", provider=provider)
            assert config.provider == provider


class TestModelConfigTableStructure:
    """Test ModelConfig table structure and metadata."""

    def test_model_config_table_exists(self):
        """Test ModelConfig table exists in metadata."""
        assert hasattr(ModelConfig, "__table__")
        assert ModelConfig.__table__.name == "modelconfig"

    def test_model_config_primary_key(self):
        """Test ModelConfig primary key."""
        table = ModelConfig.__table__
        primary_key_columns = [col.name for col in table.primary_key.columns]
        assert primary_key_columns == ["id"]

    def test_model_config_indexes(self):
        """Test ModelConfig indexes."""
        table = ModelConfig.__table__
        indexed_columns = []

        for column in table.columns:
            if column.index:
                indexed_columns.append(column.name)

        assert "group_id" in indexed_columns

    def test_model_config_nullable_columns(self):
        """Test ModelConfig nullable column configuration."""
        table = ModelConfig.__table__

        # Non-nullable columns
        non_nullable = ["id", "key", "name"]
        for col_name in non_nullable:
            column = table.columns[col_name]
            assert not column.nullable, f"Column {col_name} should not be nullable"

        # Nullable columns
        nullable = [
            "provider",
            "temperature",
            "context_window",
            "max_output_tokens",
            "group_id",
            "created_by_email",
        ]
        for col_name in nullable:
            column = table.columns[col_name]
            assert column.nullable, f"Column {col_name} should be nullable"

    def test_model_config_column_defaults(self):
        """Test ModelConfig column default values."""
        table = ModelConfig.__table__

        # Columns with defaults
        extended_thinking_col = table.columns["extended_thinking"]
        assert extended_thinking_col.default is not None

        enabled_col = table.columns["enabled"]
        assert enabled_col.default is not None

        created_at_col = table.columns["created_at"]
        assert created_at_col.default is not None

        updated_at_col = table.columns["updated_at"]
        assert updated_at_col.default is not None
        assert updated_at_col.onupdate is not None

    def test_model_config_sqlalchemy_attributes(self):
        """Test ModelConfig has required SQLAlchemy attributes."""
        assert hasattr(ModelConfig, "__mapper__")
        assert hasattr(ModelConfig, "metadata")
        assert hasattr(ModelConfig, "__table__")
        assert hasattr(ModelConfig, "__tablename__")

    def test_model_config_column_types(self):
        """Test ModelConfig column types are correct."""
        table = ModelConfig.__table__

        type_mapping = {
            "id": Integer,
            "key": String,
            "name": String,
            "provider": String,
            "temperature": Float,
            "context_window": Integer,
            "max_output_tokens": Integer,
            "extended_thinking": Boolean,
            "enabled": Boolean,
            "group_id": String,
            "created_by_email": String,
            "created_at": DateTime,
            "updated_at": DateTime,
        }

        for col_name, expected_type in type_mapping.items():
            column = table.columns[col_name]
            assert isinstance(
                column.type, expected_type
            ), f"Column {col_name} should be {expected_type}"
