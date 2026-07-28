"""
Unit tests for ModelConfig model class.

Tests the SQLAlchemy model attributes, defaults, table name, and column definitions.
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.models.model_config import ModelConfig
from src.db.base import Base


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
            "id", "key", "name", "provider", "temperature",
            "context_window", "max_output_tokens", "extended_thinking",
            "enabled", "group_id", "created_by_email",
            "created_at", "updated_at",
        ]
        actual_columns = [c.name for c in ModelConfig.__table__.columns]
        for col_name in expected_columns:
            assert col_name in actual_columns, f"Missing column: {col_name}"

    def test_no_unexpected_columns(self):
        """Test that there are no unexpected extra columns."""
        expected_columns = {
            "id", "key", "name", "provider", "temperature",
            "context_window", "max_output_tokens", "extended_thinking",
            "enabled", "group_id", "created_by_email",
            "created_at", "updated_at",
        }
        actual_columns = {c.name for c in ModelConfig.__table__.columns}
        assert actual_columns == expected_columns
