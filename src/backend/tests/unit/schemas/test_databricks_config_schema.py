"""
Unit tests for databricks_config Pydantic schemas.

Tests validation, defaults, required fields, and schema interactions.
"""

import pytest
from pydantic import ValidationError

from src.schemas.databricks_config import (
    DatabricksConfigBase,
    DatabricksConfigCreate,
    DatabricksConfigResponse,
    DatabricksTokenStatus,
)


class TestDatabricksConfigBase:
    """Test cases for DatabricksConfigBase schema."""

    def test_defaults(self):
        """Test DatabricksConfigBase with default values."""
        config = DatabricksConfigBase()
        assert config.workspace_url == ""
        assert config.warehouse_id == ""
        assert config.catalog == ""
        assert config.db_schema == ""
        assert config.enabled is True
        assert config.mlflow_enabled is False
        assert config.mlflow_experiment_name == "kasal-crew-execution-traces"
        assert config.evaluation_enabled is False
        assert config.evaluation_judge_model is None
        assert config.volume_enabled is False
        assert config.volume_path is None
        assert config.volume_file_format == "json"
        assert config.volume_create_date_dirs is True
        assert config.knowledge_volume_enabled is False
        assert config.knowledge_volume_path is None
        assert config.knowledge_chunk_size == 1000
        assert config.knowledge_chunk_overlap == 200

    def test_full_config(self):
        """Test DatabricksConfigBase with all fields specified."""
        config = DatabricksConfigBase(
            workspace_url="https://example.com",
            warehouse_id="abc123",
            catalog="main",
            schema="default",
            enabled=True,
            mlflow_enabled=True,
            mlflow_experiment_name="test-exp",
            evaluation_enabled=True,
            evaluation_judge_model="databricks:/endpoint",
            volume_enabled=True,
            volume_path="/volumes/test",
            volume_file_format="parquet",
            volume_create_date_dirs=False,
            knowledge_volume_enabled=True,
            knowledge_volume_path="/volumes/knowledge",
            knowledge_chunk_size=500,
            knowledge_chunk_overlap=100,
        )
        assert config.workspace_url == "https://example.com"
        assert config.warehouse_id == "abc123"
        assert config.catalog == "main"
        assert config.db_schema == "default"
        assert config.mlflow_enabled is True
        assert config.mlflow_experiment_name == "test-exp"
        assert config.evaluation_enabled is True
        assert config.volume_enabled is True
        assert config.volume_file_format == "parquet"
        assert config.knowledge_chunk_size == 500

    def test_schema_alias(self):
        """Test that 'schema' alias maps to db_schema field."""
        config = DatabricksConfigBase(schema="my_schema")
        assert config.db_schema == "my_schema"

    def test_boolean_conversion(self):
        """Test boolean field conversions from string values."""
        config = DatabricksConfigBase(enabled="true")
        assert config.enabled is True

        config_false = DatabricksConfigBase(enabled="false")
        assert config_false.enabled is False


class TestDatabricksConfigCreate:
    """Test cases for DatabricksConfigCreate schema."""

    def test_disabled_config_no_validation(self):
        """Test that disabled config does not require warehouse_id, catalog, etc."""
        config = DatabricksConfigCreate(enabled=False)
        assert config.enabled is False

    def test_valid_enabled_config(self):
        """Test valid enabled config with all required fields."""
        config = DatabricksConfigCreate(
            warehouse_id="abc123",
            catalog="main",
            schema="default",
            enabled=True,
        )
        assert config.warehouse_id == "abc123"
        assert config.catalog == "main"
        assert config.db_schema == "default"
        assert config.enabled is True

    def test_enabled_missing_required_fields(self):
        """Test validation error when enabled but missing required fields."""
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfigCreate(enabled=True)

        error_msg = str(exc_info.value)
        assert "warehouse_id" in error_msg
        assert "catalog" in error_msg
        assert "db_schema" in error_msg

    def test_enabled_partial_missing(self):
        """Test validation error with only some required fields provided."""
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfigCreate(
                warehouse_id="abc123",
                enabled=True,
            )

        error_msg = str(exc_info.value)
        assert "catalog" in error_msg
        assert "db_schema" in error_msg

    def test_enabled_empty_strings_fail_validation(self):
        """Test that empty strings for required fields fail validation."""
        with pytest.raises(ValueError) as exc_info:
            DatabricksConfigCreate(
                warehouse_id="",
                catalog="main",
                schema="",
                enabled=True,
            )

        error_msg = str(exc_info.value)
        assert "warehouse_id" in error_msg
        assert "db_schema" in error_msg

    def test_required_fields_property_disabled(self):
        """Test required_fields property when disabled."""
        config = DatabricksConfigCreate(enabled=False)
        assert config.required_fields == []

    def test_required_fields_property_enabled(self):
        """Test required_fields property when enabled."""
        config = DatabricksConfigCreate(
            warehouse_id="wh1",
            catalog="cat1",
            schema="sch1",
            enabled=True,
        )
        assert set(config.required_fields) == {"warehouse_id", "catalog", "db_schema"}


class TestDatabricksConfigResponse:
    """Test cases for DatabricksConfigResponse schema."""

    def test_inheritance_from_base(self):
        """Test that DatabricksConfigResponse inherits from DatabricksConfigBase."""
        assert issubclass(DatabricksConfigResponse, DatabricksConfigBase)

    def test_minimal_response(self):
        """Test DatabricksConfigResponse with minimal (default) data."""
        response = DatabricksConfigResponse()
        assert response.workspace_url == ""
        assert response.warehouse_id == ""
        assert response.catalog == ""
        assert response.db_schema == ""
        assert response.enabled is True

    def test_full_response(self):
        """Test DatabricksConfigResponse with full data."""
        response = DatabricksConfigResponse(
            workspace_url="https://example.com",
            warehouse_id="wh1",
            catalog="catalog1",
            schema="schema1",
            enabled=True,
        )
        assert response.workspace_url == "https://example.com"
        assert response.warehouse_id == "wh1"
        assert response.db_schema == "schema1"


class TestDatabricksTokenStatus:
    """Test cases for DatabricksTokenStatus schema."""

    def test_token_required(self):
        """Test DatabricksTokenStatus when token is required."""
        status = DatabricksTokenStatus(
            personal_token_required=True,
            message="Personal access token is required",
        )
        assert status.personal_token_required is True
        assert status.message == "Personal access token is required"

    def test_token_not_required(self):
        """Test DatabricksTokenStatus when token is not required."""
        status = DatabricksTokenStatus(
            personal_token_required=False,
            message="Workspace is accessible",
        )
        assert status.personal_token_required is False

    def test_missing_message_field(self):
        """Test validation with missing message field."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksTokenStatus(personal_token_required=True)

        errors = exc_info.value.errors()
        missing_fields = [e["loc"][0] for e in errors if e["type"] == "missing"]
        assert "message" in missing_fields

    def test_missing_token_required_field(self):
        """Test validation with missing personal_token_required field."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksTokenStatus(message="test")

        errors = exc_info.value.errors()
        missing_fields = [e["loc"][0] for e in errors if e["type"] == "missing"]
        assert "personal_token_required" in missing_fields

    def test_boolean_coercion(self):
        """Test boolean field coercion from non-boolean values."""
        status = DatabricksTokenStatus(
            personal_token_required="true",
            message="Test",
        )
        assert status.personal_token_required is True

        status_false = DatabricksTokenStatus(
            personal_token_required=0,
            message="Test",
        )
        assert status_false.personal_token_required is False
