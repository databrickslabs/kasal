"""
Comprehensive tests for memory_backend_factory.py

Tests cover:
- DatabricksIndexValidationError exception
- _validate_databricks_indexes method
- Index state validation (READY, PROVISIONING, MISSING)
- create_memory_backends with validation
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any

from src.engines.crewai.memory.memory_backend_factory import (
    MemoryBackendFactory,
    DatabricksIndexValidationError
)
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    DatabricksMemoryConfig
)

# Correct path for patching - the import happens inside the method
REPO_PATCH_PATH = 'src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository'


class TestDatabricksIndexValidationError:
    """Tests for the DatabricksIndexValidationError exception class."""

    def test_exception_creation_with_missing_indexes(self):
        """Test exception creation with missing indexes error type."""
        validation_result = {
            "valid": False,
            "valid_indexes": [],
            "missing_indexes": ["short_term: catalog.schema.stm_index"],
            "provisioning_indexes": [],
            "error_message": "Indexes not found",
            "error_type": "missing_indexes"
        }

        error = DatabricksIndexValidationError("Test error", validation_result)

        assert str(error) == "Test error"
        assert error.error_type == "missing_indexes"
        assert error.missing_indexes == ["short_term: catalog.schema.stm_index"]
        assert error.provisioning_indexes == []
        assert error.validation_result == validation_result

    def test_exception_creation_with_provisioning_indexes(self):
        """Test exception creation with provisioning indexes error type."""
        validation_result = {
            "valid": False,
            "valid_indexes": [],
            "missing_indexes": [],
            "provisioning_indexes": ["entity: catalog.schema.entity_index (state: PROVISIONING)"],
            "error_message": "Indexes provisioning",
            "error_type": "provisioning_indexes"
        }

        error = DatabricksIndexValidationError("Provisioning error", validation_result)

        assert error.error_type == "provisioning_indexes"
        assert error.missing_indexes == []
        assert error.provisioning_indexes == ["entity: catalog.schema.entity_index (state: PROVISIONING)"]

    def test_exception_creation_with_unknown_error_type(self):
        """Test exception creation with unknown error type defaults correctly."""
        validation_result = {
            "valid": False,
            "error_message": "Unknown error"
        }

        error = DatabricksIndexValidationError("Unknown", validation_result)

        assert error.error_type == "unknown"
        assert error.missing_indexes == []
        assert error.provisioning_indexes == []


class TestCreateMemoryBackendsValidation:
    """Tests for create_memory_backends with index validation."""

    @pytest.fixture
    def databricks_config(self):
        """Create a standard Databricks memory config for testing."""
        return DatabricksMemoryConfig(memory_index="catalog.schema.memory_index", 
            workspace_url="https://example.databricks.com",
            endpoint_name="test-endpoint",
            short_term_index="catalog.schema.stm_index",
            embedding_dimension=1024
        )

    @pytest.fixture
    def memory_config(self, databricks_config):
        """Create a memory backend config with Databricks enabled."""
        return MemoryBackendConfig(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config,
            enable_short_term=True,
            enable_long_term=False,
            enable_entity=False
        )

    @pytest.mark.asyncio
    async def test_create_default_backend_skips_validation(self):
        """Test that DEFAULT backend type skips Databricks validation."""
        config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=True
        )

        # Should not raise any errors - DEFAULT backend doesn't validate Databricks indexes
        result = await MemoryBackendFactory.create_memory_backends(
            config=config,
            crew_id="test_crew_123",
            embedder=None
        )

        assert result == {}  # Default backend returns empty dict


