"""Unit tests for memory backend router."""

import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

# Set database type to sqlite for testing
os.environ["DATABASE_TYPE"] = "sqlite"
os.environ["SQLITE_DB_PATH"] = ":memory:"

# NOTE: this module used to stub `src.services.tools.base` into sys.modules for its
# whole run. kasal_engine is a real vendored package here, and any src.* module
# imported for the first time inside that window stayed cached holding a Mock —
# poisoning every later test file on the same xdist worker. `asyncpg` is a real
# dependency too. No stubs.

from src.models.memory_backend import MemoryBackend

# Import only what we need for testing
from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    MemoryBackendCreate,
    MemoryBackendType,
    MemoryBackendUpdate,
)
from src.services.memory.config.backend_service import MemoryBackendService


@pytest.fixture
def mock_memory_backend_service():
    """Create a mock MemoryBackendService."""
    service = AsyncMock(spec=MemoryBackendService)
    return service


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "test-group-id"
    context.group_email = "test@example.com"
    return context


@pytest.fixture
def sample_databricks_config():
    """Create a sample Databricks configuration."""
    return DatabricksMemoryConfig(
        memory_index="catalog.schema.memory_index",
        endpoint_name="test-endpoint",
        short_term_index="test.catalog.short_term",
        long_term_index="test.catalog.long_term",
        entity_index="test.catalog.entity",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768,
    )


@pytest.fixture
def sample_memory_backend():
    """Create a sample memory backend."""
    backend = MagicMock(spec=MemoryBackend)
    backend.id = "test-backend-id"
    backend.group_id = "test-group-id"
    backend.name = "Test Backend"
    backend.description = None
    backend.backend_type = MemoryBackendType.DATABRICKS

    # Create a proper DatabricksMemoryConfig object that will be serialized correctly
    databricks_config_dict = {
        "endpoint_name": "test-endpoint",
        "short_term_index": "test.catalog.short_term",
        "workspace_url": "https://test.databricks.com",
        "embedding_dimension": 768,
    }
    backend.databricks_config = databricks_config_dict
    backend.configure_mock(databricks_config=databricks_config_dict)

    backend.enable_short_term = True
    backend.enable_long_term = True
    backend.enable_entity = True
    backend.custom_config = None
    backend.is_active = True
    backend.is_default = True
    backend.created_at = datetime.utcnow()
    backend.updated_at = datetime.utcnow()
    return backend


class TestMemoryBackendService:
    """Test memory backend service methods with proper async handling."""

    @pytest.mark.asyncio
    async def test_memory_backend_crud_operations(
        self, mock_memory_backend_service, sample_memory_backend
    ):
        """Test CRUD operations for memory backend."""
        # Test create_memory_backend
        mock_memory_backend_service.create_memory_backend.return_value = (
            sample_memory_backend
        )
        create_data = MemoryBackendCreate(
            name="Test Backend", backend_type=MemoryBackendType.DATABRICKS
        )
        result = await mock_memory_backend_service.create_memory_backend(
            "test-group-id", create_data
        )
        assert result.id == "test-backend-id"

        # Test get_memory_backends
        mock_memory_backend_service.get_memory_backends.return_value = [
            sample_memory_backend
        ]
        result = await mock_memory_backend_service.get_memory_backends("test-group-id")
        assert len(result) == 1
        assert result[0].id == "test-backend-id"

        # Test get_memory_backend
        mock_memory_backend_service.get_memory_backend.return_value = (
            sample_memory_backend
        )
        result = await mock_memory_backend_service.get_memory_backend(
            "test-group-id", "test-backend-id"
        )
        assert result.id == "test-backend-id"

        # Test update_memory_backend
        mock_memory_backend_service.update_memory_backend.return_value = (
            sample_memory_backend
        )
        update_data = MemoryBackendUpdate(name="Updated Backend")
        result = await mock_memory_backend_service.update_memory_backend(
            "test-group-id", "test-backend-id", update_data
        )
        assert result.id == "test-backend-id"

        # Test delete_memory_backend
        mock_memory_backend_service.delete_memory_backend.return_value = True
        result = await mock_memory_backend_service.delete_memory_backend(
            "test-group-id", "test-backend-id"
        )
        assert result is True

        # Test set_default_backend
        mock_memory_backend_service.set_default_backend.return_value = True
        result = await mock_memory_backend_service.set_default_backend(
            "test-group-id", "test-backend-id"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_memory_stats_operations(self, mock_memory_backend_service):
        """Test memory statistics operations."""
        # Test get_memory_stats
        mock_memory_backend_service.get_memory_stats.return_value = {
            "short_term": 10,
            "long_term": 20,
            "entity": 5,
        }
        result = await mock_memory_backend_service.get_memory_stats(
            "test-group-id", "test-crew-id"
        )
        assert result["short_term"] == 10
        assert result["long_term"] == 20
        assert result["entity"] == 5

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_configuration_mode_operations(self, mock_memory_backend_service):
        """Test configuration mode operations."""
        # Test delete_all_and_create_disabled
        mock_memory_backend_service.delete_all_and_create_disabled.return_value = {
            "success": True,
            "message": "Switched to disabled mode",
            "deleted_count": 2,
        }
        result = await mock_memory_backend_service.delete_all_and_create_disabled(
            "test-group-id"
        )
        assert result["success"] is True
        assert result["deleted_count"] == 2

        # Test delete_disabled_configurations
        mock_memory_backend_service.delete_disabled_configurations.return_value = 3
        result = await mock_memory_backend_service.delete_disabled_configurations(
            "test-group-id"
        )
        assert result == 3
