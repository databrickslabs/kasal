"""Unit tests for memory backend router."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from datetime import datetime
import os
import sys
import atexit

# Set database type to sqlite for testing
os.environ["DATABASE_TYPE"] = "sqlite"
os.environ["SQLITE_DB_PATH"] = ":memory:"

# Create a fixture to handle mocking
@pytest.fixture(scope="module", autouse=True)
def mock_problematic_modules():
    """Mock modules that cause issues during testing."""
    # Store original modules
    original_crewai_tools = sys.modules.get('crewai_tools')
    original_asyncpg = sys.modules.get('asyncpg')
    
    # Mock the modules
    sys.modules['crewai_tools'] = Mock()
    sys.modules['asyncpg'] = Mock()
    
    yield
    
    # Restore original modules
    if original_crewai_tools is not None:
        sys.modules['crewai_tools'] = original_crewai_tools
    else:
        sys.modules.pop('crewai_tools', None)
    
    if original_asyncpg is not None:
        sys.modules['asyncpg'] = original_asyncpg
    else:
        sys.modules.pop('asyncpg', None)

# Import only what we need for testing
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    DatabricksMemoryConfig,
    MemoryBackendCreate,
    MemoryBackendUpdate,
    MemoryBackendResponse,
    MemoryBackendType,
)
from src.models.memory_backend import MemoryBackend
from src.services.memory_backend_service import MemoryBackendService


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
        endpoint_name="test-endpoint",
        short_term_index="test.catalog.short_term",
        long_term_index="test.catalog.long_term",
        entity_index="test.catalog.entity",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768
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
        "embedding_dimension": 768
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
    async def test_memory_backend_crud_operations(self, mock_memory_backend_service, sample_memory_backend):
        """Test CRUD operations for memory backend."""
        # Test create_memory_backend
        mock_memory_backend_service.create_memory_backend.return_value = sample_memory_backend
        create_data = MemoryBackendCreate(
            name="Test Backend",
            backend_type=MemoryBackendType.DATABRICKS
        )
        result = await mock_memory_backend_service.create_memory_backend("test-group-id", create_data)
        assert result.id == "test-backend-id"
        
        # Test get_memory_backends
        mock_memory_backend_service.get_memory_backends.return_value = [sample_memory_backend]
        result = await mock_memory_backend_service.get_memory_backends("test-group-id")
        assert len(result) == 1
        assert result[0].id == "test-backend-id"
        
        # Test get_memory_backend
        mock_memory_backend_service.get_memory_backend.return_value = sample_memory_backend
        result = await mock_memory_backend_service.get_memory_backend("test-group-id", "test-backend-id")
        assert result.id == "test-backend-id"
        
        # Test update_memory_backend
        mock_memory_backend_service.update_memory_backend.return_value = sample_memory_backend
        update_data = MemoryBackendUpdate(name="Updated Backend")
        result = await mock_memory_backend_service.update_memory_backend("test-group-id", "test-backend-id", update_data)
        assert result.id == "test-backend-id"
        
        # Test delete_memory_backend
        mock_memory_backend_service.delete_memory_backend.return_value = True
        result = await mock_memory_backend_service.delete_memory_backend("test-group-id", "test-backend-id")
        assert result is True
        
        # Test set_default_backend
        mock_memory_backend_service.set_default_backend.return_value = True
        result = await mock_memory_backend_service.set_default_backend("test-group-id", "test-backend-id")
        assert result is True
    
    @pytest.mark.asyncio
    async def test_memory_stats_operations(self, mock_memory_backend_service):
        """Test memory statistics operations."""
        # Test get_memory_stats
        mock_memory_backend_service.get_memory_stats.return_value = {
            "short_term": 10,
            "long_term": 20,
            "entity": 5
        }
        result = await mock_memory_backend_service.get_memory_stats("test-group-id", "test-crew-id")
        assert result["short_term"] == 10
        assert result["long_term"] == 20
        assert result["entity"] == 5
    
    @pytest.mark.asyncio
    async def test_databricks_connection_operations(self, mock_memory_backend_service, sample_databricks_config):
        """Test Databricks connection operations."""
        # Test test_databricks_connection
        mock_memory_backend_service.test_databricks_connection.return_value = {
            "success": True,
            "message": "Connection successful"
        }
        result = await mock_memory_backend_service.test_databricks_connection(sample_databricks_config, "test-token")
        assert result["success"] is True
        assert result["message"] == "Connection successful"
        
        # Test get_databricks_indexes
        mock_memory_backend_service.get_databricks_indexes.return_value = {
            "indexes": ["index1", "index2"]
        }
        result = await mock_memory_backend_service.get_databricks_indexes(sample_databricks_config, "test-token")
        assert result["indexes"] == ["index1", "index2"]
        
        # Test create_databricks_index
        mock_memory_backend_service.create_databricks_index.return_value = {
            "success": True,
            "index_name": "test.catalog.short_term"
        }
        result = await mock_memory_backend_service.create_databricks_index(
            sample_databricks_config, "test-token", "short_term", "test", "catalog", "short_term"
        )
        assert result["success"] is True
        assert result["index_name"] == "test.catalog.short_term"
    
    @pytest.mark.asyncio
    async def test_databricks_setup_operations(self, mock_memory_backend_service):
        """Test Databricks setup operations."""
        # Test one_click_databricks_setup
        mock_memory_backend_service.one_click_databricks_setup.return_value = {
            "success": True,
            "endpoint": "test-endpoint",
            "indexes": ["index1", "index2"]
        }
        result = await mock_memory_backend_service.one_click_databricks_setup(
            "test-group-id", "test-token", "https://test.databricks.com", "ml", "agents", 768
        )
        assert result["success"] is True
        assert result["endpoint"] == "test-endpoint"
        
        # Test empty_index
        mock_memory_backend_service.empty_index.return_value = {
            "success": True,
            "message": "Index emptied successfully",
            "num_deleted": 100
        }
        result = await mock_memory_backend_service.empty_index(
            "test-token", "https://test.databricks.com", "test.catalog.short_term",
            "test-endpoint", "short_term", 768
        )
        assert result["success"] is True
        assert result["num_deleted"] == 100
    
    @pytest.mark.asyncio
    async def test_databricks_resource_management(self, mock_memory_backend_service):
        """Test Databricks resource management operations."""
        # Test verify_databricks_resources
        mock_memory_backend_service.get_default_memory_backend.return_value = MagicMock()
        mock_memory_backend_service.verify_databricks_resources.return_value = {
            "endpoint_exists": True,
            "indexes_exist": ["index1"]
        }
        backend = await mock_memory_backend_service.get_default_memory_backend("test-group-id")
        result = await mock_memory_backend_service.verify_databricks_resources(
            backend, "test-token", "https://test.databricks.com"
        )
        assert result["endpoint_exists"] is True
        assert "index1" in result["indexes_exist"]
        
        # Test get_databricks_endpoint_status
        mock_memory_backend_service.get_databricks_endpoint_status.return_value = {
            "status": "ONLINE",
            "state": "RUNNING"
        }
        result = await mock_memory_backend_service.get_databricks_endpoint_status(
            "test-token", "https://test.databricks.com", "test-endpoint"
        )
        assert result["status"] == "ONLINE"
        assert result["state"] == "RUNNING"
        
        # Test delete_databricks_index
        mock_memory_backend_service.delete_databricks_index.return_value = {
            "success": True,
            "message": "Index deleted"
        }
        result = await mock_memory_backend_service.delete_databricks_index(
            "test-token", "https://test.databricks.com", "test.catalog.short_term", "test-endpoint"
        )
        assert result["success"] is True
        
        # Test delete_databricks_endpoint
        mock_memory_backend_service.delete_databricks_endpoint.return_value = {
            "success": True,
            "message": "Endpoint deleted"
        }
        result = await mock_memory_backend_service.delete_databricks_endpoint(
            "test-token", "https://test.databricks.com", "test-endpoint"
        )
        assert result["success"] is True
    
    @pytest.mark.asyncio
    async def test_configuration_mode_operations(self, mock_memory_backend_service):
        """Test configuration mode operations."""
        # Test delete_all_and_create_disabled
        mock_memory_backend_service.delete_all_and_create_disabled.return_value = {
            "success": True,
            "message": "Switched to disabled mode",
            "deleted_count": 2
        }
        result = await mock_memory_backend_service.delete_all_and_create_disabled("test-group-id")
        assert result["success"] is True
        assert result["deleted_count"] == 2
        
        # Test delete_disabled_configurations
        mock_memory_backend_service.delete_disabled_configurations.return_value = 3
        result = await mock_memory_backend_service.delete_disabled_configurations("test-group-id")
        assert result == 3
        
        # Test get_index_info
        mock_memory_backend_service.get_index_info.return_value = {
            "index_name": "test.catalog.short_term",
            "document_count": 150,
            "status": "ONLINE"
        }
        result = await mock_memory_backend_service.get_index_info(
            "test-token", "https://test.databricks.com", "test.catalog.short_term", "test-endpoint"
        )
        assert result["document_count"] == 150
        assert result["status"] == "ONLINE"