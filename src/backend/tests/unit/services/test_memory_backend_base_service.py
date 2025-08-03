"""
Unit tests for MemoryBackendBaseService.

Tests core CRUD operations for memory backend configurations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from uuid import uuid4

from src.services.memory_backend_base_service import MemoryBackendBaseService
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendCreate,
    MemoryBackendUpdate,
    MemoryBackendType,
    DatabricksMemoryConfig
)
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    uow.memory_backend_repository = AsyncMock()
    return uow


@pytest.fixture
def service(mock_uow):
    """Create a MemoryBackendBaseService instance."""
    return MemoryBackendBaseService(mock_uow)


@pytest.fixture
def sample_backend():
    """Create a sample memory backend."""
    return MemoryBackend(
        id=str(uuid4()),
        group_id="test-group-123",
        name="Test Backend",
        description="Test Description",
        backend_type=MemoryBackendType.DATABRICKS,
        databricks_config={
            "endpoint_name": "test-endpoint",
            "short_term_index": "test.schema.short_term",
            "long_term_index": "test.schema.long_term",
            "entity_index": "test.schema.entity",
            "workspace_url": "https://test.databricks.com",
            "embedding_dimension": 768
        },
        enable_short_term=True,
        enable_long_term=True,
        enable_entity=True,
        is_active=True,
        is_default=False,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def databricks_config():
    """Create a sample Databricks configuration."""
    return DatabricksMemoryConfig(
        endpoint_name="test-endpoint",
        short_term_index="test.schema.short_term",
        long_term_index="test.schema.long_term",
        entity_index="test.schema.entity",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768
    )


class TestMemoryBackendBaseService:
    """Test cases for MemoryBackendBaseService."""
    
    @pytest.mark.asyncio
    async def test_create_memory_backend_success(self, service, mock_uow, databricks_config):
        """Test successful creation of a memory backend."""
        # Arrange
        group_id = "test-group-123"
        config = MemoryBackendCreate(
            name="Test Backend",
            description="Test Description",
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config,
            enable_short_term=True,
            enable_long_term=True,
            enable_entity=True
        )
        
        mock_uow.memory_backend_repository.get_by_name.return_value = None
        mock_uow.memory_backend_repository.create.return_value = MagicMock(id="backend-123")
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [MagicMock()]
        
        # Act
        result = await service.create_memory_backend(group_id, config)
        
        # Assert
        assert result.id == "backend-123"
        mock_uow.memory_backend_repository.get_by_name.assert_called_once_with(group_id, config.name)
        mock_uow.memory_backend_repository.create.assert_called_once()
        mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_memory_backend_duplicate_name(self, service, mock_uow, databricks_config):
        """Test creation fails with duplicate name."""
        # Arrange
        group_id = "test-group-123"
        config = MemoryBackendCreate(
            name="Existing Backend",
            description="Test Description",
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config
        )
        
        mock_uow.memory_backend_repository.get_by_name.return_value = MagicMock()
        
        # Act & Assert
        with pytest.raises(ValueError, match="already exists"):
            await service.create_memory_backend(group_id, config)
    
    @pytest.mark.asyncio
    async def test_create_memory_backend_missing_databricks_config(self, service, mock_uow):
        """Test creation fails when Databricks config is missing."""
        # Arrange
        group_id = "test-group-123"
        config = MemoryBackendCreate(
            name="Test Backend",
            description="Test Description",
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=None
        )
        
        mock_uow.memory_backend_repository.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(ValueError, match="Databricks configuration is required"):
            await service.create_memory_backend(group_id, config)
    
    @pytest.mark.asyncio
    async def test_create_first_backend_sets_default(self, service, mock_uow, databricks_config):
        """Test first backend is automatically set as default."""
        # Arrange
        group_id = "test-group-123"
        config = MemoryBackendCreate(
            name="First Backend",
            description="Test Description",
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=databricks_config
        )
        
        created_backend = MagicMock(id="backend-123")
        mock_uow.memory_backend_repository.get_by_name.return_value = None
        mock_uow.memory_backend_repository.create.return_value = created_backend
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [created_backend]
        
        # Act
        await service.create_memory_backend(group_id, config)
        
        # Assert
        mock_uow.memory_backend_repository.set_default.assert_called_once_with(group_id, "backend-123")
    
    @pytest.mark.asyncio
    async def test_get_memory_backends(self, service, mock_uow, sample_backend):
        """Test getting all memory backends for a group."""
        # Arrange
        group_id = "test-group-123"
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [sample_backend]
        
        # Act
        result = await service.get_memory_backends(group_id)
        
        # Assert
        assert len(result) == 1
        assert result[0] == sample_backend
        mock_uow.memory_backend_repository.get_by_group_id.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_get_memory_backend_success(self, service, mock_uow, sample_backend):
        """Test getting a specific memory backend."""
        # Arrange
        group_id = "test-group-123"
        backend_id = sample_backend.id
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        
        # Act
        result = await service.get_memory_backend(group_id, backend_id)
        
        # Assert
        assert result == sample_backend
        mock_uow.memory_backend_repository.get.assert_called_once_with(backend_id)
    
    @pytest.mark.asyncio
    async def test_get_memory_backend_wrong_group(self, service, mock_uow, sample_backend):
        """Test getting backend returns None for wrong group."""
        # Arrange
        wrong_group_id = "wrong-group-456"
        backend_id = sample_backend.id
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        
        # Act
        result = await service.get_memory_backend(wrong_group_id, backend_id)
        
        # Assert
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_default_memory_backend(self, service, mock_uow, sample_backend):
        """Test getting the default memory backend."""
        # Arrange
        group_id = "test-group-123"
        mock_uow.memory_backend_repository.get_default_by_group_id.return_value = sample_backend
        
        # Act
        result = await service.get_default_memory_backend(group_id)
        
        # Assert
        assert result == sample_backend
        mock_uow.memory_backend_repository.get_default_by_group_id.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_update_memory_backend_success(self, service, mock_uow, sample_backend):
        """Test successful update of a memory backend."""
        # Arrange
        group_id = "test-group-123"
        backend_id = sample_backend.id
        update_data = MemoryBackendUpdate(
            name="Updated Backend",
            description="Updated Description",
            enable_short_term=False
        )
        
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        
        # Act
        result = await service.update_memory_backend(group_id, backend_id, update_data)
        
        # Assert
        assert result == sample_backend
        assert sample_backend.name == "Updated Backend"
        assert sample_backend.description == "Updated Description"
        assert sample_backend.enable_short_term == False
        mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_backend_type_clears_databricks_config(self, service, mock_uow, sample_backend):
        """Test updating backend type clears databricks config."""
        # Arrange
        group_id = "test-group-123"
        backend_id = sample_backend.id
        update_data = MemoryBackendUpdate(backend_type=MemoryBackendType.DEFAULT)
        
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        
        # Act
        await service.update_memory_backend(group_id, backend_id, update_data)
        
        # Assert
        assert sample_backend.backend_type == MemoryBackendType.DEFAULT
        assert sample_backend.databricks_config is None
    
    @pytest.mark.asyncio
    async def test_delete_memory_backend_success(self, service, mock_uow, sample_backend):
        """Test successful deletion of a memory backend."""
        # Arrange
        group_id = "test-group-123"
        backend_id = sample_backend.id
        other_backend = MagicMock(id="other-backend-123", is_default=False)
        
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [sample_backend, other_backend]
        
        # Act
        result = await service.delete_memory_backend(group_id, backend_id)
        
        # Assert
        assert result is True
        mock_uow.memory_backend_repository.delete.assert_called_once_with(backend_id)
        mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_last_backend_fails(self, service, mock_uow, sample_backend):
        """Test deletion fails for the last backend."""
        # Arrange
        group_id = "test-group-123"
        backend_id = sample_backend.id
        
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [sample_backend]
        
        # Act & Assert
        with pytest.raises(ValueError, match="Cannot delete the only memory backend"):
            await service.delete_memory_backend(group_id, backend_id)
    
    @pytest.mark.asyncio
    async def test_delete_default_backend_sets_new_default(self, service, mock_uow, sample_backend):
        """Test deleting default backend sets another as default."""
        # Arrange
        group_id = "test-group-123"
        backend_id = sample_backend.id
        sample_backend.is_default = True
        other_backend = MagicMock(id="other-backend-123", is_default=False)
        
        mock_uow.memory_backend_repository.get.return_value = sample_backend
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [sample_backend, other_backend]
        
        # Act
        await service.delete_memory_backend(group_id, backend_id)
        
        # Assert
        mock_uow.memory_backend_repository.set_default.assert_called_once_with(group_id, "other-backend-123")
    
    @pytest.mark.asyncio
    async def test_set_default_backend(self, service, mock_uow):
        """Test setting a backend as default."""
        # Arrange
        group_id = "test-group-123"
        backend_id = "backend-123"
        mock_uow.memory_backend_repository.set_default.return_value = True
        
        # Act
        result = await service.set_default_backend(group_id, backend_id)
        
        # Assert
        assert result is True
        mock_uow.memory_backend_repository.set_default.assert_called_once_with(group_id, backend_id)
        mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_memory_stats(self, service, mock_uow, sample_backend):
        """Test getting memory statistics."""
        # Arrange
        group_id = "test-group-123"
        crew_id = "crew-123"
        mock_uow.memory_backend_repository.get_default_by_group_id.return_value = sample_backend
        
        # Act
        result = await service.get_memory_stats(group_id, crew_id)
        
        # Assert
        assert result["backend_type"] == "databricks"
        assert result["backend_name"] == "Test Backend"
        assert "short_term_count" in result
        assert "long_term_count" in result
        assert "entity_count" in result
        assert "total_size_mb" in result
    
    @pytest.mark.asyncio
    async def test_delete_all_and_create_disabled(self, service, mock_uow):
        """Test deleting all configs and creating disabled one."""
        # Arrange
        group_id = "test-group-123"
        mock_uow.memory_backend_repository.delete_all_by_group_id.return_value = 3
        disabled_backend = MagicMock(id="disabled-123")
        mock_uow.memory_backend_repository.create.return_value = disabled_backend
        
        # Act
        result = await service.delete_all_and_create_disabled(group_id)
        
        # Assert
        assert result["success"] is True
        assert result["deleted_count"] == 3
        assert "Deleted 3 configurations" in result["message"]
        mock_uow.memory_backend_repository.delete_all_by_group_id.assert_called_once_with(group_id)
        mock_uow.memory_backend_repository.create.assert_called_once()
        mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_disabled_configurations(self, service, mock_uow):
        """Test deleting only disabled configurations."""
        # Arrange
        group_id = "test-group-123"
        disabled_backend = MagicMock(
            id="disabled-123",
            backend_type=MemoryBackendType.DEFAULT
        )
        enabled_backend = MagicMock(
            id="enabled-123",
            backend_type=MemoryBackendType.DATABRICKS
        )
        
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            disabled_backend,
            enabled_backend
        ]
        
        # Act
        result = await service.delete_disabled_configurations(group_id)
        
        # Assert
        assert result == 1
        mock_uow.memory_backend_repository.delete.assert_called_once_with("disabled-123")
        mock_uow.commit.assert_called_once()