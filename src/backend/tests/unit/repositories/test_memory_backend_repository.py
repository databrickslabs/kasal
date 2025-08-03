"""Unit tests for memory backend repository."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

from src.repositories.memory_backend_repository import MemoryBackendRepository
from src.models.memory_backend import MemoryBackend, MemoryBackendTypeEnum


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def repository(mock_session):
    """Create a MemoryBackendRepository instance with mock session."""
    return MemoryBackendRepository(mock_session)


@pytest.fixture
def sample_memory_backend():
    """Create a sample memory backend for testing."""
    return MemoryBackend(
        id="test-backend-id",
        group_id="test-group-id",
        name="Test Backend",
        backend_type=MemoryBackendTypeEnum.DATABRICKS,
        databricks_config={
            "endpoint_name": "test-endpoint",
            "short_term_index": "test.catalog.short_term",
            "workspace_url": "https://test.databricks.com",
            "embedding_dimension": 768
        },
        is_default=True,
        is_active=True,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_disabled_backend():
    """Create a sample disabled (DEFAULT type) memory backend."""
    return MemoryBackend(
        id="disabled-backend-id",
        group_id="test-group-id",
        name="Disabled Backend",
        backend_type=MemoryBackendTypeEnum.DEFAULT,
        is_default=False,
        is_active=True,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


class TestMemoryBackendRepository:
    """Test cases for MemoryBackendRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_group_id_success(self, repository, mock_session, sample_memory_backend):
        """Test successfully getting memory backends by group ID."""
        # Mock the query execution
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sample_memory_backend]
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_by_group_id("test-group-id")
        
        assert len(result) == 1
        assert result[0].id == "test-backend-id"
        assert result[0].group_id == "test-group-id"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_group_id_empty(self, repository, mock_session):
        """Test getting memory backends when none exist for group."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_by_group_id("non-existent-group")
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_by_group_id_error(self, repository, mock_session):
        """Test error handling in get_by_group_id."""
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        result = await repository.get_by_group_id("test-group-id")
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_default_by_group_id_success(self, repository, mock_session, sample_memory_backend):
        """Test successfully getting default memory backend."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_memory_backend
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_default_by_group_id("test-group-id")
        
        assert result is not None
        assert result.id == "test-backend-id"
        assert result.is_default is True
    
    @pytest.mark.asyncio
    async def test_get_default_by_group_id_none(self, repository, mock_session):
        """Test getting default when none exists."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_default_by_group_id("test-group-id")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_default_by_group_id_error(self, repository, mock_session):
        """Test error handling in get_default_by_group_id."""
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        result = await repository.get_default_by_group_id("test-group-id")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_by_name_success(self, repository, mock_session, sample_memory_backend):
        """Test successfully getting memory backend by name."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = sample_memory_backend
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_by_name("test-group-id", "Test Backend")
        
        assert result is not None
        assert result.name == "Test Backend"
    
    @pytest.mark.asyncio
    async def test_get_by_name_not_found(self, repository, mock_session):
        """Test getting by name when not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_by_name("test-group-id", "Non-existent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_set_default_success(self, repository, mock_session, sample_memory_backend):
        """Test successfully setting a backend as default."""
        # Mock getting existing defaults
        mock_result1 = MagicMock()
        mock_result1.scalars.return_value.all.return_value = []
        
        # Mock getting the backend to set as default
        mock_session.execute.return_value = mock_result1
        repository.get = AsyncMock(return_value=sample_memory_backend)
        
        result = await repository.set_default("test-group-id", "test-backend-id")
        
        assert result is True
        assert sample_memory_backend.is_default is True
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_set_default_with_existing_default(self, repository, mock_session, sample_memory_backend):
        """Test setting default when another default exists."""
        existing_default = MemoryBackend(
            id="existing-default-id",
            group_id="test-group-id",
            name="Existing Default",
            backend_type=MemoryBackendTypeEnum.DATABRICKS,
            is_default=True
        )
        
        # Mock getting existing defaults
        mock_result1 = MagicMock()
        mock_result1.scalars.return_value.all.return_value = [existing_default]
        
        mock_session.execute.return_value = mock_result1
        repository.get = AsyncMock(return_value=sample_memory_backend)
        
        result = await repository.set_default("test-group-id", "test-backend-id")
        
        assert result is True
        assert existing_default.is_default is False  # Old default should be unset
        assert sample_memory_backend.is_default is True
    
    @pytest.mark.asyncio
    async def test_set_default_backend_not_found(self, repository, mock_session):
        """Test setting default when backend doesn't exist."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        repository.get = AsyncMock(return_value=None)
        
        result = await repository.set_default("test-group-id", "non-existent-id")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_set_default_wrong_group(self, repository, mock_session):
        """Test setting default for backend from different group."""
        wrong_group_backend = MemoryBackend(
            id="wrong-group-backend",
            group_id="different-group-id",
            name="Wrong Group Backend",
            backend_type=MemoryBackendTypeEnum.DATABRICKS
        )
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        repository.get = AsyncMock(return_value=wrong_group_backend)
        
        result = await repository.set_default("test-group-id", "wrong-group-backend")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_set_default_error(self, repository, mock_session):
        """Test error handling in set_default."""
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        result = await repository.set_default("test-group-id", "test-backend-id")
        
        assert result is False
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_type_success(self, repository, mock_session, sample_memory_backend, sample_disabled_backend):
        """Test successfully getting backends by type."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sample_memory_backend]
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_by_type("test-group-id", MemoryBackendTypeEnum.DATABRICKS)
        
        assert len(result) == 1
        assert result[0].backend_type == MemoryBackendTypeEnum.DATABRICKS
    
    @pytest.mark.asyncio
    async def test_get_by_type_empty(self, repository, mock_session):
        """Test getting by type when none exist."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_by_type("test-group-id", MemoryBackendTypeEnum.DATABRICKS)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_by_type_error(self, repository, mock_session):
        """Test error handling in get_by_type."""
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        result = await repository.get_by_type("test-group-id", MemoryBackendTypeEnum.DATABRICKS)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_all_success(self, repository, mock_session, sample_memory_backend, sample_disabled_backend):
        """Test successfully getting all backends."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sample_memory_backend, sample_disabled_backend]
        mock_session.execute.return_value = mock_result
        
        result = await repository.get_all()
        
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_get_all_error(self, repository, mock_session):
        """Test error handling in get_all."""
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        result = await repository.get_all()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_delete_all_by_group_id_success(self, repository, mock_session, sample_memory_backend, sample_disabled_backend):
        """Test successfully deleting all backends for a group."""
        # Mock get_by_group_id to return backends
        repository.get_by_group_id = AsyncMock(return_value=[sample_memory_backend, sample_disabled_backend])
        
        result = await repository.delete_all_by_group_id("test-group-id")
        
        assert result == 2
        assert mock_session.delete.call_count == 2
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_by_group_id_empty(self, repository, mock_session):
        """Test deleting when no backends exist."""
        repository.get_by_group_id = AsyncMock(return_value=[])
        
        result = await repository.delete_all_by_group_id("test-group-id")
        
        assert result == 0
        mock_session.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_all_by_group_id_error(self, repository, mock_session):
        """Test error handling in delete_all_by_group_id."""
        repository.get_by_group_id = AsyncMock(side_effect=SQLAlchemyError("Database error"))
        
        result = await repository.delete_all_by_group_id("test-group-id")
        
        assert result == 0
        mock_session.rollback.assert_called_once()


class TestBaseRepositoryMethods:
    """Test cases for inherited BaseRepository methods."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, repository, mock_session, sample_memory_backend):
        """Test creating a new memory backend."""
        mock_session.add = MagicMock()
        mock_session.commit = AsyncMock()
        mock_session.flush = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        # Create data as dict
        new_backend_data = {
            "group_id": "test-group-id",
            "name": "New Backend",
            "backend_type": MemoryBackendTypeEnum.DATABRICKS,
            "databricks_config": {
                "endpoint_name": "new-endpoint",
                "short_term_index": "new.catalog.short_term",
                "workspace_url": "https://new.databricks.com",
                "embedding_dimension": 768
            }
        }
        
        # We need to mock the model constructor at the repository's model attribute
        with patch.object(repository, 'model') as mock_model:
            mock_model.__name__ = "MemoryBackend"  # Add __name__ attribute
            mock_instance = MagicMock()
            mock_instance.id = "new-backend-id"
            mock_instance.group_id = "test-group-id"
            mock_instance.name = "New Backend"
            mock_instance.backend_type = MemoryBackendTypeEnum.DATABRICKS
            mock_model.return_value = mock_instance
            
            result = await repository.create(new_backend_data)
            
            assert result == mock_instance
            mock_model.assert_called_once_with(**new_backend_data)
            mock_session.add.assert_called_once_with(mock_instance)
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_success(self, repository, mock_session, sample_memory_backend):
        """Test getting a backend by ID."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = sample_memory_backend
        mock_session.execute.return_value = mock_result
        
        result = await repository.get("test-backend-id")
        
        assert result is not None
        assert result == sample_memory_backend
        assert result.id == "test-backend-id"
    
    @pytest.mark.asyncio
    async def test_update_success(self, repository, mock_session, sample_memory_backend):
        """Test updating a memory backend."""
        mock_session.commit = AsyncMock()
        mock_session.flush = AsyncMock()
        mock_session.execute = AsyncMock()
        
        # Create updated backend with new name
        updated_backend = MagicMock()
        updated_backend.id = "test-backend-id"
        updated_backend.name = "Updated Backend"
        updated_backend.backend_type = sample_memory_backend.backend_type
        updated_backend.group_id = sample_memory_backend.group_id
        
        # Mock repository.get to return sample_memory_backend first, then updated_backend
        with patch.object(repository, 'get', new=AsyncMock()) as mock_get:
            mock_get.side_effect = [sample_memory_backend, updated_backend]
            
            # Update data
            update_data = {"name": "Updated Backend"}
            
            result = await repository.update("test-backend-id", update_data)
            
            assert result == updated_backend
            assert result.name == "Updated Backend"
            mock_session.execute.assert_called_once()
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_success(self, repository, mock_session, sample_memory_backend):
        """Test deleting a memory backend."""
        mock_session.delete = MagicMock()
        mock_session.commit = AsyncMock()
        mock_session.flush = AsyncMock()
        
        # Mock get to return the backend
        repository.get = AsyncMock(return_value=sample_memory_backend)
        
        result = await repository.delete("test-backend-id")
        
        assert result is True
        repository.get.assert_called_once_with("test-backend-id")
        mock_session.delete.assert_called_once_with(sample_memory_backend)