"""
Unit tests for DocumentationEmbeddingService.

Tests the DocumentationEmbeddingService's Databricks storage initialization
and documentation embedding operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any
from datetime import datetime
import uuid

from src.services.documentation_embedding_service import DocumentationEmbeddingService
from src.core.unit_of_work import UnitOfWork
from src.schemas.memory_backend import MemoryBackendType, MemoryBackendConfig
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.models.memory_backend import MemoryBackend


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    # Mock memory_backend_repository
    uow.memory_backend_repository = AsyncMock()
    return uow


@pytest.fixture
def service(mock_uow):
    """Create a DocumentationEmbeddingService instance."""
    return DocumentationEmbeddingService(mock_uow)


class TestDocumentationEmbeddingService:
    """Test suite for DocumentationEmbeddingService."""

    @pytest.mark.asyncio
    async def test_check_databricks_config_with_active_backend(self, service, mock_uow):
        """Test _check_databricks_config when active Databricks backend exists."""
        # Arrange
        databricks_backend = MagicMock(spec=MemoryBackend)
        databricks_backend.is_active = True
        databricks_backend.backend_type = MemoryBackendType.DATABRICKS
        databricks_backend.created_at = datetime.utcnow()
        databricks_backend.group_id = "test-group"
        databricks_backend.databricks_config = {
            "workspace_url": "https://test.databricks.com",
            "endpoint_name": "test-endpoint",
            "short_term_index": "ml.test.short_term",
            "document_index": "ml.docs.embeddings",
            "embedding_dimension": 1024
        }
        databricks_backend.enable_short_term = True
        databricks_backend.enable_long_term = True
        databricks_backend.enable_entity = True
        databricks_backend.custom_config = {}
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Act
        result = await service._check_databricks_config()
        
        # Assert
        assert result is True
        assert service._memory_config is not None
        assert service._memory_config.backend_type == MemoryBackendType.DATABRICKS
        assert service._checked_config is True
        mock_uow.memory_backend_repository.get_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_databricks_config_no_active_backend(self, service, mock_uow):
        """Test _check_databricks_config when no active Databricks backend exists."""
        # Arrange
        # Create an inactive backend
        inactive_backend = MagicMock(spec=MemoryBackend)
        inactive_backend.is_active = False
        inactive_backend.backend_type = MemoryBackendType.DATABRICKS
        
        mock_uow.memory_backend_repository.get_all.return_value = [inactive_backend]
        
        # Act
        result = await service._check_databricks_config()
        
        # Assert
        assert result is False
        assert service._memory_config is None
        assert service._checked_config is True

    @pytest.mark.asyncio
    async def test_check_databricks_config_caching(self, service, mock_uow):
        """Test that _check_databricks_config caches its result."""
        # Arrange
        databricks_backend = MagicMock(spec=MemoryBackend)
        databricks_backend.is_active = True
        databricks_backend.backend_type = MemoryBackendType.DATABRICKS
        databricks_backend.created_at = datetime.utcnow()
        databricks_backend.databricks_config = {
            "workspace_url": "https://test.databricks.com",
            "endpoint_name": "test-endpoint",
            "short_term_index": "ml.test.short_term"
        }
        databricks_backend.enable_short_term = True
        databricks_backend.enable_long_term = True
        databricks_backend.enable_entity = True
        databricks_backend.custom_config = {}
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Act - call twice
        result1 = await service._check_databricks_config()
        result2 = await service._check_databricks_config()
        
        # Assert
        assert result1 is True
        assert result2 is True
        # Should only call repository once due to caching
        mock_uow.memory_backend_repository.get_all.assert_called_once()

    @pytest.mark.asyncio
    @patch('src.services.databricks_index_service.DatabricksIndexService')
    @patch('src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage')
    async def test_get_databricks_storage_with_user_token(
        self, mock_vector_storage_class, mock_index_service_class, service, mock_uow
    ):
        """Test _get_databricks_storage passes user_token to index service."""
        # Arrange
        databricks_backend = MagicMock(spec=MemoryBackend)
        databricks_backend.is_active = True
        databricks_backend.backend_type = MemoryBackendType.DATABRICKS
        databricks_backend.created_at = datetime.utcnow()
        databricks_backend.group_id = "test-group"
        databricks_backend.databricks_config = {
            "workspace_url": "https://test.databricks.com",
            "endpoint_name": "test-endpoint",
            "short_term_index": "ml.test.short_term",
            "document_index": "ml.docs.embeddings",
            "embedding_dimension": 1024,
            "personal_access_token": "test-token",
            "service_principal_client_id": None,
            "service_principal_client_secret": None
        }
        databricks_backend.enable_short_term = True
        databricks_backend.enable_long_term = True
        databricks_backend.enable_entity = True
        databricks_backend.custom_config = {}
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Mock index service to return ready
        mock_index_service = AsyncMock()
        mock_index_service.wait_for_index_ready.return_value = {
            "ready": True,
            "message": "Index is ready",
            "attempts": 1,
            "elapsed_time": 0.5
        }
        mock_index_service_class.return_value = mock_index_service
        
        # Mock vector storage
        mock_vector_storage = MagicMock()
        mock_vector_storage.index_name = "ml.docs.embeddings"
        mock_vector_storage_class.return_value = mock_vector_storage
        
        user_token = "user-token-xyz"
        
        # Act
        result = await service._get_databricks_storage(user_token=user_token)
        
        # Assert
        assert result is not None
        assert result == mock_vector_storage
        
        # Verify index service was called with user_token
        mock_index_service.wait_for_index_ready.assert_called_once_with(
            workspace_url="https://test.databricks.com",
            index_name="ml.docs.embeddings",
            endpoint_name="test-endpoint",
            max_wait_seconds=60,
            check_interval_seconds=5,
            user_token=user_token
        )
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_index_service.DatabricksIndexService')
    @patch('src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage')
    async def test_get_databricks_storage_success(
        self, mock_vector_storage_class, mock_index_service_class, service, mock_uow
    ):
        """Test _get_databricks_storage successfully creates storage instance."""
        # Arrange
        databricks_backend = MagicMock(spec=MemoryBackend)
        databricks_backend.is_active = True
        databricks_backend.backend_type = MemoryBackendType.DATABRICKS
        databricks_backend.created_at = datetime.utcnow()
        databricks_backend.group_id = "test-group"
        databricks_backend.databricks_config = {
            "workspace_url": "https://test.databricks.com",
            "endpoint_name": "test-endpoint",
            "short_term_index": "ml.test.short_term",
            "document_index": "ml.docs.embeddings",
            "embedding_dimension": 1024,
            "personal_access_token": "test-token",
            "service_principal_client_id": None,
            "service_principal_client_secret": None
        }
        databricks_backend.enable_short_term = True
        databricks_backend.enable_long_term = True
        databricks_backend.enable_entity = True
        databricks_backend.custom_config = {}
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Mock index service to return ready
        mock_index_service = AsyncMock()
        mock_index_service.wait_for_index_ready.return_value = {
            "ready": True,
            "message": "Index is ready",
            "attempts": 1,
            "elapsed_time": 0.5
        }
        mock_index_service_class.return_value = mock_index_service
        
        # Mock vector storage
        mock_vector_storage = MagicMock()
        mock_vector_storage.index_name = "ml.docs.embeddings"
        mock_vector_storage_class.return_value = mock_vector_storage
        
        # Act
        result = await service._get_databricks_storage()
        
        # Assert
        assert result is not None
        assert result == mock_vector_storage
        
        # Verify index service was called
        mock_index_service.wait_for_index_ready.assert_called_once_with(
            workspace_url="https://test.databricks.com",
            index_name="ml.docs.embeddings",
            endpoint_name="test-endpoint",
            max_wait_seconds=60,
            check_interval_seconds=5,
            user_token=None
        )
        
        # Verify vector storage was created with correct params
        mock_vector_storage_class.assert_called_once_with(
            endpoint_name="test-endpoint",
            index_name="ml.docs.embeddings",
            crew_id="documentation",
            memory_type="document",
            embedding_dimension=1024,
            workspace_url="https://test.databricks.com",
            personal_access_token="test-token",
            service_principal_client_id=None,
            service_principal_client_secret=None,
            user_token=None  # Added expected user_token parameter
        )

    @pytest.mark.asyncio
    @patch('src.services.databricks_index_service.DatabricksIndexService')
    async def test_get_databricks_storage_index_not_ready(
        self, mock_index_service_class, service, mock_uow
    ):
        """Test _get_databricks_storage when index is not ready."""
        # Arrange
        databricks_backend = MagicMock(spec=MemoryBackend)
        databricks_backend.is_active = True
        databricks_backend.backend_type = MemoryBackendType.DATABRICKS
        databricks_backend.created_at = datetime.utcnow()
        databricks_backend.databricks_config = {
            "workspace_url": "https://test.databricks.com",
            "endpoint_name": "test-endpoint",
            "short_term_index": "ml.test.short_term",
            "document_index": "ml.docs.embeddings",
            "embedding_dimension": 1024
        }
        databricks_backend.enable_short_term = True
        databricks_backend.enable_long_term = True
        databricks_backend.enable_entity = True
        databricks_backend.custom_config = {}
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Mock index service to return not ready
        mock_index_service = AsyncMock()
        mock_index_service.wait_for_index_ready.return_value = {
            "ready": False,
            "message": "Index is still provisioning",
            "attempts": 12,
            "elapsed_time": 60.0
        }
        mock_index_service_class.return_value = mock_index_service
        
        # Act
        result = await service._get_databricks_storage()
        
        # Assert
        assert result is None
        
        # Verify index service was called
        mock_index_service.wait_for_index_ready.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_databricks_storage_no_config(self, service, mock_uow):
        """Test _get_databricks_storage when no Databricks config exists."""
        # Arrange
        mock_uow.memory_backend_repository.get_all.return_value = []
        
        # Act
        result = await service._get_databricks_storage()
        
        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_get_databricks_storage_caching(self, service):
        """Test that _get_databricks_storage caches the storage instance."""
        # Arrange
        mock_storage = MagicMock()
        service._databricks_storage = mock_storage
        
        # Act
        result = await service._get_databricks_storage()
        
        # Assert
        assert result == mock_storage

    @pytest.mark.asyncio
    @patch('src.services.databricks_index_service.DatabricksIndexService')
    async def test_get_databricks_storage_with_document_endpoint_name(
        self, mock_index_service_class, service, mock_uow
    ):
        """Test _get_databricks_storage uses document_endpoint_name when available."""
        # Arrange
        databricks_backend = MagicMock(spec=MemoryBackend)
        databricks_backend.is_active = True
        databricks_backend.backend_type = MemoryBackendType.DATABRICKS
        databricks_backend.created_at = datetime.utcnow()
        
        # Use a dictionary for databricks_config to match expected format
        databricks_backend.databricks_config = {
            "workspace_url": "https://test.databricks.com",
            "endpoint_name": "default-endpoint",
            "document_endpoint_name": "document-specific-endpoint",
            "short_term_index": "ml.test.short_term",  # Required field
            "document_index": "ml.docs.embeddings",
            "embedding_dimension": 1024,
            "personal_access_token": "test-token",
            "service_principal_client_id": None,
            "service_principal_client_secret": None
        }
        databricks_backend.enable_short_term = True
        databricks_backend.enable_long_term = True
        databricks_backend.enable_entity = True
        databricks_backend.custom_config = {}
        
        mock_uow.memory_backend_repository.get_all.return_value = [databricks_backend]
        
        # Mock index service to return ready
        mock_index_service = AsyncMock()
        mock_index_service.wait_for_index_ready.return_value = {
            "ready": True,
            "message": "Index is ready",
            "attempts": 1,
            "elapsed_time": 0.5
        }
        mock_index_service_class.return_value = mock_index_service
        
        # Act
        with patch('src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage') as mock_vector_storage_class:
            mock_vector_storage = MagicMock()
            mock_vector_storage.index_name = "ml.docs.embeddings"
            mock_vector_storage_class.return_value = mock_vector_storage
            
            result = await service._get_databricks_storage()
            
            # Assert - should use document_endpoint_name
            mock_vector_storage_class.assert_called_once()
            call_kwargs = mock_vector_storage_class.call_args[1]
            assert call_kwargs['endpoint_name'] == "document-specific-endpoint"

    @pytest.mark.asyncio
    @patch.object(DocumentationEmbeddingService, '_get_databricks_storage')
    async def test_create_documentation_embedding_with_databricks(
        self, mock_get_storage, service
    ):
        """Test create_documentation_embedding with Databricks storage."""
        # Arrange
        mock_storage = AsyncMock()
        mock_storage.index_name = "ml.docs.embeddings"
        mock_storage.save = AsyncMock()
        mock_storage.get_stats = AsyncMock(return_value={"count": 100})
        mock_get_storage.return_value = mock_storage
        
        doc_embedding = DocumentationEmbeddingCreate(
            source="test.md",
            title="Test Document",
            content="Test content",
            embedding=[0.1, 0.2, 0.3],
            doc_metadata={"category": "test"}
        )
        
        # Act
        result = await service.create_documentation_embedding(doc_embedding)
        
        # Assert
        assert result is not None
        assert result.source == "test.md"
        assert result.title == "Test Document"
        assert result.content == "Test content"
        
        # Verify _get_databricks_storage was called with user_token=None
        mock_get_storage.assert_called_once_with(user_token=None)
        
        # Verify save was called with correct data structure
        mock_storage.save.assert_called_once()
        saved_data = mock_storage.save.call_args[0][0]
        assert 'content' in saved_data
        assert 'embedding' in saved_data
        assert 'metadata' in saved_data
        assert saved_data['content'] == "Test content"
        assert saved_data['embedding'] == [0.1, 0.2, 0.3]
    
    @pytest.mark.asyncio
    @patch.object(DocumentationEmbeddingService, '_get_databricks_storage')
    async def test_create_documentation_embedding_with_user_token(
        self, mock_get_storage, service
    ):
        """Test create_documentation_embedding passes user_token correctly."""
        # Arrange
        mock_storage = AsyncMock()
        mock_storage.index_name = "ml.docs.embeddings"
        mock_storage.save = AsyncMock()
        mock_storage.get_stats = AsyncMock(return_value={"count": 100})
        mock_get_storage.return_value = mock_storage
        
        doc_embedding = DocumentationEmbeddingCreate(
            source="test.md",
            title="Test Document",
            content="Test content",
            embedding=[0.1, 0.2, 0.3],
            doc_metadata={"category": "test"}
        )
        
        user_token = "test-user-token-123"
        
        # Act
        result = await service.create_documentation_embedding(doc_embedding, user_token=user_token)
        
        # Assert
        assert result is not None
        
        # Verify _get_databricks_storage was called with the user_token
        mock_get_storage.assert_called_once_with(user_token=user_token)

    @pytest.mark.asyncio
    @patch.object(DocumentationEmbeddingService, '_get_databricks_storage')
    async def test_create_documentation_embedding_databricks_error_not_ready(
        self, mock_get_storage, service
    ):
        """Test create_documentation_embedding returns placeholder for 'not ready' errors."""
        # Arrange
        mock_storage = AsyncMock()
        mock_storage.index_name = "ml.docs.embeddings"
        mock_storage.save = AsyncMock(side_effect=Exception("Index not ready"))
        mock_get_storage.return_value = mock_storage
        
        doc_embedding = DocumentationEmbeddingCreate(
            source="test.md",
            title="Test Document",
            content="Test content",
            embedding=[0.1, 0.2, 0.3]
        )
        
        # Act
        result = await service.create_documentation_embedding(doc_embedding)
        
        # Assert - should return placeholder for "not ready" error
        assert result is not None
        assert result.id.startswith("pending-")
        assert result.source == "test.md"
        assert result.title == "Test Document"
        assert result.content == "Test content"
    
    @pytest.mark.asyncio
    @patch.object(DocumentationEmbeddingService, '_get_databricks_storage')
    async def test_create_documentation_embedding_databricks_error_other(
        self, mock_get_storage, service
    ):
        """Test create_documentation_embedding raises for non-'not ready' errors."""
        # Arrange
        mock_storage = AsyncMock()
        mock_storage.index_name = "ml.docs.embeddings"
        mock_storage.save = AsyncMock(side_effect=Exception("Connection error"))
        mock_get_storage.return_value = mock_storage
        
        doc_embedding = DocumentationEmbeddingCreate(
            source="test.md",
            title="Test Document",
            content="Test content",
            embedding=[0.1, 0.2, 0.3]
        )
        
        # Act & Assert - should raise the exception
        with pytest.raises(Exception, match="Connection error"):
            await service.create_documentation_embedding(doc_embedding)