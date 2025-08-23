"""Unit tests for documentation embeddings router."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient
from datetime import datetime

from src.main import app
from src.schemas.documentation_embedding import (
    DocumentationEmbedding as DocumentationEmbeddingSchema,
    DocumentationEmbeddingCreate,
)
from src.models.documentation_embedding import DocumentationEmbedding
from src.services.documentation_embedding_service import DocumentationEmbeddingService

client = TestClient(app)


@pytest.fixture
def sample_embedding_data():
    """Create sample embedding data."""
    return DocumentationEmbeddingCreate(
        source="test_source",
        title="Test Documentation",
        content="This is test documentation content",
        embedding=[0.1, 0.2, 0.3, 0.4, 0.5],
        doc_metadata={"category": "test", "version": "1.0"}
    )


@pytest.fixture
def sample_documentation_embedding():
    """Create a sample documentation embedding model."""
    return DocumentationEmbedding(
        id=1,
        source="test_source",
        title="Test Documentation",
        content="This is test documentation content",
        embedding=[0.1, 0.2, 0.3, 0.4, 0.5],
        doc_metadata={"category": "test", "version": "1.0"},
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def mock_uow():
    """Create a mock UnitOfWork."""
    uow = AsyncMock()
    uow.commit = AsyncMock()
    return uow


@pytest.fixture
def mock_uow_context():
    """Create a mock UnitOfWork context manager."""
    uow_instance = AsyncMock()
    uow_instance.__aenter__ = AsyncMock()
    uow_instance.__aexit__ = AsyncMock(return_value=None)
    return uow_instance


@pytest.fixture
def mock_documentation_service():
    """Create a mock DocumentationEmbeddingService."""
    service = AsyncMock(spec=DocumentationEmbeddingService)
    return service


class TestCreateDocumentationEmbedding:
    """Test cases for create_documentation_embedding endpoint."""
    
    def test_create_documentation_embedding_success(self, sample_embedding_data, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test successful creation of documentation embedding."""
        mock_documentation_service.create_documentation_embedding.return_value = sample_documentation_embedding
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.post("/api/v1/documentation-embeddings/", json=sample_embedding_data.model_dump())
        
        assert response.status_code == 200
        result = response.json()
        assert result["id"] == 1
        assert result["source"] == "test_source"
        assert result["title"] == "Test Documentation"
        # Verify that create_documentation_embedding was called with the right arguments
        args, kwargs = mock_documentation_service.create_documentation_embedding.call_args
        assert args[0].source == "test_source"
        assert kwargs.get("user_token") is None  # No auth headers provided
        # Commit is not called here because it's handled by the UnitOfWork context manager
    
    def test_create_documentation_embedding_with_auth_headers(self, sample_embedding_data, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test creation with authentication headers."""
        mock_documentation_service.create_documentation_embedding.return_value = sample_documentation_embedding
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                headers = {
                    "X-Forwarded-Access-Token": "test-token-123",
                    "X-Auth-Request-Access-Token": "oauth-token-456"
                }
                response = client.post("/api/v1/documentation-embeddings/", json=sample_embedding_data.model_dump(), headers=headers)
        
        assert response.status_code == 200
        # Verify OAuth2-Proxy token takes priority
        args, kwargs = mock_documentation_service.create_documentation_embedding.call_args
        assert kwargs.get("user_token") == "oauth-token-456"
    
    def test_create_documentation_embedding_with_databricks_token_only(self, sample_embedding_data, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test creation with only Databricks Apps token."""
        mock_documentation_service.create_documentation_embedding.return_value = sample_documentation_embedding
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                headers = {"X-Forwarded-Access-Token": "databricks-token-789"}
                response = client.post("/api/v1/documentation-embeddings/", json=sample_embedding_data.model_dump(), headers=headers)
        
        assert response.status_code == 200
        # Verify Databricks token is used when OAuth2-Proxy token is absent
        args, kwargs = mock_documentation_service.create_documentation_embedding.call_args
        assert kwargs.get("user_token") == "databricks-token-789"
    
    def test_create_documentation_embedding_error(self, sample_embedding_data, mock_uow, mock_documentation_service):
        """Test error handling during creation."""
        mock_documentation_service.create_documentation_embedding.side_effect = Exception("Database error")
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.post("/api/v1/documentation-embeddings/", json=sample_embedding_data.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestSearchDocumentationEmbeddings:
    """Test cases for search_documentation_embeddings endpoint."""
    
    def test_search_embeddings_success(self, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test successful search of documentation embeddings."""
        query_embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
        mock_documentation_service.search_similar_embeddings.return_value = [sample_documentation_embedding]
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/search", params={"query_embedding": query_embedding, "limit": 5})
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 1
        assert result[0]["id"] == 1
        mock_documentation_service.search_similar_embeddings.assert_called_once_with(
            query_embedding=query_embedding,
            limit=5
        )
    
    def test_search_embeddings_with_custom_limit(self, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test search with custom limit."""
        query_embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
        embeddings = [sample_documentation_embedding] * 10
        mock_documentation_service.search_similar_embeddings.return_value = embeddings
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/search", params={"query_embedding": query_embedding, "limit": 10})
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 10
        mock_documentation_service.search_similar_embeddings.assert_called_once_with(
            query_embedding=query_embedding,
            limit=10
        )
    
    def test_search_embeddings_error(self, mock_uow, mock_documentation_service):
        """Test error handling during search."""
        query_embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
        mock_documentation_service.search_similar_embeddings.side_effect = Exception("Search error")
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/search", params={"query_embedding": query_embedding})
        
        assert response.status_code == 500
        assert "Search error" in response.json()["detail"]


class TestGetDocumentationEmbeddings:
    """Test cases for get_documentation_embeddings endpoint."""
    
    def test_get_embeddings_no_filter(self, sample_documentation_embedding):
        """Test getting embeddings without filters."""
        # Create a mock object with the necessary attributes
        mock_embedding = MagicMock()
        mock_embedding.id = 1
        mock_embedding.source = "test_source"
        mock_embedding.title = "Test Documentation"
        mock_embedding.content = "Test content"
        mock_embedding.doc_metadata = {}
        mock_embedding.created_at = sample_documentation_embedding.created_at
        mock_embedding.updated_at = sample_documentation_embedding.updated_at
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        
        # Create mock service that returns expected data
        mock_service = AsyncMock()
        mock_service.get_documentation_embeddings.return_value = [mock_embedding]
        
        # Create mock UOW
        mock_uow = AsyncMock()
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            # Make UnitOfWork() return an async context manager
            mock_uow_context = AsyncMock()
            mock_uow_context.__aenter__.return_value = mock_uow
            mock_uow_context.__aexit__.return_value = None
            mock_uow_class.return_value = mock_uow_context
            
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                # Make DocumentationEmbeddingService(uow) return our mock service
                mock_service_class.return_value = mock_service
                
                response = client.get("/api/v1/documentation-embeddings/")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["embedding"] == []  # Embeddings cleared for list view
        mock_service.get_documentation_embeddings.assert_called_once_with(0, 100)
    
    def test_get_embeddings_with_source_filter(self, sample_documentation_embedding):
        """Test getting embeddings filtered by source."""
        # Create a mock object with the necessary attributes
        mock_embedding = MagicMock()
        mock_embedding.id = 1
        mock_embedding.source = "test_source"
        mock_embedding.title = "Test Documentation"
        mock_embedding.content = "Test content"
        mock_embedding.doc_metadata = {}
        mock_embedding.created_at = sample_documentation_embedding.created_at
        mock_embedding.updated_at = sample_documentation_embedding.updated_at
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        
        # Create mock service
        mock_service = AsyncMock()
        mock_service.search_by_source.return_value = [mock_embedding]
        
        # Create mock UOW
        mock_uow = AsyncMock()
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_context = AsyncMock()
            mock_uow_context.__aenter__.return_value = mock_uow
            mock_uow_context.__aexit__.return_value = None
            mock_uow_class.return_value = mock_uow_context
            
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_service
                response = client.get("/api/v1/documentation-embeddings/?source=test_source")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 1
        assert result[0]["source"] == "test_source"
        mock_service.search_by_source.assert_called_once_with("test_source", 0, 100)
    
    def test_get_embeddings_with_title_filter(self, sample_documentation_embedding):
        """Test getting embeddings filtered by title."""
        # Create a mock object with the necessary attributes
        mock_embedding = MagicMock()
        mock_embedding.id = 1
        mock_embedding.source = "test_source"
        mock_embedding.title = "Test Documentation"
        mock_embedding.content = "Test content"
        mock_embedding.doc_metadata = {}
        mock_embedding.created_at = sample_documentation_embedding.created_at
        mock_embedding.updated_at = sample_documentation_embedding.updated_at
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        
        mock_service = AsyncMock()
        mock_service.search_by_title.return_value = [mock_embedding]
        
        mock_uow = AsyncMock()
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_context = AsyncMock()
            mock_uow_context.__aenter__.return_value = mock_uow
            mock_uow_context.__aexit__.return_value = None
            mock_uow_class.return_value = mock_uow_context
            
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_service
                response = client.get("/api/v1/documentation-embeddings/?title=Test")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 1
        assert result[0]["title"] == "Test Documentation"
        mock_service.search_by_title.assert_called_once_with("Test", 0, 100)
    
    def test_get_embeddings_with_pagination(self, sample_documentation_embedding):
        """Test getting embeddings with pagination."""
        # Create a mock object with the necessary attributes
        mock_embedding = MagicMock()
        mock_embedding.id = 1
        mock_embedding.source = "test_source"
        mock_embedding.title = "Test Documentation"
        mock_embedding.content = "Test content"
        mock_embedding.doc_metadata = {}
        mock_embedding.created_at = sample_documentation_embedding.created_at
        mock_embedding.updated_at = sample_documentation_embedding.updated_at
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        
        mock_service = AsyncMock()
        mock_service.get_documentation_embeddings.return_value = [mock_embedding]
        
        mock_uow = AsyncMock()
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_context = AsyncMock()
            mock_uow_context.__aenter__.return_value = mock_uow
            mock_uow_context.__aexit__.return_value = None
            mock_uow_class.return_value = mock_uow_context
            
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_service
                response = client.get("/api/v1/documentation-embeddings/?skip=10&limit=20")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 1
        mock_service.get_documentation_embeddings.assert_called_once_with(10, 20)
    
    def test_get_embeddings_error(self):
        """Test error handling when getting embeddings."""
        mock_service = AsyncMock()
        mock_service.get_documentation_embeddings.side_effect = Exception("Database error")
        
        mock_uow = AsyncMock()
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_context = AsyncMock()
            mock_uow_context.__aenter__.return_value = mock_uow
            mock_uow_context.__aexit__.return_value = None
            mock_uow_class.return_value = mock_uow_context
            
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_service
                response = client.get("/api/v1/documentation-embeddings/")
                # The error should be handled and return a 500 status
                assert response.status_code == 500


class TestGetDocumentationEmbeddingById:
    """Test cases for get_documentation_embedding endpoint."""
    
    def test_get_embedding_by_id_success(self, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test successfully getting embedding by ID."""
        mock_documentation_service.get_documentation_embedding.return_value = sample_documentation_embedding
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/1")
        
        assert response.status_code == 200
        result = response.json()
        assert result["id"] == 1
        assert result["source"] == "test_source"
        mock_documentation_service.get_documentation_embedding.assert_called_once_with(1)
    
    def test_get_embedding_by_id_not_found(self, mock_uow, mock_documentation_service):
        """Test getting non-existent embedding."""
        mock_documentation_service.get_documentation_embedding.return_value = None
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/999")
        
        assert response.status_code == 404
        assert "Documentation embedding not found" in response.json()["detail"]
    
    def test_get_embedding_by_id_error(self, mock_uow, mock_documentation_service):
        """Test error handling when getting by ID."""
        mock_documentation_service.get_documentation_embedding.side_effect = Exception("Database error")
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/1")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDeleteDocumentationEmbedding:
    """Test cases for delete_documentation_embedding endpoint."""
    
    def test_delete_embedding_success(self, mock_uow, mock_documentation_service):
        """Test successful deletion of embedding."""
        mock_documentation_service.delete_documentation_embedding.return_value = True
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.delete("/api/v1/documentation-embeddings/1")
        
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Documentation embedding deleted successfully"
        mock_documentation_service.delete_documentation_embedding.assert_called_once_with(1)
        mock_uow.commit.assert_called_once()
    
    def test_delete_embedding_not_found(self, mock_uow, mock_documentation_service):
        """Test deleting non-existent embedding."""
        mock_documentation_service.delete_documentation_embedding.return_value = False
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.delete("/api/v1/documentation-embeddings/999")
        
        assert response.status_code == 404
        assert "Documentation embedding not found" in response.json()["detail"]
    
    def test_delete_embedding_error(self, mock_uow, mock_documentation_service):
        """Test error handling during deletion."""
        mock_documentation_service.delete_documentation_embedding.side_effect = Exception("Database error")
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.delete("/api/v1/documentation-embeddings/1")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetRecentDocumentationEmbeddings:
    """Test cases for get_recent_documentation_embeddings endpoint."""
    
    def test_get_recent_embeddings_success(self, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test successfully getting recent embeddings."""
        recent_embeddings = [sample_documentation_embedding] * 5
        mock_documentation_service.get_recent_embeddings.return_value = recent_embeddings
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/recent?limit=5")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 5
        mock_documentation_service.get_recent_embeddings.assert_called_once_with(5)
    
    def test_get_recent_embeddings_custom_limit(self, sample_documentation_embedding, mock_uow, mock_documentation_service):
        """Test getting recent embeddings with custom limit."""
        recent_embeddings = [sample_documentation_embedding] * 20
        mock_documentation_service.get_recent_embeddings.return_value = recent_embeddings
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/recent?limit=20")
        
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 20
        mock_documentation_service.get_recent_embeddings.assert_called_once_with(20)
    
    def test_get_recent_embeddings_error(self, mock_uow, mock_documentation_service):
        """Test error handling when getting recent embeddings."""
        mock_documentation_service.get_recent_embeddings.side_effect = Exception("Database error")
        
        with patch('src.api.documentation_embeddings_router.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            with patch('src.api.documentation_embeddings_router.DocumentationEmbeddingService') as mock_service_class:
                mock_service_class.return_value = mock_documentation_service
                response = client.get("/api/v1/documentation-embeddings/recent")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestSeedAllDocumentationEmbeddings:
    """Test cases for seed_all_documentation_embeddings endpoint."""
    
    def test_seed_all_success(self):
        """Test successful re-seeding of documentation."""
        with patch('src.seeds.documentation.seed_documentation_embeddings') as mock_seed:
            mock_seed.return_value = None
            response = client.post("/api/v1/documentation-embeddings/seed-all")
        
        assert response.status_code == 200
        result = response.json()
        assert result["success"] is True
        assert "successfully" in result["message"]
        # Verify seed function was called with user_token=None (no headers)
        mock_seed.assert_called_once_with(user_token=None)
    
    def test_seed_all_with_auth_headers(self):
        """Test re-seeding with authentication headers."""
        with patch('src.seeds.documentation.seed_documentation_embeddings') as mock_seed:
            mock_seed.return_value = None
            headers = {
                "X-Forwarded-Access-Token": "test-token",
                "X-Auth-Request-Access-Token": "oauth-token"
            }
            response = client.post("/api/v1/documentation-embeddings/seed-all", headers=headers)
        
        assert response.status_code == 200
        result = response.json()
        assert result["success"] is True
        # Verify OAuth2-Proxy token takes priority
        mock_seed.assert_called_once_with(user_token="oauth-token")
    
    def test_seed_all_with_databricks_token_only(self):
        """Test re-seeding with only Databricks Apps token."""
        with patch('src.seeds.documentation.seed_documentation_embeddings') as mock_seed:
            mock_seed.return_value = None
            headers = {"X-Forwarded-Access-Token": "databricks-token"}
            response = client.post("/api/v1/documentation-embeddings/seed-all", headers=headers)
        
        assert response.status_code == 200
        result = response.json()
        assert result["success"] is True
        # Verify Databricks token is used
        mock_seed.assert_called_once_with(user_token="databricks-token")
    
    def test_seed_all_error(self):
        """Test error handling during re-seeding."""
        with patch('src.seeds.documentation.seed_documentation_embeddings') as mock_seed:
            mock_seed.side_effect = Exception("Seeding error")
            response = client.post("/api/v1/documentation-embeddings/seed-all")
        
        assert response.status_code == 200  # Note: endpoint returns 200 even on error
        result = response.json()
        assert result["success"] is False
        assert "Failed" in result["message"]
        assert "Seeding error" in result["message"]