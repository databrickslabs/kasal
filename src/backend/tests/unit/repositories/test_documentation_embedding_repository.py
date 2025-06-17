"""
Unit tests for DocumentationEmbeddingRepository.

Tests the functionality of documentation embedding repository including
CRUD operations, search functionality, and similarity operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate


# Mock documentation embedding model
class MockDocumentationEmbedding:
    def __init__(self, id=1, source="test_source.md", title="Test Document", 
                 content="Test content", embedding=None, doc_metadata=None, 
                 created_at=None, **kwargs):
        self.id = id
        self.source = source
        self.title = title
        self.content = content
        self.embedding = embedding or [0.1, 0.2, 0.3, 0.4, 0.5]
        self.doc_metadata = doc_metadata or {}
        self.created_at = created_at or datetime.now()
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock documentation embedding create schema
class MockDocumentationEmbeddingCreate:
    def __init__(self, source="test_source.md", title="Test Document", 
                 content="Test content", embedding=None, doc_metadata=None):
        self.source = source
        self.title = title
        self.content = content
        self.embedding = embedding or [0.1, 0.2, 0.3, 0.4, 0.5]
        self.doc_metadata = doc_metadata or {}


# Mock SQLAlchemy query object
class MockQuery:
    def __init__(self, results=None):
        self.results = results or []
        self._filter_applied = False
        self._order_applied = False
        self._offset_applied = False
        self._limit_applied = False
    
    def filter(self, *args):
        self._filter_applied = True
        return self
    
    def order_by(self, *args):
        self._order_applied = True
        return self
    
    def offset(self, offset):
        self._offset_applied = True
        return self
    
    def limit(self, limit):
        self._limit_applied = True
        return self
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.add = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.query = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.refresh = MagicMock()
    session.rollback = MagicMock()
    session.delete = MagicMock()
    return session


@pytest.fixture
def sample_embeddings():
    """Create sample documentation embeddings for testing."""
    return [
        MockDocumentationEmbedding(
            id=1, source="intro.md", title="Introduction", 
            content="Getting started guide", embedding=[0.1, 0.2, 0.3]
        ),
        MockDocumentationEmbedding(
            id=2, source="api.md", title="API Reference", 
            content="API documentation", embedding=[0.4, 0.5, 0.6]
        ),
        MockDocumentationEmbedding(
            id=3, source="tutorial.md", title="Tutorial", 
            content="Step by step tutorial", embedding=[0.7, 0.8, 0.9]
        ),
        MockDocumentationEmbedding(
            id=4, source="faq.md", title="FAQ", 
            content="Frequently asked questions", embedding=[0.2, 0.4, 0.6]
        )
    ]


@pytest.fixture
def sample_embedding_create():
    """Create sample embedding create schema for testing."""
    return MockDocumentationEmbeddingCreate(
        source="new_doc.md",
        title="New Document",
        content="This is a new document",
        embedding=[0.1, 0.2, 0.3, 0.4, 0.5],
        doc_metadata={"author": "test", "version": "1.0"}
    )


class TestDocumentationEmbeddingRepositoryCreate:
    """Test create functionality."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, mock_async_session, sample_embedding_create):
        """Test create documentation embedding successfully."""
        created_embedding = MockDocumentationEmbedding(
            id=1,
            source=sample_embedding_create.source,
            title=sample_embedding_create.title,
            content=sample_embedding_create.content,
            embedding=sample_embedding_create.embedding,
            doc_metadata=sample_embedding_create.doc_metadata
        )
        
        with patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding') as mock_model:
            mock_model.return_value = created_embedding
            
            result = await DocumentationEmbeddingRepository.create(
                mock_async_session, sample_embedding_create
            )
            
            assert result == created_embedding
            mock_async_session.add.assert_called_once_with(created_embedding)
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(created_embedding)
            
            # Verify DocumentationEmbedding was created with correct parameters
            call_args = mock_model.call_args[1]
            assert call_args['source'] == sample_embedding_create.source
            assert call_args['title'] == sample_embedding_create.title
            assert call_args['content'] == sample_embedding_create.content
            assert call_args['embedding'] == sample_embedding_create.embedding
            assert call_args['doc_metadata'] == sample_embedding_create.doc_metadata
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, mock_async_session, sample_embedding_create):
        """Test create handles database errors."""
        mock_async_session.commit.side_effect = SQLAlchemyError("Commit failed")
        
        with patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding'):
            with pytest.raises(SQLAlchemyError):
                await DocumentationEmbeddingRepository.create(
                    mock_async_session, sample_embedding_create
                )


class TestDocumentationEmbeddingRepositoryGetById:
    """Test get by ID functionality."""
    
    def test_get_by_id_found(self, mock_sync_session, sample_embeddings):
        """Test get by ID when embedding is found."""
        target_embedding = sample_embeddings[0]
        mock_query = MockQuery([target_embedding])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.get_by_id(mock_sync_session, 1)
        
        assert result == target_embedding
        mock_sync_session.query.assert_called_once_with(DocumentationEmbedding)
        assert mock_query._filter_applied
    
    def test_get_by_id_not_found(self, mock_sync_session):
        """Test get by ID when embedding is not found."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.get_by_id(mock_sync_session, 999)
        
        assert result is None
        mock_sync_session.query.assert_called_once_with(DocumentationEmbedding)
    
    def test_get_by_id_database_error(self, mock_sync_session):
        """Test get by ID handles database errors."""
        mock_sync_session.query.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            DocumentationEmbeddingRepository.get_by_id(mock_sync_session, 1)


class TestDocumentationEmbeddingRepositoryGetAll:
    """Test get all functionality."""
    
    def test_get_all_default_params(self, mock_sync_session, sample_embeddings):
        """Test get all with default parameters."""
        mock_query = MockQuery(sample_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.get_all(mock_sync_session)
        
        assert len(result) == 4
        assert result == sample_embeddings
        mock_sync_session.query.assert_called_once_with(DocumentationEmbedding)
        assert mock_query._offset_applied
        assert mock_query._limit_applied
    
    def test_get_all_with_pagination(self, mock_sync_session, sample_embeddings):
        """Test get all with custom pagination."""
        paginated_embeddings = sample_embeddings[1:3]  # Skip 1, limit 2
        mock_query = MockQuery(paginated_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.get_all(mock_sync_session, skip=1, limit=2)
        
        assert len(result) == 2
        assert result == paginated_embeddings
    
    def test_get_all_empty(self, mock_sync_session):
        """Test get all when no embeddings exist."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.get_all(mock_sync_session)
        
        assert result == []


class TestDocumentationEmbeddingRepositoryUpdate:
    """Test update functionality."""
    
    def test_update_success(self, mock_sync_session, sample_embeddings):
        """Test update embedding successfully."""
        target_embedding = sample_embeddings[0]
        mock_query = MockQuery([target_embedding])
        mock_sync_session.query.return_value = mock_query
        
        update_data = {"title": "Updated Title", "content": "Updated content"}
        
        result = DocumentationEmbeddingRepository.update(mock_sync_session, 1, update_data)
        
        assert result == target_embedding
        assert target_embedding.title == "Updated Title"
        assert target_embedding.content == "Updated content"
        mock_sync_session.commit.assert_called_once()
        mock_sync_session.refresh.assert_called_once_with(target_embedding)
    
    def test_update_not_found(self, mock_sync_session):
        """Test update when embedding is not found."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        update_data = {"title": "Updated Title"}
        
        result = DocumentationEmbeddingRepository.update(mock_sync_session, 999, update_data)
        
        assert result is None
        mock_sync_session.commit.assert_not_called()
        mock_sync_session.refresh.assert_not_called()
    
    def test_update_partial_data(self, mock_sync_session, sample_embeddings):
        """Test update with partial data."""
        target_embedding = sample_embeddings[0]
        mock_query = MockQuery([target_embedding])
        mock_sync_session.query.return_value = mock_query
        
        original_content = target_embedding.content
        update_data = {"title": "Only Title Updated"}
        
        result = DocumentationEmbeddingRepository.update(mock_sync_session, 1, update_data)
        
        assert result == target_embedding
        assert target_embedding.title == "Only Title Updated"
        assert target_embedding.content == original_content  # Unchanged
    
    def test_update_database_error(self, mock_sync_session, sample_embeddings):
        """Test update handles database errors."""
        target_embedding = sample_embeddings[0]
        mock_query = MockQuery([target_embedding])
        mock_sync_session.query.return_value = mock_query
        mock_sync_session.commit.side_effect = SQLAlchemyError("Commit failed")
        
        update_data = {"title": "Updated Title"}
        
        with pytest.raises(SQLAlchemyError):
            DocumentationEmbeddingRepository.update(mock_sync_session, 1, update_data)


class TestDocumentationEmbeddingRepositoryDelete:
    """Test delete functionality."""
    
    def test_delete_success(self, mock_sync_session, sample_embeddings):
        """Test delete embedding successfully."""
        target_embedding = sample_embeddings[0]
        mock_query = MockQuery([target_embedding])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.delete(mock_sync_session, 1)
        
        assert result is True
        mock_sync_session.delete.assert_called_once_with(target_embedding)
        mock_sync_session.commit.assert_called_once()
    
    def test_delete_not_found(self, mock_sync_session):
        """Test delete when embedding is not found."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.delete(mock_sync_session, 999)
        
        assert result is False
        mock_sync_session.delete.assert_not_called()
        mock_sync_session.commit.assert_not_called()
    
    def test_delete_database_error(self, mock_sync_session, sample_embeddings):
        """Test delete handles database errors."""
        target_embedding = sample_embeddings[0]
        mock_query = MockQuery([target_embedding])
        mock_sync_session.query.return_value = mock_query
        mock_sync_session.delete.side_effect = SQLAlchemyError("Delete failed")
        
        with pytest.raises(SQLAlchemyError):
            DocumentationEmbeddingRepository.delete(mock_sync_session, 1)


class TestDocumentationEmbeddingRepositorySearchSimilar:
    """Test search similar functionality."""
    
    @patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding')
    def test_search_similar_success(self, mock_model, mock_sync_session, sample_embeddings):
        """Test search similar embeddings successfully."""
        query_embedding = [0.1, 0.2, 0.3]
        similar_embeddings = sample_embeddings[:2]  # Return first 2 as most similar
        
        # Mock the embedding field and cosine_distance method
        mock_embedding_field = MagicMock()
        mock_embedding_field.cosine_distance.return_value = "mocked_cosine_distance"
        mock_model.embedding = mock_embedding_field
        
        mock_query = MockQuery(similar_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_similar(
            mock_sync_session, query_embedding, limit=2
        )
        
        assert len(result) == 2
        assert result == similar_embeddings
        mock_sync_session.query.assert_called_once_with(mock_model)
        assert mock_query._order_applied
        assert mock_query._limit_applied
        mock_embedding_field.cosine_distance.assert_called_once_with(query_embedding)
    
    @patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding')
    def test_search_similar_default_limit(self, mock_model, mock_sync_session, sample_embeddings):
        """Test search similar with default limit."""
        query_embedding = [0.1, 0.2, 0.3]
        
        # Mock the embedding field and cosine_distance method
        mock_embedding_field = MagicMock()
        mock_embedding_field.cosine_distance.return_value = "mocked_cosine_distance"
        mock_model.embedding = mock_embedding_field
        
        mock_query = MockQuery(sample_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_similar(
            mock_sync_session, query_embedding
        )
        
        assert result == sample_embeddings
        mock_embedding_field.cosine_distance.assert_called_once_with(query_embedding)
    
    @patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding')
    def test_search_similar_empty_results(self, mock_model, mock_sync_session):
        """Test search similar when no similar embeddings found."""
        query_embedding = [0.1, 0.2, 0.3]
        
        # Mock the embedding field and cosine_distance method
        mock_embedding_field = MagicMock()
        mock_embedding_field.cosine_distance.return_value = "mocked_cosine_distance"
        mock_model.embedding = mock_embedding_field
        
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_similar(
            mock_sync_session, query_embedding
        )
        
        assert result == []
        mock_embedding_field.cosine_distance.assert_called_once_with(query_embedding)


class TestDocumentationEmbeddingRepositorySearchBySource:
    """Test search by source functionality."""
    
    def test_search_by_source_success(self, mock_sync_session, sample_embeddings):
        """Test search by source successfully."""
        filtered_embeddings = [sample_embeddings[1]]  # api.md
        mock_query = MockQuery(filtered_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_by_source(
            mock_sync_session, "api"
        )
        
        assert len(result) == 1
        assert result == filtered_embeddings
        mock_sync_session.query.assert_called_once_with(DocumentationEmbedding)
        assert mock_query._filter_applied
        assert mock_query._offset_applied
        assert mock_query._limit_applied
    
    def test_search_by_source_with_pagination(self, mock_sync_session, sample_embeddings):
        """Test search by source with pagination."""
        mock_query = MockQuery(sample_embeddings[1:3])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_by_source(
            mock_sync_session, "md", skip=1, limit=2
        )
        
        assert len(result) == 2
    
    def test_search_by_source_no_results(self, mock_sync_session):
        """Test search by source when no matches found."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_by_source(
            mock_sync_session, "nonexistent"
        )
        
        assert result == []


class TestDocumentationEmbeddingRepositorySearchByTitle:
    """Test search by title functionality."""
    
    def test_search_by_title_success(self, mock_sync_session, sample_embeddings):
        """Test search by title successfully."""
        filtered_embeddings = [sample_embeddings[1]]  # API Reference
        mock_query = MockQuery(filtered_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_by_title(
            mock_sync_session, "API"
        )
        
        assert len(result) == 1
        assert result == filtered_embeddings
        mock_sync_session.query.assert_called_once_with(DocumentationEmbedding)
        assert mock_query._filter_applied
        assert mock_query._offset_applied
        assert mock_query._limit_applied
    
    def test_search_by_title_with_pagination(self, mock_sync_session, sample_embeddings):
        """Test search by title with pagination."""
        mock_query = MockQuery(sample_embeddings[0:2])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_by_title(
            mock_sync_session, "Reference", skip=0, limit=2
        )
        
        assert len(result) == 2
    
    def test_search_by_title_no_results(self, mock_sync_session):
        """Test search by title when no matches found."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        result = DocumentationEmbeddingRepository.search_by_title(
            mock_sync_session, "nonexistent"
        )
        
        assert result == []


class TestDocumentationEmbeddingRepositoryGetRecent:
    """Test get recent functionality."""
    
    @patch('src.repositories.documentation_embedding_repository.desc')
    def test_get_recent_success(self, mock_desc, mock_sync_session, sample_embeddings):
        """Test get recent embeddings successfully."""
        recent_embeddings = sample_embeddings[:3]  # Most recent 3
        mock_query = MockQuery(recent_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        # Mock desc function to return a mock object
        mock_desc.return_value = "mocked_desc_order"
        
        result = DocumentationEmbeddingRepository.get_recent(mock_sync_session, limit=3)
        
        assert len(result) == 3
        assert result == recent_embeddings
        mock_sync_session.query.assert_called_once_with(DocumentationEmbedding)
        assert mock_query._order_applied
        assert mock_query._limit_applied
    
    @patch('src.repositories.documentation_embedding_repository.desc')
    def test_get_recent_default_limit(self, mock_desc, mock_sync_session, sample_embeddings):
        """Test get recent with default limit."""
        mock_query = MockQuery(sample_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        # Mock desc function to return a mock object
        mock_desc.return_value = "mocked_desc_order"
        
        result = DocumentationEmbeddingRepository.get_recent(mock_sync_session)
        
        assert result == sample_embeddings
    
    @patch('src.repositories.documentation_embedding_repository.desc')
    def test_get_recent_empty(self, mock_desc, mock_sync_session):
        """Test get recent when no embeddings exist."""
        mock_query = MockQuery([])
        mock_sync_session.query.return_value = mock_query
        
        # Mock desc function to return a mock object
        mock_desc.return_value = "mocked_desc_order"
        
        result = DocumentationEmbeddingRepository.get_recent(mock_sync_session)
        
        assert result == []


class TestDocumentationEmbeddingRepositoryIntegration:
    """Test integration scenarios and workflows."""
    
    @pytest.mark.asyncio
    async def test_full_crud_workflow(self, mock_async_session, mock_sync_session, sample_embedding_create):
        """Test complete CRUD workflow."""
        # 1. Create embedding
        created_embedding = MockDocumentationEmbedding(id=1)
        with patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding') as mock_model:
            mock_model.return_value = created_embedding
            
            create_result = await DocumentationEmbeddingRepository.create(
                mock_async_session, sample_embedding_create
            )
            assert create_result == created_embedding
        
        # 2. Get by ID
        mock_query = MockQuery([created_embedding])
        mock_sync_session.query.return_value = mock_query
        
        get_result = DocumentationEmbeddingRepository.get_by_id(mock_sync_session, 1)
        assert get_result == created_embedding
        
        # 3. Update
        update_data = {"title": "Updated Title"}
        update_result = DocumentationEmbeddingRepository.update(mock_sync_session, 1, update_data)
        assert update_result == created_embedding
        assert created_embedding.title == "Updated Title"
        
        # 4. Delete
        delete_result = DocumentationEmbeddingRepository.delete(mock_sync_session, 1)
        assert delete_result is True
    
    @patch('src.repositories.documentation_embedding_repository.desc')
    @patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding')
    def test_search_workflow(self, mock_model, mock_desc, mock_sync_session, sample_embeddings):
        """Test complete search workflow."""
        mock_query = MockQuery(sample_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        # Mock desc function to return a mock object
        mock_desc.return_value = "mocked_desc_order"
        
        # 1. Get all embeddings
        all_embeddings = DocumentationEmbeddingRepository.get_all(mock_sync_session)
        assert len(all_embeddings) == 4
        
        # 2. Search by source
        source_results = DocumentationEmbeddingRepository.search_by_source(mock_sync_session, "api")
        assert isinstance(source_results, list)
        
        # 3. Search by title
        title_results = DocumentationEmbeddingRepository.search_by_title(mock_sync_session, "Tutorial")
        assert isinstance(title_results, list)
        
        # 4. Get recent
        recent_results = DocumentationEmbeddingRepository.get_recent(mock_sync_session)
        assert isinstance(recent_results, list)
        
        # 5. Search similar
        # Mock the embedding field and cosine_distance method
        mock_embedding_field = MagicMock()
        mock_embedding_field.cosine_distance.return_value = "mocked_cosine_distance"
        mock_model.embedding = mock_embedding_field
        
        query_embedding = [0.1, 0.2, 0.3]
        similar_results = DocumentationEmbeddingRepository.search_similar(
            mock_sync_session, query_embedding
        )
        assert isinstance(similar_results, list)
    
    def test_pagination_consistency(self, mock_sync_session, sample_embeddings):
        """Test pagination consistency across different methods."""
        # All methods that support pagination should handle skip/limit consistently
        mock_query = MockQuery(sample_embeddings[1:3])  # Skip 1, limit 2
        mock_sync_session.query.return_value = mock_query
        
        # get_all with pagination
        paginated_all = DocumentationEmbeddingRepository.get_all(
            mock_sync_session, skip=1, limit=2
        )
        assert len(paginated_all) == 2
        
        # search_by_source with pagination
        paginated_source = DocumentationEmbeddingRepository.search_by_source(
            mock_sync_session, "api", skip=1, limit=2
        )
        assert isinstance(paginated_source, list)
        
        # search_by_title with pagination
        paginated_title = DocumentationEmbeddingRepository.search_by_title(
            mock_sync_session, "Tutorial", skip=1, limit=2
        )
        assert isinstance(paginated_title, list)
    
    @patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding')
    def test_error_handling_consistency(self, mock_model, mock_sync_session):
        """Test that all methods handle errors consistently."""
        error = SQLAlchemyError("Consistent error")
        
        # Mock the embedding field for search_similar method
        mock_embedding_field = MagicMock()
        mock_embedding_field.cosine_distance.return_value = "mocked_cosine_distance"
        mock_model.embedding = mock_embedding_field
        
        # All sync methods should propagate SQLAlchemyError
        sync_methods = [
            ("get_by_id", (mock_sync_session, 1)),
            ("get_all", (mock_sync_session,)),
            ("search_by_source", (mock_sync_session, "test")),
            ("search_by_title", (mock_sync_session, "test")),
            ("get_recent", (mock_sync_session,)),
            ("search_similar", (mock_sync_session, [0.1, 0.2, 0.3]))
        ]
        
        for method_name, args in sync_methods:
            mock_sync_session.reset_mock()
            mock_sync_session.query.side_effect = error
            
            with pytest.raises(SQLAlchemyError):
                method = getattr(DocumentationEmbeddingRepository, method_name)
                method(*args)
    
    def test_static_method_behavior(self):
        """Test that all methods are static and don't require instance."""
        # All methods should be static and accessible without instantiation
        static_methods = [
            'create', 'get_by_id', 'get_all', 'update', 'delete',
            'search_similar', 'search_by_source', 'search_by_title', 'get_recent'
        ]
        
        for method_name in static_methods:
            method = getattr(DocumentationEmbeddingRepository, method_name)
            # Static methods are callable and don't have __self__ attribute
            assert callable(method)
            assert not hasattr(method, '__self__') or method.__self__ is None
    
    @patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding')
    def test_vector_similarity_workflow(self, mock_model, mock_sync_session, sample_embeddings):
        """Test vector similarity search workflow."""
        # Simulate a similarity search workflow
        query_embedding = [0.15, 0.25, 0.35]  # Similar to first embedding
        
        # Mock the embedding field and cosine_distance method
        mock_embedding_field = MagicMock()
        mock_embedding_field.cosine_distance.return_value = "mocked_cosine_distance"
        mock_model.embedding = mock_embedding_field
        
        # Mock the vector similarity results (ordered by similarity)
        similar_embeddings = [sample_embeddings[0], sample_embeddings[3]]  # Most similar first
        mock_query = MockQuery(similar_embeddings)
        mock_sync_session.query.return_value = mock_query
        
        # Search for similar embeddings
        results = DocumentationEmbeddingRepository.search_similar(
            mock_sync_session, query_embedding, limit=2
        )
        
        assert len(results) == 2
        assert results == similar_embeddings
        
        # Verify the query used cosine distance ordering
        mock_sync_session.query.assert_called_once_with(mock_model)
        assert mock_query._order_applied
        assert mock_query._limit_applied
        mock_embedding_field.cosine_distance.assert_called_once_with(query_embedding)