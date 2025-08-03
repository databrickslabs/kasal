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
    session = MagicMock(spec=AsyncSession)
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    session.delete = AsyncMock()
    # Create an AsyncMock for execute that can be configured with return_value
    session.execute = AsyncMock()
    session.get_bind = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.query = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.flush = MagicMock()
    session.refresh = MagicMock()
    session.rollback = MagicMock()
    session.delete = MagicMock()
    session.execute = AsyncMock()
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


@pytest.fixture
def repository_with_async_session(mock_async_session):
    """Create repository instance with async session."""
    return DocumentationEmbeddingRepository(mock_async_session)


@pytest.fixture
def repository_with_sync_session(mock_sync_session):
    """Create repository instance with sync session."""
    return DocumentationEmbeddingRepository(mock_sync_session)


class TestDocumentationEmbeddingRepositoryCreate:
    """Test create functionality."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, repository_with_async_session, sample_embedding_create):
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
            
            result = await repository_with_async_session.create(sample_embedding_create)
            
            assert result == created_embedding
            repository_with_async_session.db.add.assert_called_once_with(created_embedding)
            repository_with_async_session.db.flush.assert_called_once()
            
            # Verify DocumentationEmbedding was created with correct parameters
            call_args = mock_model.call_args[1]
            assert call_args['source'] == sample_embedding_create.source
            assert call_args['title'] == sample_embedding_create.title
            assert call_args['content'] == sample_embedding_create.content
            assert call_args['embedding'] == sample_embedding_create.embedding
            assert call_args['doc_metadata'] == sample_embedding_create.doc_metadata
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, repository_with_async_session, sample_embedding_create):
        """Test create handles database errors."""
        repository_with_async_session.db.flush.side_effect = SQLAlchemyError("Flush failed")
        
        with patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding'):
            with pytest.raises(SQLAlchemyError):
                await repository_with_async_session.create(sample_embedding_create)


class TestDocumentationEmbeddingRepositoryGetById:
    """Test get by ID functionality."""
    
    @pytest.mark.asyncio
    async def test_get_by_id_found(self, repository_with_async_session, sample_embeddings):
        """Test get by ID when embedding is found."""
        target_embedding = sample_embeddings[0]
        mock_result = MagicMock()
        # Make scalar_one_or_none a method that returns the target embedding
        mock_result.scalar_one_or_none = MagicMock(return_value=target_embedding)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_by_id(1)
        
        # The execute method returns a coroutine, need to check the mock was called properly
        assert repository_with_async_session.db.execute.called
        # Result should be the target embedding from scalar_one_or_none
        assert result == target_embedding
        repository_with_async_session.db.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, repository_with_async_session):
        """Test get by ID when embedding is not found."""
        mock_result = MagicMock()
        # Make scalar_one_or_none a method that returns None
        mock_result.scalar_one_or_none = MagicMock(return_value=None)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_by_id(999)
        
        # The execute method returns a coroutine, need to check the mock was called properly
        assert repository_with_async_session.db.execute.called
        # Result should be None from scalar_one_or_none
        assert result is None
        repository_with_async_session.db.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_id_database_error(self, repository_with_async_session):
        """Test get by ID handles database errors."""
        repository_with_async_session.db.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await repository_with_async_session.get_by_id(1)


class TestDocumentationEmbeddingRepositoryGetAll:
    """Test get all functionality."""
    
    @pytest.mark.asyncio
    async def test_get_all_default_params(self, repository_with_async_session, sample_embeddings):
        """Test get all with default parameters."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_all()
        
        # Check the mock was called and result is correct
        assert repository_with_async_session.db.execute.called
        assert len(result) == 4
        assert result == sample_embeddings
        repository_with_async_session.db.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_all_with_pagination(self, repository_with_async_session, sample_embeddings):
        """Test get all with custom pagination."""
        paginated_embeddings = sample_embeddings[1:3]  # Skip 1, limit 2
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=paginated_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_all(skip=1, limit=2)
        
        # Check the mock was called and result is correct
        assert repository_with_async_session.db.execute.called
        assert len(result) == 2
        assert result == paginated_embeddings
    
    @pytest.mark.asyncio
    async def test_get_all_empty(self, repository_with_async_session):
        """Test get all when no embeddings exist."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=[])
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_all()
        
        # Check the mock was called and result is empty
        assert repository_with_async_session.db.execute.called
        assert result == []


class TestDocumentationEmbeddingRepositoryUpdate:
    """Test update functionality."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, repository_with_async_session, sample_embeddings):
        """Test update embedding successfully."""
        target_embedding = sample_embeddings[0]
        
        # Mock get_by_id to return the target embedding (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=target_embedding)):
            update_data = {"title": "Updated Title", "content": "Updated content"}
            
            result = await repository_with_async_session.update(1, update_data)
            
            assert result == target_embedding
            assert target_embedding.title == "Updated Title"
            assert target_embedding.content == "Updated content"
            repository_with_async_session.db.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, repository_with_async_session):
        """Test update when embedding is not found."""
        # Mock get_by_id to return None (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=None)):
            update_data = {"title": "Updated Title"}
            
            result = await repository_with_async_session.update(999, update_data)
            
            assert result is None
            repository_with_async_session.db.flush.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_partial_data(self, repository_with_async_session, sample_embeddings):
        """Test update with partial data."""
        target_embedding = sample_embeddings[0]
        original_content = target_embedding.content
        
        # Mock get_by_id to return the target embedding (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=target_embedding)):
            update_data = {"title": "Only Title Updated"}
            
            result = await repository_with_async_session.update(1, update_data)
            
            assert result == target_embedding
            assert target_embedding.title == "Only Title Updated"
            assert target_embedding.content == original_content  # Unchanged
    
    @pytest.mark.asyncio
    async def test_update_database_error(self, repository_with_async_session, sample_embeddings):
        """Test update handles database errors."""
        target_embedding = sample_embeddings[0]
        
        # Mock get_by_id to return the target embedding (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=target_embedding)):
            repository_with_async_session.db.flush.side_effect = SQLAlchemyError("Flush failed")
            
            update_data = {"title": "Updated Title"}
            
            with pytest.raises(SQLAlchemyError):
                await repository_with_async_session.update(1, update_data)


class TestDocumentationEmbeddingRepositoryDelete:
    """Test delete functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, repository_with_async_session, sample_embeddings):
        """Test delete embedding successfully."""
        target_embedding = sample_embeddings[0]
        
        # Mock get_by_id to return the target embedding (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=target_embedding)):
            result = await repository_with_async_session.delete(1)
            
            assert result is True
            repository_with_async_session.db.delete.assert_called_once_with(target_embedding)
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, repository_with_async_session):
        """Test delete when embedding is not found."""
        # Mock get_by_id to return None (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=None)):
            result = await repository_with_async_session.delete(999)
            
            assert result is False
            repository_with_async_session.db.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_database_error(self, repository_with_async_session, sample_embeddings):
        """Test delete handles database errors."""
        target_embedding = sample_embeddings[0]
        
        # Mock get_by_id to return the target embedding (async)
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=target_embedding)):
            repository_with_async_session.db.delete.side_effect = SQLAlchemyError("Delete failed")
            
            with pytest.raises(SQLAlchemyError):
                await repository_with_async_session.delete(1)


class TestDocumentationEmbeddingRepositorySearchSimilar:
    """Test search similar functionality."""
    
    @pytest.mark.asyncio
    async def test_search_similar_success(self, repository_with_async_session, sample_embeddings):
        """Test search similar embeddings successfully."""
        query_embedding = [0.1, 0.2, 0.3]
        similar_embeddings = sample_embeddings[:2]  # Return first 2 as most similar
        
        # Mock database type detection (async)
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(return_value='postgresql')):
            # Mock the postgres search method (async)
            with patch.object(repository_with_async_session, '_search_similar_postgres', AsyncMock(return_value=similar_embeddings)):
                result = await repository_with_async_session.search_similar(query_embedding, limit=2)
                
                assert len(result) == 2
                assert result == similar_embeddings
    
    @pytest.mark.asyncio
    async def test_search_similar_default_limit(self, repository_with_async_session, sample_embeddings):
        """Test search similar with default limit."""
        query_embedding = [0.1, 0.2, 0.3]
        
        # Mock database type detection (async)
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(return_value='postgresql')):
            # Mock the postgres search method (async)
            with patch.object(repository_with_async_session, '_search_similar_postgres', AsyncMock(return_value=sample_embeddings)):
                result = await repository_with_async_session.search_similar(query_embedding)
                
                assert result == sample_embeddings
    
    @pytest.mark.asyncio
    async def test_search_similar_empty_results(self, repository_with_async_session):
        """Test search similar when no similar embeddings found."""
        query_embedding = [0.1, 0.2, 0.3]
        
        # Mock database type detection (async)
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(return_value='postgresql')):
            # Mock the postgres search method (async)
            with patch.object(repository_with_async_session, '_search_similar_postgres', AsyncMock(return_value=[])):
                result = await repository_with_async_session.search_similar(query_embedding)
                
                assert result == []


class TestDocumentationEmbeddingRepositorySearchBySource:
    """Test search by source functionality."""
    
    @pytest.mark.asyncio
    async def test_search_by_source_success(self, repository_with_async_session, sample_embeddings):
        """Test search by source successfully."""
        filtered_embeddings = [sample_embeddings[1]]  # api.md
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=filtered_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.search_by_source("api")
        
        assert len(result) == 1
        assert result == filtered_embeddings
        repository_with_async_session.db.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_by_source_with_pagination(self, repository_with_async_session, sample_embeddings):
        """Test search by source with pagination."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings[1:3])
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.search_by_source("md", skip=1, limit=2)
        
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_search_by_source_no_results(self, repository_with_async_session):
        """Test search by source when no matches found."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=[])
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.search_by_source("nonexistent")
        
        assert result == []


class TestDocumentationEmbeddingRepositorySearchByTitle:
    """Test search by title functionality."""
    
    @pytest.mark.asyncio
    async def test_search_by_title_success(self, repository_with_async_session, sample_embeddings):
        """Test search by title successfully."""
        filtered_embeddings = [sample_embeddings[1]]  # API Reference
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=filtered_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.search_by_title("API")
        
        assert len(result) == 1
        assert result == filtered_embeddings
        repository_with_async_session.db.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_by_title_with_pagination(self, repository_with_async_session, sample_embeddings):
        """Test search by title with pagination."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings[0:2])
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.search_by_title("Reference", skip=0, limit=2)
        
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_search_by_title_no_results(self, repository_with_async_session):
        """Test search by title when no matches found."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=[])
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.search_by_title("nonexistent")
        
        assert result == []


class TestDocumentationEmbeddingRepositoryGetRecent:
    """Test get recent functionality."""
    
    @pytest.mark.asyncio
    async def test_get_recent_success(self, repository_with_async_session, sample_embeddings):
        """Test get recent embeddings successfully."""
        recent_embeddings = sample_embeddings[:3]  # Most recent 3
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=recent_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_recent(limit=3)
        
        assert len(result) == 3
        assert result == recent_embeddings
        repository_with_async_session.db.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_recent_default_limit(self, repository_with_async_session, sample_embeddings):
        """Test get recent with default limit."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_recent()
        
        assert result == sample_embeddings
    
    @pytest.mark.asyncio
    async def test_get_recent_empty(self, repository_with_async_session):
        """Test get recent when no embeddings exist."""
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=[])
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        result = await repository_with_async_session.get_recent()
        
        assert result == []


class TestDocumentationEmbeddingRepositoryIntegration:
    """Test integration scenarios and workflows."""
    
    @pytest.mark.asyncio
    async def test_full_crud_workflow(self, repository_with_async_session, sample_embedding_create):
        """Test complete CRUD workflow."""
        # 1. Create embedding
        created_embedding = MockDocumentationEmbedding(id=1)
        with patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding') as mock_model:
            mock_model.return_value = created_embedding
            
            create_result = await repository_with_async_session.create(sample_embedding_create)
            assert create_result == created_embedding
        
        # 2. Get by ID
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=created_embedding)):
            get_result = await repository_with_async_session.get_by_id(1)
            assert get_result == created_embedding
        
        # 3. Update
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=created_embedding)):
            update_data = {"title": "Updated Title"}
            update_result = await repository_with_async_session.update(1, update_data)
            assert update_result == created_embedding
            assert created_embedding.title == "Updated Title"
        
        # 4. Delete
        with patch.object(repository_with_async_session, 'get_by_id', AsyncMock(return_value=created_embedding)):
            delete_result = await repository_with_async_session.delete(1)
            assert delete_result is True
    
    @pytest.mark.asyncio
    async def test_search_workflow(self, repository_with_async_session, sample_embeddings):
        """Test complete search workflow."""
        # 1. Get all embeddings
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings)
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        all_embeddings = await repository_with_async_session.get_all()
        assert len(all_embeddings) == 4
        
        # 2. Search by source
        repository_with_async_session.db.execute.reset_mock()
        mock_scalars.all = MagicMock(return_value=[sample_embeddings[1]])
        repository_with_async_session.db.execute.return_value = mock_result
        
        source_results = await repository_with_async_session.search_by_source("api")
        assert isinstance(source_results, list)
        
        # 3. Search by title
        repository_with_async_session.db.execute.reset_mock()
        mock_scalars.all = MagicMock(return_value=[sample_embeddings[2]])
        repository_with_async_session.db.execute.return_value = mock_result
        
        title_results = await repository_with_async_session.search_by_title("Tutorial")
        assert isinstance(title_results, list)
        
        # 4. Get recent
        repository_with_async_session.db.execute.reset_mock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings[:3])
        repository_with_async_session.db.execute.return_value = mock_result
        
        recent_results = await repository_with_async_session.get_recent()
        assert isinstance(recent_results, list)
        
        # 5. Search similar
        query_embedding = [0.1, 0.2, 0.3]
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(return_value='postgresql')):
            with patch.object(repository_with_async_session, '_search_similar_postgres', AsyncMock(return_value=sample_embeddings[:2])):
                similar_results = await repository_with_async_session.search_similar(query_embedding)
                assert isinstance(similar_results, list)
    
    @pytest.mark.asyncio
    async def test_pagination_consistency(self, repository_with_async_session, sample_embeddings):
        """Test pagination consistency across different methods."""
        # All methods that support pagination should handle skip/limit consistently
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all = MagicMock(return_value=sample_embeddings[1:3])  # Skip 1, limit 2
        mock_result.scalars = MagicMock(return_value=mock_scalars)
        repository_with_async_session.db.execute.return_value = mock_result
        
        # get_all with pagination
        paginated_all = await repository_with_async_session.get_all(skip=1, limit=2)
        assert len(paginated_all) == 2
        
        # search_by_source with pagination
        repository_with_async_session.db.execute.reset_mock()
        repository_with_async_session.db.execute.return_value = mock_result
        
        paginated_source = await repository_with_async_session.search_by_source("api", skip=1, limit=2)
        assert isinstance(paginated_source, list)
        
        # search_by_title with pagination
        repository_with_async_session.db.execute.reset_mock()
        repository_with_async_session.db.execute.return_value = mock_result
        
        paginated_title = await repository_with_async_session.search_by_title("Tutorial", skip=1, limit=2)
        assert isinstance(paginated_title, list)
    
    @pytest.mark.asyncio
    async def test_error_handling_consistency(self, repository_with_async_session):
        """Test that all methods handle errors consistently."""
        error = SQLAlchemyError("Consistent error")
        
        # All async methods should propagate SQLAlchemyError
        async_methods = [
            ("get_by_id", (1,)),
            ("get_all", ()),
            ("search_by_source", ("test",)),
            ("search_by_title", ("test",)),
            ("get_recent", ()),
        ]
        
        for method_name, args in async_methods:
            repository_with_async_session.db.execute.side_effect = error
            
            with pytest.raises(SQLAlchemyError):
                method = getattr(repository_with_async_session, method_name)
                await method(*args)
            
            repository_with_async_session.db.execute.reset_mock()
        
        # Test search_similar with error
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(side_effect=error)):
            with pytest.raises(SQLAlchemyError):
                await repository_with_async_session.search_similar([0.1, 0.2, 0.3])
    
    def test_instance_method_behavior(self):
        """Test that all methods are instance methods and require instantiation."""
        # All methods should be instance methods
        instance_methods = [
            'create', 'get_by_id', 'get_all', 'update', 'delete',
            'search_similar', 'search_by_source', 'search_by_title', 'get_recent'
        ]
        
        for method_name in instance_methods:
            method = getattr(DocumentationEmbeddingRepository, method_name)
            # Instance methods are callable
            assert callable(method)
    
    @pytest.mark.asyncio
    async def test_vector_similarity_workflow(self, repository_with_async_session, sample_embeddings):
        """Test vector similarity search workflow."""
        # Simulate a similarity search workflow
        query_embedding = [0.15, 0.25, 0.35]  # Similar to first embedding
        
        # Mock the vector similarity results (ordered by similarity)
        similar_embeddings = [sample_embeddings[0], sample_embeddings[3]]  # Most similar first
        
        # Test PostgreSQL similarity search
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(return_value='postgresql')):
            with patch.object(repository_with_async_session, '_search_similar_postgres', AsyncMock(return_value=similar_embeddings)):
                results = await repository_with_async_session.search_similar(query_embedding, limit=2)
                
                assert len(results) == 2
                assert results == similar_embeddings
        
        # Test SQLite similarity search
        with patch.object(repository_with_async_session, '_get_database_type', AsyncMock(return_value='sqlite')):
            with patch.object(repository_with_async_session, '_search_similar_sqlite', AsyncMock(return_value=similar_embeddings)):
                results = await repository_with_async_session.search_similar(query_embedding, limit=2)
                
                assert len(results) == 2
                assert results == similar_embeddings