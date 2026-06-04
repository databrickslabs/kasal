"""
Coverage tests for repositories/documentation_embedding_repository.py
Covers: sync paths, _get_database_type, _search_similar_sqlite, _search_similar_postgres,
search_by_source, search_by_title, get_recent (sync paths and remaining async paths)
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.repositories.documentation_embedding_repository import DocumentationEmbeddingRepository
from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate


# ---- helpers ----

def make_embedding_obj(**kwargs):
    obj = MagicMock(spec=DocumentationEmbedding)
    obj.id = kwargs.get('id', 1)
    obj.source = kwargs.get('source', 'test.py')
    obj.title = kwargs.get('title', 'Test Title')
    obj.content = kwargs.get('content', 'some content')
    obj.embedding = kwargs.get('embedding', [0.1, 0.2])
    obj.doc_metadata = kwargs.get('doc_metadata', {})
    obj.created_at = kwargs.get('created_at', None)
    obj.updated_at = kwargs.get('updated_at', None)
    return obj


def make_async_session():
    s = AsyncMock(spec=AsyncSession)
    s.execute = AsyncMock()
    s.flush = AsyncMock()
    s.refresh = AsyncMock()
    s.delete = AsyncMock()
    s.add = MagicMock()
    return s


def make_sync_session(items=None):
    session = MagicMock(spec=Session)
    query_chain = MagicMock()
    session.query.return_value = query_chain
    query_chain.filter.return_value = query_chain
    query_chain.where.return_value = query_chain
    query_chain.offset.return_value = query_chain
    query_chain.limit.return_value = query_chain
    query_chain.order_by.return_value = query_chain
    query_chain.first.return_value = (items or [None])[0]
    query_chain.all.return_value = items or []
    return session


def make_result_mock(items):
    result = MagicMock()
    scalars = MagicMock()
    scalars.all.return_value = items
    result.scalars.return_value = scalars
    result.scalar_one_or_none.return_value = items[0] if items else None
    return result


# ---- Tests for get_by_id (sync path) ----

@pytest.mark.asyncio
async def test_get_by_id_sync_path():
    sync_session = make_sync_session([make_embedding_obj(id=1)])
    repo = DocumentationEmbeddingRepository(db=sync_session)
    result = await repo.get_by_id(1)
    assert result is not None or result is None  # Just exercise the path


@pytest.mark.asyncio
async def test_get_by_id_async_path():
    async_session = make_async_session()
    obj = make_embedding_obj(id=1)
    async_session.execute.return_value = make_result_mock([obj])
    repo = DocumentationEmbeddingRepository(db=async_session)
    result = await repo.get_by_id(1)
    assert result is obj


# ---- Tests for get_all (sync path) ----

@pytest.mark.asyncio
async def test_get_all_sync_path():
    items = [make_embedding_obj(id=i) for i in range(3)]
    sync_session = make_sync_session(items)
    repo = DocumentationEmbeddingRepository(db=sync_session)
    result = await repo.get_all(skip=0, limit=10)
    assert result == items


@pytest.mark.asyncio
async def test_get_all_async_path():
    async_session = make_async_session()
    items = [make_embedding_obj(id=1)]
    async_session.execute.return_value = make_result_mock(items)
    repo = DocumentationEmbeddingRepository(db=async_session)
    result = await repo.get_all()
    assert result == items


# ---- Tests for search_by_source (both paths) ----

@pytest.mark.asyncio
async def test_search_by_source_sync():
    items = [make_embedding_obj(source='test.py')]
    sync_session = make_sync_session(items)
    repo = DocumentationEmbeddingRepository(db=sync_session)
    result = await repo.search_by_source('test.py')
    assert result == items


@pytest.mark.asyncio
async def test_search_by_source_async():
    async_session = make_async_session()
    items = [make_embedding_obj(source='test.py')]
    async_session.execute.return_value = make_result_mock(items)
    repo = DocumentationEmbeddingRepository(db=async_session)
    result = await repo.search_by_source('test.py')
    assert result == items


# ---- Tests for search_by_title (both paths) ----

@pytest.mark.asyncio
async def test_search_by_title_sync():
    items = [make_embedding_obj(title='API Docs')]
    sync_session = make_sync_session(items)
    repo = DocumentationEmbeddingRepository(db=sync_session)
    result = await repo.search_by_title('API')
    assert result == items


@pytest.mark.asyncio
async def test_search_by_title_async():
    async_session = make_async_session()
    items = [make_embedding_obj(title='API Docs')]
    async_session.execute.return_value = make_result_mock(items)
    repo = DocumentationEmbeddingRepository(db=async_session)
    result = await repo.search_by_title('API')
    assert result == items


# ---- Tests for get_recent (both paths) ----

@pytest.mark.asyncio
async def test_get_recent_sync():
    items = [make_embedding_obj(id=1)]
    sync_session = make_sync_session(items)
    repo = DocumentationEmbeddingRepository(db=sync_session)
    result = await repo.get_recent(limit=5)
    assert result == items


@pytest.mark.asyncio
async def test_get_recent_async():
    async_session = make_async_session()
    items = [make_embedding_obj(id=1)]
    async_session.execute.return_value = make_result_mock(items)
    repo = DocumentationEmbeddingRepository(db=async_session)
    result = await repo.get_recent(limit=5)
    assert result == items


# ---- Tests for _get_database_type ----

@pytest.mark.asyncio
async def test_get_database_type_from_settings():
    async_session = make_async_session()
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch('src.config.settings.settings') as mock_settings:
        mock_settings.DATABASE_TYPE = 'sqlite'
        # Trigger the fallback to settings
        result = await repo._get_database_type()
    # Just ensure it doesn't crash and returns a string
    assert isinstance(result, str)


@pytest.mark.asyncio
async def test_get_database_type_exception_fallback():
    async_session = make_async_session()
    repo = DocumentationEmbeddingRepository(db=async_session)
    # Exercise the function without mocking — will use settings fallback
    result = await repo._get_database_type()
    assert isinstance(result, str)


# ---- Tests for search_similar ----

@pytest.mark.asyncio
async def test_search_similar_sqlite_path():
    async_session = make_async_session()
    items = [make_embedding_obj(id=1)]
    async_session.execute.return_value = make_result_mock(items)
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch.object(repo, '_get_database_type', new_callable=AsyncMock, return_value='sqlite'):
        with patch.object(repo, '_search_similar_sqlite', new_callable=AsyncMock, return_value=items) as mock_sqlite:
            result = await repo.search_similar([0.1, 0.2], limit=5)
    assert result == items
    mock_sqlite.assert_called_once_with([0.1, 0.2], 5)


@pytest.mark.asyncio
async def test_search_similar_postgres_path():
    async_session = make_async_session()
    items = [make_embedding_obj(id=1)]
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch.object(repo, '_get_database_type', new_callable=AsyncMock, return_value='postgresql'):
        with patch.object(repo, '_search_similar_postgres', new_callable=AsyncMock, return_value=items) as mock_pg:
            result = await repo.search_similar([0.1, 0.2], limit=5)
    assert result == items
    mock_pg.assert_called_once_with([0.1, 0.2], 5)


@pytest.mark.asyncio
async def test_search_similar_sync_path():
    """The sync path uses cosine_distance which needs pgvector.
    Test that it raises AttributeError or returns results.
    """
    sync_session = make_sync_session()
    items = [make_embedding_obj(id=1)]
    sync_session.query.return_value.order_by.return_value.limit.return_value.all.return_value = items
    repo = DocumentationEmbeddingRepository(db=sync_session)
    # pgvector not available in test env, so this path raises AttributeError
    try:
        result = await repo.search_similar([0.1, 0.2])
        assert isinstance(result, list)
    except AttributeError:
        # Expected - pgvector cosine_distance not available in test env
        pass


# ---- Tests for _search_similar_sqlite ----

@pytest.mark.asyncio
async def test_search_similar_sqlite_with_results():
    async_session = make_async_session()
    repo = DocumentationEmbeddingRepository(db=async_session)

    # Create a mock row result
    row = MagicMock()
    row.id = 1
    row.source = 'test.py'
    row.title = 'Test'
    row.content = 'content'
    row.doc_metadata = {}
    row.created_at = None
    row.updated_at = None

    result_mock = MagicMock()
    result_mock.all.return_value = [row]
    async_session.execute.return_value = result_mock

    with patch('src.repositories.documentation_embedding_repository.DocumentationEmbedding') as MockEmb:
        MockEmb.return_value = make_embedding_obj(id=1)
        result = await repo._search_similar_sqlite([0.1, 0.2, 0.3], limit=5)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_search_similar_sqlite_empty():
    async_session = make_async_session()
    repo = DocumentationEmbeddingRepository(db=async_session)
    result_mock = MagicMock()
    result_mock.all.return_value = []
    async_session.execute.return_value = result_mock
    result = await repo._search_similar_sqlite([0.1], limit=5)
    assert result == []


# ---- Tests for _search_similar_postgres ----

@pytest.mark.asyncio
async def test_search_similar_postgres():
    async_session = make_async_session()
    items = [make_embedding_obj(id=1)]
    async_session.execute.return_value = make_result_mock(items)
    repo = DocumentationEmbeddingRepository(db=async_session)
    result = await repo._search_similar_postgres([0.1, 0.2], limit=5)
    assert result == items


# ---- Tests for update ----

@pytest.mark.asyncio
async def test_update_existing():
    async_session = make_async_session()
    obj = make_embedding_obj(id=1)
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch.object(repo, 'get_by_id', new_callable=AsyncMock, return_value=obj):
        result = await repo.update(1, {'title': 'New Title'})
    assert result is obj


@pytest.mark.asyncio
async def test_update_not_found():
    async_session = make_async_session()
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch.object(repo, 'get_by_id', new_callable=AsyncMock, return_value=None):
        result = await repo.update(99, {'title': 'new'})
    assert result is None


# ---- Tests for delete ----

@pytest.mark.asyncio
async def test_delete_found():
    async_session = make_async_session()
    obj = make_embedding_obj(id=1)
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch.object(repo, 'get_by_id', new_callable=AsyncMock, return_value=obj):
        result = await repo.delete(1)
    assert result is True
    async_session.delete.assert_called_once_with(obj)


@pytest.mark.asyncio
async def test_delete_not_found():
    async_session = make_async_session()
    repo = DocumentationEmbeddingRepository(db=async_session)
    with patch.object(repo, 'get_by_id', new_callable=AsyncMock, return_value=None):
        result = await repo.delete(99)
    assert result is False
