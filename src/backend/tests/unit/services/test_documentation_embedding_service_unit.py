"""
Unit tests for DocumentationEmbeddingService.

Tests the functionality of the documentation embedding service including:
- Databricks configuration detection and caching
- Databricks storage initialization (object vs dict config, index readiness)
- Creating embeddings via Databricks, SQLite queue, PostgreSQL repository, and skip-db paths
- CRUD operations (get, list, update, delete) with session validation
- Similarity search across Databricks and database backends
- Search by source, title, and recent embeddings
- Error handling and fallback paths
"""
import asyncio
import os
import uuid
import pytest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.services.documentation_embedding_service import DocumentationEmbeddingService
from src.schemas.memory_backend import MemoryBackendType


# ---------------------------------------------------------------------------
# Patch path constants
#
# The service uses LOCAL imports (inside method bodies), so we must patch at
# the *source* module where the symbol lives, not on the service module.
# The service's MemoryBackendRepository and embedding_queue are top-level
# imports, so they ARE on the service module namespace.
# ---------------------------------------------------------------------------
_SVC = "src.services.documentation_embedding_service"

# Top-level imports in the service - can patch on service module directly
_MEMORY_BACKEND_REPO = f"{_SVC}.MemoryBackendRepository"
_EMBEDDING_QUEUE = f"{_SVC}.embedding_queue"

# Local imports inside methods - must patch at SOURCE modules
_DOC_EMBEDDING_REPO_CLS = "src.repositories.documentation_embedding_repository.DocumentationEmbeddingRepository"
_ASYNC_SESSION_FACTORY = "src.db.session.request_scoped_session"
_DATABRICKS_INDEX_SERVICE = "src.services.databricks_index_service.DatabricksIndexService"
_DATABRICKS_VECTOR_STORAGE = "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage"
_DATABRICKS_VECTOR_INDEX_REPO = "src.repositories.databricks_vector_index_repository.DatabricksVectorIndexRepository"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(session=None) -> DocumentationEmbeddingService:
    """Create a fresh service instance with an optional mock session."""
    return DocumentationEmbeddingService(session=session)


def _make_mock_session() -> AsyncMock:
    """Create a mock AsyncSession."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.execute = AsyncMock()
    return session


def _make_backend(
    is_active=True,
    backend_type=MemoryBackendType.DATABRICKS,
    created_at=None,
    databricks_config=None,
    group_id="test-group",
    cognitive_config=None,
    custom_config=None,
):
    """Create a mock MemoryBackend model object.

    Updated for app-modes: uses memory_index (not short_term_index) and
    removed enable_short_term/enable_long_term/enable_entity fields.
    """
    backend = SimpleNamespace(
        is_active=is_active,
        backend_type=backend_type,
        created_at=created_at or datetime(2024, 6, 1),
        databricks_config=databricks_config or {
            "endpoint_name": "test-endpoint",
            "memory_index": "catalog.schema.crew_memory",
            "document_index": "catalog.schema.documents",
            "workspace_url": "https://example.com",
            "embedding_dimension": 1024,
            "personal_access_token": "dapi-test",
            "service_principal_client_id": None,
            "service_principal_client_secret": None,
            "document_endpoint_name": None,
        },
        group_id=group_id,
        cognitive_config=cognitive_config,
        custom_config=custom_config,
    )
    return backend


def _make_doc_embedding_create(
    source="crewai-docs",
    title="Test Document",
    content="Some documentation content",
    embedding=None,
    doc_metadata=None,
):
    """Create a mock DocumentationEmbeddingCreate schema."""
    return SimpleNamespace(
        source=source,
        title=title,
        content=content,
        embedding=embedding or [0.1] * 10,
        doc_metadata=doc_metadata,
    )


def _make_doc_embedding_model(
    id=1,
    source="crewai-docs",
    title="Test Document",
    content="Some content",
    embedding=None,
    doc_metadata=None,
    created_at=None,
    updated_at=None,
):
    """Create a SimpleNamespace simulating a DocumentationEmbedding model."""
    return SimpleNamespace(
        id=id,
        source=source,
        title=title,
        content=content,
        embedding=embedding or [],
        doc_metadata=doc_metadata or {},
        created_at=created_at or datetime(2024, 6, 1),
        updated_at=updated_at or datetime(2024, 6, 1),
    )


def _make_databricks_config_object(**overrides):
    """Create a SimpleNamespace mimicking DatabricksMemoryConfig as an object.

    Updated for app-modes: uses memory_index (not short_term_index).
    """
    defaults = dict(
        endpoint_name="test-endpoint",
        document_endpoint_name=None,
        memory_index="catalog.schema.crew_memory",
        document_index="catalog.schema.documents",
        workspace_url="https://example.com",
        embedding_dimension=1024,
        personal_access_token="dapi-test",
        service_principal_client_id=None,
        service_principal_client_secret=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_memory_config(databricks_config=None, backend_type=MemoryBackendType.DATABRICKS):
    """Create a SimpleNamespace mimicking MemoryBackendConfig."""
    return SimpleNamespace(
        backend_type=backend_type,
        databricks_config=databricks_config or _make_databricks_config_object(),
    )


# ===================================================================
# _check_databricks_config
# ===================================================================

class TestCheckDatabricksConfig:
    """Tests for _check_databricks_config method."""

    @pytest.mark.asyncio
    async def test_returns_cached_true_when_databricks_configured(self):
        """When already checked and Databricks is configured, return True from cache."""
        svc = _make_service()
        svc._checked_config = True
        svc._memory_config = _make_memory_config()

        result = await svc._check_databricks_config()
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_cached_false_when_no_config(self):
        """When already checked and no config, return False from cache."""
        svc = _make_service()
        svc._checked_config = True
        svc._memory_config = None

        result = await svc._check_databricks_config()
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_cached_false_when_config_not_databricks(self):
        """When config is not Databricks type, return False from cache."""
        svc = _make_service()
        svc._checked_config = True
        svc._memory_config = _make_memory_config(backend_type=MemoryBackendType.DEFAULT)

        result = await svc._check_databricks_config()
        assert result is False

    @pytest.mark.asyncio
    async def test_finds_active_databricks_backend_with_session(self):
        """When session is provided, use it to find active Databricks backends."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        backend = _make_backend()
        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=[backend])

        with patch(_MEMORY_BACKEND_REPO, return_value=mock_repo):
            result = await svc._check_databricks_config()

        assert result is True
        assert svc._checked_config is True
        assert svc._memory_config is not None
        assert svc._memory_config.backend_type == MemoryBackendType.DATABRICKS

    @pytest.mark.asyncio
    async def test_finds_most_recent_databricks_backend(self):
        """When multiple active Databricks backends exist, pick the most recent."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        backend_old = _make_backend(created_at=datetime(2024, 1, 1))
        backend_new = _make_backend(created_at=datetime(2024, 6, 1))
        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=[backend_old, backend_new])

        with patch(_MEMORY_BACKEND_REPO, return_value=mock_repo):
            result = await svc._check_databricks_config()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_active_databricks_backends(self):
        """When no active Databricks backends exist, return False."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        # An inactive Databricks backend
        inactive = _make_backend(is_active=False)
        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=[inactive])

        with patch(_MEMORY_BACKEND_REPO, return_value=mock_repo):
            result = await svc._check_databricks_config()

        assert result is False
        assert svc._memory_config is None

    @pytest.mark.asyncio
    async def test_returns_false_when_only_default_backends(self):
        """When only DEFAULT type backends exist, return False."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        default_backend = _make_backend(backend_type=MemoryBackendType.DEFAULT)
        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=[default_backend])

        with patch(_MEMORY_BACKEND_REPO, return_value=mock_repo):
            result = await svc._check_databricks_config()

        assert result is False

    @pytest.mark.asyncio
    async def test_uses_session_factory_when_no_session(self):
        """When no session is provided, use request_scoped_session."""
        svc = _make_service(session=None)

        backend = _make_backend()
        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=[backend])

        mock_session = _make_mock_session()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(_ASYNC_SESSION_FACTORY, return_value=mock_ctx), \
             patch(_MEMORY_BACKEND_REPO, return_value=mock_repo):
            result = await svc._check_databricks_config()

        assert result is True

    @pytest.mark.asyncio
    async def test_handles_exception_gracefully(self):
        """When repository throws, return False and log warning."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(side_effect=RuntimeError("DB error"))

        with patch(_MEMORY_BACKEND_REPO, return_value=mock_repo):
            result = await svc._check_databricks_config()

        assert result is False
        assert svc._memory_config is None


# ===================================================================
# _get_databricks_storage
# ===================================================================

class TestGetDatabricksStorage:
    """Tests for _get_databricks_storage method."""

    @pytest.mark.asyncio
    async def test_returns_cached_storage_with_updated_token(self):
        """When storage is already cached, return it and update user_token."""
        svc = _make_service()
        mock_storage = MagicMock()
        mock_storage.user_token = None
        svc._databricks_storage = mock_storage

        result = await svc._get_databricks_storage(user_token="new-token")
        assert result is mock_storage
        assert mock_storage.user_token == "new-token"

    @pytest.mark.asyncio
    async def test_returns_cached_storage_without_token(self):
        """When cached and no user_token, return without modification."""
        svc = _make_service()
        mock_storage = MagicMock()
        mock_storage.user_token = "old-token"
        svc._databricks_storage = mock_storage

        result = await svc._get_databricks_storage()
        assert result is mock_storage
        assert mock_storage.user_token == "old-token"

    @pytest.mark.asyncio
    async def test_returns_none_when_databricks_not_configured(self):
        """When Databricks is not configured, return None."""
        svc = _make_service()
        svc._check_databricks_config = AsyncMock(return_value=False)

        result = await svc._get_databricks_storage()
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_databricks_config_is_none(self):
        """When databricks_config within memory_config is None, return None."""
        svc = _make_service()
        svc._checked_config = True
        svc._memory_config = SimpleNamespace(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=None,
        )
        svc._check_databricks_config = AsyncMock(return_value=True)

        result = await svc._get_databricks_storage()
        assert result is None

    @pytest.mark.asyncio
    async def test_uses_document_index_from_object_config(self):
        """When config is an object with document_index, use it."""
        svc = _make_service()
        db_config = _make_databricks_config_object(document_index="catalog.schema.my_docs")
        svc._memory_config = _make_memory_config(databricks_config=db_config)
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 2.0
        })
        mock_storage_instance = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, return_value=mock_storage_instance), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            result = await svc._get_databricks_storage(user_token="test-token")

        assert result is mock_storage_instance
        assert svc._databricks_storage is mock_storage_instance

    @pytest.mark.asyncio
    async def test_derives_index_name_from_memory_index_when_no_document_index(self):
        """When no document_index, derive documentation_embeddings from memory_index."""
        svc = _make_service()
        db_config = _make_databricks_config_object(
            document_index=None,
            memory_index="catalog.schema.crew_memory",
        )
        svc._memory_config = _make_memory_config(databricks_config=db_config)
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 1.0
        })
        mock_storage_cls = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, new=mock_storage_cls), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            result = await svc._get_databricks_storage()

        # The derived index should be "catalog.schema.documentation_embeddings"
        call_kwargs = mock_storage_cls.call_args[1]
        assert call_kwargs["index_name"] == "catalog.schema.documentation_embeddings"

    @pytest.mark.asyncio
    async def test_uses_dict_config_with_document_index(self):
        """When databricks_config is a dict with document_index key, use it."""
        svc = _make_service()
        dict_config = {
            "endpoint_name": "test-endpoint",
            "document_index": "catalog.schema.dict_docs",
            "memory_index": "catalog.schema.crew_memory",
            "workspace_url": "https://example.com",
            "embedding_dimension": 1024,
            "personal_access_token": "dapi-test",
            "service_principal_client_id": None,
            "service_principal_client_secret": None,
            "document_endpoint_name": "doc-endpoint",
        }
        svc._memory_config = SimpleNamespace(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=dict_config,
        )
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 0.5
        })
        mock_storage_cls = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, new=mock_storage_cls), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            result = await svc._get_databricks_storage()

        call_kwargs = mock_storage_cls.call_args[1]
        assert call_kwargs["index_name"] == "catalog.schema.dict_docs"
        # document_endpoint_name should be used over endpoint_name
        assert call_kwargs["endpoint_name"] == "doc-endpoint"

    @pytest.mark.asyncio
    async def test_uses_document_endpoint_name_over_endpoint_name_for_object(self):
        """When config object has document_endpoint_name, prefer it."""
        svc = _make_service()
        db_config = _make_databricks_config_object(
            document_endpoint_name="doc-ep",
            endpoint_name="default-ep",
        )
        svc._memory_config = _make_memory_config(databricks_config=db_config)
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 0.5
        })
        mock_storage_cls = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, new=mock_storage_cls), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            await svc._get_databricks_storage()

        call_kwargs = mock_storage_cls.call_args[1]
        assert call_kwargs["endpoint_name"] == "doc-ep"

    @pytest.mark.asyncio
    async def test_returns_none_when_index_not_ready(self):
        """When index is not ready, return None."""
        svc = _make_service()
        svc._memory_config = _make_memory_config()
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": False, "message": "PROVISIONING", "attempts": 12, "elapsed_time": 60.0
        })

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            result = await svc._get_databricks_storage()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_initialization_exception(self):
        """When initialization throws, return None and log error."""
        svc = _make_service()
        svc._memory_config = _make_memory_config()
        svc._check_databricks_config = AsyncMock(return_value=True)

        with patch(_DATABRICKS_INDEX_SERVICE, side_effect=RuntimeError("Init error")), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            result = await svc._get_databricks_storage()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_unexpected_config_type(self):
        """When databricks_config is neither dict nor object with expected attrs, return None."""
        svc = _make_service()
        # Use an integer -- no 'endpoint_name' attr and not a dict
        svc._memory_config = SimpleNamespace(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=42,
        )
        svc._check_databricks_config = AsyncMock(return_value=True)

        with patch(_DATABRICKS_VECTOR_INDEX_REPO):
            result = await svc._get_databricks_storage()

        assert result is None

    @pytest.mark.asyncio
    async def test_fallback_index_name_when_no_memory_index(self):
        """When both document_index and memory_index are missing/empty, use default name."""
        svc = _make_service()
        db_config = _make_databricks_config_object(
            document_index=None,
            memory_index="",
        )
        svc._memory_config = _make_memory_config(databricks_config=db_config)
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 0.5
        })
        mock_storage_cls = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, new=mock_storage_cls), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            await svc._get_databricks_storage()

        call_kwargs = mock_storage_cls.call_args[1]
        assert call_kwargs["index_name"] == "documentation_embeddings"

    @pytest.mark.asyncio
    async def test_dict_config_without_document_index_derives_from_memory_index(self):
        """When dict config has no document_index, derive from memory_index."""
        svc = _make_service()
        dict_config = {
            "endpoint_name": "ep",
            "memory_index": "cat.sch.crew_memory",
            "workspace_url": "https://example.com",
            "embedding_dimension": 1024,
            "personal_access_token": None,
            "service_principal_client_id": None,
            "service_principal_client_secret": None,
        }
        svc._memory_config = SimpleNamespace(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=dict_config,
        )
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 0.5
        })
        mock_storage_cls = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, new=mock_storage_cls), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            await svc._get_databricks_storage()

        call_kwargs = mock_storage_cls.call_args[1]
        assert call_kwargs["index_name"] == "cat.sch.documentation_embeddings"

    @pytest.mark.asyncio
    async def test_dict_config_empty_memory_index_uses_default(self):
        """When dict config has empty memory_index, use bare default name."""
        svc = _make_service()
        dict_config = {
            "endpoint_name": "ep",
            "memory_index": "",
            "workspace_url": "https://example.com",
            "embedding_dimension": 1024,
            "personal_access_token": None,
            "service_principal_client_id": None,
            "service_principal_client_secret": None,
        }
        svc._memory_config = SimpleNamespace(
            backend_type=MemoryBackendType.DATABRICKS,
            databricks_config=dict_config,
        )
        svc._check_databricks_config = AsyncMock(return_value=True)

        mock_index_svc = MagicMock()
        mock_index_svc.wait_for_index_ready = AsyncMock(return_value={
            "ready": True, "attempts": 1, "elapsed_time": 0.5
        })
        mock_storage_cls = MagicMock()

        with patch(_DATABRICKS_INDEX_SERVICE, return_value=mock_index_svc), \
             patch(_DATABRICKS_VECTOR_STORAGE, new=mock_storage_cls), \
             patch(_DATABRICKS_VECTOR_INDEX_REPO):
            await svc._get_databricks_storage()

        call_kwargs = mock_storage_cls.call_args[1]
        assert call_kwargs["index_name"] == "documentation_embeddings"


# ===================================================================
# create_documentation_embedding
# ===================================================================

class TestCreateDocumentationEmbedding:
    """Tests for create_documentation_embedding method."""

    @pytest.mark.asyncio
    async def test_create_uses_sqlite_queue(self):
        """SQLite: use the embedding queue (batched writes to reduce lock contention)."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_queue = AsyncMock()
        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}), \
             patch(_EMBEDDING_QUEUE, mock_queue):
            result = await svc.create_documentation_embedding(doc_create)

        mock_queue.add_embedding.assert_awaited_once()
        assert str(result.id).startswith("queued-")

    @pytest.mark.asyncio
    async def test_create_uses_postgres_repository(self):
        """PostgreSQL / Lakebase: store directly via the pgvector repository."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        expected_model = _make_doc_embedding_model()
        mock_repo.create = AsyncMock(return_value=expected_model)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "localhost"}), \
             patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.create_documentation_embedding(doc_create)

        mock_repo.create.assert_awaited_once_with(doc_create)
        assert result is expected_model

    @pytest.mark.asyncio
    async def test_create_stores_in_pgvector_for_production_postgres(self):
        """Production Postgres / Lakebase (remote host) now stores in the pgvector
        repository too — it is no longer skipped, and Databricks Vector Search is
        never used for document embeddings."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        expected_model = _make_doc_embedding_model()
        mock_repo.create = AsyncMock(return_value=expected_model)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}), \
             patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.create_documentation_embedding(doc_create, user_token="token")

        mock_repo.create.assert_awaited_once_with(doc_create)
        assert result is expected_model

    @pytest.mark.asyncio
    async def test_create_raises_when_no_session_for_db_storage(self):
        """When there is no DB session, raise ValueError."""
        svc = _make_service(session=None)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "localhost"}):
            with pytest.raises(ValueError, match="Session is required"):
                await svc.create_documentation_embedding(doc_create)


# ===================================================================
# get_documentation_embedding
# ===================================================================

class TestGetDocumentationEmbedding:
    """Tests for get_documentation_embedding method."""

    @pytest.mark.asyncio
    async def test_get_returns_model_from_repository(self):
        """Get by ID returns the model from the repository."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        expected = _make_doc_embedding_model(id=42)
        mock_repo = MagicMock()
        mock_repo.get_by_id = AsyncMock(return_value=expected)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.get_documentation_embedding(42)

        assert result is expected
        mock_repo.get_by_id.assert_awaited_once_with(42)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(self):
        """Get by ID returns None when not found."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.get_by_id = AsyncMock(return_value=None)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.get_documentation_embedding(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_raises_without_session(self):
        """Get by ID raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.get_documentation_embedding(1)


# ===================================================================
# get_documentation_embeddings
# ===================================================================

class TestGetDocumentationEmbeddings:
    """Tests for get_documentation_embeddings (list) method."""

    @pytest.mark.asyncio
    async def test_list_returns_paginated_results(self):
        """List returns embeddings with skip and limit."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        items = [_make_doc_embedding_model(id=i) for i in range(3)]
        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=items)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.get_documentation_embeddings(skip=0, limit=10)

        assert len(result) == 3
        mock_repo.get_all.assert_awaited_once_with(0, 10)

    @pytest.mark.asyncio
    async def test_list_uses_default_pagination(self):
        """List uses default skip=0, limit=100 when not specified."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.get_all = AsyncMock(return_value=[])

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            await svc.get_documentation_embeddings()

        mock_repo.get_all.assert_awaited_once_with(0, 100)

    @pytest.mark.asyncio
    async def test_list_raises_without_session(self):
        """List raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.get_documentation_embeddings()


# ===================================================================
# update_documentation_embedding
# ===================================================================

class TestUpdateDocumentationEmbedding:
    """Tests for update_documentation_embedding method."""

    @pytest.mark.asyncio
    async def test_update_returns_updated_model(self):
        """Update delegates to repository and returns updated model."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        updated_model = _make_doc_embedding_model(id=1, title="Updated Title")
        mock_repo = MagicMock()
        mock_repo.update = AsyncMock(return_value=updated_model)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.update_documentation_embedding(1, {"title": "Updated Title"})

        assert result.title == "Updated Title"
        mock_repo.update.assert_awaited_once_with(1, {"title": "Updated Title"})

    @pytest.mark.asyncio
    async def test_update_returns_none_when_not_found(self):
        """Update returns None when embedding not found."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.update = AsyncMock(return_value=None)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.update_documentation_embedding(999, {"title": "X"})

        assert result is None

    @pytest.mark.asyncio
    async def test_update_raises_without_session(self):
        """Update raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.update_documentation_embedding(1, {"title": "X"})


# ===================================================================
# delete_documentation_embedding
# ===================================================================

class TestDeleteDocumentationEmbedding:
    """Tests for delete_documentation_embedding method."""

    @pytest.mark.asyncio
    async def test_delete_returns_true_on_success(self):
        """Delete returns True when embedding is deleted."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.delete = AsyncMock(return_value=True)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.delete_documentation_embedding(1)

        assert result is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_not_found(self):
        """Delete returns False when embedding not found."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.delete = AsyncMock(return_value=False)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            result = await svc.delete_documentation_embedding(999)

        assert result is False

    @pytest.mark.asyncio
    async def test_delete_raises_without_session(self):
        """Delete raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.delete_documentation_embedding(1)


# ===================================================================
# search_similar_embeddings
# ===================================================================

class TestSearchSimilarEmbeddings:
    """Tests for search_similar_embeddings method."""

    @pytest.mark.asyncio
    async def test_search_uses_pgvector_repository(self):
        """Similarity search runs against the pgvector repository (Lakebase/SQLite)."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        db_results = [_make_doc_embedding_model(id=5)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        query_emb = [0.1] * 10
        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings(query_emb, limit=3)

        # Default scope = built-in docs (group_id=None); no file_paths filter.
        mock_repo.search_similar.assert_awaited_once_with(
            query_emb, 3, group_id=None, file_paths=None
        )
        assert len(results) == 1
        assert results[0].id == 5

    @pytest.mark.asyncio
    async def test_search_scopes_by_group_and_file_paths(self):
        """Workspace knowledge search forwards group_id + file_paths to the repo."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=[])

        query_emb = [0.2] * 10
        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            await svc.search_similar_embeddings(
                query_emb, limit=7, group_id="grp-1", file_paths=["/Volumes/a/b.txt"]
            )

        mock_repo.search_similar.assert_awaited_once_with(
            query_emb, 7, group_id="grp-1", file_paths=["/Volumes/a/b.txt"]
        )

    @pytest.mark.asyncio
    async def test_search_returns_empty_when_no_session(self):
        """When there is no DB session, return an empty list."""
        svc = _make_service(session=None)
        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert results == []

    @pytest.mark.asyncio
    async def test_search_returns_empty_on_repository_error(self):
        """When the repository search throws, swallow and return an empty list."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(side_effect=RuntimeError("Total failure"))

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert results == []


# ===================================================================
# search_by_source
# ===================================================================

class TestSearchBySource:
    """Tests for search_by_source method."""

    @pytest.mark.asyncio
    async def test_search_by_source_delegates_to_repository(self):
        """Search by source delegates to repository."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        items = [_make_doc_embedding_model(source="crewai")]
        mock_repo = MagicMock()
        mock_repo.search_by_source = AsyncMock(return_value=items)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_by_source("crewai", skip=0, limit=10)

        mock_repo.search_by_source.assert_awaited_once_with("crewai", 0, 10)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_search_by_source_raises_without_session(self):
        """Search by source raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.search_by_source("crewai")


# ===================================================================
# search_by_title
# ===================================================================

class TestSearchByTitle:
    """Tests for search_by_title method."""

    @pytest.mark.asyncio
    async def test_search_by_title_delegates_to_repository(self):
        """Search by title delegates to repository."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        items = [_make_doc_embedding_model(title="Getting Started")]
        mock_repo = MagicMock()
        mock_repo.search_by_title = AsyncMock(return_value=items)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_by_title("Getting", skip=5, limit=20)

        mock_repo.search_by_title.assert_awaited_once_with("Getting", 5, 20)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_search_by_title_raises_without_session(self):
        """Search by title raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.search_by_title("Getting")


# ===================================================================
# get_recent_embeddings
# ===================================================================

class TestGetRecentEmbeddings:
    """Tests for get_recent_embeddings method."""

    @pytest.mark.asyncio
    async def test_get_recent_delegates_to_repository(self):
        """Get recent embeddings delegates to repository."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        items = [_make_doc_embedding_model(id=i) for i in range(3)]
        mock_repo = MagicMock()
        mock_repo.get_recent = AsyncMock(return_value=items)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.get_recent_embeddings(limit=3)

        mock_repo.get_recent.assert_awaited_once_with(3)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_get_recent_uses_default_limit(self):
        """Get recent uses default limit of 10."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_repo = MagicMock()
        mock_repo.get_recent = AsyncMock(return_value=[])

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            await svc.get_recent_embeddings()

        mock_repo.get_recent.assert_awaited_once_with(10)

    @pytest.mark.asyncio
    async def test_get_recent_raises_without_session(self):
        """Get recent raises ValueError when no session."""
        svc = _make_service(session=None)

        with pytest.raises(ValueError, match="Session is required"):
            await svc.get_recent_embeddings()


# ===================================================================
# Constructor / Initialization
# ===================================================================

class TestServiceInit:
    """Tests for __init__ and basic state."""

    def test_init_with_session(self):
        """Service stores session and initializes internal state."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        assert svc.session is session
        assert svc._databricks_storage is None
        assert svc._memory_config is None
        assert svc._checked_config is False

    def test_init_without_session(self):
        """Service initializes with None session."""
        svc = _make_service()

        assert svc.session is None
        assert svc._databricks_storage is None
        assert svc._checked_config is False
