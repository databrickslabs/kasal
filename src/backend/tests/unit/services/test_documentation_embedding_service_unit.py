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
    enable_short_term=True,
    enable_long_term=True,
    enable_entity=True,
    custom_config=None,
):
    """Create a mock MemoryBackend model object."""
    backend = SimpleNamespace(
        is_active=is_active,
        backend_type=backend_type,
        created_at=created_at or datetime(2024, 6, 1),
        databricks_config=databricks_config or {
            "endpoint_name": "test-endpoint",
            "short_term_index": "catalog.schema.short_term",
            "document_index": "catalog.schema.documents",
            "workspace_url": "https://example.com",
            "embedding_dimension": 1024,
            "personal_access_token": "dapi-test",
            "service_principal_client_id": None,
            "service_principal_client_secret": None,
            "document_endpoint_name": None,
        },
        group_id=group_id,
        enable_short_term=enable_short_term,
        enable_long_term=enable_long_term,
        enable_entity=enable_entity,
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
    """Create a SimpleNamespace mimicking DatabricksMemoryConfig as an object."""
    defaults = dict(
        endpoint_name="test-endpoint",
        document_endpoint_name=None,
        short_term_index="catalog.schema.short_term",
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
    async def test_derives_index_name_from_short_term_when_no_document_index(self):
        """When no document_index, derive from short_term_index."""
        svc = _make_service()
        db_config = _make_databricks_config_object(
            document_index=None,
            short_term_index="catalog.schema.short_term",
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
            "short_term_index": "catalog.schema.short_term",
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
    async def test_fallback_index_name_when_no_short_term_index(self):
        """When both document_index and short_term_index are missing, use default name."""
        svc = _make_service()
        db_config = _make_databricks_config_object(
            document_index=None,
            short_term_index="",
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
    async def test_dict_config_without_document_index_derives_from_short_term(self):
        """When dict config has no document_index, derive from short_term_index."""
        svc = _make_service()
        dict_config = {
            "endpoint_name": "ep",
            "short_term_index": "cat.sch.st",
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
    async def test_dict_config_empty_short_term_index_uses_default(self):
        """When dict config has empty short_term_index, use bare default name."""
        svc = _make_service()
        dict_config = {
            "endpoint_name": "ep",
            "short_term_index": "",
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
    async def test_create_via_databricks_storage(self):
        """When Databricks storage is available, save there and return model."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.save = AsyncMock()
        mock_storage.get_stats = AsyncMock(return_value={"count": 1})
        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        doc_create = _make_doc_embedding_create(doc_metadata={"key": "value"})

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}):
            result = await svc.create_documentation_embedding(doc_create, user_token="token")

        mock_storage.save.assert_awaited_once()
        assert result.source == "crewai-docs"
        assert result.title == "Test Document"
        assert "key" in result.doc_metadata
        assert "source" in result.doc_metadata
        assert "title" in result.doc_metadata

    @pytest.mark.asyncio
    async def test_create_via_databricks_handles_not_ready_error(self):
        """When Databricks raises 'not ready' error, return pending placeholder."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.save = AsyncMock(side_effect=RuntimeError("Index not ready"))
        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}):
            result = await svc.create_documentation_embedding(doc_create)

        assert str(result.id).startswith("pending-")

    @pytest.mark.asyncio
    async def test_create_via_databricks_reraises_non_ready_error(self):
        """When Databricks raises a non-ready error, re-raise it."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.save = AsyncMock(side_effect=ValueError("Permission denied"))
        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}):
            with pytest.raises(ValueError, match="Permission denied"):
                await svc.create_documentation_embedding(doc_create)

    @pytest.mark.asyncio
    async def test_create_skips_db_in_non_local_dev(self):
        """When not local dev and no Databricks, skip DB and return placeholder."""
        session = _make_mock_session()
        svc = _make_service(session=session)
        svc._get_databricks_storage = AsyncMock(return_value=None)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}):
            result = await svc.create_documentation_embedding(doc_create)

        assert str(result.id).startswith("skip-db-")

    @pytest.mark.asyncio
    async def test_create_uses_sqlite_queue(self):
        """When local dev with SQLite, use embedding queue."""
        session = _make_mock_session()
        svc = _make_service(session=session)
        svc._get_databricks_storage = AsyncMock(return_value=None)

        mock_queue = AsyncMock()

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}), \
             patch(_EMBEDDING_QUEUE, mock_queue):
            result = await svc.create_documentation_embedding(doc_create)

        mock_queue.add_embedding.assert_awaited_once()
        assert str(result.id).startswith("queued-")

    @pytest.mark.asyncio
    async def test_create_uses_postgres_repository(self):
        """When local dev with PostgreSQL, use repository directly."""
        session = _make_mock_session()
        svc = _make_service(session=session)
        svc._get_databricks_storage = AsyncMock(return_value=None)

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
    async def test_create_raises_when_no_session_for_db_storage(self):
        """When local dev but no session, raise ValueError."""
        svc = _make_service(session=None)
        svc._get_databricks_storage = AsyncMock(return_value=None)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "localhost"}):
            with pytest.raises(ValueError, match="Session is required"):
                await svc.create_documentation_embedding(doc_create)

    @pytest.mark.asyncio
    async def test_create_via_databricks_with_none_metadata(self):
        """When doc_metadata is None, default to empty dict for metadata merge."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.save = AsyncMock()
        mock_storage.get_stats = AsyncMock(return_value={"count": 1})
        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        doc_create = _make_doc_embedding_create(doc_metadata=None)

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}):
            result = await svc.create_documentation_embedding(doc_create)

        # Metadata should contain source and title even when doc_metadata was None
        assert result.doc_metadata["source"] == "crewai-docs"
        assert result.doc_metadata["title"] == "Test Document"

    @pytest.mark.asyncio
    async def test_create_via_databricks_handles_stats_error(self):
        """When get_stats raises after save, the save result is still returned."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.save = AsyncMock()
        mock_storage.get_stats = AsyncMock(side_effect=RuntimeError("stats error"))
        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        doc_create = _make_doc_embedding_create()

        with patch.dict(os.environ, {"DATABASE_TYPE": "postgres", "POSTGRES_SERVER": "remote-host"}):
            result = await svc.create_documentation_embedding(doc_create)

        assert result.source == "crewai-docs"


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
    async def test_search_via_databricks_returns_converted_results(self):
        """When Databricks is ready, search there and convert results."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        # Mock repo.get_index to report ready
        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(return_value=SimpleNamespace(
            success=True,
            index=SimpleNamespace(ready=True),
        ))
        mock_storage.repository = mock_idx_repo

        created_str = datetime(2024, 5, 1).isoformat()
        mock_storage.search = AsyncMock(return_value=[
            {
                "id": "doc-1",
                "content": "Hello world",
                "metadata": {
                    "source": "docs",
                    "title": "Greeting",
                    "created_at": created_str,
                    "updated_at": created_str,
                },
            }
        ])

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        query_emb = [0.1] * 10
        results = await svc.search_similar_embeddings(query_emb, limit=5)

        assert len(results) == 1
        assert results[0].content == "Hello world"
        assert results[0].source == "docs"

    @pytest.mark.asyncio
    async def test_search_databricks_index_not_ready_falls_to_db(self):
        """When Databricks index is not ready, fall back to database search."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        # Return not-ready index
        mock_idx_repo.get_index = AsyncMock(return_value=SimpleNamespace(
            success=True,
            index=SimpleNamespace(ready=False),
        ))
        mock_storage.repository = mock_idx_repo

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        db_results = [_make_doc_embedding_model(id=10)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        query_emb = [0.1] * 10
        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings(query_emb, limit=5)

        assert len(results) == 1
        assert results[0].id == 10

    @pytest.mark.asyncio
    async def test_search_databricks_search_fails_falls_to_db(self):
        """When Databricks search throws, fall back to database."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(return_value=SimpleNamespace(
            success=True,
            index=SimpleNamespace(ready=True),
        ))
        mock_storage.repository = mock_idx_repo
        mock_storage.search = AsyncMock(side_effect=RuntimeError("Search timeout"))

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        db_results = [_make_doc_embedding_model(id=20)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        query_emb = [0.1] * 10
        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings(query_emb, limit=5)

        assert len(results) == 1
        assert results[0].id == 20

    @pytest.mark.asyncio
    async def test_search_without_databricks_uses_db_repository(self):
        """When no Databricks storage, use database repository."""
        session = _make_mock_session()
        svc = _make_service(session=session)
        svc._get_databricks_storage = AsyncMock(return_value=None)

        db_results = [_make_doc_embedding_model(id=5)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        query_emb = [0.1] * 10
        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings(query_emb, limit=3)

        mock_repo.search_similar.assert_awaited_once_with(query_emb, 3)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_search_returns_empty_when_no_session(self):
        """When no Databricks and no session, return empty list."""
        svc = _make_service(session=None)
        svc._get_databricks_storage = AsyncMock(return_value=None)

        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert results == []

    @pytest.mark.asyncio
    async def test_search_handles_top_level_exception(self):
        """When the entire search method throws, return empty list."""
        session = _make_mock_session()
        svc = _make_service(session=session)
        svc._get_databricks_storage = AsyncMock(side_effect=RuntimeError("Total failure"))

        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert results == []

    @pytest.mark.asyncio
    async def test_search_databricks_with_none_results(self):
        """When Databricks search returns None, handle gracefully."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(return_value=SimpleNamespace(
            success=True,
            index=SimpleNamespace(ready=True),
        ))
        mock_storage.repository = mock_idx_repo
        mock_storage.search = AsyncMock(return_value=None)

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert results == []

    @pytest.mark.asyncio
    async def test_search_databricks_readiness_check_timeout(self):
        """When readiness check times out, skip Databricks and fall to DB."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(side_effect=asyncio.TimeoutError())
        mock_storage.repository = mock_idx_repo

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        db_results = [_make_doc_embedding_model(id=30)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings([0.1] * 10, limit=5)

        assert len(results) == 1
        assert results[0].id == 30

    @pytest.mark.asyncio
    async def test_search_databricks_readiness_check_exception(self):
        """When readiness check raises an unexpected error, skip Databricks."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(side_effect=RuntimeError("Connection refused"))
        mock_storage.repository = mock_idx_repo

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        db_results = [_make_doc_embedding_model(id=31)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings([0.1] * 10, limit=5)

        assert len(results) == 1
        assert results[0].id == 31

    @pytest.mark.asyncio
    async def test_search_databricks_dict_shaped_index_info_ready(self):
        """When index info is a dict with status.ready=True, proceed with Databricks."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(return_value={
            "status": {"ready": True}
        })
        mock_storage.repository = mock_idx_repo

        mock_storage.search = AsyncMock(return_value=[
            {
                "id": "d1",
                "content": "Dict check",
                "metadata": {
                    "source": "src",
                    "title": "t",
                    "created_at": datetime.utcnow().isoformat(),
                },
            }
        ])

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert len(results) == 1
        assert results[0].content == "Dict check"

    @pytest.mark.asyncio
    async def test_search_databricks_dict_shaped_index_info_not_ready(self):
        """When index info is a dict with status.ready=False, skip Databricks."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None

        mock_idx_repo = AsyncMock()
        mock_idx_repo.get_index = AsyncMock(return_value={
            "status": {"ready": False}
        })
        mock_storage.repository = mock_idx_repo

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        db_results = [_make_doc_embedding_model(id=40)]
        mock_repo = MagicMock()
        mock_repo.search_similar = AsyncMock(return_value=db_results)

        with patch(_DOC_EMBEDDING_REPO_CLS, return_value=mock_repo):
            results = await svc.search_similar_embeddings([0.1] * 10, limit=5)

        assert len(results) == 1
        assert results[0].id == 40

    @pytest.mark.asyncio
    async def test_search_databricks_with_no_repo_attribute(self):
        """When storage has no repository attribute, skip readiness check."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None
        # No 'repository' attribute at all
        del mock_storage.repository

        mock_storage.search = AsyncMock(return_value=[
            {
                "id": "nr1",
                "content": "No repo",
                "metadata": {"source": "s", "title": "t", "created_at": datetime.utcnow().isoformat()},
            }
        ])

        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        # repo is None from getattr, so the readiness block is skipped entirely
        # and use_databricks stays True, so search proceeds
        assert len(results) == 1
        assert results[0].content == "No repo"

    @pytest.mark.asyncio
    async def test_search_databricks_result_without_updated_at(self):
        """When Databricks result metadata lacks updated_at, fallback to created_at."""
        session = _make_mock_session()
        svc = _make_service(session=session)

        mock_storage = AsyncMock()
        mock_storage.index_name = "catalog.schema.docs"
        mock_storage.endpoint_name = "ep"
        mock_storage.user_token = None
        del mock_storage.repository

        created_str = "2024-03-15T10:00:00"
        mock_storage.search = AsyncMock(return_value=[
            {
                "id": "r1",
                "content": "Content",
                "metadata": {
                    "source": "src",
                    "title": "Title",
                    "created_at": created_str,
                    # No updated_at key
                },
            }
        ])
        svc._get_databricks_storage = AsyncMock(return_value=mock_storage)

        results = await svc.search_similar_embeddings([0.1] * 10, limit=5)
        assert len(results) == 1
        assert results[0].updated_at == datetime.fromisoformat(created_str)


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
