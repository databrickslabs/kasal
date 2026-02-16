"""
Unit tests for KnowledgeSearchService.

Tests cover the three public/private methods:
- search() - main vector search orchestration
- _get_vector_storage() - vector storage instance construction
- _resolve_file_paths() - filename to /Volumes path resolution

All heavy dependencies are lazily imported inside method bodies, so patches
target the canonical source modules where the symbols are defined.
"""
import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.services.knowledge_search_service import KnowledgeSearchService

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GROUP_ID = "test-group-123"
USER_TOKEN = "tok-abc-123"
EXECUTION_ID = "exec-456"
AGENT_ID = "agent-789"
QUERY = "How does authentication work?"
EMBEDDING = [0.1] * 1024

# Mirrors DatabricksIndexSchemas.DOCUMENT_SEARCH_COLUMNS
DOCUMENT_SEARCH_COLUMNS = [
    "id", "title", "content", "source", "document_type",
    "section", "chunk_index", "chunk_size", "parent_document_id",
    "agent_ids", "created_at", "updated_at", "doc_metadata", "group_id",
    "embedding_model", "version",
]
DOCUMENT_COLUMN_POSITIONS = {col: idx for idx, col in enumerate(DOCUMENT_SEARCH_COLUMNS)}

# Patch targets - local imports require patching at the source module
SCHEMAS_MODULE = "src.schemas.databricks_index_schemas.DatabricksIndexSchemas"
LLM_MODULE = "src.core.llm_manager.LLMManager"
DVS_MODULE = "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage"
MBS_MODULE = "src.services.memory_backend_service.MemoryBackendService"
MBC_MODULE = "src.schemas.memory_backend.MemoryBackendConfig"
MBT_MODULE = "src.schemas.memory_backend.MemoryBackendType"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_data_row(
    content: str = "Sample content",
    source: str = "/Volumes/catalog/schema/vol/doc.pdf",
    title: str = "Doc Title",
    chunk_index: int = 0,
    score: float = 0.92,
) -> list:
    """Build a single data_array row matching the document schema column order."""
    row = [""] * len(DOCUMENT_SEARCH_COLUMNS)
    row[DOCUMENT_COLUMN_POSITIONS["id"]] = "row-id-1"
    row[DOCUMENT_COLUMN_POSITIONS["content"]] = content
    row[DOCUMENT_COLUMN_POSITIONS["source"]] = source
    row[DOCUMENT_COLUMN_POSITIONS["title"]] = title
    row[DOCUMENT_COLUMN_POSITIONS["chunk_index"]] = chunk_index
    row[DOCUMENT_COLUMN_POSITIONS["group_id"]] = GROUP_ID
    # score appended after all schema columns
    row.append(score)
    return row


def _make_search_response(rows: Optional[List[list]] = None) -> dict:
    """Build the nested dict that the repository similarity_search returns."""
    if rows is None:
        rows = [_make_data_row()]
    return {
        "success": True,
        "results": {
            "result": {
                "data_array": rows,
            }
        },
        "message": "ok",
    }


def _make_vector_storage(
    index_name: str = "catalog.schema.doc_index",
    endpoint_name: str = "vs-endpoint",
) -> MagicMock:
    """Create a mock DatabricksVectorStorage with required attributes."""
    storage = MagicMock()
    storage.index_name = index_name
    storage.endpoint_name = endpoint_name
    storage.repository = MagicMock()
    storage.repository.get_index = AsyncMock()
    storage.repository.similarity_search = AsyncMock()
    return storage


def _make_ready_index_info() -> SimpleNamespace:
    """Index info object that indicates readiness via attribute access."""
    return SimpleNamespace(
        success=True,
        index=SimpleNamespace(ready=True),
    )


def _make_not_ready_index_info() -> SimpleNamespace:
    """Index info object that indicates NOT ready."""
    return SimpleNamespace(
        success=True,
        index=SimpleNamespace(ready=False),
    )


def _make_ready_index_info_dict() -> dict:
    """Index info as a dict that indicates readiness."""
    return {"status": {"ready": True}}


def _make_backend(
    is_active: bool = True,
    backend_type: str = "databricks",
    databricks_config: Any = None,
    created_at: str = "2025-01-01T00:00:00",
) -> SimpleNamespace:
    """Create a mock memory backend object."""
    return SimpleNamespace(
        is_active=is_active,
        backend_type=backend_type,
        databricks_config=databricks_config,
        enable_short_term=True,
        enable_long_term=True,
        enable_entity=True,
        custom_config=None,
        created_at=created_at,
    )


def _make_db_config_object() -> SimpleNamespace:
    """Create an object-style Databricks memory config."""
    return SimpleNamespace(
        document_index="catalog.schema.doc_index",
        endpoint_name="vs-endpoint",
        document_endpoint_name="vs-doc-endpoint",
        workspace_url="https://example.com",
        embedding_dimension=1024,
        personal_access_token="pat-123",
        service_principal_client_id="sp-id",
        service_principal_client_secret="sp-secret",
    )


def _make_db_config_dict() -> dict:
    """Create a dict-style Databricks memory config."""
    return {
        "document_index": "catalog.schema.doc_idx",
        "endpoint_name": "ep",
        "document_endpoint_name": "doc-ep",
        "workspace_url": "https://example.com",
        "embedding_dimension": 1024,
        "personal_access_token": "pat",
        "service_principal_client_id": "sp-id",
        "service_principal_client_secret": "sp-secret",
    }


# ---------------------------------------------------------------------------
# TestInit
# ---------------------------------------------------------------------------


class TestKnowledgeSearchServiceInit:
    """Tests for constructor."""

    def test_init_stores_session_and_group_id(self):
        session = Mock()
        service = KnowledgeSearchService(session, GROUP_ID)

        assert service.session is session
        assert service.group_id == GROUP_ID
        assert service._memory_backend_service is None


# ---------------------------------------------------------------------------
# TestSearch - success paths
# ---------------------------------------------------------------------------


class TestSearchSuccess:
    """Tests for the search() method returning valid results."""

    def _setup_service(self):
        self.session = Mock()
        self.service = KnowledgeSearchService(self.session, GROUP_ID)
        self.mock_storage = _make_vector_storage()

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_returns_formatted_results(self, mock_llm_cls, mock_schemas_cls):
        self._setup_service()

        # Mock _get_vector_storage to avoid all downstream Databricks setup
        self.service._get_vector_storage = AsyncMock(return_value=self.mock_storage)

        # Schema mocks
        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS

        # Embedding
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        # Repo get_index -> ready; similarity_search -> results
        index_info = _make_ready_index_info()
        self.mock_storage.repository.get_index = AsyncMock(return_value=index_info)
        self.mock_storage.repository.similarity_search = AsyncMock(
            return_value=_make_search_response()
        )

        results = await self.service.search(
            QUERY, execution_id=EXECUTION_ID, user_token=USER_TOKEN
        )

        assert len(results) == 1
        assert results[0]["content"] == "Sample content"
        assert results[0]["metadata"]["source"] == "/Volumes/catalog/schema/vol/doc.pdf"
        assert results[0]["metadata"]["title"] == "Doc Title"
        assert results[0]["metadata"]["chunk_index"] == 0
        assert results[0]["metadata"]["score"] == 0.92
        assert results[0]["metadata"]["group_id"] == GROUP_ID
        assert results[0]["metadata"]["execution_id"] == EXECUTION_ID

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_multiple_results(self, mock_llm_cls, mock_schemas_cls):
        self._setup_service()
        self.service._get_vector_storage = AsyncMock(return_value=self.mock_storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        rows = [
            _make_data_row(content="First", score=0.95),
            _make_data_row(content="Second", score=0.88),
            _make_data_row(content="Third", score=0.72),
        ]
        self.mock_storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info()
        )
        self.mock_storage.repository.similarity_search = AsyncMock(
            return_value=_make_search_response(rows)
        )

        results = await self.service.search(QUERY, limit=3, user_token=USER_TOKEN)

        assert len(results) == 3
        assert results[0]["content"] == "First"
        assert results[2]["metadata"]["score"] == 0.72

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_with_file_paths_filter(self, mock_llm_cls, mock_schemas_cls):
        """When file_paths are supplied, _resolve_file_paths is called and results used."""
        self._setup_service()
        self.service._get_vector_storage = AsyncMock(return_value=self.mock_storage)
        resolved = ["/Volumes/cat/sch/vol/a.pdf"]
        self.service._resolve_file_paths = AsyncMock(return_value=resolved)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        self.mock_storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info()
        )
        self.mock_storage.repository.similarity_search = AsyncMock(
            return_value=_make_search_response()
        )

        results = await self.service.search(
            QUERY, file_paths=["a.pdf"], user_token=USER_TOKEN
        )

        assert len(results) == 1
        self.service._resolve_file_paths.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_with_execution_id_in_metadata(
        self, mock_llm_cls, mock_schemas_cls
    ):
        """execution_id is passed through to each result's metadata."""
        self._setup_service()
        self.service._get_vector_storage = AsyncMock(return_value=self.mock_storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        self.mock_storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info()
        )
        self.mock_storage.repository.similarity_search = AsyncMock(
            return_value=_make_search_response()
        )

        results = await self.service.search(QUERY, execution_id="my-exec-id")

        assert results[0]["metadata"]["execution_id"] == "my-exec-id"

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_index_ready_via_dict(self, mock_llm_cls, mock_schemas_cls):
        """Index readiness detected from a dict response."""
        self._setup_service()
        self.service._get_vector_storage = AsyncMock(return_value=self.mock_storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        self.mock_storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info_dict()
        )
        self.mock_storage.repository.similarity_search = AsyncMock(
            return_value=_make_search_response()
        )

        results = await self.service.search(QUERY)

        assert len(results) == 1


# ---------------------------------------------------------------------------
# TestSearch - empty / failure paths
# ---------------------------------------------------------------------------


class TestSearchEmpty:
    """Tests for search() returning empty list on various failure conditions."""

    def _setup_service(self):
        self.session = Mock()
        self.service = KnowledgeSearchService(self.session, GROUP_ID)

    @pytest.mark.asyncio
    async def test_search_returns_empty_when_no_vector_storage(self):
        self._setup_service()
        self.service._get_vector_storage = AsyncMock(return_value=None)

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_returns_empty_when_index_not_ready(
        self, mock_llm_cls, mock_schemas_cls
    ):
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        storage.repository.get_index = AsyncMock(
            return_value=_make_not_ready_index_info()
        )

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_returns_empty_on_search_timeout(
        self, mock_llm_cls, mock_schemas_cls
    ):
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info()
        )
        storage.repository.similarity_search = AsyncMock(
            side_effect=asyncio.TimeoutError("timed out")
        )

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_returns_empty_on_search_exception(
        self, mock_llm_cls, mock_schemas_cls
    ):
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info()
        )
        storage.repository.similarity_search = AsyncMock(
            side_effect=RuntimeError("connection failed")
        )

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_returns_empty_when_no_data_array(
        self, mock_llm_cls, mock_schemas_cls
    ):
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        storage.repository.get_index = AsyncMock(
            return_value=_make_ready_index_info()
        )
        empty_response: Dict[str, Any] = {"results": {"result": {"data_array": []}}}
        storage.repository.similarity_search = AsyncMock(return_value=empty_response)

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    async def test_search_returns_empty_when_embedding_fails(self, mock_llm_cls):
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_llm_cls.get_embedding = AsyncMock(return_value=None)

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    async def test_search_returns_empty_when_embedding_raises(self, mock_llm_cls):
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_llm_cls.get_embedding = AsyncMock(
            side_effect=RuntimeError("embedding error")
        )

        results = await self.service.search(QUERY)

        assert results == []

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    @patch(LLM_MODULE)
    async def test_search_returns_empty_on_index_readiness_timeout(
        self, mock_llm_cls, mock_schemas_cls
    ):
        """If the get_index call itself times out, search returns []."""
        self._setup_service()
        storage = _make_vector_storage()
        self.service._get_vector_storage = AsyncMock(return_value=storage)

        mock_schemas_cls.get_search_columns.return_value = DOCUMENT_SEARCH_COLUMNS
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)

        storage.repository.get_index = AsyncMock(
            side_effect=asyncio.TimeoutError("provisioning in progress")
        )

        results = await self.service.search(QUERY)

        assert results == []


# ---------------------------------------------------------------------------
# TestGetVectorStorage
# ---------------------------------------------------------------------------


class TestGetVectorStorage:
    """Tests for _get_vector_storage()."""

    def _setup_service(self):
        self.session = Mock()
        self.service = KnowledgeSearchService(self.session, GROUP_ID)

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_returns_storage_when_backend_configured(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        """Happy path: active Databricks backend with object-style config."""
        self._setup_service()

        db_config = _make_db_config_object()
        backend = _make_backend(databricks_config=db_config)

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[backend])
        mock_mbs_cls.return_value = mock_mbs_instance

        mock_config_obj = MagicMock()
        mock_config_obj.databricks_config = db_config
        mock_mbc_cls.return_value = mock_config_obj

        mock_storage_instance = MagicMock()
        mock_dvs_cls.return_value = mock_storage_instance

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is mock_storage_instance
        mock_dvs_cls.assert_called_once_with(
            endpoint_name="vs-doc-endpoint",
            index_name="catalog.schema.doc_index",
            crew_id="knowledge_files",
            memory_type="document",
            embedding_dimension=1024,
            workspace_url="https://example.com",
            personal_access_token="pat-123",
            service_principal_client_id="sp-id",
            service_principal_client_secret="sp-secret",
            user_token=USER_TOKEN,
        )

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_returns_none_when_no_backends(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        self._setup_service()

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[])
        mock_mbs_cls.return_value = mock_mbs_instance

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is None
        mock_dvs_cls.assert_not_called()

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_returns_none_when_no_active_databricks_backend(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        self._setup_service()

        # Backend exists but is inactive
        backend = _make_backend(is_active=False)

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[backend])
        mock_mbs_cls.return_value = mock_mbs_instance

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is None

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_handles_config_as_dict(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        """When databricks_config is a plain dict rather than a Pydantic model."""
        self._setup_service()

        db_config_dict = _make_db_config_dict()
        backend = _make_backend(databricks_config=db_config_dict)

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[backend])
        mock_mbs_cls.return_value = mock_mbs_instance

        mock_config_obj = MagicMock()
        mock_config_obj.databricks_config = db_config_dict
        mock_mbc_cls.return_value = mock_config_obj

        mock_storage_instance = MagicMock()
        mock_dvs_cls.return_value = mock_storage_instance

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is mock_storage_instance
        mock_dvs_cls.assert_called_once_with(
            endpoint_name="doc-ep",
            index_name="catalog.schema.doc_idx",
            crew_id="knowledge_files",
            memory_type="document",
            embedding_dimension=1024,
            workspace_url="https://example.com",
            personal_access_token="pat",
            service_principal_client_id="sp-id",
            service_principal_client_secret="sp-secret",
            user_token=USER_TOKEN,
        )

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_returns_none_when_document_index_missing(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        """If document_index is not set in config, returns None."""
        self._setup_service()

        db_config = SimpleNamespace(
            document_index=None,
            endpoint_name="ep",
            workspace_url="https://example.com",
            embedding_dimension=1024,
            personal_access_token=None,
            service_principal_client_id=None,
            service_principal_client_secret=None,
        )
        backend = _make_backend(databricks_config=db_config)

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[backend])
        mock_mbs_cls.return_value = mock_mbs_instance

        mock_config_obj = MagicMock()
        mock_config_obj.databricks_config = db_config
        mock_mbc_cls.return_value = mock_config_obj

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is None
        mock_dvs_cls.assert_not_called()

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_returns_none_when_databricks_config_is_none(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        self._setup_service()

        backend = _make_backend(databricks_config=None)

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[backend])
        mock_mbs_cls.return_value = mock_mbs_instance

        mock_config_obj = MagicMock()
        mock_config_obj.databricks_config = None
        mock_mbc_cls.return_value = mock_config_obj

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is None

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_returns_none_on_exception(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        """If an unexpected error occurs, _get_vector_storage returns None."""
        self._setup_service()

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(
            side_effect=RuntimeError("db connection lost")
        )
        mock_mbs_cls.return_value = mock_mbs_instance

        result = await self.service._get_vector_storage(USER_TOKEN)

        assert result is None

    @pytest.mark.asyncio
    @patch(DVS_MODULE)
    @patch(MBC_MODULE)
    @patch(MBS_MODULE)
    async def test_lazy_init_memory_backend_service(
        self, mock_mbs_cls, mock_mbc_cls, mock_dvs_cls
    ):
        """_memory_backend_service is lazily initialised on first call."""
        self._setup_service()
        assert self.service._memory_backend_service is None

        mock_mbs_instance = MagicMock()
        mock_mbs_instance.get_memory_backends = AsyncMock(return_value=[])
        mock_mbs_cls.return_value = mock_mbs_instance

        await self.service._get_vector_storage(USER_TOKEN)

        mock_mbs_cls.assert_called_once_with(self.session)
        assert self.service._memory_backend_service is mock_mbs_instance


# ---------------------------------------------------------------------------
# TestResolveFilePaths
# ---------------------------------------------------------------------------


class TestResolveFilePaths:
    """Tests for _resolve_file_paths()."""

    def _setup(self):
        self.session = Mock()
        self.service = KnowledgeSearchService(self.session, GROUP_ID)
        self.index_repo = MagicMock()
        self.index_repo.similarity_search = AsyncMock()
        self.doc_index = "catalog.schema.doc_index"
        self.endpoint = "ep"
        self.embedding = EMBEDDING
        self.columns = DOCUMENT_SEARCH_COLUMNS

    @pytest.mark.asyncio
    async def test_returns_full_paths_unchanged(self):
        """Paths already starting with /Volumes are returned as-is without querying."""
        self._setup()

        paths = ["/Volumes/cat/sch/vol/a.pdf", "/Volumes/cat/sch/vol/b.pdf"]

        result = await self.service._resolve_file_paths(
            paths, self.index_repo, self.doc_index, self.endpoint,
            self.embedding, self.columns
        )

        assert result == paths
        self.index_repo.similarity_search.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    async def test_resolves_filenames_to_full_paths(self, mock_schemas_cls):
        """Plain filenames are resolved by querying the vector index."""
        self._setup()

        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        source_pos = DOCUMENT_COLUMN_POSITIONS["source"]

        # Build rows with distinct source paths
        row1 = [""] * len(DOCUMENT_SEARCH_COLUMNS)
        row1[source_pos] = "/Volumes/cat/sch/vol/report.pdf"

        row2 = [""] * len(DOCUMENT_SEARCH_COLUMNS)
        row2[source_pos] = "/Volumes/cat/sch/vol/notes.txt"

        search_response = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [row1, row2],
                }
            },
            "message": "ok",
        }

        self.index_repo.similarity_search = AsyncMock(return_value=search_response)

        result = await self.service._resolve_file_paths(
            ["report.pdf"],
            self.index_repo, self.doc_index, self.endpoint,
            self.embedding, self.columns
        )

        assert result == ["/Volumes/cat/sch/vol/report.pdf"]

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    async def test_resolves_mixed_full_and_filename_paths(self, mock_schemas_cls):
        """A mix of /Volumes paths and bare filenames are both handled."""
        self._setup()

        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        source_pos = DOCUMENT_COLUMN_POSITIONS["source"]

        row = [""] * len(DOCUMENT_SEARCH_COLUMNS)
        row[source_pos] = "/Volumes/cat/sch/vol/notes.txt"

        search_response = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [row],
                }
            },
            "message": "ok",
        }
        self.index_repo.similarity_search = AsyncMock(return_value=search_response)

        result = await self.service._resolve_file_paths(
            ["/Volumes/cat/sch/vol/existing.pdf", "notes.txt"],
            self.index_repo, self.doc_index, self.endpoint,
            self.embedding, self.columns
        )

        assert "/Volumes/cat/sch/vol/existing.pdf" in result
        assert "/Volumes/cat/sch/vol/notes.txt" in result

    @pytest.mark.asyncio
    async def test_returns_none_on_resolution_failure(self):
        """If the index query fails, returns None."""
        self._setup()

        self.index_repo.similarity_search = AsyncMock(
            side_effect=RuntimeError("index error")
        )

        result = await self.service._resolve_file_paths(
            ["unknown.pdf"],
            self.index_repo, self.doc_index, self.endpoint,
            self.embedding, self.columns
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_search_not_successful(self):
        """If the search response indicates failure, returns None."""
        self._setup()

        failed_response = {"success": False, "results": {}, "message": "error"}
        self.index_repo.similarity_search = AsyncMock(return_value=failed_response)

        result = await self.service._resolve_file_paths(
            ["missing.pdf"],
            self.index_repo, self.doc_index, self.endpoint,
            self.embedding, self.columns
        )

        assert result is None

    @pytest.mark.asyncio
    @patch(SCHEMAS_MODULE)
    async def test_returns_none_when_filename_not_found_in_index(
        self, mock_schemas_cls
    ):
        """If the filename does not match any source in the index, returns None."""
        self._setup()

        mock_schemas_cls.get_column_positions.return_value = DOCUMENT_COLUMN_POSITIONS
        source_pos = DOCUMENT_COLUMN_POSITIONS["source"]

        row = [""] * len(DOCUMENT_SEARCH_COLUMNS)
        row[source_pos] = "/Volumes/cat/sch/vol/other_file.pdf"

        search_response = {
            "success": True,
            "results": {"result": {"data_array": [row]}},
            "message": "ok",
        }
        self.index_repo.similarity_search = AsyncMock(return_value=search_response)

        result = await self.service._resolve_file_paths(
            ["nonexistent.pdf"],
            self.index_repo, self.doc_index, self.endpoint,
            self.embedding, self.columns
        )

        # No match found => combined list is empty => returns None
        assert result is None
