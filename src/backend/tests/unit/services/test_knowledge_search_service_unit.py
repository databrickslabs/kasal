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
# search() now queries the pgvector documentation_embeddings table via the
# DocumentationEmbeddingService instead of the Databricks Vector Search index.
# Knowledge search reads via DocumentationEmbeddingRepository(model=KnowledgeEmbedding).
DOC_SVC_MODULE = "src.repositories.documentation_embedding_repository.DocumentationEmbeddingRepository"
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


def _pg_row(
    content: str = "Sample content",
    title: str = "Doc Title",
    source: str = "orig.md",
    file_path: str = "/Volumes/catalog/schema/vol/doc.pdf",
    group_id: str = GROUP_ID,
    chunk_index: int = 0,
    score: float = 0.92,
) -> SimpleNamespace:
    """Build a DocumentationEmbedding-like row as returned by the pgvector repo."""
    return SimpleNamespace(
        content=content,
        title=title,
        source=source,
        file_path=file_path,
        group_id=group_id,
        doc_metadata={"chunk_index": chunk_index, "score": score},
    )


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

    @pytest.fixture(autouse=True)
    def _force_app_session(self):
        # Force the app-DB read path (no Lakebase) for these unit tests.
        with patch(
            "src.services.knowledge_embedding_session.resolve_lakebase_instance",
            new=AsyncMock(return_value=None),
        ):
            yield

    def _setup_service(self):
        self.session = Mock()
        self.service = KnowledgeSearchService(self.session, GROUP_ID)

    @staticmethod
    def _doc_service(rows):
        """A patched repository whose search_similar returns `rows`.

        When a specific file is requested, the service first resolves the
        requested basename(s) to stored full paths via ``list_group_file_paths``
        (so it can rank scoped to that file), then calls ``search_similar`` —
        mock both. The path list mirrors the stored rows so basename resolution
        finds them.
        """
        repo = MagicMock()
        repo.search_similar = AsyncMock(return_value=rows)
        repo.list_group_file_paths = AsyncMock(
            return_value=[getattr(r, "file_path", None) for r in rows]
        )
        return repo

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_returns_formatted_results(self, mock_doc_cls, mock_llm_cls):
        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        doc_svc = self._doc_service([_pg_row()])
        mock_doc_cls.return_value = doc_svc

        results = await self.service.search(
            QUERY, execution_id=EXECUTION_ID, user_token=USER_TOKEN
        )

        assert len(results) == 1
        assert results[0]["content"] == "Sample content"
        # source prefers the file_path of the stored knowledge row
        assert results[0]["metadata"]["source"] == "/Volumes/catalog/schema/vol/doc.pdf"
        assert results[0]["metadata"]["title"] == "Doc Title"
        assert results[0]["metadata"]["chunk_index"] == 0
        assert results[0]["metadata"]["score"] == 0.92
        assert results[0]["metadata"]["group_id"] == GROUP_ID
        assert results[0]["metadata"]["execution_id"] == EXECUTION_ID

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_multiple_results(self, mock_doc_cls, mock_llm_cls):
        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        rows = [
            _pg_row(content="First", score=0.95),
            _pg_row(content="Second", score=0.88),
            _pg_row(content="Third", score=0.72),
        ]
        mock_doc_cls.return_value = self._doc_service(rows)

        results = await self.service.search(QUERY, limit=3, user_token=USER_TOKEN)

        assert len(results) == 3
        assert results[0]["content"] == "First"
        assert results[2]["metadata"]["score"] == 0.72

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_with_file_paths_filter(self, mock_doc_cls, mock_llm_cls):
        """file_paths + group_id scope the search; a MATCHING file returns its row."""
        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        # _pg_row default basename is "doc.pdf"; request the same file by basename.
        doc_svc = self._doc_service([_pg_row()])
        mock_doc_cls.return_value = doc_svc

        results = await self.service.search(
            QUERY, file_paths=["/Volumes/cat/sch/vol/doc.pdf"], user_token=USER_TOKEN
        )

        assert len(results) == 1
        _, kwargs = doc_svc.search_similar.call_args
        assert kwargs["group_id"] == GROUP_ID
        # The service resolves the requested basename to the stored full path(s)
        # and ranks SCOPED to them (file_paths set) — not group-wide-then-filter,
        # which let a more query-similar OTHER file crowd the requested one out of
        # the top-k and return nothing.
        assert kwargs["file_paths"] == ["/Volumes/catalog/schema/vol/doc.pdf"]

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_requested_file_not_in_store_returns_empty(
        self, mock_doc_cls, mock_llm_cls
    ):
        """A requested file that matches no stored row returns NO results — the
        service must never substitute other files in the group (which would make
        the agent answer from the wrong document)."""
        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        # Group has only "doc.pdf"; the agent asks for a different file.
        mock_doc_cls.return_value = self._doc_service([_pg_row()])

        results = await self.service.search(
            QUERY, file_paths=["/Volumes/cat/sch/vol/not-uploaded.pdf"], user_token=USER_TOKEN
        )

        assert results == []

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_matches_basename_across_unicode_normalization(
        self, mock_doc_cls, mock_llm_cls
    ):
        """A macOS-NFD stored path ("a" + combining ¨) must match an NFC request
        ("ä"): basename resolution normalizes both to NFC. Without it, accented
        filenames (e.g. "Kindeswohlgefährdung") silently scope to nothing and the
        agent answers from no source."""
        import unicodedata

        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        nfd_path = unicodedata.normalize("NFD", "/Volumes/v/Kindeswohlgefährdung.pdf")
        nfc_name = unicodedata.normalize("NFC", "Kindeswohlgefährdung.pdf")
        # sanity: the stored path really is NFD (differs from its NFC form)
        assert nfd_path != unicodedata.normalize("NFC", nfd_path)
        doc_svc = self._doc_service([_pg_row(file_path=nfd_path)])
        mock_doc_cls.return_value = doc_svc

        results = await self.service.search(
            QUERY, file_paths=[nfc_name], user_token=USER_TOKEN
        )

        # The NFC request resolved to the stored NFD full path and returned its row
        assert len(results) == 1
        _, kwargs = doc_svc.search_similar.call_args
        assert kwargs["file_paths"] == [nfd_path]

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_with_execution_id_in_metadata(self, mock_doc_cls, mock_llm_cls):
        """execution_id is passed through to each result's metadata."""
        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        mock_doc_cls.return_value = self._doc_service([_pg_row()])

        results = await self.service.search(QUERY, execution_id="my-exec-id")

        assert results[0]["metadata"]["execution_id"] == "my-exec-id"

    @pytest.mark.asyncio
    @patch(LLM_MODULE)
    @patch(DOC_SVC_MODULE)
    async def test_search_defaults_chunk_and_score_when_metadata_missing(
        self, mock_doc_cls, mock_llm_cls
    ):
        """Rows without chunk_index/score in metadata get safe defaults."""
        self._setup_service()
        mock_llm_cls.get_embedding = AsyncMock(return_value=EMBEDDING)
        row = SimpleNamespace(
            content="c", title="t", source="s.md",
            file_path=None, group_id=GROUP_ID, doc_metadata=None,
        )
        mock_doc_cls.return_value = self._doc_service([row])

        results = await self.service.search(QUERY)

        assert len(results) == 1
        # file_path None -> falls back to source field
        assert results[0]["metadata"]["source"] == "s.md"
        assert results[0]["metadata"]["chunk_index"] == 0
        assert results[0]["metadata"]["score"] == 0.0


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


# --- Legacy Vector-Search storage builder (_get_vector_storage internals) ---
from datetime import datetime as _dt  # noqa: E402
from src.schemas.memory_backend import DatabricksMemoryConfig, MemoryBackendType  # noqa: E402

_SEARCH_DVS = "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage"


def _search_databricks_backend(db_config):
    b = MagicMock()
    b.is_active = True
    b.backend_type = MemoryBackendType.DATABRICKS
    b.created_at = _dt(2024, 1, 1)
    b.databricks_config = db_config
    b.cognitive_config = None
    b.custom_config = None
    return b


class TestSearchGetVectorStorageInternals:
    def _svc(self):
        return KnowledgeSearchService(MagicMock(), GROUP_ID)

    @pytest.mark.asyncio
    async def test_builds_storage_from_databricks_backend(self):
        svc = self._svc()
        cfg = DatabricksMemoryConfig(
            workspace_url="https://example.databricks.com",
            endpoint_name="ep",
            memory_index="cat.sch.mem",
            document_index="cat.sch.doc",
            embedding_dimension=1024,
        )
        svc._memory_backend_service = AsyncMock()
        svc._memory_backend_service.get_memory_backends = AsyncMock(return_value=[_search_databricks_backend(cfg)])
        with patch(_SEARCH_DVS) as MockStore:
            result = await svc._get_vector_storage(user_token="tok")
        assert result is MockStore.return_value

    @pytest.mark.asyncio
    async def test_returns_none_when_no_document_index(self):
        svc = self._svc()
        cfg = DatabricksMemoryConfig(workspace_url="https://example.databricks.com", endpoint_name="ep", memory_index="cat.sch.mem")
        svc._memory_backend_service = AsyncMock()
        svc._memory_backend_service.get_memory_backends = AsyncMock(return_value=[_search_databricks_backend(cfg)])
        assert await svc._get_vector_storage() is None

    @pytest.mark.asyncio
    async def test_returns_none_when_databricks_config_missing(self):
        svc = self._svc()
        svc._memory_backend_service = AsyncMock()
        svc._memory_backend_service.get_memory_backends = AsyncMock(return_value=[_search_databricks_backend(None)])
        assert await svc._get_vector_storage() is None
