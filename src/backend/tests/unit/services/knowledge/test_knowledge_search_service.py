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

from src.services.knowledge.search_service import KnowledgeSearchService

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
    "id",
    "title",
    "content",
    "source",
    "document_type",
    "section",
    "chunk_index",
    "chunk_size",
    "parent_document_id",
    "agent_ids",
    "created_at",
    "updated_at",
    "doc_metadata",
    "group_id",
    "embedding_model",
    "version",
]
DOCUMENT_COLUMN_POSITIONS = {
    col: idx for idx, col in enumerate(DOCUMENT_SEARCH_COLUMNS)
}

# Patch targets - local imports require patching at the source module
SCHEMAS_MODULE = "src.schemas.databricks_index_schemas.DatabricksIndexSchemas"
LLM_MODULE = "src.services.llm.manager.LLMManager"
# search() now queries the pgvector documentation_embeddings table via the
# DocumentationEmbeddingService instead of the Databricks Vector Search index.
# Knowledge search reads via DocumentationEmbeddingRepository(model=KnowledgeEmbedding).
DOC_SVC_MODULE = "src.repositories.documentation_embedding_repository.DocumentationEmbeddingRepository"
DVS_MODULE = "src.services.memory.databricks_vector_storage.DatabricksVectorStorage"
MBS_MODULE = "src.services.memory.config.backend_service.MemoryBackendService"
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
            "src.services.knowledge.embedding_session.resolve_lakebase_instance",
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
            QUERY,
            file_paths=["/Volumes/cat/sch/vol/not-uploaded.pdf"],
            user_token=USER_TOKEN,
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
    async def test_search_with_execution_id_in_metadata(
        self, mock_doc_cls, mock_llm_cls
    ):
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
            content="c",
            title="t",
            source="s.md",
            file_path=None,
            group_id=GROUP_ID,
            doc_metadata=None,
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


def _search_databricks_backend(db_config):
    b = MagicMock()
    b.is_active = True
    b.backend_type = MemoryBackendType.DATABRICKS
    b.created_at = _dt(2024, 1, 1)
    b.databricks_config = db_config
    b.cognitive_config = None
    b.custom_config = None
    return b
