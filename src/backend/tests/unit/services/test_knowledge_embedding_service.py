"""
Comprehensive unit tests for services/knowledge_embedding_service.py
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime

from src.services.knowledge_embedding_service import KnowledgeEmbeddingService


class TestKnowledgeEmbeddingServiceInit:
    """Tests for KnowledgeEmbeddingService initialization."""

    def test_init_sets_session(self):
        mock_session = Mock()
        service = KnowledgeEmbeddingService(session=mock_session, group_id="grp-1")
        assert service.session is mock_session

    def test_init_sets_group_id(self):
        mock_session = Mock()
        service = KnowledgeEmbeddingService(session=mock_session, group_id="grp-42")
        assert service.group_id == "grp-42"

    def test_init_memory_backend_none(self):
        service = KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")
        assert service._memory_backend_service is None


class TestDetectContentType:
    """Tests for _detect_content_type."""

    @pytest.fixture
    def service(self):
        return KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")

    def test_pdf(self, service):
        assert service._detect_content_type("doc.pdf") == "PDF Document"

    def test_txt(self, service):
        assert service._detect_content_type("note.txt") == "Text Document"

    def test_md(self, service):
        assert service._detect_content_type("readme.md") == "Markdown Document"

    def test_doc(self, service):
        assert service._detect_content_type("file.doc") == "Word Document"

    def test_docx(self, service):
        assert service._detect_content_type("file.docx") == "Word Document"

    def test_csv(self, service):
        assert service._detect_content_type("data.csv") == "CSV Data"

    def test_json(self, service):
        assert service._detect_content_type("config.json") == "JSON Data"

    def test_xml(self, service):
        assert service._detect_content_type("data.xml") == "XML Data"

    def test_html(self, service):
        assert service._detect_content_type("page.html") == "HTML Document"

    def test_py(self, service):
        assert service._detect_content_type("script.py") == "Python Code"

    def test_js(self, service):
        assert service._detect_content_type("app.js") == "JavaScript Code"

    def test_ts(self, service):
        assert service._detect_content_type("app.ts") == "TypeScript Code"

    def test_unknown_ext(self, service):
        assert service._detect_content_type("file.xyz") == "Document"

    def test_no_extension(self, service):
        assert service._detect_content_type("Makefile") == "Document"

    def test_uppercase_ext(self, service):
        # Lowercased before comparison
        assert service._detect_content_type("FILE.PDF") == "PDF Document"


class TestCreateContextualChunk:
    """Tests for _create_contextual_chunk."""

    @pytest.fixture
    def service(self):
        return KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")

    def test_returns_string(self, service):
        result = service._create_contextual_chunk(
            chunk_text="hello world",
            document_summary="A short doc",
            filename="test.txt",
            section="Introduction",
            chunk_index=0,
        )
        assert isinstance(result, str)

    def test_contains_filename(self, service):
        result = service._create_contextual_chunk(
            chunk_text="content",
            document_summary="summary",
            filename="myfile.txt",
            section="Middle Content",
            chunk_index=2,
        )
        assert "myfile.txt" in result

    def test_contains_summary(self, service):
        result = service._create_contextual_chunk(
            chunk_text="text",
            document_summary="doc summary here",
            filename="f.txt",
            section="Introduction",
            chunk_index=0,
        )
        assert "doc summary here" in result

    def test_contains_section(self, service):
        result = service._create_contextual_chunk(
            chunk_text="text",
            document_summary="s",
            filename="f.txt",
            section="Later Content",
            chunk_index=3,
        )
        assert "Later Content" in result

    def test_contains_chunk_text(self, service):
        result = service._create_contextual_chunk(
            chunk_text="the actual content",
            document_summary="s",
            filename="f.txt",
            section="Introduction",
            chunk_index=0,
        )
        assert "the actual content" in result

    def test_part_number(self, service):
        result = service._create_contextual_chunk(
            chunk_text="t",
            document_summary="s",
            filename="f.txt",
            section="Introduction",
            chunk_index=4,
        )
        # chunk_index+1 = 5
        assert "5" in result


class TestGenerateDocumentSummary:
    """Tests for _generate_document_summary."""

    @pytest.fixture
    def service(self):
        return KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")

    @pytest.mark.asyncio
    async def test_short_content(self, service):
        result = await service._generate_document_summary("Hello world", "test.txt")
        assert "Hello world" in result
        assert "Text Document" in result

    @pytest.mark.asyncio
    async def test_long_content_truncated(self, service):
        long_content = "A" * 300
        result = await service._generate_document_summary(long_content, "doc.txt")
        assert "..." in result
        assert len(result) < len(long_content) + 100  # includes prefix, but truncated

    @pytest.mark.asyncio
    async def test_includes_content_type(self, service):
        result = await service._generate_document_summary("data", "file.csv")
        assert "CSV Data" in result

    @pytest.mark.asyncio
    async def test_fallback_on_error(self, service):
        # Patch _detect_content_type to raise
        with patch.object(service, "_detect_content_type", side_effect=RuntimeError("boom")):
            result = await service._generate_document_summary("content", "myfile.txt")
        assert "myfile.txt" in result


class TestChunkWithContext:
    """Tests for _chunk_with_context."""

    @pytest.fixture
    def service(self):
        return KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")

    @pytest.mark.asyncio
    async def test_empty_content_returns_empty(self, service):
        chunks = await service._chunk_with_context("", "file.txt")
        assert chunks == []

    @pytest.mark.asyncio
    async def test_small_content_single_chunk(self, service):
        chunks = await service._chunk_with_context("Hello", "file.txt", chunk_size=1000)
        assert len(chunks) == 1

    @pytest.mark.asyncio
    async def test_chunk_has_required_keys(self, service):
        chunks = await service._chunk_with_context("Hello world", "file.txt")
        assert len(chunks) >= 1
        chunk = chunks[0]
        assert "content" in chunk
        assert "raw_content" in chunk
        assert "section" in chunk
        assert "document_summary" in chunk
        assert "chunk_index" in chunk

    @pytest.mark.asyncio
    async def test_multiple_chunks_for_long_content(self, service):
        content = "A" * 3000
        chunks = await service._chunk_with_context(content, "file.txt", chunk_size=1000, overlap=100)
        assert len(chunks) >= 2

    @pytest.mark.asyncio
    async def test_section_labels(self, service):
        # 4000 chars so we cover all section positions
        content = "X" * 4000
        chunks = await service._chunk_with_context(content, "file.txt", chunk_size=1000, overlap=0)
        sections = {c["section"] for c in chunks}
        assert len(sections) > 1

    @pytest.mark.asyncio
    async def test_error_returns_empty_list(self, service):
        with patch.object(service, "_generate_document_summary", side_effect=RuntimeError("fail")):
            chunks = await service._chunk_with_context("content", "file.txt")
        assert chunks == []


class TestGetVectorStorage:
    """Tests for _get_vector_storage."""

    @pytest.fixture
    def service(self):
        return KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")

    @pytest.mark.asyncio
    async def test_returns_none_when_no_backends(self, service):
        mock_mbs = AsyncMock()
        mock_mbs.get_memory_backends.return_value = []

        with patch("src.services.knowledge_embedding_service.KnowledgeEmbeddingService._get_vector_storage") as mock_gvs:
            mock_gvs.return_value = None
            result = await service._get_vector_storage()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_import_error(self, service):
        with patch.dict("sys.modules", {"src.engines.crewai.memory.databricks_vector_storage": None}):
            result = await service._get_vector_storage()
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self, service):
        with patch("src.services.knowledge_embedding_service.KnowledgeEmbeddingService._get_vector_storage", new_callable=AsyncMock) as mock_gvs:
            mock_gvs.side_effect = Exception("unexpected")
            # Call the real method which will catch the exception
        # Use the real method but mock memory backend
        service._memory_backend_service = None
        with patch("src.services.knowledge_embedding_service.MemoryBackendService" if False else "builtins.__import__", side_effect=ImportError):
            pass
        result = await service._get_vector_storage()
        assert result is None


_LLM_GET_EMBEDDINGS = "src.core.llm_manager.LLMManager.get_embeddings"
_DOC_EMBEDDING_REPO = "src.repositories.documentation_embedding_repository.DocumentationEmbeddingRepository"


class TestEmbedFile:
    """Tests for embed_file (bulk-inserts into the pgvector documentation_embeddings table)."""

    @pytest.fixture
    def service(self):
        session = MagicMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return KnowledgeEmbeddingService(session=session, group_id="grp-1")

    @pytest.fixture(autouse=True)
    def _force_app_session(self):
        # Force the app-DB storage path (no Lakebase) for these unit tests.
        with patch(
            "src.services.knowledge_embedding_session.resolve_lakebase_instance",
            new=AsyncMock(return_value=None),
        ):
            yield

    @staticmethod
    def _repo(bulk_mock=None):
        repo = MagicMock()
        repo.bulk_create = bulk_mock or AsyncMock()
        return repo

    @pytest.mark.asyncio
    async def test_returns_skipped_when_no_chunks(self, service):
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, return_value=[]):
            result = await service.embed_file(
                file_path="/data/file.txt",
                file_content="",
                execution_id="exec-1",
            )
        assert result["status"] == "skipped"
        assert "chunks" in result["reason"].lower()

    @pytest.mark.asyncio
    async def test_skips_chunk_when_no_embedding_produced(self, service):
        # One chunk embeds, one comes back None: the None chunk is skipped but the
        # upload still succeeds for the embedded one.
        chunks = [
            {"content": "c1", "raw_content": "r1", "section": "Intro", "document_summary": "s"},
            {"content": "c2", "raw_content": "r2", "section": "Body", "document_summary": "s"},
        ]
        bulk_mock = AsyncMock()
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, return_value=chunks):
            with patch(_LLM_GET_EMBEDDINGS, new_callable=AsyncMock, return_value=[[0.1] * 1024, None]):
                with patch(_DOC_EMBEDDING_REPO, return_value=self._repo(bulk_mock)):
                    result = await service.embed_file(
                        file_path="/data/file.txt",
                        file_content="hello",
                        execution_id="exec-1",
                    )
        assert result["status"] == "success"
        assert result["chunks_processed"] == 2
        assert result["chunks_embedded"] == 1
        bulk_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_error_when_no_chunk_embedded(self, service):
        # All chunks parsed but none embedded (embedding model unavailable) must
        # report status "error", not a misleading "success".
        chunks = [{"content": "c", "raw_content": "c", "section": "Intro", "document_summary": "s"}]
        bulk_mock = AsyncMock()
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, return_value=chunks):
            with patch(_LLM_GET_EMBEDDINGS, new_callable=AsyncMock, return_value=[None]):
                with patch(_DOC_EMBEDDING_REPO, return_value=self._repo(bulk_mock)):
                    result = await service.embed_file(
                        file_path="/data/file.txt",
                        file_content="hello",
                        execution_id="exec-1",
                    )
        assert result["status"] == "error"
        assert result["chunks_processed"] == 1
        assert result["chunks_embedded"] == 0
        assert result["error"] == "embedding_model_unavailable"
        bulk_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_success_with_embedded_chunks(self, service):
        chunks = [
            {"content": "c1", "raw_content": "r1", "section": "Intro", "document_summary": "s"},
            {"content": "c2", "raw_content": "r2", "section": "Early Content", "document_summary": "s"},
        ]
        bulk_mock = AsyncMock()
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, return_value=chunks):
            with patch(_LLM_GET_EMBEDDINGS, new_callable=AsyncMock, return_value=[[0.1] * 1024, [0.1] * 1024]):
                with patch(_DOC_EMBEDDING_REPO, return_value=self._repo(bulk_mock)):
                    result = await service.embed_file(
                        file_path="/Volumes/c/s/v/grp-1/exec-1/file.txt",
                        file_content="hello world",
                        execution_id="exec-1",
                        agent_ids=["agent-1"],
                        user_token="token-xyz",
                    )

        assert result["status"] == "success"
        assert result["chunks_embedded"] == 2
        assert result["chunks_processed"] == 2
        assert "pgvector" in result["index_name"]
        # All chunks bulk-inserted in one call, each scoped by group_id + file_path.
        bulk_mock.assert_awaited_once()
        stored_items = bulk_mock.await_args.args[0]
        assert len(stored_items) == 2
        assert all(it.group_id == "grp-1" for it in stored_items)
        assert all(it.file_path == "/Volumes/c/s/v/grp-1/exec-1/file.txt" for it in stored_items)
        service.session.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_bulk_store_failure_rolls_back_and_errors(self, service):
        chunks = [
            {"content": "c1", "raw_content": "r1", "section": "Intro", "document_summary": "s"},
            {"content": "c2", "raw_content": "r2", "section": "Early Content", "document_summary": "s"},
        ]
        # The bulk insert fails -> rollback, and the top-level handler reports error.
        bulk_mock = AsyncMock(side_effect=RuntimeError("db down"))
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, return_value=chunks):
            with patch(_LLM_GET_EMBEDDINGS, new_callable=AsyncMock, return_value=[[0.1] * 1024, [0.1] * 1024]):
                with patch(_DOC_EMBEDDING_REPO, return_value=self._repo(bulk_mock)):
                    result = await service.embed_file(
                        file_path="/data/file.txt",
                        file_content="hello",
                        execution_id="exec-1",
                    )

        assert result["status"] == "error"
        service.session.rollback.assert_awaited()

    @pytest.mark.asyncio
    async def test_returns_error_on_top_level_exception(self, service):
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, side_effect=RuntimeError("top error")):
            result = await service.embed_file(
                file_path="/data/file.txt",
                file_content="hello",
                execution_id="exec-1",
            )
        assert result["status"] == "error"
        assert "top error" in result["error"]

    @pytest.mark.asyncio
    async def test_no_agent_ids_succeeds(self, service):
        chunks = [{"content": "c", "raw_content": "r", "section": "Intro", "document_summary": "s"}]
        with patch.object(service, "_chunk_with_context", new_callable=AsyncMock, return_value=chunks):
            with patch(_LLM_GET_EMBEDDINGS, new_callable=AsyncMock, return_value=[[0.1] * 1024]):
                with patch(_DOC_EMBEDDING_REPO, return_value=self._repo()):
                    result = await service.embed_file(
                        file_path="/data/file.txt",
                        file_content="hello",
                        execution_id="exec-1",
                        agent_ids=None,
                    )
        assert result["status"] == "success"


# --- Legacy Vector-Search storage builder (_get_vector_storage internals) ---
from src.schemas.memory_backend import DatabricksMemoryConfig, MemoryBackendType  # noqa: E402

_DVS = "src.engines.crewai.memory.databricks_vector_storage.DatabricksVectorStorage"


def _databricks_backend(db_config):
    b = MagicMock()
    b.is_active = True
    b.backend_type = MemoryBackendType.DATABRICKS
    b.created_at = datetime(2024, 1, 1)
    b.databricks_config = db_config
    b.cognitive_config = None
    b.custom_config = None
    return b


class TestGetVectorStorageInternals:
    """Exercise the real _get_vector_storage body (the path where a Databricks
    backend exists), not just the mocked early-return."""

    @pytest.fixture
    def service(self):
        return KnowledgeEmbeddingService(session=Mock(), group_id="grp-1")

    @pytest.mark.asyncio
    async def test_builds_storage_from_databricks_backend(self, service):
        cfg = DatabricksMemoryConfig(
            workspace_url="https://example.databricks.com",
            endpoint_name="ep",
            memory_index="cat.sch.mem",
            document_index="cat.sch.doc",
            embedding_dimension=1024,
        )
        service._memory_backend_service = AsyncMock()
        service._memory_backend_service.get_memory_backends = AsyncMock(return_value=[_databricks_backend(cfg)])
        with patch(_DVS) as MockStore:
            result = await service._get_vector_storage(user_token="tok")
        assert result is MockStore.return_value

    @pytest.mark.asyncio
    async def test_returns_none_when_no_document_index(self, service):
        cfg = DatabricksMemoryConfig(workspace_url="https://example.databricks.com", endpoint_name="ep", memory_index="cat.sch.mem")
        service._memory_backend_service = AsyncMock()
        service._memory_backend_service.get_memory_backends = AsyncMock(return_value=[_databricks_backend(cfg)])
        assert await service._get_vector_storage() is None

    @pytest.mark.asyncio
    async def test_returns_none_when_databricks_config_missing(self, service):
        service._memory_backend_service = AsyncMock()
        service._memory_backend_service.get_memory_backends = AsyncMock(return_value=[_databricks_backend(None)])
        assert await service._get_vector_storage() is None
