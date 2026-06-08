"""
Unit tests for services/knowledge_search_service.py

Knowledge search now runs against the application's pgvector
documentation_embeddings table (Lakebase in production, SQLite locally),
scoped by group_id and optionally by file_paths. These tests verify the
query-embedding step, the scoping forwarded to the embedding service, the
result mapping, and graceful failure handling.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch

from src.services.knowledge_search_service import KnowledgeSearchService


def _make_service(group_id="grp-1"):
    return KnowledgeSearchService(session=MagicMock(), group_id=group_id)


def _row(content="chunk text", title="Doc", source="orig.md",
         file_path="/Volumes/c/s/v/grp-1/exec/file.txt", group_id="grp-1",
         doc_metadata=None):
    return SimpleNamespace(
        content=content,
        title=title,
        source=source,
        file_path=file_path,
        group_id=group_id,
        doc_metadata=doc_metadata if doc_metadata is not None else {"chunk_index": 2, "score": 0.87},
    )


def _patch_embedding(value):
    return patch("src.core.llm_manager.LLMManager.get_embedding", AsyncMock(return_value=value))


def _patch_doc_service(search_mock):
    # Knowledge search goes through DocumentationEmbeddingRepository(model=KnowledgeEmbedding).
    repo = MagicMock()
    repo.search_similar = search_mock
    return patch(
        "src.repositories.documentation_embedding_repository.DocumentationEmbeddingRepository",
        return_value=repo,
    )


class TestKnowledgeSearchService:
    """Tests for KnowledgeSearchService"""

    @pytest.fixture(autouse=True)
    def _force_app_session(self):
        # Force the app-DB read path (no Lakebase) for these unit tests.
        with patch(
            "src.services.knowledge_embedding_session.resolve_lakebase_instance",
            new=AsyncMock(return_value=None),
        ):
            yield

    def test_initialization(self):
        svc = KnowledgeSearchService(session=MagicMock(), group_id="g")
        assert svc.group_id == "g"
        assert svc.session is not None

    @pytest.mark.asyncio
    async def test_search_maps_pgvector_rows(self):
        """Rows from pgvector are mapped to the {content, metadata} shape."""
        svc = _make_service()
        search_mock = AsyncMock(return_value=[_row()])

        with _patch_embedding([0.1] * 1024), _patch_doc_service(search_mock):
            results = await svc.search("how do I", execution_id="exec-9", limit=5)

        assert len(results) == 1
        r = results[0]
        assert r["content"] == "chunk text"
        # source prefers the file_path over the original source field
        assert r["metadata"]["source"] == "/Volumes/c/s/v/grp-1/exec/file.txt"
        assert r["metadata"]["title"] == "Doc"
        assert r["metadata"]["chunk_index"] == 2
        assert r["metadata"]["score"] == pytest.approx(0.87)
        assert r["metadata"]["group_id"] == "grp-1"
        assert r["metadata"]["execution_id"] == "exec-9"

    @pytest.mark.asyncio
    async def test_search_forwards_group_and_file_paths(self):
        """group_id (tenant isolation) + file_paths are forwarded to the embedding service."""
        svc = _make_service(group_id="grp-7")
        search_mock = AsyncMock(return_value=[])

        with _patch_embedding([0.2] * 1024), _patch_doc_service(search_mock):
            await svc.search("q", file_paths=["/Volumes/a/b.txt"], limit=8)

        search_mock.assert_awaited_once()
        args, kwargs = search_mock.call_args
        assert kwargs["group_id"] == "grp-7"
        # file_paths are filtered in-memory by the service, not forwarded to the repo
        assert kwargs["file_paths"] is None
        assert kwargs["limit"] == 8

    @pytest.mark.asyncio
    async def test_search_empty_file_paths_become_none(self):
        """An empty file_paths list is normalized to None (no filter)."""
        svc = _make_service()
        search_mock = AsyncMock(return_value=[])

        with _patch_embedding([0.1] * 1024), _patch_doc_service(search_mock):
            await svc.search("q", file_paths=[])

        _, kwargs = search_mock.call_args
        assert kwargs["file_paths"] is None

    @pytest.mark.asyncio
    async def test_search_returns_empty_when_embedding_fails(self):
        """No embedding -> empty results, embedding service not called."""
        svc = _make_service()
        search_mock = AsyncMock(return_value=[_row()])

        with _patch_embedding(None), _patch_doc_service(search_mock):
            results = await svc.search("q")

        assert results == []
        search_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_returns_empty_on_embedding_exception(self):
        """Embedding generation raising is swallowed -> empty results."""
        svc = _make_service()
        with patch("src.core.llm_manager.LLMManager.get_embedding",
                   AsyncMock(side_effect=RuntimeError("boom"))):
            results = await svc.search("q")
        assert results == []

    @pytest.mark.asyncio
    async def test_search_returns_empty_on_search_exception(self):
        """Embedding-service search raising is swallowed -> empty results."""
        svc = _make_service()
        search_mock = AsyncMock(side_effect=RuntimeError("db down"))

        with _patch_embedding([0.1] * 1024), _patch_doc_service(search_mock):
            results = await svc.search("q")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_source_falls_back_to_source_field(self):
        """When file_path is missing, source field is used."""
        svc = _make_service()
        row = _row(file_path=None, source="builtin-doc.md")
        search_mock = AsyncMock(return_value=[row])

        with _patch_embedding([0.1] * 1024), _patch_doc_service(search_mock):
            results = await svc.search("q")

        assert results[0]["metadata"]["source"] == "builtin-doc.md"
