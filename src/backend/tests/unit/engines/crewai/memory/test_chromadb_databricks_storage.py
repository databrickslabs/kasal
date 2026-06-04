"""
Comprehensive unit tests for ChromaDBDatabricksStorage.

chromadb and asyncpg are stubbed via the package-level conftest.py so all
imports here work without the real third-party packages.
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

# The conftest.py in this directory already placed a chromadb stub in
# sys.modules, so this import resolves cleanly.
from src.engines.crewai.memory.chromadb_databricks_storage import ChromaDBDatabricksStorage  # noqa: E402


# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_storage(
    collection_name="test_collection",
    storage_path=None,
    embedding_function=None,
    memory_type="short_term",
    job_id=None,
):
    """Return (storage, mock_client, mock_collection) with fresh mocks."""
    mock_collection = MagicMock()
    mock_collection.count.return_value = 0

    mock_client = MagicMock()
    mock_client.get_or_create_collection.return_value = mock_collection

    # Point the stub's PersistentClient at our fresh mock_client
    chromadb_stub = sys.modules["chromadb"]
    chromadb_stub.PersistentClient.return_value = mock_client
    chromadb_stub.Settings.return_value = MagicMock()

    emb_fn = embedding_function or MagicMock()
    path = storage_path or Path("/tmp/test_chroma_storage")

    storage = ChromaDBDatabricksStorage(
        storage_path=path,
        collection_name=collection_name,
        embedding_function=emb_fn,
        memory_type=memory_type,
        job_id=job_id,
    )

    # Override internals so each test has independent control
    storage._client = mock_client
    storage._collection = mock_collection
    return storage, mock_client, mock_collection


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestChromaDBDatabricksStorageInit:
    def test_init_sets_collection_name(self):
        storage, _, _ = _make_storage(collection_name="my_col")
        assert storage.collection_name == "my_col"

    def test_init_sets_memory_type(self):
        storage, _, _ = _make_storage(memory_type="entity")
        assert storage.type == "entity"

    def test_init_sets_job_id(self):
        storage, _, _ = _make_storage(job_id="job_99")
        assert storage.job_id == "job_99"

    def test_init_job_id_is_none_by_default(self):
        storage, _, _ = _make_storage()
        assert storage.job_id is None

    def test_init_allow_reset_is_true(self):
        storage, _, _ = _make_storage()
        assert storage.allow_reset is True

    def test_init_stores_embedding_function(self):
        emb_fn = MagicMock()
        storage, _, _ = _make_storage(embedding_function=emb_fn)
        assert storage.embedding_function is emb_fn

    def test_init_long_term_memory_type(self):
        storage, _, _ = _make_storage(memory_type="long_term")
        assert storage.type == "long_term"

    def test_init_short_term_memory_type(self):
        storage, _, _ = _make_storage(memory_type="short_term")
        assert storage.type == "short_term"


class TestChromaDBDatabricksStorageSave:
    def test_save_calls_collection_add(self):
        storage, _, mock_collection = _make_storage()
        storage.save("Hello world")
        mock_collection.add.assert_called_once()

    def test_save_converts_non_string_to_string(self):
        storage, _, mock_collection = _make_storage()
        storage.save(99999)
        call_args = mock_collection.add.call_args
        docs = call_args.kwargs.get("documents") or call_args[1].get("documents")
        assert "99999" in str(docs)

    def test_save_generates_unique_ids(self):
        storage, _, mock_collection = _make_storage()
        storage.save("text1")
        storage.save("text2")
        ids_1 = (
            mock_collection.add.call_args_list[0].kwargs.get("ids")
            or mock_collection.add.call_args_list[0][1].get("ids")
        )
        ids_2 = (
            mock_collection.add.call_args_list[1].kwargs.get("ids")
            or mock_collection.add.call_args_list[1][1].get("ids")
        )
        assert ids_1 != ids_2

    def test_save_passes_metadata(self):
        storage, _, mock_collection = _make_storage()
        storage.save("text", metadata={"key": "val"})
        call_args = mock_collection.add.call_args
        metadatas = call_args.kwargs.get("metadatas") or call_args[1].get("metadatas")
        assert metadatas is not None
        assert "key" in metadatas[0]

    def test_save_short_term_adds_session_id_when_job_id_set(self):
        storage, _, mock_collection = _make_storage(
            memory_type="short_term", job_id="sess_abc"
        )
        storage.save("memory text")
        call_args = mock_collection.add.call_args
        metadatas = call_args.kwargs.get("metadatas") or call_args[1].get("metadatas")
        assert metadatas is not None
        assert metadatas[0].get("session_id") == "sess_abc"

    def test_save_short_term_no_session_id_when_no_job_id(self):
        storage, _, mock_collection = _make_storage(memory_type="short_term", job_id=None)
        storage.save("memory")
        call_args = mock_collection.add.call_args
        metadatas = call_args.kwargs.get("metadatas") or call_args[1].get("metadatas")
        if metadatas:
            assert "session_id" not in metadatas[0]

    def test_save_long_term_does_not_add_session_id(self):
        storage, _, mock_collection = _make_storage(memory_type="long_term", job_id="j1")
        storage.save("long term text")
        call_args = mock_collection.add.call_args
        metadatas = call_args.kwargs.get("metadatas") or call_args[1].get("metadatas")
        if metadatas:
            assert "session_id" not in metadatas[0]

    def test_save_raises_when_collection_add_fails(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.add.side_effect = Exception("DB write error")
        with pytest.raises(Exception, match="DB write error"):
            storage.save("text")

    def test_save_logs_collection_count_after_add(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 3
        storage.save("text")
        mock_collection.count.assert_called()

    def test_save_with_none_metadata_uses_empty_dict(self):
        storage, _, mock_collection = _make_storage()
        storage.save("text", metadata=None)
        mock_collection.add.assert_called_once()

    def test_save_entity_type_no_session_id(self):
        storage, _, mock_collection = _make_storage(memory_type="entity", job_id="j1")
        storage.save("entity text")
        call_args = mock_collection.add.call_args
        metadatas = call_args.kwargs.get("metadatas") or call_args[1].get("metadatas")
        if metadatas:
            assert "session_id" not in metadatas[0]


class TestChromaDBDatabricksStorageSearch:
    def test_search_returns_empty_when_collection_is_empty(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 0
        result = storage.search("query")
        assert result == []

    def test_search_returns_formatted_results(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 2
        mock_collection.query.return_value = {"documents": [["doc1", "doc2"]]}
        results = storage.search("my query")
        assert len(results) == 2
        assert results[0] == {"content": "doc1"}
        assert results[1] == {"content": "doc2"}

    def test_search_uses_correct_n_results(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 10
        mock_collection.query.return_value = {"documents": [[]]}
        storage.search("q", limit=7)
        call_args = mock_collection.query.call_args
        n = call_args.kwargs.get("n_results") or call_args[1].get("n_results")
        assert n == 7

    def test_search_default_limit_is_3(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 5
        mock_collection.query.return_value = {"documents": [[]]}
        storage.search("q")
        call_args = mock_collection.query.call_args
        n = call_args.kwargs.get("n_results") or call_args[1].get("n_results")
        assert n == 3

    def test_search_short_term_adds_session_id_filter(self):
        storage, _, mock_collection = _make_storage(
            memory_type="short_term", job_id="j_abc"
        )
        mock_collection.count.return_value = 5
        mock_collection.query.return_value = {"documents": [[]]}
        storage.search("q")
        call_args = mock_collection.query.call_args
        where = call_args.kwargs.get("where") or call_args[1].get("where")
        assert where is not None
        assert where.get("session_id") == "j_abc"

    def test_search_long_term_no_session_filter(self):
        storage, _, mock_collection = _make_storage(
            memory_type="long_term", job_id="j_xyz"
        )
        mock_collection.count.return_value = 5
        mock_collection.query.return_value = {"documents": [[]]}
        storage.search("q")
        call_args = mock_collection.query.call_args
        where = call_args.kwargs.get("where") or call_args[1].get("where")
        if where:
            assert "session_id" not in where

    def test_search_returns_empty_when_no_documents_key(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 2
        mock_collection.query.return_value = {}
        result = storage.search("q")
        assert result == []

    def test_search_returns_empty_on_exception(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 3
        mock_collection.query.side_effect = Exception("Query error")
        result = storage.search("q")
        assert result == []

    def test_search_merges_existing_filter_with_session_id(self):
        storage, _, mock_collection = _make_storage(
            memory_type="short_term", job_id="j1"
        )
        mock_collection.count.return_value = 1
        mock_collection.query.return_value = {"documents": [[]]}
        storage.search("q", filter={"agent": "tester"})
        call_args = mock_collection.query.call_args
        where = call_args.kwargs.get("where") or call_args[1].get("where")
        assert where.get("session_id") == "j1"
        assert where.get("agent") == "tester"

    def test_search_passes_query_text(self):
        storage, _, mock_collection = _make_storage()
        mock_collection.count.return_value = 1
        mock_collection.query.return_value = {"documents": [["r"]]}
        storage.search("specific query text")
        call_args = mock_collection.query.call_args
        qt = call_args.kwargs.get("query_texts") or call_args[1].get("query_texts")
        assert "specific query text" in qt

    def test_search_short_term_no_job_id_no_where_filter(self):
        storage, _, mock_collection = _make_storage(
            memory_type="short_term", job_id=None
        )
        mock_collection.count.return_value = 2
        mock_collection.query.return_value = {"documents": [["d"]]}
        storage.search("q")
        call_args = mock_collection.query.call_args
        where = call_args.kwargs.get("where") or call_args[1].get("where")
        if where:
            assert "session_id" not in where


class TestChromaDBDatabricksStorageReset:
    def test_reset_deletes_collection(self):
        storage, mock_client, _ = _make_storage(collection_name="c1")
        storage.reset()
        mock_client.delete_collection.assert_called_once_with(name="c1")

    def test_reset_recreates_collection(self):
        storage, mock_client, _ = _make_storage(collection_name="c1")
        new_collection = MagicMock()
        mock_client.get_or_create_collection.return_value = new_collection
        storage.reset()
        mock_client.get_or_create_collection.assert_called()

    def test_reset_updates_internal_collection_reference(self):
        storage, mock_client, _ = _make_storage()
        new_coll = MagicMock()
        mock_client.get_or_create_collection.return_value = new_coll
        storage.reset()
        assert storage._collection is new_coll

    def test_reset_keeps_embedding_function(self):
        emb_fn = MagicMock()
        storage, mock_client, _ = _make_storage(
            collection_name="col", embedding_function=emb_fn
        )
        storage.reset()
        call_kwargs = mock_client.get_or_create_collection.call_args
        passed_fn = (
            call_kwargs.kwargs.get("embedding_function")
            or call_kwargs[1].get("embedding_function")
        )
        assert passed_fn is emb_fn

    def test_reset_handles_delete_error_gracefully(self):
        storage, mock_client, _ = _make_storage()
        mock_client.delete_collection.side_effect = Exception("Delete failed")
        storage.reset()  # Should not raise
