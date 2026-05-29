"""Unit tests for engines/crewai/memory/databricks_storage_backend.py.

Exercises the CrewAI 1.10+ unified ``StorageBackend`` implementation backed by
Databricks Vector Search. The underlying ``DatabricksVectorIndexRepository`` is
mocked so no network calls occur; tests drive the public protocol methods (which
bridge to async via ``_run_sync``) plus the serialization/embedding helpers.
"""
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from crewai.memory.types import MemoryRecord

from src.engines.crewai.memory import databricks_storage_backend as mod
from src.engines.crewai.memory.databricks_storage_backend import (
    DatabricksStorageBackend,
    _loads_or_empty,
    _loads_or_list,
    _parse_datetime,
)

_COLUMNS = mod._SCHEMA_COLUMNS
_POSITIONS = {c: i for i, c in enumerate(_COLUMNS)}


def _make_record(**overrides):
    base = dict(
        id="rec-1",
        content="hello world",
        scope="/crew/research",
        categories=["fact"],
        importance=0.7,
        source="agent-1",
        private=False,
        metadata={"agent_id": "a1", "llm_model": "gpt", "tools_used": ["t1"]},
        created_at=datetime(2024, 1, 1, 12, 0, 0),
        last_accessed=datetime(2024, 1, 2, 12, 0, 0),
        embedding=[0.1, 0.2, 0.3],
    )
    base.update(overrides)
    return MemoryRecord(**base)


def _row_for(record_fields, score=0.9):
    """Build a Databricks data_array row in column order, score appended last."""
    defaults = {
        "id": "rec-1",
        "content": "hello",
        "scope": "/crew",
        "categories": json.dumps(["fact"]),
        "importance": 0.7,
        "source": "agent-1",
        "private": False,
        "metadata": json.dumps({"k": "v"}),
        "created_at": datetime(2024, 1, 1).isoformat(),
        "last_accessed": datetime(2024, 1, 2).isoformat(),
        "crew_id": "crew-1",
        "agent_id": "a1",
        "group_id": "group-1",
        "session_id": "sess-1",
        "llm_model": "gpt",
        "tools_used": json.dumps(["t1"]),
        "embedding_model": "databricks-gte-large-en",
        "version": 1,
    }
    defaults.update(record_fields)
    row = [defaults[c] for c in _COLUMNS]
    row.append(score)
    return row


def _search_response(rows):
    return {"result": {"data_array": rows}}


@pytest.fixture
def backend():
    with patch.object(mod, "DatabricksVectorIndexRepository", MagicMock()):
        be = DatabricksStorageBackend(
            index_name="cat.sch.idx",
            endpoint_name="ep",
            workspace_url="https://example.databricks.com",
            crew_id="crew-1",
            group_id="group-1",
            session_id="sess-1",
            embedding_dimension=3,
        )
    be._repo = AsyncMock()
    return be


class TestInit:
    def test_sets_attributes_and_builds_repo(self):
        with patch.object(mod, "DatabricksVectorIndexRepository") as repo_cls:
            be = DatabricksStorageBackend(
                index_name="idx",
                endpoint_name="ep",
                workspace_url="https://ws",
                crew_id="c",
                group_id="g",
            )
        assert be.index_name == "idx"
        assert be.embedding_dimension == 1024
        assert be.embedding_model == mod._DEFAULT_EMBEDDING_MODEL
        repo_cls.assert_called_once_with("https://ws", group_id="g")


class TestSave:
    def test_asave_empty_is_noop(self, backend):
        backend.save([])
        backend._repo.upsert.assert_not_called()

    def test_save_serializes_and_upserts(self, backend):
        backend.save([_make_record()])
        backend._repo.upsert.assert_awaited_once()
        kwargs = backend._repo.upsert.await_args.kwargs
        assert kwargs["index_name"] == "cat.sch.idx"
        row = kwargs["records"][0]
        assert row["id"] == "rec-1"
        assert row["categories"] == json.dumps(["fact"])
        assert row["embedding"] == [0.1, 0.2, 0.3]
        assert row["agent_id"] == "a1"
        assert row["group_id"] == "group-1"
        assert row["version"] == mod._SCHEMA_VERSION

    def test_record_to_row_without_embedding_or_embedder_raises(self, backend):
        rec = _make_record(embedding=None)
        with pytest.raises(ValueError):
            backend._record_to_row(rec)

    def test_record_to_row_embeds_when_embedding_missing(self, backend):
        backend.embedder = lambda texts: [[1.0, 2.0, 3.0]]
        row = backend._record_to_row(_make_record(embedding=None))
        assert row["embedding"] == [1.0, 2.0, 3.0]

    def test_record_to_row_generates_id_when_missing(self, backend):
        row = backend._record_to_row(_make_record(id=""))
        assert row["id"]

    def test_update_delegates_to_asave(self, backend):
        backend.update(_make_record())
        backend._repo.upsert.assert_awaited_once()


class TestSearch:
    def test_filters_by_min_score_and_scope_prefix(self, backend):
        rows = [
            _row_for({"id": "a", "scope": "/crew/research", "private": False}, score=0.9),
            _row_for({"id": "b", "scope": "/crew/other", "private": False}, score=0.8),
            _row_for({"id": "c", "scope": "/crew/research", "private": False}, score=0.1),
        ]
        backend._repo.similarity_search.return_value = _search_response(rows)
        out = backend.search([0.1, 0.2, 0.3], scope_prefix="/crew/research", min_score=0.5)
        ids = [r.id for r, _ in out]
        assert ids == ["a"]  # b filtered by scope, c filtered by min_score

    def test_private_record_excluded_for_other_session(self, backend):
        rows = [_row_for({"id": "p", "private": True, "source": "someone-else"}, score=0.9)]
        backend._repo.similarity_search.return_value = _search_response(rows)
        out = backend.search([0.0, 0.0, 0.0])
        assert out == []

    def test_private_record_included_for_own_session(self, backend):
        rows = [_row_for({"id": "p", "private": True, "source": "sess-1"}, score=0.9)]
        backend._repo.similarity_search.return_value = _search_response(rows)
        out = backend.search([0.0, 0.0, 0.0])
        assert [r.id for r, _ in out] == ["p"]

    def test_category_and_metadata_filters_passed_through(self, backend):
        backend._repo.similarity_search.return_value = _search_response([])
        backend.search(
            [0.0, 0.0, 0.0],
            categories=["fact"],
            metadata_filter={"agent_id": "a1"},
        )
        filters = backend._repo.similarity_search.await_args.kwargs["filters"]
        assert filters["categories"] == ["fact"]
        assert filters["agent_id"] == "a1"

    def test_limit_truncates_results(self, backend):
        rows = [_row_for({"id": str(i)}, score=0.9) for i in range(5)]
        backend._repo.similarity_search.return_value = _search_response(rows)
        out = backend.search([0.0, 0.0, 0.0], limit=2)
        assert len(out) == 2


class TestDelete:
    def test_delete_by_record_ids_dict_result(self, backend):
        backend._repo.delete_records.return_value = {"deleted": 2}
        assert backend.delete(record_ids=["a", "b"]) == 2

    def test_delete_by_record_ids_non_dict_result(self, backend):
        backend._repo.delete_records.return_value = None
        assert backend.delete(record_ids=["a", "b"]) == 2

    def test_delete_by_filters_collects_and_deletes(self, backend):
        rows = [_row_for({"id": "a"}), _row_for({"id": "b"})]
        backend._repo.similarity_search.return_value = _search_response(rows)
        assert backend.delete(scope_prefix="/crew", categories=["fact"]) == 2
        backend._repo.delete_records.assert_awaited_once()

    def test_delete_older_than_excludes_recent(self, backend):
        old = datetime(2020, 1, 1).isoformat()
        new = datetime(2024, 1, 1).isoformat()
        rows = [
            _row_for({"id": "old", "created_at": old}),
            _row_for({"id": "new", "created_at": new}),
        ]
        backend._repo.similarity_search.return_value = _search_response(rows)
        deleted = backend.delete(older_than=datetime(2022, 1, 1))
        assert deleted == 1

    def test_delete_no_matches_returns_zero(self, backend):
        backend._repo.similarity_search.return_value = _search_response([])
        assert backend.delete(metadata_filter={"x": "y"}) == 0
        backend._repo.delete_records.assert_not_called()

    def test_reset_calls_delete(self, backend):
        backend._repo.similarity_search.return_value = _search_response([])
        backend.reset(scope_prefix="/crew")
        backend._repo.similarity_search.assert_awaited()


class TestRecordFetchers:
    def test_get_record_returns_first(self, backend):
        backend._repo.similarity_search.return_value = _search_response(
            [_row_for({"id": "found"})]
        )
        rec = backend.get_record("found")
        assert rec.id == "found"

    def test_get_record_returns_none_when_empty(self, backend):
        backend._repo.similarity_search.return_value = _search_response([])
        assert backend.get_record("missing") is None

    def test_list_records_sorts_desc_and_paginates(self, backend):
        rows = [
            _row_for({"id": "old", "created_at": datetime(2020, 1, 1).isoformat()}),
            _row_for({"id": "new", "created_at": datetime(2024, 1, 1).isoformat()}),
            _row_for({"id": "mid", "created_at": datetime(2022, 1, 1).isoformat()}),
        ]
        backend._repo.similarity_search.return_value = _search_response(rows)
        out = backend.list_records(limit=2, offset=0)
        assert [r.id for r in out] == ["new", "mid"]

    def test_list_records_with_scope_prefix(self, backend):
        backend._repo.similarity_search.return_value = _search_response([])
        backend.list_records(scope_prefix="/crew")
        filters = backend._repo.similarity_search.await_args.kwargs["filters"]
        assert filters["scope"] == "/crew"


class TestScopesAndCategories:
    def test_get_scope_info_aggregates(self, backend):
        rows = [
            _row_for(
                {
                    "id": "a",
                    "scope": "/crew",
                    "categories": json.dumps(["x"]),
                    "created_at": datetime(2020, 1, 1).isoformat(),
                }
            ),
            _row_for(
                {
                    "id": "b",
                    "scope": "/crew/child",
                    "categories": json.dumps(["y"]),
                    "created_at": datetime(2024, 1, 1).isoformat(),
                }
            ),
        ]
        backend._repo.similarity_search.return_value = _search_response(rows)
        info = backend.get_scope_info("/crew")
        assert info.path == "/crew"
        assert info.record_count == 2
        assert set(info.categories) == {"x", "y"}
        assert info.oldest_record == datetime(2020, 1, 1)
        assert info.newest_record == datetime(2024, 1, 1)
        assert "/crew/child" in info.child_scopes

    def test_list_scopes_derives_children(self, backend):
        rows = [
            _row_for({"id": "a", "scope": "/crew/research"}),
            _row_for({"id": "b", "scope": "/crew/notes/deep"}),
            _row_for({"id": "c", "scope": "/crew"}),  # not under prefix -> skipped
            _row_for({"id": "d", "scope": "/crew/"}),  # equals prefix, empty remainder
        ]
        backend._repo.similarity_search.return_value = _search_response(rows)
        scopes = backend.list_scopes("/crew")
        assert scopes == ["/crew/notes", "/crew/research"]

    def test_list_categories_counts(self, backend):
        rows = [
            _row_for({"id": "a", "categories": json.dumps(["x", "y"])}),
            _row_for({"id": "b", "categories": json.dumps(["x"])}),
        ]
        backend._repo.similarity_search.return_value = _search_response(rows)
        counts = backend.list_categories(scope_prefix="/crew")
        assert counts == {"x": 2, "y": 1}


class TestCount:
    def test_count_dict_result(self, backend):
        backend._repo.count_documents.return_value = {"count": 7}
        assert backend.count() == 7

    def test_count_scalar_result(self, backend):
        backend._repo.count_documents.return_value = 4
        assert backend.count(scope_prefix="/crew") == 4

    def test_count_none_result(self, backend):
        backend._repo.count_documents.return_value = None
        assert backend.count() == 0


class TestSimilarityQueryParsing:
    def test_bad_score_defaults_to_zero(self, backend):
        row = _row_for({"id": "a"}, score="not-a-number")
        backend._repo.similarity_search.return_value = _search_response([row])
        out = backend.search([0.0, 0.0, 0.0])
        assert out[0][1] == 0.0

    def test_none_response_yields_empty(self, backend):
        backend._repo.similarity_search.return_value = None
        assert backend.search([0.0, 0.0, 0.0]) == []


class TestEmbedSync:
    def test_no_embedder_raises(self, backend):
        backend.embedder = None
        with pytest.raises(ValueError):
            backend._embed_sync("x")

    def test_callable_embedder(self, backend):
        backend.embedder = lambda texts: [[1.0, 2.0]]
        assert backend._embed_sync("x") == [1.0, 2.0]

    def test_embed_documents_object(self, backend):
        obj = MagicMock()
        obj.embed_documents.return_value = [[3.0, 4.0]]
        del obj.__call__  # ensure not treated as callable path first
        backend.embedder = obj
        # MagicMock is callable; force the embed_documents branch via a real object
        class E:
            def embed_documents(self, texts):
                return [[3.0, 4.0]]
        backend.embedder = E()
        assert backend._embed_sync("x") == [3.0, 4.0]

    def test_dict_embedder_callable_inner(self, backend):
        backend.embedder = {"config": {"embedder": lambda texts: [[5.0]]}}
        assert backend._embed_sync("x") == [5.0]

    def test_dict_embedder_embed_documents_inner(self, backend):
        class Inner:
            def embed_documents(self, texts):
                return [[6.0]]
        backend.embedder = {"config": {"embedder": Inner()}}
        assert backend._embed_sync("x") == [6.0]

    def test_dict_embedder_bad_shape_raises(self, backend):
        backend.embedder = {"config": {"embedder": 123}}
        with pytest.raises(TypeError):
            backend._embed_sync("x")

    def test_unsupported_embedder_type_raises(self, backend):
        backend.embedder = 123
        with pytest.raises(TypeError):
            backend._embed_sync("x")

    def test_empty_result_raises(self, backend):
        backend.embedder = lambda texts: []
        with pytest.raises(RuntimeError):
            backend._embed_sync("x")

    def test_numpy_like_tolist(self, backend):
        class Vec:
            def tolist(self):
                return [9.0, 8.0]
        backend.embedder = lambda texts: [Vec()]
        assert backend._embed_sync("x") == [9.0, 8.0]


class TestRunSyncAndHelpers:
    def test_zero_vector_length(self, backend):
        assert backend._zero_vector() == [0.0, 0.0, 0.0]

    def test_tenant_filters(self, backend):
        assert backend._tenant_filters() == {"crew_id": "crew-1", "group_id": "group-1"}

    def test_run_sync_passthrough_non_coroutine(self, backend):
        assert backend._run_sync(42) == 42

    @pytest.mark.asyncio
    async def test_run_sync_threadpath_under_running_loop(self, backend):
        # Called inside a running loop -> uses the worker-thread fallback.
        backend._repo.count_documents.return_value = {"count": 3}
        assert backend.count() == 3


class TestModuleHelpers:
    def test_loads_or_empty(self):
        assert _loads_or_empty(None) == {}
        assert _loads_or_empty({"a": 1}) == {"a": 1}
        assert _loads_or_empty('{"a": 1}') == {"a": 1}
        assert _loads_or_empty("[1,2]") == {}  # not a dict
        assert _loads_or_empty("not json") == {}

    def test_loads_or_list(self):
        assert _loads_or_list(None) == []
        assert _loads_or_list([1, 2]) == [1, 2]
        assert _loads_or_list("[1,2]") == [1, 2]
        assert _loads_or_list('{"a":1}') == []  # not a list
        assert _loads_or_list("nope") == []

    def test_parse_datetime(self):
        dt = datetime(2024, 1, 1, 5, 0, 0)
        assert _parse_datetime(dt) is dt
        assert isinstance(_parse_datetime(None), datetime)
        assert _parse_datetime("2024-01-01T05:00:00").year == 2024
        assert _parse_datetime("2024-01-01T05:00:00Z").year == 2024
        assert isinstance(_parse_datetime("garbage"), datetime)

    def test_parse_datetime_normalizes_to_naive_utc(self):
        # Offset-aware ISO timestamps must come back naive UTC so CrewAI's
        # recency math (datetime.utcnow() - created_at) doesn't crash.
        assert _parse_datetime("2026-05-29T08:00:00+00:00").tzinfo is None
        assert _parse_datetime("2026-05-29T10:00:00+02:00") == datetime(2026, 5, 29, 8, 0, 0)
        from datetime import timezone
        aware = datetime(2026, 5, 29, 8, 0, 0, tzinfo=timezone.utc)
        assert _parse_datetime(aware).tzinfo is None


def test_module_import_enables_nullpool(monkeypatch):
    """Importing the backend sets USE_NULLPOOL so background-save threads don't
    bind pooled connections to short-lived event loops."""
    import importlib
    import os as _os
    monkeypatch.delenv("USE_NULLPOOL", raising=False)
    import src.engines.crewai.memory.databricks_storage_backend as m
    importlib.reload(m)
    assert _os.environ.get("USE_NULLPOOL") == "true"
