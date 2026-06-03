"""Unit tests for engines/crewai/memory/lakebase_storage_backend.py.

Exercises the CrewAI 1.10+ unified ``StorageBackend`` implementation backed by
Lakebase (Postgres + pgvector). The ``get_lakebase_session`` async context
manager is mocked so no database is required; tests drive the public protocol
methods (which bridge to async via ``_run_sync``) and the helpers.
"""
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from crewai.memory.types import MemoryRecord

from src.engines.crewai.memory import lakebase_storage_backend as mod
from src.engines.crewai.memory.lakebase_storage_backend import (
    LakebaseStorageBackend,
    _loads_or_empty,
    _parse_datetime,
    _vector_to_pg,
)


class _FakeAsyncCM:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *exc):
        return False


def _result(*, fetchone=None, fetchall=None, scalar=None, rowcount=None):
    r = MagicMock()
    r.fetchone.return_value = fetchone
    r.fetchall.return_value = fetchall if fetchall is not None else []
    r.scalar.return_value = scalar
    r.rowcount = rowcount
    return r


def _make_record(**overrides):
    base = dict(
        id="rec-1",
        content="hello world",
        scope="/crew/research",
        categories=["fact"],
        importance=0.7,
        source="agent-1",
        private=False,
        metadata={"k": "v"},
        created_at=datetime(2024, 1, 1, 12, 0, 0),
        last_accessed=datetime(2024, 1, 2, 12, 0, 0),
        embedding=[0.1, 0.2, 0.3],
    )
    base.update(overrides)
    return MemoryRecord(**base)


def _db_row(
    *,
    id="rec-1",
    content="hello",
    metadata=None,
    created_at=None,
    updated_at=None,
    agent="agent-1",
    score=None,
):
    """A row shaped like the SELECTs: (id, content, metadata, created_at, updated_at, agent[, score])."""
    md = metadata if metadata is not None else {"scope": "/crew", "categories": ["x"]}
    row = [
        id,
        content,
        md,
        created_at or datetime(2024, 1, 1),
        updated_at or datetime(2024, 1, 2),
        agent,
    ]
    if score is not None:
        row.append(score)
    return tuple(row)


@pytest.fixture
def session():
    s = MagicMock()
    s.execute = AsyncMock()
    return s


@pytest.fixture
def backend(session, monkeypatch):
    monkeypatch.setattr(mod, "get_lakebase_session", lambda **kw: _FakeAsyncCM(session))
    be = LakebaseStorageBackend(
        table_name="kasal.memory_records",
        crew_id="crew-1",
        group_id="group-1",
        session_id="sess-1",
        embedding_dimension=3,
        instance_name="inst-1",
    )
    return be


class TestInit:
    def test_sets_attributes(self, backend):
        assert backend.table_name == "kasal.memory_records"
        assert backend.crew_id == "crew-1"
        assert backend.embedding_dimension == 3
        assert backend.instance_name == "inst-1"


class TestSave:
    def test_asave_empty_is_noop(self, backend, session):
        backend.save([])
        session.execute.assert_not_called()

    def test_save_inserts_with_metadata_payload(self, backend, session):
        backend.save([_make_record()])
        session.execute.assert_awaited_once()
        params = session.execute.await_args.args[1]
        assert params["id"] == "rec-1"
        assert params["crew_id"] == "crew-1"
        assert params["session_id"] == "sess-1"
        md = json.loads(params["metadata"])
        assert md["scope"] == "/crew/research"
        assert md["categories"] == ["fact"]
        assert md["importance"] == 0.7
        assert md["private"] is False
        assert params["embedding"] == "[0.1,0.2,0.3]"

    def test_save_embeds_when_embedding_missing(self, backend, session):
        backend.embedder = lambda texts: [[1.0, 2.0, 3.0]]
        backend.save([_make_record(embedding=None)])
        params = session.execute.await_args.args[1]
        assert params["embedding"] == "[1.0,2.0,3.0]"

    def test_save_generates_id_when_missing(self, backend, session):
        backend.save([_make_record(id="")])
        params = session.execute.await_args.args[1]
        assert params["id"]

    def test_update_delegates_to_asave(self, backend, session):
        backend.update(_make_record())
        session.execute.assert_awaited_once()


class TestGetRecord:
    def test_returns_record(self, backend, session):
        session.execute.return_value = _result(
            fetchone=_db_row(id="found", metadata={"scope": "/crew"})
        )
        rec = backend.get_record("found")
        assert rec.id == "found"
        assert rec.scope == "/crew"

    def test_returns_none_when_missing(self, backend, session):
        session.execute.return_value = _result(fetchone=None)
        assert backend.get_record("missing") is None


class TestListRecords:
    def test_returns_records(self, backend, session):
        session.execute.return_value = _result(
            fetchall=[_db_row(id="a"), _db_row(id="b")]
        )
        out = backend.list_records()
        assert [r.id for r in out] == ["a", "b"]

    def test_with_scope_prefix_adds_filter(self, backend, session):
        session.execute.return_value = _result(fetchall=[])
        backend.list_records(scope_prefix="/crew", limit=5, offset=2)
        params = session.execute.await_args.args[1]
        assert params["scope_prefix"] == "/crew%"
        assert params["limit"] == 5
        assert params["offset"] == 2


class TestScopesAndCategories:
    def test_get_scope_info_aggregates(self, backend, session):
        # First execute -> scope rows (metadata, created_at); second -> child scopes.
        scope_rows = [
            ({"categories": ["x"]}, datetime(2020, 1, 1)),
            ({"categories": ["y"]}, datetime(2024, 1, 1)),
        ]
        child_rows = [("/crew/child",)]
        session.execute.side_effect = [
            _result(fetchall=scope_rows),
            _result(fetchall=child_rows),
        ]
        info = backend.get_scope_info("/crew")
        assert info.path == "/crew"
        assert info.record_count == 2
        assert set(info.categories) == {"x", "y"}
        assert info.oldest_record == datetime(2020, 1, 1)
        assert info.newest_record == datetime(2024, 1, 1)
        assert "/crew/child" in info.child_scopes

    def test_get_scope_info_parses_json_metadata(self, backend, session):
        scope_rows = [(json.dumps({"categories": ["z"]}), datetime(2024, 1, 1))]
        session.execute.side_effect = [
            _result(fetchall=scope_rows),
            _result(fetchall=[]),
        ]
        info = backend.get_scope_info("/crew")
        assert info.categories == ["z"]

    def test_list_scopes_derives_children(self, backend, session):
        rows = [
            ("/crew/research",),
            ("/crew/notes/deep",),
            ("/crew",),  # not under prefix -> skipped
            ("/crew/",),  # equals prefix, empty remainder -> skipped
            (None,),  # null scope -> skipped
        ]
        session.execute.return_value = _result(fetchall=rows)
        scopes = backend.list_scopes("/crew")
        assert scopes == ["/crew/notes", "/crew/research"]

    def test_list_categories_counts(self, backend, session):
        rows = [
            ({"categories": ["x", "y"]},),
            (json.dumps({"categories": ["x"]}),),
        ]
        session.execute.return_value = _result(fetchall=rows)
        counts = backend.list_categories(scope_prefix="/crew")
        assert counts == {"x": 2, "y": 1}


class TestCount:
    def test_count_scalar(self, backend, session):
        session.execute.return_value = _result(scalar=5)
        assert backend.count() == 5

    def test_count_none_returns_zero(self, backend, session):
        session.execute.return_value = _result(scalar=None)
        assert backend.count(scope_prefix="/crew") == 0

    def test_reset_calls_delete(self, backend, session):
        session.execute.return_value = _result(rowcount=0)
        backend.reset(scope_prefix="/crew")
        session.execute.assert_awaited()


class TestSearch:
    def test_filters_min_score(self, backend, session):
        rows = [
            _db_row(id="hi", metadata={"scope": "/crew"}, score=0.9),
            _db_row(id="lo", metadata={"scope": "/crew"}, score=0.1),
        ]
        session.execute.return_value = _result(fetchall=rows)
        out = backend.search([0.1, 0.2, 0.3], min_score=0.5)
        assert [r.id for r, _ in out] == ["hi"]

    def test_private_excluded_for_other_source(self, backend, session):
        rows = [
            _db_row(
                id="p",
                metadata={"scope": "/crew", "private": True, "source": "someone-else"},
                agent="someone-else",
                score=0.9,
            )
        ]
        session.execute.return_value = _result(fetchall=rows)
        assert backend.search([0.0, 0.0, 0.0]) == []

    def test_skips_rows_that_fail_to_parse(self, backend, session):
        # Defensive branch: a row that passes the score gate but deserializes to None.
        rows = [_db_row(id="x", metadata={"scope": "/crew"}, score=0.9)]
        session.execute.return_value = _result(fetchall=rows)
        backend._row_to_record = MagicMock(return_value=None)
        assert backend.search([0.0, 0.0, 0.0]) == []

    def test_scope_category_metadata_filters_in_params(self, backend, session):
        session.execute.return_value = _result(fetchall=[])
        backend.search(
            [0.0, 0.0, 0.0],
            scope_prefix="/crew",
            categories=["fact"],
            metadata_filter={"agent_id": "a1"},
        )
        params = session.execute.await_args.args[1]
        assert params["scope_prefix"] == "/crew%"
        assert params["categories"] == ["fact"]
        assert params["mf_0"] == "a1"
        assert params["query_embedding"] == "[0.0,0.0,0.0]"


class TestDelete:
    def test_delete_by_record_ids(self, backend, session):
        session.execute.return_value = _result(rowcount=2)
        assert backend.delete(record_ids=["a", "b"]) == 2
        params = session.execute.await_args.args[1]
        assert params["record_ids"] == ["a", "b"]

    def test_delete_all_filters(self, backend, session):
        session.execute.return_value = _result(rowcount=3)
        deleted = backend.delete(
            scope_prefix="/crew",
            categories=["fact"],
            older_than=datetime(2022, 1, 1),
            metadata_filter={"agent_id": "a1"},
        )
        assert deleted == 3
        params = session.execute.await_args.args[1]
        assert params["scope_prefix"] == "/crew%"
        assert params["categories"] == ["fact"]
        assert params["older_than"] == datetime(2022, 1, 1)
        assert params["mf_0"] == "a1"

    def test_delete_missing_rowcount_returns_zero(self, backend, session):
        result = _result()
        result.rowcount = None
        session.execute.return_value = result
        assert backend.delete(record_ids=["a"]) == 0


class TestRowToRecord:
    def test_none_row_returns_none(self, backend):
        assert backend._row_to_record(None) is None

    def test_timestamptz_normalized_to_naive_utc(self, backend):
        # Postgres timestamptz comes back offset-aware; CrewAI's recency math does
        # ``datetime.utcnow() - created_at`` (naive), so records MUST be naive UTC
        # to avoid "can't subtract offset-naive and offset-aware datetimes".
        from datetime import timezone
        from crewai.memory.types import compute_composite_score, MemoryConfig
        aware = datetime(2026, 5, 29, 8, 0, 0, tzinfo=timezone.utc)
        row = _db_row(id="r", metadata={"scope": "/s"}, created_at=aware, updated_at=aware)
        rec = backend._row_to_record(row)
        assert rec.created_at.tzinfo is None
        assert rec.last_accessed.tzinfo is None
        assert rec.created_at == datetime(2026, 5, 29, 8, 0, 0)
        # The actual operation that used to crash now succeeds.
        compute_composite_score(rec, 0.9, MemoryConfig())

    def test_source_falls_back_to_agent(self, backend):
        row = _db_row(id="r", metadata={"scope": "/s"}, agent="the-agent")
        rec = backend._row_to_record(row)
        assert rec.source == "the-agent"

    def test_last_accessed_from_metadata(self, backend):
        row = _db_row(
            id="r",
            metadata={"scope": "/s", "last_accessed": "2024-06-01T00:00:00"},
        )
        rec = backend._row_to_record(row)
        assert rec.last_accessed.year == 2024 and rec.last_accessed.month == 6

    def test_last_accessed_fallback_to_updated_at(self, backend):
        row = _db_row(id="r", metadata={"scope": "/s"}, updated_at=datetime(2023, 3, 3))
        rec = backend._row_to_record(row)
        assert rec.last_accessed == datetime(2023, 3, 3)

    def test_json_string_metadata(self, backend):
        row = _db_row(id="r", metadata=json.dumps({"scope": "/json", "importance": 0.9}))
        rec = backend._row_to_record(row)
        assert rec.scope == "/json"
        assert rec.importance == 0.9

    def test_missing_id_generates_uuid(self, backend):
        row = _db_row(id=None, metadata={"scope": "/s"})
        rec = backend._row_to_record(row)
        assert rec.id


class TestEmbedSync:
    def test_no_embedder_raises(self, backend):
        backend.embedder = None
        with pytest.raises(ValueError):
            backend._embed_sync("x")

    def test_callable_embedder(self, backend):
        backend.embedder = lambda texts: [[1.0, 2.0]]
        assert backend._embed_sync("x") == [1.0, 2.0]

    def test_embed_documents_object(self, backend):
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

    def test_unsupported_type_raises(self, backend):
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
    def test_tenant_where(self, backend):
        where, params = backend._tenant_where()
        assert where == ["crew_id = :crew_id", "group_id = :group_id"]
        assert params == {"crew_id": "crew-1", "group_id": "group-1"}

    def test_run_sync_passthrough_non_coroutine(self, backend):
        assert backend._run_sync(42) == 42

    @pytest.mark.asyncio
    async def test_run_sync_threadpath_under_running_loop(self, backend, session):
        session.execute.return_value = _result(scalar=2)
        assert backend.count() == 2


class TestModuleHelpers:
    def test_vector_to_pg(self):
        assert _vector_to_pg([1, 2.5, 3]) == "[1.0,2.5,3.0]"

    def test_loads_or_empty(self):
        assert _loads_or_empty(None) == {}
        assert _loads_or_empty({"a": 1}) == {"a": 1}
        assert _loads_or_empty('{"a": 1}') == {"a": 1}
        assert _loads_or_empty("[1,2]") == {}
        assert _loads_or_empty("not json") == {}

    def test_parse_datetime(self):
        dt = datetime(2024, 1, 1, 5, 0, 0)
        assert _parse_datetime(dt) is dt
        assert isinstance(_parse_datetime(None), datetime)
        assert _parse_datetime("2024-01-01T05:00:00").year == 2024
        assert _parse_datetime("2024-01-01T05:00:00Z").year == 2024
        assert isinstance(_parse_datetime("garbage"), datetime)


def test_module_import_enables_nullpool(monkeypatch):
    """Importing the backend sets USE_NULLPOOL so Lakebase background-save
    connections aren't pooled across the per-save event loops."""
    import importlib, os as _os
    monkeypatch.delenv("USE_NULLPOOL", raising=False)
    import src.engines.crewai.memory.lakebase_storage_backend as m
    importlib.reload(m)
    assert _os.environ.get("USE_NULLPOOL") == "true"
