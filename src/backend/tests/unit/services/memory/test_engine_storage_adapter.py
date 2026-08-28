"""Tests for EngineStorageAdapter — the Memory ↔ backend protocol bridge."""

from unittest.mock import MagicMock

import pytest

from src.services.memory.engine import MemoryRecord
from src.services.memory.engine_storage_adapter import (
    EngineStorageAdapter,
    build_litellm_embedder,
    embed_text,
)


def _record(content="fact", **kwargs) -> MemoryRecord:
    return MemoryRecord(content=content, **kwargs)


class FakeBackend:
    """crewAI-1.x protocol backend (what Databricks/Lakebase/local implement)."""

    def __init__(self):
        self.embedder = lambda texts: [[0.1, 0.2, 0.3] for _ in texts]
        self.embed_calls = 0
        self.saved = []
        self.search_kwargs = None
        self.deleted = None
        self.updated = None
        self.reset_scope = "unset"
        self._records = {}

    def _embed_sync(self, text):
        self.embed_calls += 1
        return [0.1, 0.2, 0.3]

    def save(self, records):
        self.saved.extend(records)
        return None  # crewAI-1.x backends return None

    def search(
        self,
        query_embedding,
        scope_prefix=None,
        categories=None,
        metadata_filter=None,
        limit=10,
        min_score=0.0,
    ):
        self.search_kwargs = dict(
            query_embedding=query_embedding,
            scope_prefix=scope_prefix,
            limit=limit,
            min_score=min_score,
        )
        return [(_record("hit-1"), 0.91), (_record("hit-2"), 0.72)]

    def list_records(self, scope_prefix=None, limit=200, offset=0):
        return [_record("listed")]

    def list_scopes(self, parent="/"):
        return [parent, f"{parent}sub"]

    def list_categories(self, scope_prefix=None):
        return {"beta": 2, "alpha": 1}

    def get_scope_info(self, scope):
        from src.services.memory.engine import ScopeInfo

        return ScopeInfo(path=scope, record_count=3)

    def get_record(self, record_id):
        return self._records.get(record_id)

    def update(self, record):
        self.updated = record

    def delete(
        self,
        scope_prefix=None,
        categories=None,
        record_ids=None,
        older_than=None,
        metadata_filter=None,
    ):
        self.deleted = record_ids
        return len(record_ids or [])

    def reset(self, scope_prefix=None):
        self.reset_scope = scope_prefix


class TestQueryTextPassthrough:
    def test_hybrid_backend_receives_query_text(self):
        captured = {}

        class HybridBackend(FakeBackend):
            def search(
                self,
                query_embedding,
                scope_prefix=None,
                categories=None,
                metadata_filter=None,
                limit=10,
                min_score=0.0,
                query_text=None,
            ):
                captured["query_text"] = query_text
                return []

        EngineStorageAdapter(HybridBackend()).search("swiss market news")
        assert captured["query_text"] == "swiss market news"

    def test_legacy_backend_not_passed_query_text(self):
        backend = FakeBackend()  # search() has no query_text parameter
        EngineStorageAdapter(backend).search("q")
        assert "query_text" not in backend.search_kwargs


class TestEmptyScopeProbe:
    class CountingBackend(FakeBackend):
        def __init__(self, records=0):
            super().__init__()
            self.records = records
            self.count_calls = 0

        def count(self, scope_prefix=None):
            self.count_calls += 1
            return self.records

    def test_empty_scope_skips_embedding_entirely(self):
        backend = self.CountingBackend(records=0)
        adapter = EngineStorageAdapter(backend)
        assert adapter.search("q", scope="/g1") == []
        assert backend.embed_calls == 0
        assert backend.search_kwargs is None

    def test_probe_result_is_cached(self):
        backend = self.CountingBackend(records=0)
        adapter = EngineStorageAdapter(backend)
        adapter.search("q", scope="/g1")
        adapter.search("q2", scope="/g1")
        assert backend.count_calls == 1

    def test_save_invalidates_probe_cache(self):
        backend = self.CountingBackend(records=0)
        adapter = EngineStorageAdapter(backend)
        assert adapter.search("q", scope="/g1") == []

        backend.records = 1
        adapter.save([_record("now it exists")])
        results = adapter.search("q", scope="/g1")

        assert backend.count_calls == 2  # cache cleared by save
        assert results, "post-save search should reach the backend"

    def test_backend_without_count_always_searches(self):
        backend = FakeBackend()
        results = EngineStorageAdapter(backend).search("q")
        assert results  # no probe support — search proceeds


class TestSearch:
    def test_embeds_query_and_maps_kwargs(self):
        backend = FakeBackend()
        adapter = EngineStorageAdapter(backend)

        results = adapter.search(
            "what did we learn", limit=5, scope="/g1", score_threshold=0.4
        )

        assert backend.search_kwargs == {
            "query_embedding": [0.1, 0.2, 0.3],
            "scope_prefix": "/g1",
            "limit": 5,
            "min_score": 0.4,
        }
        assert [r.content for r in results] == ["hit-1", "hit-2"]

    def test_unwraps_scores_into_metadata(self):
        adapter = EngineStorageAdapter(FakeBackend())
        results = adapter.search("q")
        assert results[0].metadata["similarity"] == 0.91

    def test_score_threshold_defaults_to_zero(self):
        backend = FakeBackend()
        EngineStorageAdapter(backend).search("q")
        assert backend.search_kwargs["min_score"] == 0.0

    def test_query_embedding_is_cached(self):
        backend = FakeBackend()
        adapter = EngineStorageAdapter(backend)
        adapter.search("Same   Question")
        adapter.search("same question")  # normalized to the same cache key
        assert backend.embed_calls == 1

    def test_embed_failure_returns_empty_not_raise(self):
        backend = FakeBackend()
        backend._embed_sync = MagicMock(side_effect=RuntimeError("endpoint down"))
        adapter = EngineStorageAdapter(backend)
        assert adapter.search("q") == []

    def test_bare_record_results_pass_through(self):
        backend = FakeBackend()
        backend.search = lambda **kw: [_record("bare")]
        results = EngineStorageAdapter(backend).search("q")
        assert [r.content for r in results] == ["bare"]


class TestWriteAndCrud:
    def test_save_returns_records_when_backend_returns_none(self):
        backend = FakeBackend()
        adapter = EngineStorageAdapter(backend)
        records = [_record("a"), _record("b")]
        assert adapter.save(records) == records
        assert backend.saved == records

    def test_delete_maps_record_id_and_returns_bool(self):
        backend = FakeBackend()
        adapter = EngineStorageAdapter(backend)
        assert adapter.delete("some-id") is True
        assert backend.deleted == ["some-id"]

    def test_update_applies_changes_via_backend(self):
        backend = FakeBackend()
        record = _record("original")
        backend._records[record.id] = record
        adapter = EngineStorageAdapter(backend)

        updated = adapter.update(record.id, importance=0.9)

        assert updated.importance == 0.9
        assert backend.updated is record

    def test_update_missing_record_returns_none(self):
        assert (
            EngineStorageAdapter(FakeBackend()).update("nope", importance=1.0) is None
        )

    def test_list_categories_dict_becomes_sorted_list(self):
        assert EngineStorageAdapter(FakeBackend()).list_categories() == [
            "alpha",
            "beta",
        ]

    def test_reset_maps_scope(self):
        backend = FakeBackend()
        EngineStorageAdapter(backend).reset("/g1")
        assert backend.reset_scope == "/g1"

    def test_close_is_safe_without_backend_close(self):
        EngineStorageAdapter(FakeBackend()).close()  # no attribute — no raise


class TestEmbedText:
    def test_callable(self):
        assert embed_text(lambda texts: [[1.0, 2.0]], "x") == [1.0, 2.0]

    def test_dict_wrapped_callable(self):
        embedder = {"config": {"embedder": lambda texts: [[3.0]]}}
        assert embed_text(embedder, "x") == [3.0]

    def test_embed_documents_object(self):
        class Embedder:
            def embed_documents(self, texts):
                return [[4.0]]

        assert embed_text(Embedder(), "x") == [4.0]

    def test_none_raises(self):
        with pytest.raises(ValueError):
            embed_text(None, "x")


class TestBuildLitellmEmbedder:
    def test_none_without_model(self):
        assert build_litellm_embedder({"provider": "ollama", "config": {}}) is None
        assert build_litellm_embedder(None) is None
        assert build_litellm_embedder("not-a-dict") is None

    def test_prefixes_non_openai_provider(self, monkeypatch):
        captured = {}

        def fake_embedding(model, input, **kwargs):
            captured.update(model=model, kwargs=kwargs)
            return {"data": [{"embedding": [0.5]} for _ in input]}

        import litellm

        monkeypatch.setattr(litellm, "embedding", fake_embedding)
        embedder = build_litellm_embedder(
            {
                "provider": "ollama",
                "config": {
                    "model": "nomic-embed-text",
                    "api_base": "http://example.com",
                },
            }
        )
        assert embedder(["hi"]) == [[0.5]]
        assert captured["model"] == "ollama/nomic-embed-text"
        assert captured["kwargs"]["api_base"] == "http://example.com"

    def test_openai_model_not_prefixed(self, monkeypatch):
        captured = {}

        def fake_embedding(model, input, **kwargs):
            captured["model"] = model
            return {"data": [{"embedding": [0.1]}]}

        import litellm

        monkeypatch.setattr(litellm, "embedding", fake_embedding)
        embedder = build_litellm_embedder(
            {"provider": "openai", "config": {"model": "text-embedding-3-small"}}
        )
        embedder(["hi"])
        assert captured["model"] == "text-embedding-3-small"


class TestQueryBoilerplateSymmetry:
    """search() sheds the run scaffold from the QUERY — records already shed it
    at write time, and only symmetric treatment keeps similarity meaningful."""

    def test_query_is_stripped_before_embedding(self):
        from unittest.mock import MagicMock

        from src.services.memory.engine_storage_adapter import EngineStorageAdapter

        backend = MagicMock()
        backend.search.return_value = []
        adapter = EngineStorageAdapter(
            backend, embedder=lambda texts: [[0.1] * 3 for _ in texts]
        )
        adapter._scope_has_records = lambda scope: True
        seen = {}
        adapter._embed_query = lambda q: seen.setdefault("q", q) and [0.1]

        adapter.search(
            "Respond directly and helpfully to the user's request. "
            "USER REQUEST — this run exists to answer it:\nprovide me swiss news "
            ": browser Expected output: A helpful, complete answer to the user's request."
        )
        assert seen["q"] == "provide me swiss news"
