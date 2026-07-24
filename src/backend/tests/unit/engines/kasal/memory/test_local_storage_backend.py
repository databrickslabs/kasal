"""Tests for LocalMemoryStorage — SQLite + cosine local memory backend."""

from datetime import datetime, timedelta, timezone

from kasal_engine.memory import MemoryRecord
from src.engines.kasal.memory.local_storage_backend import LocalMemoryStorage


def _stub_embedder(texts):
    """Deterministic 3-dim embeddings: 'cat…' → x-axis, 'dog…' → y-axis."""
    vectors = []
    for text in texts:
        lowered = text.lower()
        vectors.append(
            [1.0, 0.0, 0.0]
            if "cat" in lowered
            else [0.0, 1.0, 0.0] if "dog" in lowered else [0.0, 0.0, 1.0]
        )
    return vectors


def _store(tmp_path, embedder=_stub_embedder) -> LocalMemoryStorage:
    return LocalMemoryStorage(tmp_path / "memory.db", embedder=embedder)


def _save(store, content, scope="/g1", **kwargs):
    record = MemoryRecord(content=content, scope=scope, **kwargs)
    store.save([record])
    return record


class TestSaveAndSearch:
    def test_cosine_ordering(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "the cat sat on the mat")
        _save(store, "the dog chased the ball")

        results = store.search(query_embedding=[1.0, 0.0, 0.0], scope_prefix="/g1")

        assert results[0][0].content == "the cat sat on the mat"
        assert results[0][1] > results[1][1]

    def test_min_score_filters(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "cat facts")
        _save(store, "dog facts")
        results = store.search(query_embedding=[1.0, 0.0, 0.0], min_score=0.5)
        assert [r.content for r, _ in results] == ["cat facts"]

    def test_scope_prefix_isolates_tenants(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "cat in g1", scope="/g1")
        _save(store, "cat in g2", scope="/g2")
        results = store.search(query_embedding=[1.0, 0.0, 0.0], scope_prefix="/g1")
        assert [r.content for r, _ in results] == ["cat in g1"]

    def test_categories_filter(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "cat tagged", categories=["pets"])
        _save(store, "cat untagged")
        results = store.search(query_embedding=[1.0, 0.0, 0.0], categories=["pets"])
        assert [r.content for r, _ in results] == ["cat tagged"]

    def test_persists_across_reopen(self, tmp_path):
        _save(_store(tmp_path), "cat persists")
        reopened = _store(tmp_path)
        results = reopened.search(query_embedding=[1.0, 0.0, 0.0])
        assert [r.content for r, _ in results] == ["cat persists"]

    def test_no_embedder_saves_and_ranks_by_importance(self, tmp_path):
        store = _store(tmp_path, embedder=None)
        _save(store, "minor note", importance=0.1)
        _save(store, "major note", importance=0.9)
        results = store.search(query_embedding=[1.0, 0.0, 0.0])
        assert results[0][0].content == "major note"

    def test_precomputed_embedding_is_used(self, tmp_path):
        store = _store(tmp_path, embedder=None)
        record = MemoryRecord(content="anything", scope="/g1")
        record.embedding = [1.0, 0.0, 0.0]
        store.save([record])
        results = store.search(query_embedding=[1.0, 0.0, 0.0])
        assert results[0][1] > 0.75  # blended: 0.6*semantic + recency + importance


class TestCrud:
    def test_get_update_delete(self, tmp_path):
        store = _store(tmp_path)
        record = _save(store, "cat original")

        fetched = store.get_record(record.id)
        assert fetched.content == "cat original"

        fetched.content = "cat updated"
        store.update(fetched)
        assert store.get_record(record.id).content == "cat updated"

        assert store.delete(record_ids=[record.id]) == 1
        assert store.get_record(record.id) is None

    def test_delete_older_than(self, tmp_path):
        store = _store(tmp_path)
        old = MemoryRecord(
            content="ancient cat",
            scope="/g1",
            created_at=datetime.now(timezone.utc) - timedelta(days=30),
        )
        store.save([old])
        _save(store, "fresh cat")
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        assert store.delete(scope_prefix="/g1", older_than=cutoff) == 1
        assert store.count("/g1") == 1

    def test_listings_and_counts(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "one", scope="/g1/a", categories=["x"])
        _save(store, "two", scope="/g1/b", categories=["x", "y"])

        assert store.count("/g1") == 2
        assert len(store.list_records(scope_prefix="/g1")) == 2
        assert set(store.list_scopes("/g1")) == {"/g1/a", "/g1/b"}
        assert store.list_categories("/g1") == {"x": 2, "y": 1}
        assert store.get_scope_info("/g1").record_count == 2

    def test_reset_scope(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "keep", scope="/g2")
        _save(store, "drop", scope="/g1")
        store.reset("/g1")
        assert store.count("/g1") == 0
        assert store.count("/g2") == 1


class TestHybridScoring:
    def test_keyword_overlap_breaks_semantic_ties(self, tmp_path):
        store = _store(tmp_path)
        _save(store, "cat report for zurich office")
        _save(store, "cat report for geneva office")

        results = store.search(
            query_embedding=[1.0, 0.0, 0.0],
            scope_prefix="/g1",
            query_text="geneva office report",
        )

        assert results[0][0].content == "cat report for geneva office"
        assert results[0][1] > results[1][1]

    def test_recency_prefers_fresh_over_stale_at_equal_semantics(self, tmp_path):
        store = _store(tmp_path)
        stale = MemoryRecord(
            content="cat facts from last quarter",
            scope="/g1",
            created_at=datetime.now(timezone.utc) - timedelta(days=120),
        )
        store.save([stale])
        _save(store, "cat facts from today")

        results = store.search(query_embedding=[1.0, 0.0, 0.0], scope_prefix="/g1")

        assert results[0][0].content == "cat facts from today"
