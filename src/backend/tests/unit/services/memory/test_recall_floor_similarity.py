"""The recall floor gates on SIMILARITY; recency and importance only rank.

The regression (2026-10-28): teamspace memory worked "only within a session".
It never worked at all — the in-session effect was the chat-history preamble.
The floor was the blended score (cosine + keyword + recency + importance) at
0.75, so a memory a few days old decayed below it and an earlier session's
turns were unreachable: over a real store, 0 of 22 related chat questions
recalled anything. The floor is now a cosine floor the backend applies before
ranking, on both backends, so age can reorder memories but never hide one.
"""

import math
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from src.services.memory.engine import MemoryRecord
from src.services.memory.engine.memory import Memory
from src.services.memory.run.pending import clear_pending_memory
from src.services.memory.run.recall import build_memory_preamble
from src.services.memory.storage.adapter import EngineStorageAdapter
from src.services.memory.storage.local import LocalStorageBackend
from tests.unit.services.memory.test_lakebase_kinds_sql import (
    _backend as _lakebase_backend,
)
from tests.unit.services.memory.test_lakebase_kinds_sql import (
    _capture,
)

QUERY = [1.0, 0.0, 0.0]


def _at_cosine(cos: float) -> list[float]:
    """A unit vector whose cosine with QUERY is exactly ``cos``."""
    return [cos, math.sqrt(1.0 - cos * cos), 0.0]


def _record(content, cos, *, days_old=0.0, importance=0.5, session="session-a"):
    return MemoryRecord(
        content=content,
        scope="/grp",
        importance=importance,
        metadata={"session_id": session},
        created_at=datetime.now(timezone.utc) - timedelta(days=days_old),
        embedding=_at_cosine(cos),
    )


@pytest.fixture
def store(tmp_path):
    return LocalStorageBackend(tmp_path / "memory.db")


class TestLocalBackendGatesOnSimilarity:
    def test_an_old_relevant_memory_is_recalled(self, store):
        record = _record("User: the George app\nAssistant: ...", 0.8, days_old=60)
        store.save([record])

        hits = store.search(query_embedding=QUERY, min_semantic=0.6, query_text="q")

        assert [r.id for r, _ in hits] == [record.id]
        # The blend it ranks by is far below the old 0.75 floor — which is
        # exactly why the old floor returned nothing for an earlier session.
        assert hits[0][1] < 0.75
        assert store.search(query_embedding=QUERY, min_score=0.75) == []

    def test_recency_importance_and_keywords_cannot_lift_an_unrelated_memory(
        self, store
    ):
        # Fresh, maximally important, every query word present: blend ~0.70.
        store.save([_record("swiss news today", 0.5, importance=1.0)])

        assert (
            store.search(
                query_embedding=QUERY, min_semantic=0.6, query_text="swiss news today"
            )
            == []
        )

    def test_a_low_floor_never_lowers_the_relevance_gate(self, store):
        store.save([_record("unrelated", 0.3)])

        assert store.search(query_embedding=QUERY, min_semantic=0.1) == []

    def test_a_record_without_a_vector_cannot_clear_a_floor(self, store):
        """A turn saved while the embedder was down has no similarity. Under a
        floor it is excluded, as Lakebase excludes it (NULL never clears the
        gate); found live: failed 'Execution failed' turns recalled for an
        off-topic question. Without a floor the keyword fallback still finds it."""
        unscored = _record("User: hi there\nAssistant: Execution failed", 0.0)
        unscored.embedding = None
        store.save([unscored])

        assert store.search(query_embedding=QUERY, min_semantic=0.6) == []
        assert [r.id for r, _ in store.search(query_embedding=QUERY)] == [unscored.id]


class TestLakebaseGatesOnSimilarity:
    @pytest.mark.asyncio
    async def test_the_floor_raises_the_cosine_gate(self):
        sql, params = await _capture(
            lambda s: _lakebase_backend().asearch(
                query_embedding=[0.1, 0.2, 0.3, 0.4], min_semantic=0.6
            )
        )
        assert "c.semantic >= :relevance_threshold" in sql
        assert params["relevance_threshold"] == 0.6

    @pytest.mark.asyncio
    async def test_a_low_floor_keeps_the_relevance_threshold(self):
        _, params = await _capture(
            lambda s: _lakebase_backend().asearch(
                query_embedding=[0.1, 0.2, 0.3, 0.4], min_semantic=0.1
            )
        )
        assert params["relevance_threshold"] == 0.35


class TestAdapterHandsTheFloorToTheSemanticGate:
    def _adapter(self, search):
        backend = MagicMock()
        backend.search = search
        backend.count.return_value = 1
        return EngineStorageAdapter(backend, embedder=lambda texts: [QUERY])

    def test_a_hybrid_backend_receives_min_semantic(self):
        calls = []

        def search(
            query_embedding,
            scope_prefix=None,
            limit=10,
            min_score=0.0,
            query_text=None,
            min_semantic=0.0,
        ):
            calls.append({"min_score": min_score, "min_semantic": min_semantic})
            return []

        self._adapter(search).search("q", score_threshold=0.6)

        assert calls == [{"min_score": 0.0, "min_semantic": 0.6}]

    def test_a_backend_without_the_gate_gets_it_as_min_score(self):
        calls = []

        def search(query_embedding, scope_prefix=None, limit=10, min_score=0.0):
            calls.append(min_score)
            return []

        self._adapter(search).search("q", score_threshold=0.6)

        assert calls == [0.6]


class TestTeamspaceMemoryReachesAnotherSession:
    """The reported scenario, end to end through the real local store."""

    def test_a_turn_from_an_earlier_session_is_recalled(self, tmp_path):
        clear_pending_memory()
        query = "what was in the Swiss news?"
        vectors = {query: QUERY}
        store = LocalStorageBackend(
            tmp_path / "memory.db", embedder=lambda texts: [vectors[t] for t in texts]
        )
        earlier = _record(
            "User: gather latest swiss news\nAssistant: Here are today's Swiss "
            "headlines: the Federal Council ...",
            0.75,
            days_old=45,
            importance=0.45,
        )
        store.save([earlier, _record("User: bake bread\nAssistant: ...", 0.3)])
        # A new session: nothing about session-a reaches this Memory.
        memory = Memory(storage=EngineStorageAdapter(store), root_scope="/grp")

        block = build_memory_preamble(memory, query)

        assert "Swiss headlines" in block
        assert "bake bread" not in block
