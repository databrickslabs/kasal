"""Tests for memory_maintenance — LLM-free exact-content dedupe."""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from kasal_engine.memory import Memory
from src.services.memory.engine_storage_adapter import EngineStorageAdapter
from src.services.memory.local_storage_backend import LocalMemoryStorage
from src.engines.kasal.memory.memory_hooks import flush_memory_writes, remember_async
from src.services.memory.maintenance import consolidate_memory


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


class TestConsolidateEndToEnd:
    def test_dedupes_repeated_records_keeps_one(self, tmp_path):
        backend = LocalMemoryStorage(tmp_path / "m.db", embedder=_embedder)
        memory = Memory(storage=EngineStorageAdapter(backend), root_scope="/g1")
        for _ in range(3):
            memory.remember("User: what was Q2 revenue? Assistant: $4.2M")
            time.sleep(0.01)  # distinct created_at ordering
        memory.remember("a different memory entirely")

        stats = consolidate_memory(memory)

        assert stats["scanned"] == 4
        assert stats["deleted"] == 2
        assert backend.count("/g1") == 2

    def test_whitespace_and_case_variants_are_duplicates(self, tmp_path):
        backend = LocalMemoryStorage(tmp_path / "m.db", embedder=_embedder)
        memory = Memory(storage=EngineStorageAdapter(backend), root_scope="/g1")
        memory.remember("Swiss  market GREW 4%")
        memory.remember("swiss market grew 4%")

        assert consolidate_memory(memory)["deleted"] == 1

    def test_runs_after_async_writes_flush(self, tmp_path):
        backend = LocalMemoryStorage(tmp_path / "m.db", embedder=_embedder)
        memory = Memory(storage=EngineStorageAdapter(backend), root_scope="/g1")
        remember_async(memory, "same content", source="crew_task")
        remember_async(memory, "same content", source="crew_task")
        assert flush_memory_writes(timeout=10.0) == 0

        stats = consolidate_memory(memory)

        assert stats["deleted"] == 1
        assert backend.count("/g1") == 1


class TestConsolidateGuards:
    def test_sentinel_memory_is_noop(self):
        assert consolidate_memory(None) == {"scanned": 0, "deleted": 0}
        assert consolidate_memory(False) == {"scanned": 0, "deleted": 0}
        assert consolidate_memory(True) == {"scanned": 0, "deleted": 0}

    def test_listing_failure_returns_zero_stats(self):
        memory = MagicMock()
        memory.list_records.side_effect = RuntimeError("backend down")
        assert consolidate_memory(memory) == {"scanned": 0, "deleted": 0}

    def test_delete_failures_are_swallowed(self):
        record = SimpleNamespace(id="r1", content="dup")
        memory = MagicMock()
        memory.list_records.return_value = [
            record,
            SimpleNamespace(id="r2", content="dup"),
        ]
        memory.storage.delete.side_effect = RuntimeError("locked")

        stats = consolidate_memory(memory)

        assert stats == {"scanned": 2, "deleted": 0}


class TestCognitiveWeightPlumbing:
    def test_factory_maps_cognitive_config_to_scoring_kwargs(self):
        from src.services.memory.backend_factory import (
            MemoryBackendFactory,
        )
        from src.schemas.memory_backend import MemoryBackendConfig

        config = MemoryBackendConfig(
            backend_type="default",
            cognitive_config={
                "semantic_weight": 0.8,
                "recency_weight": 0.1,
                "importance_weight": 0.1,
                "recency_half_life_days": 7,
            },
        )

        kwargs = MemoryBackendFactory._cognitive_scoring_kwargs(config)

        assert kwargs == {
            "semantic_weight": 0.8,
            "recency_weight": 0.1,
            "importance_weight": 0.1,
            "recency_half_life_days": 7.0,
        }

    def test_factory_omits_unset_fields(self):
        from src.services.memory.backend_factory import (
            MemoryBackendFactory,
        )
        from src.schemas.memory_backend import MemoryBackendConfig

        config = MemoryBackendConfig(backend_type="default")
        assert MemoryBackendFactory._cognitive_scoring_kwargs(config) == {}

    def test_local_backend_ctor_overrides(self, tmp_path):
        store = LocalMemoryStorage(
            tmp_path / "m.db",
            embedder=_embedder,
            semantic_weight=0.9,
            recency_half_life_days=7,
        )
        assert store.SEMANTIC_WEIGHT == 0.9
        assert store.RECENCY_HALF_LIFE_DAYS == 7.0
        # Untouched knobs keep the class defaults.
        assert store.KEYWORD_WEIGHT == LocalMemoryStorage.KEYWORD_WEIGHT


class _FakeLLM:
    def __init__(self, reply):
        self.reply = reply
        self.prompts = []

    def call(self, prompt):
        self.prompts.append(prompt)
        return self.reply


def _memory_with_records(tmp_path, n, llm=None):
    backend = LocalMemoryStorage(tmp_path / "m.db", embedder=_embedder)
    memory = Memory(storage=EngineStorageAdapter(backend), root_scope="/g1", llm=llm)
    for i in range(n):
        memory.remember(f"unique fact number {i}")
    return memory, backend


class TestMergeSimilarMemories:
    def test_merges_clusters_and_replaces_records(self, tmp_path):
        from src.services.memory.maintenance import (
            merge_similar_memories,
        )

        llm = _FakeLLM('[{"merge": [0, 1], "text": "merged fact"}]')
        memory, backend = _memory_with_records(tmp_path, 30, llm=llm)
        before = backend.count("/g1")

        stats = merge_similar_memories(memory)

        assert stats["merged_clusters"] == 1
        assert stats["records_replaced"] == 2
        # Two deleted, one merged record added.
        assert backend.count("/g1") == before - 1
        assert llm.prompts, "LLM was never consulted"
        merged = [
            r for r in memory.list_records(limit=100) if r.source == "consolidation"
        ]
        assert merged and merged[0].content == "merged fact"

    def test_skips_below_min_records(self, tmp_path):
        from src.services.memory.maintenance import (
            merge_similar_memories,
        )

        llm = _FakeLLM("[]")
        memory, _ = _memory_with_records(tmp_path, 5, llm=llm)
        stats = merge_similar_memories(memory)
        assert stats["merged_clusters"] == 0
        assert not llm.prompts, "LLM should not run under the record threshold"

    def test_skips_without_llm(self, tmp_path):
        from src.services.memory.maintenance import (
            merge_similar_memories,
        )

        memory, _ = _memory_with_records(tmp_path, 30, llm=None)
        assert merge_similar_memories(memory)["merged_clusters"] == 0

    def test_env_kill_switch(self, tmp_path, monkeypatch):
        from src.services.memory.maintenance import (
            merge_similar_memories,
        )

        monkeypatch.setenv("KASAL_MEMORY_LLM_CONSOLIDATION", "false")
        llm = _FakeLLM("[]")
        memory, _ = _memory_with_records(tmp_path, 30, llm=llm)
        merge_similar_memories(memory)
        assert not llm.prompts

    def test_malformed_llm_reply_is_noop(self, tmp_path):
        from src.services.memory.maintenance import (
            merge_similar_memories,
        )

        llm = _FakeLLM("I could not find anything to merge, sorry!")
        memory, backend = _memory_with_records(tmp_path, 30, llm=llm)
        before = backend.count("/g1")
        stats = merge_similar_memories(memory)
        assert stats["merged_clusters"] == 0
        assert backend.count("/g1") == before

    def test_invalid_indices_are_ignored(self, tmp_path):
        from src.services.memory.maintenance import (
            merge_similar_memories,
        )

        llm = _FakeLLM(
            '[{"merge": [0, 999], "text": "bogus"}, {"merge": [1], "text": "single"}]'
        )
        memory, backend = _memory_with_records(tmp_path, 30, llm=llm)
        before = backend.count("/g1")
        stats = merge_similar_memories(memory)
        assert stats["merged_clusters"] == 0
        assert backend.count("/g1") == before


class TestRunMemoryMaintenance:
    def test_combines_dedupe_and_merge_stats(self, tmp_path):
        from src.services.memory.maintenance import (
            run_memory_maintenance,
        )

        memory, backend = _memory_with_records(tmp_path, 3)
        memory.remember("unique fact number 0")  # exact duplicate

        stats = run_memory_maintenance(memory)

        assert stats["deleted"] == 1
        assert stats["merged_clusters"] == 0  # below LLM threshold / no llm
        assert "records_replaced" in stats
