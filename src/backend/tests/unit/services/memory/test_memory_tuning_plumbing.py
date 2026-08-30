"""Every Memory Tuning knob reaches the layer that uses it — nothing is
silently dropped between the panel and the code."""

from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    MemoryTuningConfig,
)
from src.services.memory.engine import Memory
from src.services.memory.run.crew_memory import CrewMemoryService
from src.services.memory.storage.factory import MemoryBackendFactory

FULL = MemoryTuningConfig(
    semantic_weight=0.55,
    keyword_weight=0.2,
    recency_weight=0.15,
    importance_weight=0.1,
    recency_half_life_days=14,
    relevance_threshold=0.4,
    recall_min_score=0.66,
    consolidation_threshold=0.9,
    consolidation_limit=3,
    default_importance=0.4,
    confidence_threshold_high=0.85,
    confidence_threshold_low=0.45,
    complex_query_threshold=0.65,
    exploration_budget=2,
    query_analysis_threshold=120,
)


def _config(tuning=FULL) -> MemoryBackendConfig:
    return MemoryBackendConfig(
        backend_type=MemoryBackendType.DEFAULT, cognitive_config=tuning
    )


class TestStorageSide:
    def test_scoring_kwargs_carry_every_weight_including_keyword(self):
        assert MemoryBackendFactory._scoring_kwargs(_config()) == {
            "semantic_weight": 0.55,
            "keyword_weight": 0.2,
            "recency_weight": 0.15,
            "importance_weight": 0.1,
            "recency_half_life_days": 14.0,
            "relevance_threshold": 0.4,
        }


class TestEngineSide:
    def _kwargs(self):
        return CrewMemoryService({"group_id": "grp"})._build_memory_kwargs(
            {"agents": []}, None, "grp_crew", _config(), None
        )

    def test_memory_kwargs_carry_every_engine_knob(self):
        kwargs = self._kwargs()
        for key, value in {
            "consolidation_threshold": 0.9,
            "consolidation_limit": 3,
            "default_importance": 0.4,
            "confidence_threshold_high": 0.85,
            "confidence_threshold_low": 0.45,
            "complex_query_threshold": 0.65,
            "exploration_budget": 2,
            "query_analysis_threshold": 120,
            "recall_min_score": 0.66,
        }.items():
            assert kwargs[key] == value, key

    def test_storage_only_knobs_are_not_handed_to_memory(self):
        """Memory ignores unknown kwargs — handing it the weights is how they
        used to vanish. They travel via _scoring_kwargs instead."""
        kwargs = self._kwargs()
        for key in (
            "semantic_weight",
            "keyword_weight",
            "recency_weight",
            "importance_weight",
            "recency_half_life_days",
            "relevance_threshold",
        ):
            assert key not in kwargs, key

    def test_memory_accepts_the_kwargs_and_keeps_every_value(self):
        kwargs = self._kwargs()
        memory = Memory(**kwargs)
        assert memory.consolidation_threshold == 0.9
        assert memory.consolidation_limit == 3
        assert memory.default_importance == 0.4
        assert memory.confidence_threshold_high == 0.85
        assert memory.confidence_threshold_low == 0.45
        assert memory.complex_query_threshold == 0.65
        assert memory.exploration_budget == 2
        assert memory.query_analysis_threshold == 120
        assert memory.recall_min_score == 0.66
        assert memory.root_scope == "/grp"
