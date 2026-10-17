"""Every Memory Tuning knob reaches the layer that uses it — nothing is
silently dropped between the panel and the code."""

import inspect
from unittest.mock import patch

import pytest

from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    MemoryTuningConfig,
)
from src.services.memory.engine import Memory, MemoryRecord
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


def _embedder(texts):
    """Deterministic 3-d embedding: one axis per topic word."""
    if isinstance(texts, str):
        texts = [texts]
    return [
        [float("deploy" in t), float("lunch" in t), 1.0 if not t else 0.1]
        for t in (x.lower() for x in texts)
    ]


class TestLocalBackendTakesEveryWeight:
    """Moving the Keyword weight slider used to raise TypeError inside
    ``_create_local_storage``; the broad except turned that into "no storage",
    and memory went off for every run in the teamspace."""

    def _storage(self, tmp_path, tuning):
        with patch(
            "src.utils.storage_paths.db_storage_path", return_value=str(tmp_path)
        ):
            return CrewMemoryService({"group_id": "grp"})._create_local_storage(
                {"embedder": None}, _embedder, _config(tuning)
            )

    def test_keyword_weight_keeps_memory_on_and_recalls(self, tmp_path):
        storage = self._storage(tmp_path, MemoryTuningConfig(keyword_weight=0.2))

        assert storage is not None, "memory turned off by the keyword weight"
        assert storage.KEYWORD_WEIGHT == 0.2
        storage.save([MemoryRecord(content="We deploy on Fridays", scope="/grp")])
        hits = storage.search(
            _embedder("when do we deploy")[0],
            scope_prefix="/grp",
            query_text="when do we deploy",
        )
        assert [r.content for r, _ in hits] == ["We deploy on Fridays"]

    def test_the_keyword_weight_changes_the_score(self, tmp_path):
        """Reaching the backend is not enough — it must be the weight used."""
        scores = {}
        for weight in (0.0, 0.5):
            storage = self._storage(
                tmp_path / str(weight), MemoryTuningConfig(keyword_weight=weight)
            )
            storage.save([MemoryRecord(content="We deploy on Fridays", scope="/grp")])
            [(_, scores[weight])] = storage.search(
                _embedder("deploy fridays")[0],
                scope_prefix="/grp",
                query_text="deploy fridays",
            )
        assert scores[0.5] == pytest.approx(scores[0.0] + 0.5)

    def test_every_scoring_knob_is_a_parameter_of_both_backends(self):
        """A knob added to ``_scoring_kwargs`` must exist on BOTH stores, or
        the one that lacks it raises and memory goes off there."""
        from src.services.memory.storage.lakebase import LakebaseStorageBackend
        from src.services.memory.storage.local import LocalStorageBackend

        knobs = set(MemoryBackendFactory._scoring_kwargs(_config()))
        for backend in (LocalStorageBackend, LakebaseStorageBackend):
            params = set(inspect.signature(backend.__init__).parameters)
            assert knobs <= params, (backend.__name__, knobs - params)


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
