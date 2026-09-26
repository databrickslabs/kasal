"""The recall floor — teamspace tuning → deployment env → embedder default.

Why it exists: the 0.75 floor was calibrated with the Databricks embedder. On a
workstation the embedder falls back to Ollama's nomic-embed-text, whose cosine
scale is compressed — a crew's own previous task output scored 0.68 fused
against the task description that produced it, and every Memory Read came back
``[]`` over a store holding exactly the right record. The floor is now chosen
where the embedder is known (CrewMemoryService), so chat, agent builder and
flow builder — on either harness — apply the same one.
"""

from unittest.mock import MagicMock

from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    MemoryTuningConfig,
)
from src.services.memory.engine.memory import Memory, default_recall_min_score
from src.services.memory.run.crew_memory import CrewMemoryService, _embedder_provider

OLLAMA = {
    "provider": "ollama",
    "config": {"model": "nomic-embed-text", "url": "http://localhost:11434"},
}


def _memory_kwargs(tuning=None, custom_embedder=None, crew_embedder=None):
    cfg = MemoryBackendConfig(
        backend_type=MemoryBackendType.DEFAULT, cognitive_config=tuning
    )
    crew_kwargs = {"agents": []}
    if crew_embedder is not None:
        crew_kwargs["embedder"] = crew_embedder
    return CrewMemoryService({"group_id": "grp"})._build_memory_kwargs(
        crew_kwargs, custom_embedder, "grp_crew", cfg, None
    )


class TestDefaultFloor:
    def test_calibrated_default_without_an_embedder_hint(self):
        assert default_recall_min_score() == 0.75
        assert default_recall_min_score("databricks") == 0.75

    def test_ollama_gets_the_compressed_scale_floor(self):
        assert default_recall_min_score("ollama") == 0.62
        assert default_recall_min_score("Ollama") == 0.62

    def test_the_environment_is_not_read(self, monkeypatch):
        """KASAL_MEMORY_RECALL_MIN_SCORE is gone: Memory Tuning sets the floor."""
        monkeypatch.setenv("KASAL_MEMORY_RECALL_MIN_SCORE", "0.5")
        assert default_recall_min_score("ollama") == 0.62
        assert default_recall_min_score() == 0.75


class TestEmbedderProvider:
    def test_custom_callable_is_the_databricks_embedder(self):
        assert _embedder_provider(lambda texts: [[0.0]], OLLAMA) == "databricks"

    def test_embed_documents_object_is_the_databricks_embedder(self):
        obj = MagicMock(spec=["embed_documents"])
        assert _embedder_provider(obj, None) == "databricks"

    def test_provider_dict_names_itself(self):
        assert _embedder_provider(None, OLLAMA) == "ollama"
        assert _embedder_provider(None, {"provider": "openai"}) == "openai"

    def test_nothing_resolved(self):
        assert _embedder_provider(None, None) is None
        assert _embedder_provider(None, {"config": {}}) is None


class TestBuildMemoryKwargsFloor:
    """Every path builds Memory through _build_memory_kwargs, so this IS the
    rule for chat, agent builder and flow builder alike."""

    def test_ollama_fallback_run_gets_0_62(self):
        assert _memory_kwargs(crew_embedder=OLLAMA)["recall_min_score"] == 0.62

    def test_databricks_run_keeps_0_75(self):
        kwargs = _memory_kwargs(custom_embedder=lambda texts: [[0.0]])
        assert kwargs["recall_min_score"] == 0.75

    def test_no_embedder_keeps_0_75(self):
        assert _memory_kwargs()["recall_min_score"] == 0.75

    def test_teamspace_tuning_wins_over_the_embedder_default(self):
        tuning = MemoryTuningConfig(recall_min_score=0.5)
        assert _memory_kwargs(tuning, crew_embedder=OLLAMA)["recall_min_score"] == 0.5

    def test_hygiene_and_retention_tuning_reach_memory(self):
        """The knobs that replaced the KASAL_MEMORY_* env vars are forwarded,
        and each is a declared Memory field (so pydantic cannot drop it)."""
        tuning = MemoryTuningConfig(
            recall_max_drop=0.3,
            write_screening="annotate",
            forgetting_enabled=True,
            superseded_retention_days=30,
            episodic_ttl_days=60,
            importance_floor=0.6,
            supersession_enabled=False,
            llm_consolidation_enabled=False,
        )
        kwargs = _memory_kwargs(tuning)
        memory = Memory(storage=MagicMock(), **kwargs)
        assert memory.recall_max_drop == 0.3
        assert memory.write_screening == "annotate"
        assert memory.forgetting_enabled is True
        assert memory.superseded_retention_days == 30
        assert memory.episodic_ttl_days == 60
        assert memory.importance_floor == 0.6
        assert memory.supersession_enabled is False
        assert memory.llm_consolidation_enabled is False

    def test_kwargs_construct_a_memory(self):
        kwargs = _memory_kwargs(crew_embedder=OLLAMA)
        storage = MagicMock()
        storage.search.return_value = []
        memory = Memory(storage=storage, **kwargs)
        memory.recall("q")
        assert storage.search.call_args.kwargs["score_threshold"] == 0.62


class TestMemoryRecallUsesTheConfiguredFloor:
    def _memory(self, **kw):
        storage = MagicMock()
        storage.search.return_value = []
        return Memory(storage=storage, **kw), storage

    def test_configured_floor_beats_the_default(self):
        memory, storage = self._memory(recall_min_score=0.62)
        memory.recall("q")
        assert storage.search.call_args.kwargs["score_threshold"] == 0.62

    def test_explicit_threshold_still_wins(self):
        memory, storage = self._memory(recall_min_score=0.62)
        memory.recall("q", score_threshold=0.1)
        assert storage.search.call_args.kwargs["score_threshold"] == 0.1
        memory.recall("q", score_threshold=0.0)
        assert storage.search.call_args.kwargs["score_threshold"] == 0.0

    def test_unset_falls_back_to_the_calibrated_default(self):
        memory, storage = self._memory()
        memory.recall("q")
        assert storage.search.call_args.kwargs["score_threshold"] == 0.75
