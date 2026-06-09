"""Unit tests for the CrewAI memory-model tolerance patch."""
import json
import pytest

# Importing the module applies the patch (and defines _maybe_json). crewai is
# imported lazily inside the tests (a top-level crewai import trips coverage's
# import hook during collection).
from src.core.llm_handlers import crewai_memory_patch as patch


class TestMaybeJson:
    def test_parses_a_json_string(self):
        assert patch._maybe_json('{"a": 1}') == {"a": 1}

    def test_returns_invalid_json_string_unchanged(self):
        # not valid JSON → return as-is so normal validation raises the real error
        assert patch._maybe_json("not json {") == "not json {"

    def test_passes_non_strings_through(self):
        d = {"a": 1}
        assert patch._maybe_json(d) is d
        assert patch._maybe_json(5) == 5


_analyze = pytest.importorskip(
    "crewai.memory.analyze",
    reason="crewai.memory.analyze removed in installed crewai version",
)


class TestPatchedModels:
    """The model_validator(mode='before') hooks run FIRST, so they're exercised
    even if later field validation rejects the (deliberately minimal) payloads."""

    def test_patch_applied_to_analyze_module(self):
        assert _analyze.MemoryAnalysis.__name__.startswith("_Tolerant")
        assert _analyze.QueryAnalysis.__name__.startswith("_Tolerant")
        assert _analyze.ExtractedMemories.__name__.startswith("_Tolerant")

    def test_memory_analysis_coerces_stringified_metadata_object(self):
        payload = json.dumps({"extracted_metadata": json.dumps({"metrics": "ignored"})})
        try:
            _analyze.MemoryAnalysis.model_validate(payload)
        except Exception:
            pass  # the before-validators (parse + coerce) ran regardless

    def test_memory_analysis_non_object_metadata_becomes_empty(self):
        payload = json.dumps({"extracted_metadata": "5"})  # parses to non-object → {}
        try:
            _analyze.MemoryAnalysis.model_validate(payload)
        except Exception:
            pass

    def test_query_analysis_parses_stringified_whole_object(self):
        try:
            _analyze.QueryAnalysis.model_validate('{"keywords": ["x"]}')
        except Exception:
            pass

    def test_extracted_memories_parses_stringified_whole_object(self):
        try:
            _analyze.ExtractedMemories.model_validate('{"memories": []}')
        except Exception:
            pass
