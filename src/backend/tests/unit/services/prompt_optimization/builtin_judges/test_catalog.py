"""The built-in judge catalog: what a run may select, and what exists.

Availability is read from the INSTALLED mlflow, so a version that drops a
class disables its entry instead of crashing; the evaluation runner's old
``Groundedness``/``Relevance``/``ContextSufficiency`` names are the failure
this prevents (never present in mlflow 3.x, silently skipped on every run).
"""

import asyncio
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from src.services.prompt_optimization.builtin_judges import catalog


@pytest.fixture
def scorers_without_completeness():
    """An mlflow.genai.scorers that lacks Completeness (a version drift)."""
    import mlflow.genai.scorers as real

    fake = types.ModuleType("mlflow.genai.scorers")
    for name in ("Safety", "RelevanceToQuery", "Guidelines", "Correctness"):
        setattr(fake, name, getattr(real, name))
    genai = types.ModuleType("mlflow.genai")
    genai.scorers = fake
    with patch.dict(sys.modules, {"mlflow.genai": genai, "mlflow.genai.scorers": fake}):
        yield fake


class TestCatalog:
    def test_phase_one_is_the_four_no_label_judges_and_a_labelled_seam(self):
        selectable = [j.id for j in catalog.CATALOG if not j.needs_labels]
        assert selectable == [
            "Safety",
            "RelevanceToQuery",
            "Guidelines",
            "Completeness",
        ]
        assert [j.id for j in catalog.CATALOG if j.needs_labels] == ["Correctness"]

    def test_safety_is_the_only_gate(self):
        assert [j.id for j in catalog.CATALOG if j.role == "gate"] == ["Safety"]

    def test_every_entry_exists_in_the_installed_mlflow(self):
        assert all(catalog.is_available(j.id) for j in catalog.CATALOG)

    def test_a_class_missing_from_mlflow_is_unavailable(
        self, scorers_without_completeness
    ):
        assert catalog.is_available("Safety") is True
        assert catalog.is_available("Completeness") is False

    def test_listing_reports_availability_per_entry(self, scorers_without_completeness):
        entries = asyncio.run(catalog.list_with_availability())
        available = {judge.id: ok for judge, ok in entries}
        assert available["Completeness"] is False
        assert available["Safety"] is True
        assert len(entries) == len(catalog.CATALOG)


class TestResolveSelection:
    def test_returns_catalog_order_without_duplicates(self):
        chosen = catalog.resolve_selection(["Completeness", "Safety", "Safety"])
        assert [j.id for j in chosen] == ["Safety", "Completeness"]

    def test_empty_selection_is_empty(self):
        assert catalog.resolve_selection([]) == []

    @pytest.mark.parametrize(
        "judge_id,message",
        [
            ("Nope", "Unknown built-in judge"),
            ("Correctness", "needs labelled examples"),
        ],
    )
    def test_rejects_what_a_run_cannot_use(self, judge_id, message):
        with pytest.raises(ValueError, match=message):
            catalog.resolve_selection([judge_id])

    def test_rejects_a_judge_the_installed_mlflow_lacks(
        self, scorers_without_completeness
    ):
        with pytest.raises(ValueError, match="not provided by the installed MLflow"):
            catalog.resolve_selection(["Completeness"])


class TestBuildScorer:
    def test_builds_the_real_class_with_the_model_uri(self):
        from mlflow.genai.scorers import Safety

        scorer = catalog.build_scorer("Safety", "openai:/kasal-judge--k")
        assert isinstance(scorer, Safety)
        assert scorer.model == "openai:/kasal-judge--k"

    def test_guidelines_get_the_run_guidelines_or_a_default(self):
        with_text = catalog.build_scorer("Guidelines", None, ["Cite sources"])
        assert with_text.guidelines == ["Cite sources"]
        default = catalog.build_scorer("Guidelines", None)
        assert default.guidelines == [catalog.DEFAULT_GUIDELINE]

    def test_no_model_keeps_mlflows_default_and_passes_no_kwarg(self):
        fake = MagicMock()
        genai = types.ModuleType("mlflow.genai")
        genai.scorers = fake
        with patch.dict(
            sys.modules, {"mlflow.genai": genai, "mlflow.genai.scorers": fake}
        ):
            catalog.build_scorer("RelevanceToQuery", None)
        fake.RelevanceToQuery.assert_called_once_with()

    def test_unknown_id_raises(self):
        with pytest.raises(ValueError, match="not available"):
            catalog.build_scorer("Groundedness", None)


class TestEvaluationScorers:
    """Phase 1c: the evaluation runner builds its scorers from this catalog."""

    def test_core_judges_always(self):
        scorers = catalog.evaluation_scorers(
            None, has_reference=False, has_context=False
        )
        assert [type(s).__name__ for s in scorers] == ["RelevanceToQuery", "Safety"]

    def test_correctness_when_rows_carry_a_reference(self):
        scorers = catalog.evaluation_scorers(
            "databricks:/judge", has_reference=True, has_context=False
        )
        assert [type(s).__name__ for s in scorers] == [
            "RelevanceToQuery",
            "Safety",
            "Correctness",
        ]
        assert all(s.model == "databricks:/judge" for s in scorers)

    def test_context_rows_skip_retrieval_judges_with_a_log(self, caplog):
        caplog.set_level("INFO", logger=catalog.logger.name)
        scorers = catalog.evaluation_scorers(None, has_reference=True, has_context=True)
        names = [type(s).__name__ for s in scorers]
        assert not {"Groundedness", "Relevance", "ContextSufficiency"} & set(names)
        assert not set(catalog.RETRIEVAL_JUDGES) & set(names)
        assert "Retrieval judges" in caplog.text
        assert "RetrievalGroundedness" in caplog.text

    def test_the_retrieval_names_are_the_real_mlflow_classes(self):
        import mlflow.genai.scorers as scorers

        for name in catalog.RETRIEVAL_JUDGES:
            assert callable(getattr(scorers, name, None)), name
        for stale in ("Groundedness", "Relevance", "ContextSufficiency"):
            assert not hasattr(scorers, stale), stale

    def test_a_missing_class_is_skipped_not_fatal(self, scorers_without_completeness):
        del scorers_without_completeness.Safety
        scorers = catalog.evaluation_scorers(
            None, has_reference=False, has_context=False
        )
        assert [type(s).__name__ for s in scorers] == ["RelevanceToQuery"]
