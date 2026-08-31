"""MemAlign judge alignment from the Optimize dialog's human grades.

The dialog already captures MemAlign's input — a grade + comment per evaluated
answer, attributed to a judge — and GEPA scores candidates with the registered
judges. ``align_judge`` closes the loop: filter the crew's eval traces to those
carrying a HUMAN assessment named for the judge, run MLflow's MemAlign, and
register the aligned judge as the next version under the same name.
"""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.prompt_optimization.alignment import (
    has_human_feedback,
    memalign_models,
)
from src.services.prompt_optimization.service import PromptOptimizationService

CREW = "88ab4478-823c-4c4f-9c2e-000000000000"
PREFIX = "crew_88ab4478823c__"


def _assessment(name, source_type):
    return SimpleNamespace(name=name, source=SimpleNamespace(source_type=source_type))


def _trace(trace_id, assessments):
    return SimpleNamespace(
        info=SimpleNamespace(trace_id=trace_id, assessments=assessments),
        search_assessments=lambda: assessments,
    )


class TestMemalignModels:
    def test_databricks_wraps_bare_keys_and_defaults_the_embedder(self, monkeypatch):
        monkeypatch.delenv("KASAL_MEMALIGN_REFLECTION_MODEL", raising=False)
        monkeypatch.delenv("KASAL_MEMALIGN_EMBEDDING_MODEL", raising=False)
        monkeypatch.delenv("KASAL_MEMALIGN_EMBEDDING_DIM", raising=False)
        monkeypatch.setenv("GEPA_JUDGE_MODEL", "databricks-claude-haiku-4-5")
        m = memalign_models(SimpleNamespace(kind="databricks"))
        assert m == {
            "reflection_lm": "databricks:/databricks-claude-haiku-4-5",
            "embedding_model": "databricks:/databricks-gte-large-en",
            "embedding_dim": 1024,
        }

    def test_local_needs_litellm_uris_from_the_environment(self, monkeypatch):
        monkeypatch.delenv("KASAL_MEMALIGN_REFLECTION_MODEL", raising=False)
        with pytest.raises(ValueError, match="KASAL_MEMALIGN_REFLECTION_MODEL"):
            memalign_models(SimpleNamespace(kind="local"))
        monkeypatch.setenv("KASAL_MEMALIGN_REFLECTION_MODEL", "ollama:/qwen3:8b")
        monkeypatch.setenv("KASAL_MEMALIGN_EMBEDDING_MODEL", "ollama:/nomic-embed-text")
        monkeypatch.delenv("KASAL_MEMALIGN_EMBEDDING_DIM", raising=False)
        m = memalign_models(SimpleNamespace(kind="local"))
        assert m["reflection_lm"] == "ollama:/qwen3:8b"
        assert m["embedding_model"] == "ollama:/nomic-embed-text"
        assert m["embedding_dim"] == 768  # small open embedder default

    def test_explicit_arguments_win_and_uris_pass_through(self, monkeypatch):
        monkeypatch.setenv("KASAL_MEMALIGN_REFLECTION_MODEL", "ollama:/qwen3:8b")
        m = memalign_models(
            SimpleNamespace(kind="local"),
            reflection_model="openai:/gpt-4.1-mini",
            embedding_model="openai:/text-embedding-3-small",
            embedding_dim=512,
        )
        assert m["reflection_lm"] == "openai:/gpt-4.1-mini"
        assert m["embedding_dim"] == 512


class TestHasHumanFeedback:
    def test_matches_name_and_human_source_only(self):
        judge = f"{PREFIX}accuracy"
        assert has_human_feedback(_trace("t1", [_assessment(judge, "HUMAN")]), judge)
        # Same name, but produced by code/LLM — MemAlign would reject it.
        assert not has_human_feedback(_trace("t2", [_assessment(judge, "CODE")]), judge)
        # The generic grade is not this judge's.
        assert not has_human_feedback(
            _trace("t3", [_assessment("human_grade", "HUMAN")]), judge
        )
        # Enum-shaped source types work too.
        enum_like = SimpleNamespace(value="HUMAN")
        assert has_human_feedback(_trace("t4", [_assessment(judge, enum_like)]), judge)


def _service():
    svc = PromptOptimizationService.__new__(PromptOptimizationService)
    svc.session = MagicMock()
    return svc


@contextmanager
def _no_session(backend):
    yield


class TestAlignJudge:
    @pytest.mark.asyncio
    async def test_aligns_from_graded_traces_and_registers_the_next_version(
        self, monkeypatch
    ):
        monkeypatch.setenv("KASAL_MEMALIGN_REFLECTION_MODEL", "ollama:/qwen3:8b")
        monkeypatch.setenv("KASAL_MEMALIGN_EMBEDDING_MODEL", "ollama:/nomic-embed-text")
        judge_name = f"{PREFIX}accuracy"
        traces = [
            _trace("graded", [_assessment(judge_name, "HUMAN")]),
            _trace("ungraded", []),
            _trace("other-judge", [_assessment(f"{PREFIX}tone", "HUMAN")]),
        ]
        base_judge = MagicMock(name="judge")
        aligned = MagicMock(name="aligned")
        aligned.model_dump.return_value = {
            "semantic_memory": [
                {"guideline_text": "Prefer certified views over raw tables."},
                {"guideline_text": ""},
            ]
        }
        optimizer = MagicMock()
        optimizer.align.return_value = aligned
        optimizer_cls = MagicMock(return_value=optimizer)

        svc = _service()
        with (
            patch(
                "src.services.prompt_optimization.alignment.resolve_mlflow_backend",
                new=AsyncMock(
                    return_value=SimpleNamespace(kind="local", experiment="e")
                ),
            ),
            patch(
                "src.services.prompt_optimization.alignment.mlflow_session", _no_session
            ),
            patch("mlflow.search_traces", return_value=traces),
            patch(
                "mlflow.genai.scorers.get_scorer", return_value=base_judge
            ) as get_scorer,
            patch("mlflow.genai.judges.optimizers.MemAlignOptimizer", optimizer_cls),
        ):
            result = await svc.align_judge("accuracy", CREW)

        # Bare name → the crew-scoped registry name.
        get_scorer.assert_called_once_with(name=judge_name)
        # Only the trace with a HUMAN assessment for THIS judge is aligned on.
        aligned_traces = optimizer.align.call_args.args[1]
        assert [t.info.trace_id for t in aligned_traces] == ["graded"]
        assert optimizer.align.call_args.args[0] is base_judge
        # MemAlign is configured from the resolved URIs.
        kwargs = optimizer_cls.call_args.kwargs
        assert kwargs["reflection_lm"] == "ollama:/qwen3:8b"
        assert kwargs["embedding_model"] == "ollama:/nomic-embed-text"
        assert kwargs["retrieval_k"] == 5
        # Registered under the same name: the next GEPA run scores with it.
        aligned.register.assert_called_once_with()
        assert result["full_name"] == judge_name
        assert result["name"] == "accuracy"
        assert result["traces_used"] == 1
        assert result["guidelines"] == ["Prefer certified views over raw tables."]

    @pytest.mark.asyncio
    async def test_no_graded_answers_is_a_clear_error_not_an_mlflow_one(
        self, monkeypatch
    ):
        monkeypatch.setenv("KASAL_MEMALIGN_REFLECTION_MODEL", "ollama:/qwen3:8b")
        monkeypatch.setenv("KASAL_MEMALIGN_EMBEDDING_MODEL", "ollama:/nomic-embed-text")
        svc = _service()
        optimizer_cls = MagicMock()
        with (
            patch(
                "src.services.prompt_optimization.alignment.resolve_mlflow_backend",
                new=AsyncMock(
                    return_value=SimpleNamespace(kind="local", experiment="e")
                ),
            ),
            patch(
                "src.services.prompt_optimization.alignment.mlflow_session", _no_session
            ),
            patch("mlflow.search_traces", return_value=[_trace("t", [])]),
            patch("mlflow.genai.scorers.get_scorer", return_value=MagicMock()),
            patch("mlflow.genai.judges.optimizers.MemAlignOptimizer", optimizer_cls),
        ):
            with pytest.raises(ValueError, match="Grade a few answers"):
                await svc.align_judge(f"{PREFIX}accuracy", CREW)
        optimizer_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_requires_an_mlflow_backend(self):
        svc = _service()
        with patch(
            "src.services.prompt_optimization.alignment.resolve_mlflow_backend",
            new=AsyncMock(return_value=None),
        ):
            with pytest.raises(ValueError, match="requires MLflow"):
                await svc.align_judge("accuracy", CREW)


class TestGradesAreAttributedToTheJudge:
    @pytest.mark.asyncio
    async def test_a_judge_attributed_grade_is_logged_under_the_judges_name_as_human(
        self,
    ):
        svc = _service()
        logged = []
        with (
            patch(
                "src.services.prompt_optimization.judges.resolve_mlflow_backend",
                new=AsyncMock(
                    return_value=SimpleNamespace(kind="local", experiment="e")
                ),
            ),
            patch(
                "src.services.prompt_optimization.judges.mlflow_session", _no_session
            ),
            patch("mlflow.log_feedback", side_effect=lambda **kw: logged.append(kw)),
            patch("mlflow.log_expectation"),
        ):
            ok = await svc.add_eval_feedback(
                "t1",
                4.0,
                "too cold — open with reassurance",
                None,
                SimpleNamespace(group_email="sme@x"),
                judge=f"{PREFIX}tone",
            )
        assert ok
        names = [kw["name"] for kw in logged]
        assert names == ["human_grade", f"{PREFIX}tone"]
        for kw in logged:
            assert kw["value"] == 4.0
            assert kw["rationale"] == "too cold — open with reassurance"
            assert str(kw["source"].source_type).upper().endswith("HUMAN")
            assert kw["source"].source_id == "sme@x"

    @pytest.mark.asyncio
    async def test_without_a_judge_only_the_generic_grade_is_logged(self):
        svc = _service()
        logged = []
        with (
            patch(
                "src.services.prompt_optimization.judges.resolve_mlflow_backend",
                new=AsyncMock(
                    return_value=SimpleNamespace(kind="local", experiment="e")
                ),
            ),
            patch(
                "src.services.prompt_optimization.judges.mlflow_session", _no_session
            ),
            patch("mlflow.log_feedback", side_effect=lambda **kw: logged.append(kw)),
            patch("mlflow.log_expectation"),
        ):
            await svc.add_eval_feedback("t1", 7.0, None, None, None)
        assert [kw["name"] for kw in logged] == ["human_grade"]
