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
    crew_embedder_config,
    has_human_feedback,
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
    @staticmethod
    def _bridge(armed):
        @contextmanager
        def fake(loop, model, embedder_config, group_context=None, user_token=None):
            armed.update(model=model, embedder_config=embedder_config, loop=loop)
            yield {
                "reflection_lm": "openai:/kasal-llm-manager",
                "embedding_model": "openai:/kasal-embedder",
                "embedding_dim": 4,
            }

        return fake

    @pytest.mark.asyncio
    async def test_aligns_with_the_judges_own_model_through_llm_manager(self):
        judge_name = f"{PREFIX}accuracy"
        traces = [
            _trace("graded", [_assessment(judge_name, "HUMAN")]),
            _trace("ungraded", []),
            _trace("other-judge", [_assessment(f"{PREFIX}tone", "HUMAN")]),
        ]
        base_judge = MagicMock(name="judge")
        base_judge.model = "openai:/qwen-30b"  # stored as a URI, invoked as a key
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
        armed = {}

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
            patch(
                "src.services.prompt_optimization.alignment.crew_embedder_config",
                new=AsyncMock(return_value={"provider": "ollama"}),
            ),
            patch(
                "src.services.prompt_optimization.alignment.memalign_via_llm_manager",
                self._bridge(armed),
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
        # The judge's OWN model (chosen in the UI) distils, via LLMManager, and
        # the crew's embedder embeds — nothing read from the environment.
        assert armed["model"] == "qwen-30b"
        assert armed["embedder_config"] == {"provider": "ollama"}
        kwargs = optimizer_cls.call_args.kwargs
        assert kwargs["reflection_lm"] == "openai:/kasal-llm-manager"
        assert kwargs["embedding_dim"] == 4
        assert kwargs["retrieval_k"] == 5
        # Registered under the same name: the next GEPA run scores with it.
        aligned.register.assert_called_once_with()
        assert result["full_name"] == judge_name
        assert result["name"] == "accuracy"
        assert result["traces_used"] == 1
        assert result["model"] == "qwen-30b"
        assert result["guidelines"] == ["Prefer certified views over raw tables."]

    @pytest.mark.asyncio
    async def test_no_graded_answers_is_a_clear_error_not_an_mlflow_one(self):
        svc = _service()
        optimizer_cls = MagicMock()
        judge = MagicMock()
        judge.model = "openai:/qwen-30b"
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
            patch(
                "src.services.prompt_optimization.alignment.crew_embedder_config",
                new=AsyncMock(return_value=None),
            ),
            patch("mlflow.search_traces", return_value=[_trace("t", [])]),
            patch("mlflow.genai.scorers.get_scorer", return_value=judge),
            patch("mlflow.genai.judges.optimizers.MemAlignOptimizer", optimizer_cls),
        ):
            with pytest.raises(ValueError, match="Grade a few answers"):
                await svc.align_judge(f"{PREFIX}accuracy", CREW)
        optimizer_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_judge_without_a_model_cannot_be_aligned(self):
        svc = _service()
        judge = MagicMock()
        judge.model = None
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
            patch(
                "src.services.prompt_optimization.alignment.crew_embedder_config",
                new=AsyncMock(return_value=None),
            ),
            patch("mlflow.search_traces") as search,
            patch("mlflow.genai.scorers.get_scorer", return_value=judge),
        ):
            with pytest.raises(ValueError, match="no model"):
                await svc.align_judge("accuracy", CREW)
        search.assert_not_called()

    @pytest.mark.asyncio
    async def test_requires_an_mlflow_backend(self):
        svc = _service()
        with patch(
            "src.services.prompt_optimization.alignment.resolve_mlflow_backend",
            new=AsyncMock(return_value=None),
        ):
            with pytest.raises(ValueError, match="requires MLflow"):
                await svc.align_judge("accuracy", CREW)


class TestCrewEmbedderConfig:
    @pytest.mark.asyncio
    async def test_most_common_agent_embedder_wins(self):
        crew = SimpleNamespace(agent_ids=["a1", "a2", "a3", "a4"])
        ollama = {"provider": "ollama", "config": {"model": "nomic-embed-text"}}
        agents = {
            "a1": SimpleNamespace(embedder_config=ollama),
            "a2": SimpleNamespace(embedder_config={"provider": "databricks"}),
            "a3": SimpleNamespace(embedder_config=dict(ollama)),
            "a4": SimpleNamespace(embedder_config=None),
        }
        crew_service = MagicMock()
        crew_service.get_by_group = AsyncMock(return_value=crew)
        agent_service = MagicMock()
        agent_service.get_with_group_check = AsyncMock(
            side_effect=lambda i, _g: agents.get(i)
        )
        with (
            patch("src.services.catalog.crews.CrewService", return_value=crew_service),
            patch(
                "src.services.catalog.agents.AgentService", return_value=agent_service
            ),
        ):
            config = await crew_embedder_config(MagicMock(), CREW, MagicMock())
        assert config == ollama

    @pytest.mark.asyncio
    async def test_none_for_a_missing_crew_or_a_malformed_id(self):
        crew_service = MagicMock()
        crew_service.get_by_group = AsyncMock(return_value=None)
        with patch("src.services.catalog.crews.CrewService", return_value=crew_service):
            assert await crew_embedder_config(MagicMock(), CREW, None) is None
            assert await crew_embedder_config(MagicMock(), "not-a-uuid", None) is None
        crew_service.get_by_group.assert_awaited_once()


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
