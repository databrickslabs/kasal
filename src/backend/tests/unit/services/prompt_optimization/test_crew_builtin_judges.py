"""Crew optimization with MLflow built-in judges selected.

Drives ``_execute_crew_optimization_sync`` against the fake mlflow/gepa stack
from ``test_prompt_optimization_service`` (the file is over the size ceiling,
so these tests live here). The built-ins are the REAL mlflow scorer classes;
only the LLM is fake. What must hold:

* built-in judge calls reach LLMManager with the RUN's judge model;
* their verdicts join the judge score next to Kasal's own judge, with Safety
  as a gate, and their rationales reach GEPA;
* the selection is validated when the run starts.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.prompt_optimization import CrewOptimizationRequest
from src.services.prompt_optimization import service as svc_module
from src.services.prompt_optimization.builtin_judges import bridge
from src.services.prompt_optimization.gepa import reflection as gepa_reflection
from src.services.prompt_optimization.labels.review import ReviewHarvest
from src.services.prompt_optimization.service import PromptOptimizationService
from tests.unit.services.prompt_optimization.test_prompt_optimization_service import (
    _crew_fixture,
    _fake_completion,
    _fake_stack,
)

RUN_ID = "builtin-run"
JUDGE_CONTRACT = '"result" is your verdict'


@pytest.fixture(autouse=True)
def _real_mlflow_warm():
    """Load the real scorer/judge modules (and the bridge) BEFORE the fake
    mlflow tree shadows ``mlflow``: the scorers import lazily at call time."""
    import mlflow.genai.scorers as real_scorers

    bridge.install_bridge()
    with patch.object(
        gepa_reflection,
        "_sync_llm_completion",
        lambda *a, **k: '{"result": "yes", "rationale": "warm"}',
    ):
        with bridge.judge_route(bridge.JudgeRoute(loop=MagicMock())):
            for name in ("Safety", "RelevanceToQuery", "Completeness"):
                getattr(real_scorers, name)(model=bridge.placeholder_uri("w")).run(
                    inputs={"request": "q"}, outputs="a"
                )
    yield real_scorers
    svc_module._RUNS.pop(RUN_ID, None)


def _drive(real_scorers, builtin_judges, safety="yes"):
    baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
    calls = []
    scored = []

    def handler(call):
        if "reply with OK" in call["text"]:
            return "OK"
        if "GROUND TRUTH" in call["text"]:
            return "REFERENCE: a table"
        if JUDGE_CONTRACT in call["text"]:
            verdict = safety if "content safety" in call["text"] else "yes"
            return json.dumps({"rationale": f"{verdict} because", "result": verdict})
        return "7"  # Kasal's own judge: 7/10

    def optimize_prompts(
        *, predict_fn, train_data, prompt_uris, optimizer, scorers, aggregation, **_
    ):
        fmt, correct = scorers
        output = predict_fn(**train_data[0]["inputs"])
        feedback = correct(inputs=train_data[0]["inputs"], outputs=output)
        scores = {"output_format": fmt(outputs=output), "output_correct": feedback}
        scored.append((feedback, aggregation(scores)))
        return SimpleNamespace(
            optimized_prompts=[SimpleNamespace(template=baseline_doc, uri="p:/c/2")],
            initial_eval_score=0.4,
            final_eval_score=0.7,
        )

    def run_crew(loop, **kwargs):
        return "A long enough deliverable to clear the fifty character format floor."

    svc_module._RUNS[RUN_ID] = {"run_id": RUN_ID, "executions_used": 0}
    with _fake_stack(optimize_prompts, _fake_completion(handler, calls)) as stack:
        # The real classes, in the fake tree the crew body imports from.
        for name in ("Safety", "RelevanceToQuery", "Guidelines", "Completeness"):
            setattr(
                stack.modules["mlflow.genai.scorers"], name, getattr(real_scorers, name)
            )
        with patch.object(gepa_reflection, "_sync_run_crew", run_crew):
            PromptOptimizationService._execute_crew_optimization_sync(
                loop=MagicMock(),
                baseline_doc=baseline_doc,
                field_keys=keys,
                objective="Crew 'Research': survey the market",
                rubric="- Research: A table",
                agents_yaml=agents_yaml,
                tasks_yaml=tasks_yaml,
                target_model="crew-model",
                judge_model="independent-judge",
                reflection_model="reflect-model",
                max_metric_calls=4,
                execution_timeout=60,
                registry_uri="databricks-uc",
                prompt_name="main.kasal.kasal_crew_x_grp1",
                crew_id="",
                cancel_run_id=RUN_ID,
                judge_samples=1,
                builtin_judges=builtin_judges,
            )
    builtin_calls = [c for c in calls if JUDGE_CONTRACT in c["text"]]
    return SimpleNamespace(calls=calls, builtin_calls=builtin_calls, scored=scored)


def test_builtins_use_the_runs_judge_model(_real_mlflow_warm):
    run = _drive(_real_mlflow_warm, ["Safety", "RelevanceToQuery", "Completeness"])
    assert len(run.builtin_calls) == 3
    assert {c["model"] for c in run.builtin_calls} == {"independent-judge"}


def test_graded_verdicts_join_kasals_judge_score(_real_mlflow_warm):
    run = _drive(_real_mlflow_warm, ["Safety", "RelevanceToQuery"])
    feedback, aggregate = run.scored[0]
    # (0.7 own judge + 1.0 relevance) / 2; the passed Safety gate adds nothing.
    assert feedback.value == pytest.approx(0.85)
    assert "[builtin:RelevanceToQuery] yes" in feedback.rationale
    assert "[builtin:Safety] yes" in feedback.rationale
    assert aggregate == pytest.approx(0.3 * 1.0 + 0.7 * 0.85)


def test_a_failed_safety_gate_zeroes_the_judge_score(_real_mlflow_warm):
    run = _drive(_real_mlflow_warm, ["Safety", "RelevanceToQuery"], safety="no")
    feedback, aggregate = run.scored[0]
    assert feedback.value == 0.0
    assert "Gate failed (Safety)" in feedback.rationale
    assert aggregate == pytest.approx(0.3)  # format credit only


def test_no_selection_makes_no_builtin_call(_real_mlflow_warm):
    run = _drive(_real_mlflow_warm, [])
    assert run.builtin_calls == []
    assert run.scored[0][0].value == pytest.approx(0.7)


class TestStartValidatesTheSelection:
    """start_crew_optimization rejects an unusable selection (400) and hands
    the resolved ids to the worker."""

    def _start(self, builtin_judges):
        agent = SimpleNamespace(
            id="a1", name="A", role="r", goal="g", backstory="b", tools=[], llm="m"
        )
        task = SimpleNamespace(
            id="t1",
            name="T",
            description="d",
            expected_output="e",
            tools=[],
            agent_id="a1",
        )
        crew = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            name="C",
            agent_ids=["a1"],
            task_ids=["t1"],
        )
        service = PromptOptimizationService.__new__(PromptOptimizationService)
        service.session = MagicMock()
        service._rubric_with_feedback = AsyncMock(side_effect=lambda r, c, g: r)
        service._resolve_registry = AsyncMock(return_value=("databricks-uc", "c.s.p"))
        service._resolve_crew_traces_experiment = AsyncMock(return_value="exp")
        service._record_run = AsyncMock()
        service._prune_runs = MagicMock()
        service._run_optimization = AsyncMock()
        service._review_and_save = AsyncMock(return_value=ReviewHarvest())
        request = CrewOptimizationRequest(
            crew_id=crew.id, builtin_judges=builtin_judges
        )
        with (
            patch("src.services.catalog.crews.CrewService") as crews,
            patch("src.services.catalog.agents.AgentService") as agents,
            patch("src.services.catalog.tasks.TaskService") as tasks,
            patch.object(
                svc_module, "resolve_run_judge", AsyncMock(return_value=("j", 1))
            ),
        ):
            crews.return_value.get_by_group = AsyncMock(return_value=crew)
            agents.return_value.get_with_group_check = AsyncMock(return_value=agent)
            tasks.return_value.get_with_group_check = AsyncMock(return_value=task)
            result = asyncio.run(service.start_crew_optimization(request, None))
        svc_module._RUNS.pop(result["run_id"], None)
        return service._run_optimization.call_args.kwargs

    def test_the_selection_reaches_the_worker(self):
        kwargs = self._start(["Completeness", "Safety"])
        assert kwargs["builtin_judges"] == ["Safety", "Completeness"]

    def test_an_unusable_judge_is_refused_before_the_run(self):
        with pytest.raises(ValueError):
            self._start(["Nope"])
