"""Crew optimization with labels, on the Databricks path (mocked).

Drives ``_execute_crew_optimization_sync`` against the fake mlflow/gepa stack
with the REAL Correctness / ExpectationsGuidelines classes; only the LLM is
fake. What must hold:

* the label judges grade with the run's judge model, Correctness against the
  confirmed facts and ExpectationsGuidelines against the review requirements;
* the labels ride GEPA's expectations channel;
* on a Databricks registry the evaluation trace is still logged, tagged with
  the crew AND the workspace (it was local-only before);
* an unlabelled label judge makes no call and does not raise.
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.prompt_optimization import service as svc_module
from src.services.prompt_optimization.builtin_judges import bridge
from src.services.prompt_optimization.gepa import reflection as gepa_reflection
from src.services.prompt_optimization.service import PromptOptimizationService
from tests.unit.services.prompt_optimization.test_prompt_optimization_service import (
    _crew_fixture,
    _fake_completion,
    _fake_stack,
)

RUN_ID = "label-run"
JUDGE_CONTRACT = '"result" is your verdict'
FACTS = {"expected_facts": ["Zurich is listed", "prices are in CHF"]}


@pytest.fixture(autouse=True)
def _real_scorers():
    """The real scorer modules, loaded before the fake mlflow tree shadows them."""
    import mlflow.genai.scorers as real_scorers

    bridge.install_bridge()
    warm = {**FACTS, "guidelines": ["g"]}
    with patch.object(
        gepa_reflection,
        "_sync_llm_completion",
        lambda *a, **k: '{"result": "yes", "rationale": "warm"}',
    ):
        with bridge.judge_route(bridge.JudgeRoute(loop=MagicMock())):
            for name in ("Safety", "Correctness", "ExpectationsGuidelines"):
                getattr(real_scorers, name)(model=bridge.placeholder_uri("w")).run(
                    inputs={"request": "q"}, outputs="a", expectations=warm
                )
    yield real_scorers
    svc_module._RUNS.pop(RUN_ID, None)


@contextmanager
def _span(name):
    yield SimpleNamespace(set_inputs=lambda _i: None, set_outputs=lambda _o: None)


def _drive(real_scorers, builtin_judges, label_expectations, requirements=()):
    baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
    calls, captured, trace_tags = [], {}, []

    def handler(call):
        if "reply with OK" in call["text"]:
            return "OK"
        if JUDGE_CONTRACT in call["text"]:
            return json.dumps({"rationale": "has them", "result": "yes"})
        if "requirements checklist" in call["text"]:
            return "R1. " + (requirements[0] if requirements else "x")
        return "R1: PASS\nQ: 7"  # Kasal's own judge (checklist mode)

    def optimize_prompts(*, predict_fn, train_data, scorers, aggregation, **_):
        fmt, correct = scorers
        captured["train_data"] = train_data
        output = predict_fn(**train_data[0]["inputs"])
        captured["feedback"] = correct(inputs=train_data[0]["inputs"], outputs=output)
        return SimpleNamespace(
            optimized_prompts=[SimpleNamespace(template=baseline_doc, uri="p:/c/2")],
            initial_eval_score=0.4,
            final_eval_score=0.7,
        )

    svc_module._RUNS[RUN_ID] = {"run_id": RUN_ID, "executions_used": 0}
    with _fake_stack(optimize_prompts, _fake_completion(handler, calls)) as stack:
        for name in ("Correctness", "ExpectationsGuidelines", "Safety"):
            setattr(
                stack.modules["mlflow.genai.scorers"], name, getattr(real_scorers, name)
            )
        stack.modules["mlflow"].start_span = _span
        stack.modules["mlflow"].update_current_trace = lambda tags: trace_tags.append(
            tags
        )
        with patch.object(
            gepa_reflection,
            "_sync_run_crew",
            lambda loop, **k: "A deliverable long enough to clear the format floor.",
        ):
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
                crew_id="crew-1",
                cancel_run_id=RUN_ID,
                group_context=SimpleNamespace(
                    primary_group_id="group_a", access_token=None
                ),
                judge_samples=1,
                builtin_judges=builtin_judges,
                label_expectations=label_expectations,
                human_requirements=list(requirements),
            )
    builtin_calls = [c for c in calls if JUDGE_CONTRACT in c["text"]]
    return SimpleNamespace(
        builtin_calls=builtin_calls, trace_tags=trace_tags, **captured
    )


def test_correctness_grades_against_the_confirmed_facts(_real_scorers):
    run = _drive(_real_scorers, ["Correctness"], FACTS)
    (call,) = run.builtin_calls
    assert call["model"] == "independent-judge"
    assert "Zurich is listed" in call["text"]
    assert "[builtin:Correctness] yes" in run.feedback.rationale


def test_expectations_guidelines_grades_against_the_requirements(_real_scorers):
    run = _drive(
        _real_scorers, ["ExpectationsGuidelines"], {}, ["German-speaking side only"]
    )
    (call,) = run.builtin_calls
    assert call["model"] == "independent-judge"
    assert "German-speaking side only" in call["text"]


def test_labels_ride_gepas_expectations(_real_scorers):
    run = _drive(_real_scorers, [], FACTS, ["German-speaking side only"])
    expectations = run.train_data[0]["expectations"]
    assert expectations["expected_facts"] == FACTS["expected_facts"]
    assert "German-speaking side only" in expectations["human_requirements"]


def test_databricks_eval_traces_are_tagged_with_the_workspace(_real_scorers):
    run = _drive(_real_scorers, [], {})
    assert run.trace_tags == [{"kasal_crew_id": "crew-1", "kasal_group_id": "group_a"}]


def test_an_unlabelled_label_judge_neither_calls_nor_raises(_real_scorers):
    run = _drive(_real_scorers, ["Correctness", "Safety"], {})
    assert len(run.builtin_calls) == 1  # Safety only
    assert "[builtin:Correctness]" not in run.feedback.rationale
