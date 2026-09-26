"""With no MLflow experiment configured, evaluation stops — it never uses /Shared."""

from types import SimpleNamespace

import pytest

from src.services.mlflow.evaluation_runner import MLflowEvaluationRunner


def _runner(experiment_name=None):
    exec_obj = SimpleNamespace(id="exec-1", group_id="group-1", mlflow_trace_id=None)
    return MLflowEvaluationRunner(
        exec_obj=exec_obj,
        job_id="job-1",
        inputs_text="question",
        prediction_text="answer",
        judge_model_route="judge",
        judge_model_defaulted=False,
        experiment_name=experiment_name,
    )


def test_configured_experiment_wins():
    assert _runner("/Users/team/configured")._experiment() == "/Users/team/configured"


def test_nothing_configured_stops_instead_of_using_shared(monkeypatch):
    monkeypatch.setattr(
        "src.core.databricks_app.fallback_trace_experiment", lambda group_id: None
    )
    with pytest.raises(ValueError, match="No MLflow experiment is configured"):
        _runner()._experiment()
