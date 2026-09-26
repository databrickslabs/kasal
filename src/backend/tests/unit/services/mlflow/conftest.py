"""MLflow test defaults.

The evaluation runner no longer falls back to the shared
``/Shared/kasal-crew-execution-traces`` experiment: with none configured it
stops (``fallback_trace_experiment``). Outside Databricks Apps that fallback
reads ``MLFLOW_CREW_TRACES_EXPERIMENT``, so give every test here a private one.
Tests that exercise the "nothing configured" path delete it themselves.
"""

import pytest

TEST_TRACES_EXPERIMENT = "/Users/tests@example.com/kasal-crew-traces"


@pytest.fixture(autouse=True)
def _traces_experiment(monkeypatch):
    monkeypatch.setenv("MLFLOW_CREW_TRACES_EXPERIMENT", TEST_TRACES_EXPERIMENT)
