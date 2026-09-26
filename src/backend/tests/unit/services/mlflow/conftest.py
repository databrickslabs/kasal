"""MLflow test defaults.

The evaluation runner no longer falls back to the shared
``/Shared/kasal-crew-execution-traces`` experiment: with none configured it
stops. Real callers pass the experiment Configuration → MLflow resolves to;
tests that build a runner without one get a private experiment through the
fallback. Tests that exercise the "nothing configured" path patch it back.
"""

import pytest

TEST_TRACES_EXPERIMENT = "/Users/tests@example.com/kasal-crew-traces"


@pytest.fixture(autouse=True)
def _traces_experiment(monkeypatch):
    monkeypatch.setattr(
        "src.core.databricks_app.fallback_trace_experiment",
        lambda group_id: TEST_TRACES_EXPERIMENT,
    )
