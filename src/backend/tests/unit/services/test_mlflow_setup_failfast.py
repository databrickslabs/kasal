"""Regression tests for PERF-036: the SPN env check must run BEFORE the
~1.1s ``import mlflow`` so SPN-less environments (all local dev) never pay
the import inside the subprocess spawn→kickoff critical path."""

import builtins
import pytest
from unittest.mock import MagicMock

from src.services.otel_tracing.mlflow_setup import configure_mlflow_in_subprocess


def _db_config(enabled=True):
    cfg = MagicMock()
    cfg.mlflow_enabled = enabled
    return cfg


@pytest.mark.asyncio
async def test_spn_missing_skips_without_importing_mlflow(monkeypatch):
    for var in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)

    real_import = builtins.__import__

    def forbid_mlflow(name, *args, **kwargs):
        if name == "mlflow" or name.startswith("mlflow."):
            raise AssertionError("mlflow imported despite missing SPN credentials")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", forbid_mlflow)

    result = await configure_mlflow_in_subprocess(
        db_config=_db_config(enabled=True),
        job_id="j1",
        execution_id="e1",
        group_id="g1",
    )

    assert result.tracing_ready is False
    assert "SPN credentials required" in (result.error or "")


@pytest.mark.asyncio
async def test_disabled_workspace_skips_without_importing_mlflow(monkeypatch):
    real_import = builtins.__import__

    def forbid_mlflow(name, *args, **kwargs):
        if name == "mlflow" or name.startswith("mlflow."):
            raise AssertionError("mlflow imported despite mlflow_enabled=False")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", forbid_mlflow)

    result = await configure_mlflow_in_subprocess(
        db_config=_db_config(enabled=False),
        job_id="j1",
        execution_id="e1",
        group_id="g1",
    )

    assert result.enabled is False
