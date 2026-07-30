"""Regression tests for PERF-036: the ~1.1s ``import mlflow`` must not be paid
when no MLflow backend can be configured, because it sits inside the subprocess
spawn→kickoff critical path.

The guarantee widened when local (OSS) MLflow became a second backend. "No SPN"
no longer means "no tracing" — a dev machine with a local server is now a valid
setup, and importing mlflow for it is correct. What must still hold is that the
CHEAP checks come first: no credentials AND no reachable local server means no
import. ``_setup_local_mlflow`` tests reachability with a 2s socket connect
before importing anything, which is what keeps that true."""

import builtins
from unittest.mock import MagicMock

import pytest

from src.services.otel_tracing.mlflow_setup import configure_mlflow_in_subprocess


def _db_config(enabled=True):
    cfg = MagicMock()
    cfg.mlflow_enabled = enabled
    return cfg


def _no_local_backend(monkeypatch):
    """Neutralise the local backend so only the Databricks path is under test.

    Without this the suite depends on whether the developer running it happens
    to have an MLflow server on the default port — which is exactly the kind of
    machine-dependent test that passes on CI and fails on a laptop.
    """
    from src.services.mlflow import local

    monkeypatch.setattr(local, "local_tracking_uri", lambda: None)


@pytest.mark.asyncio
async def test_spn_missing_skips_without_importing_mlflow(monkeypatch):
    for var in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)
    _no_local_backend(monkeypatch)

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


@pytest.mark.asyncio
async def test_unreachable_local_server_skips_without_importing_mlflow(monkeypatch):
    """The widened guarantee: a configured-but-down local server must be
    detected by the socket probe, not by importing mlflow and failing."""
    for var in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)

    from src.services.mlflow import local

    monkeypatch.setattr(local, "local_tracking_uri", lambda: "http://127.0.0.1:1")
    monkeypatch.setattr(local, "is_reachable", lambda uri, timeout=2.0: False)

    real_import = builtins.__import__

    def forbid_mlflow(name, *args, **kwargs):
        if name == "mlflow" or name.startswith("mlflow."):
            raise AssertionError("mlflow imported for an unreachable local server")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", forbid_mlflow)

    result = await configure_mlflow_in_subprocess(
        db_config=_db_config(enabled=True),
        job_id="j1",
        execution_id="e1",
        group_id="g1",
    )

    assert result.tracing_ready is False
    assert "no MLflow server" in (result.error or "")
