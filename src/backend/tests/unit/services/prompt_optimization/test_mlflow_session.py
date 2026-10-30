"""Tests for the judge MLflow-backend resolver + the registry grant hint."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.mlflow import sp_auth
from src.services.prompt_optimization.gepa import mlflow_session as ms
from src.services.prompt_optimization.gepa.registry_errors import (
    is_permission_denied,
    prompt_registry_grant_hint,
)


class TestResolveBackend:
    @pytest.mark.asyncio
    async def test_local_wins_when_local_server_configured(self, monkeypatch):
        """The workspace's local server in Configuration → MLflow."""
        monkeypatch.setattr(
            "src.services.mlflow.service.MLflowService.configured_local_uri",
            AsyncMock(return_value="http://127.0.0.1:5555"),
        )
        with (
            patch("src.services.mlflow.local.is_reachable", return_value=True),
            patch.object(
                ms, "configured_experiment", AsyncMock(return_value="team-traces")
            ) as configured,
        ):
            backend = await ms.resolve_mlflow_backend(MagicMock(), MagicMock())
        assert backend is not None
        assert backend.kind == "local"
        assert backend.uri == "http://127.0.0.1:5555"
        # The experiment comes from Configuration → MLflow, not MLFLOW_EXPERIMENT_NAME.
        assert backend.experiment == "team-traces"
        configured.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_local_none_when_no_server_is_listening(self, monkeypatch):
        """A configured-but-down local server is no backend at all — the 2 s
        probe fails instead of mlflow's minutes-long retry storm."""
        monkeypatch.setattr(
            "src.services.mlflow.service.MLflowService.configured_local_uri",
            AsyncMock(return_value="http://127.0.0.1:5555"),
        )
        with patch(
            "src.services.mlflow.local.is_reachable", return_value=False
        ) as probe:
            backend = await ms.resolve_mlflow_backend(MagicMock(), MagicMock())
        assert backend is None
        probe.assert_called_once_with("http://127.0.0.1:5555")

    @pytest.mark.asyncio
    async def test_none_when_no_backend(self, monkeypatch):
        # No group_id → cannot resolve a Databricks workspace either.
        backend = await ms.resolve_mlflow_backend(MagicMock(), None)
        assert backend is None

    @pytest.mark.asyncio
    async def test_databricks_when_workspace_configured(self, monkeypatch):
        group = MagicMock()
        group.primary_group_id = "grp1"
        fake_svc = MagicMock()
        fake_svc.configured_local_uri = AsyncMock(return_value=None)
        fake_svc._configured_workspace_url = AsyncMock(
            return_value="https://ws.example.com"
        )
        fake_svc._setup_mlflow_auth = AsyncMock(return_value=MagicMock())
        # The backend now pins the CONFIGURED experiment (source of truth),
        # resolved via this method rather than a hardcoded default.
        fake_svc.configured_crew_traces_experiment = AsyncMock(
            return_value="/Shared/kasal-team-traces"
        )
        # Reading traces from a UC-backed experiment needs the SQL warehouse id,
        # so the backend now carries it (mlflow_session exports it as
        # MLFLOW_TRACING_SQL_WAREHOUSE_ID for the session window).
        fake_svc._get_uc_trace_config = AsyncMock(return_value=("cat", "sch", "wh-123"))
        with patch("src.services.mlflow.service.MLflowService", return_value=fake_svc):
            backend = await ms.resolve_mlflow_backend(MagicMock(), group)
        assert backend is not None
        assert backend.kind == "databricks"
        assert backend.experiment == "/Shared/kasal-team-traces"
        assert backend.warehouse_id == "wh-123"

    @pytest.mark.asyncio
    async def test_databricks_none_when_auth_unavailable(self, monkeypatch):
        group = MagicMock()
        group.primary_group_id = "grp1"
        fake_svc = MagicMock()
        fake_svc.configured_local_uri = AsyncMock(return_value=None)
        fake_svc._configured_workspace_url = AsyncMock(
            return_value="https://ws.example.com"
        )
        fake_svc._setup_mlflow_auth = AsyncMock(return_value=None)  # no auth
        with patch("src.services.mlflow.service.MLflowService", return_value=fake_svc):
            backend = await ms.resolve_mlflow_backend(MagicMock(), group)
        assert backend is None


class TestMlflowSession:
    def test_databricks_scopes_credentials_without_touching_env(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "old-host")
        auth = MagicMock()
        auth.workspace_url = "https://ws.example.com"
        auth.token = "tok"
        backend = ms.MLflowBackend(kind="databricks", experiment="/Shared/x", auth=auth)

        fake_mlflow = MagicMock()
        fake_mlflow.get_tracking_uri.return_value = "prev"
        with patch.dict("sys.modules", {"mlflow": fake_mlflow}):
            with ms.mlflow_session(backend):
                creds = sp_auth.current_credentials()
                assert creds is not None
                assert (creds.host, creds.token) == ("https://ws.example.com", "tok")
                # Nothing of the credential reaches the shared process env.
                assert "DATABRICKS_TOKEN" not in os.environ
                assert os.environ["DATABRICKS_HOST"] == "old-host"
                fake_mlflow.set_tracking_uri.assert_called_with("databricks")
        assert sp_auth.current_credentials() is None

    def test_local_sets_uri_no_env_swap(self):
        backend = ms.MLflowBackend(
            kind="local", experiment="kasal", uri="http://localhost:5555"
        )
        fake_mlflow = MagicMock()
        fake_mlflow.get_tracking_uri.return_value = "prev"
        with patch.dict("sys.modules", {"mlflow": fake_mlflow}):
            with ms.mlflow_session(backend):
                fake_mlflow.set_tracking_uri.assert_called_with("http://localhost:5555")
                fake_mlflow.set_experiment.assert_called_with("kasal")


class TestGrantHint:
    def test_permission_denied_detection(self):
        assert is_permission_denied(Exception("PERMISSION_DENIED: nope"))
        assert is_permission_denied(Exception("Permission denied to update prompt"))
        assert not is_permission_denied(Exception("something else"))

    def test_hint_names_schema_catalog_and_manage(self):
        hint = prompt_registry_grant_hint("ai_specialist.kasal.kasal_crew_abc")
        assert "ai_specialist.kasal" in hint
        assert "USE CATALOG ON CATALOG ai_specialist" in hint
        assert "MANAGE" in hint
