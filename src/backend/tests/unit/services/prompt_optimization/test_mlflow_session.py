"""Tests for the judge MLflow-backend resolver + the registry grant hint."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.prompt_optimization.gepa import mlflow_session as ms
from src.services.prompt_optimization.gepa.registry_errors import (
    is_permission_denied,
    prompt_registry_grant_hint,
)


class TestResolveBackend:
    @pytest.mark.asyncio
    async def test_local_wins_when_local_server_configured(self, monkeypatch):
        monkeypatch.setenv("MCP_SERVER_ENABLED", "true")
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
        monkeypatch.delenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", raising=False)
        with patch("src.services.mlflow.local.is_reachable", return_value=True):
            backend = await ms.resolve_mlflow_backend(MagicMock(), MagicMock())
        assert backend is not None
        assert backend.kind == "local"
        assert backend.uri == "http://127.0.0.1:5555"

    @pytest.mark.asyncio
    async def test_local_none_when_no_server_is_listening(self, monkeypatch):
        """A configured-but-down local server is no backend at all — the 2 s
        probe fails instead of mlflow's minutes-long retry storm."""
        monkeypatch.setenv("MCP_SERVER_ENABLED", "true")
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
        monkeypatch.delenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", raising=False)
        with patch(
            "src.services.mlflow.local.is_reachable", return_value=False
        ) as probe:
            backend = await ms.resolve_mlflow_backend(MagicMock(), None)
        assert backend is None
        probe.assert_called_once_with("http://127.0.0.1:5555")

    @pytest.mark.asyncio
    async def test_none_when_no_backend(self, monkeypatch):
        monkeypatch.setenv("MCP_SERVER_ENABLED", "false")
        # No group_id → cannot resolve a Databricks workspace either.
        backend = await ms.resolve_mlflow_backend(MagicMock(), None)
        assert backend is None

    @pytest.mark.asyncio
    async def test_databricks_when_workspace_configured(self, monkeypatch):
        monkeypatch.setenv("MCP_SERVER_ENABLED", "false")
        group = MagicMock()
        group.primary_group_id = "grp1"
        fake_svc = MagicMock()
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
        monkeypatch.setenv("MCP_SERVER_ENABLED", "false")
        group = MagicMock()
        group.primary_group_id = "grp1"
        fake_svc = MagicMock()
        fake_svc._configured_workspace_url = AsyncMock(
            return_value="https://ws.example.com"
        )
        fake_svc._setup_mlflow_auth = AsyncMock(return_value=None)  # no auth
        with patch("src.services.mlflow.service.MLflowService", return_value=fake_svc):
            backend = await ms.resolve_mlflow_backend(MagicMock(), group)
        assert backend is None


class TestMlflowSession:
    def test_databricks_sets_and_restores_env(self, monkeypatch):
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
                assert os.environ["DATABRICKS_TOKEN"] == "tok"
                assert os.environ["DATABRICKS_HOST"] == "https://ws.example.com"
                fake_mlflow.set_tracking_uri.assert_called_with("databricks")
        # restored
        assert os.environ["DATABRICKS_HOST"] == "old-host"
        assert "DATABRICKS_TOKEN" not in os.environ

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


class TestSpSingleAuth:
    """The app SP must be presented as a SINGLE auth method (SP token, OAuth
    env vars removed) so the SDK doesn't error 'oauth and pat'."""

    def test_pins_pat_auth_when_only_token_present(self, monkeypatch):
        # No OAuth SP creds but a PAT in the env: sp_single_auth cannot derive an
        # SP bearer, but it MUST still pin DATABRICKS_AUTH_TYPE=pat so a bare
        # WorkspaceClient() built in the window (MLflow get_trace) uses the PAT
        # instead of the app-injected oauth-m2m. The token itself is untouched.
        from src.services.mlflow import sp_auth

        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "pat")
        monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "oauth-m2m")
        with sp_auth.sp_single_auth() as active:
            assert active is True
            assert os.environ["DATABRICKS_TOKEN"] == "pat"  # untouched
            assert os.environ["DATABRICKS_AUTH_TYPE"] == "pat"  # pinned
        # restored after the window
        assert os.environ["DATABRICKS_AUTH_TYPE"] == "oauth-m2m"

    def test_noop_without_any_creds(self, monkeypatch):
        # Truly no creds (no OAuth, no token) is the only genuine no-op.
        from src.services.mlflow import sp_auth

        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        with sp_auth.sp_single_auth() as active:
            assert active is False

    def test_swaps_to_sp_token_and_removes_oauth(self, monkeypatch):
        # sp_single_auth now lives in mlflow.sp_auth (prompt_optimization.gepa.sp_auth
        # is a back-compat shim). Patch derive_sp_bearer where the code calls it —
        # the canonical module — or the real WorkspaceClient runs and hangs on I/O.
        from src.services.mlflow import sp_auth

        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.example.com")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        monkeypatch.setenv("DATABRICKS_TOKEN", "stale-pat")
        monkeypatch.setattr(sp_auth, "derive_sp_bearer", lambda *a: "sp-bearer")
        with sp_auth.sp_single_auth() as active:
            assert active is True
            assert os.environ["DATABRICKS_TOKEN"] == "sp-bearer"
            # The OAuth SP variables stay: the pinned auth type is what makes
            # the SDK ignore them, and stripping them starved every concurrent
            # reader of the process env (issue #8).
            assert os.environ["DATABRICKS_CLIENT_ID"] == "cid"
            assert os.environ["DATABRICKS_CLIENT_SECRET"] == "csec"
            assert os.environ["DATABRICKS_AUTH_TYPE"] == "pat"
        # restored
        assert os.environ["DATABRICKS_CLIENT_ID"] == "cid"
        assert os.environ["DATABRICKS_TOKEN"] == "stale-pat"

    def test_noop_when_bearer_cannot_be_derived(self, monkeypatch):
        from src.services.mlflow import sp_auth

        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.example.com")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        monkeypatch.setattr(sp_auth, "derive_sp_bearer", lambda *a: None)
        with sp_auth.sp_single_auth() as active:
            assert active is False


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
