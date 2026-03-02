"""
Unit tests for the auth setup section of mlflow_setup.configure_mlflow_in_subprocess().

Covers the SPN credential extraction approach: strip PAT env vars before
WorkspaceClient(host, client_id, client_secret), extract a bearer token,
and set DATABRICKS_TOKEN for MLflow's exporter.  SPN vars are removed
from the subprocess env after extraction so the exporter only sees HOST+TOKEN.
"""

import os
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.services.otel_tracing.mlflow_setup import (
    MlflowSetupResult,
    configure_mlflow_in_subprocess,
)


def _make_db_config(mlflow_enabled=True):
    return SimpleNamespace(
        mlflow_enabled=mlflow_enabled,
        mlflow_experiment_name=None,
    )


def _make_group_context(primary_group_id="grp-1"):
    return SimpleNamespace(primary_group_id=primary_group_id)


# Env vars simulating Databricks Apps runtime
_SPN_ENV = {
    "DATABRICKS_CLIENT_ID": "test-client-id",
    "DATABRICKS_CLIENT_SECRET": "test-secret",
    "DATABRICKS_HOST": "e2-demo-west.cloud.databricks.com",
}


def _mlflow_mock():
    """Return a mock mlflow module good enough for configure_mlflow_in_subprocess."""
    m = MagicMock()
    m.set_experiment.return_value = SimpleNamespace(experiment_id="exp-1")
    m.set_tracking_uri = MagicMock()
    m.tracing = MagicMock()
    m.tracing.enable = MagicMock()
    m.config = MagicMock()
    m.config.enable_async_logging = MagicMock()
    m.get_tracking_uri = MagicMock(return_value="databricks")
    return m


def _mock_workspace_client(bearer_token="spn-tok-123"):
    """Return a mock WorkspaceClient whose authenticate() returns a headers dict."""
    mock_wc_instance = MagicMock()
    mock_wc_instance.config.authenticate.return_value = {
        "Authorization": f"Bearer {bearer_token}",
    }
    return mock_wc_instance


# ===================================================================
# SPN credential extraction
# ===================================================================


class TestSPNFromEnv:

    @pytest.mark.asyncio
    async def test_spn_env_extracts_token(self):
        """SPN env vars → SDK extracts bearer → DATABRICKS_TOKEN is set."""
        import sys

        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(return_value=_mock_workspace_client())

        with (
            patch.dict(os.environ, _SPN_ENV, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "mlflow.tracing.destination": MagicMock(),
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )

        assert result.enabled is True
        assert result.auth_method == "service_principal"

    @pytest.mark.asyncio
    async def test_spn_strips_pat_before_sdk_call(self):
        """PAT env vars are stripped before WorkspaceClient to avoid dual-auth conflict."""
        import sys

        mock_mlflow_mod = _mlflow_mock()
        captured_env_during_call = {}

        def _capturing_wc(*_args, **_kwargs):
            # Capture env state when WorkspaceClient is constructed
            captured_env_during_call["DATABRICKS_TOKEN"] = os.environ.get("DATABRICKS_TOKEN")
            captured_env_during_call["DATABRICKS_API_KEY"] = os.environ.get("DATABRICKS_API_KEY")
            return _mock_workspace_client()

        mock_wc_cls = MagicMock(side_effect=_capturing_wc)

        # Simulate Databricks Apps: both SPN and PAT env vars present
        env_with_pat = {
            **_SPN_ENV,
            "DATABRICKS_TOKEN": "dapi-old-pat",
            "DATABRICKS_API_KEY": "dapi-key",
        }

        with (
            patch.dict(os.environ, env_with_pat, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "mlflow.tracing.destination": MagicMock(),
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )

        # PAT should have been stripped during SDK call
        assert captured_env_during_call["DATABRICKS_TOKEN"] is None
        assert captured_env_during_call["DATABRICKS_API_KEY"] is None
        assert result.auth_method == "service_principal"

    @pytest.mark.asyncio
    async def test_spn_restores_pat_after_sdk_failure(self):
        """PAT is restored even when SDK call fails."""
        import sys

        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(side_effect=RuntimeError("SDK error"))

        env_with_pat = {
            **_SPN_ENV,
            "DATABRICKS_TOKEN": "dapi-original",
        }

        with (
            patch.dict(os.environ, env_with_pat, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )
            # PAT should be restored after failure
            assert os.environ.get("DATABRICKS_TOKEN") == "dapi-original"

        assert result.tracing_ready is False

    @pytest.mark.asyncio
    async def test_spn_removes_client_vars_after_extraction(self):
        """After successful extraction in subprocess, SPN vars are removed from env."""
        import sys

        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(return_value=_mock_workspace_client())

        env = {**_SPN_ENV}

        with (
            patch.dict(os.environ, env, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "mlflow.tracing.destination": MagicMock(),
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )
            # After extraction, SPN vars should be removed (subprocess only)
            assert "DATABRICKS_CLIENT_ID" not in os.environ
            assert "DATABRICKS_CLIENT_SECRET" not in os.environ

        assert result.auth_method == "service_principal"

    @pytest.mark.asyncio
    async def test_spn_extraction_failure_returns_error(self):
        """If SPN token extraction fails, returns error."""
        import sys

        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(side_effect=RuntimeError("SDK error"))

        with (
            patch.dict(os.environ, _SPN_ENV, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )

        assert result.enabled is True
        assert result.tracing_ready is False
        assert result.error == "SPN credential extraction failed"


# ===================================================================
# Missing SPN credentials
# ===================================================================


class TestMissingSPNCredentials:

    @pytest.mark.asyncio
    async def test_no_spn_env_returns_error(self):
        """When SPN env vars are absent, returns error result."""
        import sys

        mock_mlflow_mod = _mlflow_mock()

        clean = {k: v for k, v in os.environ.items()
                 if k not in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
                              "DATABRICKS_HOST", "DATABRICKS_TOKEN")}

        with (
            patch.dict(os.environ, clean, clear=True),
            patch.dict(sys.modules, {"mlflow": mock_mlflow_mod}),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )

        assert result.enabled is True
        assert result.tracing_ready is False
        assert result.error == "SPN credentials required"

    @pytest.mark.asyncio
    async def test_host_only_returns_error(self):
        """When only HOST is set (no SPN creds), returns error."""
        import sys

        mock_mlflow_mod = _mlflow_mock()

        clean = {k: v for k, v in os.environ.items()
                 if k not in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
                              "DATABRICKS_TOKEN")}
        clean["DATABRICKS_HOST"] = "https://example.com"

        with (
            patch.dict(os.environ, clean, clear=True),
            patch.dict(sys.modules, {"mlflow": mock_mlflow_mod}),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
            )

        assert result.tracing_ready is False
        assert result.error == "SPN credentials required"


# ===================================================================
# UserContext setup
# ===================================================================


class TestUserContextSetup:

    @pytest.mark.asyncio
    async def test_group_context_sets_user_context(self):
        """group_context is passed to UserContext.set_group_context."""
        import sys

        gc = _make_group_context()
        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(return_value=_mock_workspace_client())

        with (
            patch.dict(os.environ, _SPN_ENV, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "mlflow.tracing.destination": MagicMock(),
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
            patch("src.utils.user_context.UserContext.set_group_context") as mock_set_gc,
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
                group_context=gc,
            )

        mock_set_gc.assert_called_once_with(gc)
        assert result.enabled is True

    @pytest.mark.asyncio
    async def test_group_context_set_fails_gracefully(self):
        """If UserContext.set_group_context raises, execution continues."""
        import sys

        gc = _make_group_context()
        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(return_value=_mock_workspace_client())

        with (
            patch.dict(os.environ, _SPN_ENV, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "mlflow.tracing.destination": MagicMock(),
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
            patch(
                "src.utils.user_context.UserContext.set_group_context",
                side_effect=RuntimeError("context error"),
            ),
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
                group_context=gc,
            )

        assert result.enabled is True
        assert result.auth_method == "service_principal"

    @pytest.mark.asyncio
    async def test_no_group_context_skips_user_context(self):
        """When group_context is None, UserContext.set_group_context is not called."""
        import sys

        mock_mlflow_mod = _mlflow_mock()
        mock_wc_cls = MagicMock(return_value=_mock_workspace_client())

        with (
            patch.dict(os.environ, _SPN_ENV, clear=False),
            patch.dict(sys.modules, {
                "mlflow": mock_mlflow_mod,
                "mlflow.tracing.destination": MagicMock(),
                "databricks.sdk": MagicMock(WorkspaceClient=mock_wc_cls),
            }),
            patch("src.utils.user_context.UserContext.set_group_context") as mock_set_gc,
        ):
            result = await configure_mlflow_in_subprocess(
                db_config=_make_db_config(),
                job_id="job-1",
                execution_id="exec-1",
                group_id="grp-1",
                group_context=None,
            )

        mock_set_gc.assert_not_called()
        assert result.enabled is True


# ===================================================================
# MLflow disabled
# ===================================================================


class TestMlflowDisabled:

    @pytest.mark.asyncio
    async def test_mlflow_disabled_skips_auth(self):
        """When mlflow_enabled=False, returns immediately without auth."""
        result = await configure_mlflow_in_subprocess(
            db_config=_make_db_config(mlflow_enabled=False),
            job_id="job-1",
            execution_id="exec-1",
            group_id="grp-1",
        )

        assert result.enabled is False
        assert result.tracing_ready is False
        assert result.auth_method is None
