"""Tests for parent-process MLflow tracing setup.

These guarantee the dispatcher + crew/agent/task generation traces land in the
same Unity Catalog experiment as crew execution (the ``-uc`` experiment), via
the UC span exporter (no localhost OTLP), and never get pinned to the managed
Databricks(experiment_id) destination that would route them to artifact storage.
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.otel_tracing.mlflow_parent_setup import (
    configure_parent_mlflow_tracing,
    invalidate_parent_mlflow_cache,
    set_root_span_outputs,
    _setup_sync,
)
from src.services.otel_tracing.mlflow_setup import uc_experiment_name


@pytest.fixture(autouse=True)
def _restore_os_environ():
    """_setup_sync writes directly to os.environ (DATABRICKS_TOKEN /
    DATABRICKS_AUTH_TYPE / MLFLOW_TRACING_SQL_WAREHOUSE_ID / OTEL_SDK_DISABLED),
    which monkeypatch does not restore. Snapshot + restore so these tests don't
    leak auth env into later tests (e.g. the dispatcher LLM-retry tests)."""
    snapshot = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(snapshot)


@pytest.fixture(autouse=True)
def _reset_setup_memo():
    """configure_parent_mlflow_tracing memoizes per group at module level;
    reset between tests so each starts from a cold cache."""
    invalidate_parent_mlflow_cache()
    yield
    invalidate_parent_mlflow_cache()


class TestSetRootSpanOutputs:
    """The trace Response is null unless outputs are set on the root span."""

    def test_sets_dict_outputs(self):
        span = MagicMock()
        set_root_span_outputs(span, {"a": 1})
        span.set_outputs.assert_called_once_with({"a": 1})

    def test_normalizes_pydantic_via_model_dump(self):
        span = MagicMock()
        result = MagicMock()
        result.model_dump.return_value = {"task": "x"}
        set_root_span_outputs(span, result)
        span.set_outputs.assert_called_once_with({"task": "x"})

    def test_noop_on_none_span(self):
        set_root_span_outputs(None, {"a": 1})  # must not raise

    def test_swallows_set_outputs_error(self):
        span = MagicMock()
        span.set_outputs.side_effect = RuntimeError("boom")
        set_root_span_outputs(span, {"a": 1})  # must not raise


class TestUcExperimentName:
    def test_appends_suffix(self):
        assert uc_experiment_name("/Shared/foo") == "/Shared/foo-uc"

    def test_idempotent(self):
        assert uc_experiment_name("/Shared/foo-uc") == "/Shared/foo-uc"


def _spn_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csecret")


def _mock_mlflow():
    m = MagicMock()
    exp = MagicMock()
    exp.experiment_id = "exp-1"
    m.set_experiment.return_value = exp
    m.tracing = MagicMock()
    m.get_tracking_uri.return_value = "databricks"
    return m


def _mock_sdk():
    wc = MagicMock()
    wc.config.authenticate.return_value = {"Authorization": "Bearer tok"}
    sdk = MagicMock()
    sdk.WorkspaceClient = MagicMock(return_value=wc)
    return sdk


class TestSetupSyncUcRouting:
    def test_uc_active_uses_uc_experiment_and_no_databricks_destination(self, monkeypatch):
        _spn_env(monkeypatch)
        mock_mlflow = _mock_mlflow()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "databricks.sdk": _mock_sdk()}):
            with patch(
                "src.services.otel_tracing.mlflow_setup._build_uc_trace_location",
                return_value=MagicMock(),
            ):
                ok = _setup_sync(
                    "/Shared/kasal-crew-execution-traces",
                    "cat", "sch", "wh-1", "TaskGeneration",
                )
        assert ok is True
        bound = [
            (c.args[0] if c.args else c.kwargs.get("name"))
            for c in mock_mlflow.set_experiment.call_args_list
        ]
        # Dedicated -uc experiment, shared with crew execution.
        assert "/Shared/kasal-crew-execution-traces-uc" in bound
        # Managed experiment-id destination must NOT be pinned in UC mode.
        mock_mlflow.tracing.set_destination.assert_not_called()
        # litellm autolog enabled so LLM child spans flow through MLflow's tracer.
        mock_mlflow.litellm.autolog.assert_called_once()

    def test_managed_mode_sets_databricks_destination(self, monkeypatch):
        _spn_env(monkeypatch)
        mock_mlflow = _mock_mlflow()
        dest_mod = MagicMock()
        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "databricks.sdk": _mock_sdk(),
            "mlflow.tracing.destination": dest_mod,
        }):
            with patch(
                "src.services.otel_tracing.mlflow_setup._build_uc_trace_location",
                return_value=None,
            ):
                ok = _setup_sync("/Shared/base", "cat", "sch", None, "Dispatcher")
        assert ok is True
        bound = [
            (c.args[0] if c.args else c.kwargs.get("name"))
            for c in mock_mlflow.set_experiment.call_args_list
        ]
        assert bound == ["/Shared/base"]  # no -uc suffix in managed mode
        mock_mlflow.tracing.set_destination.assert_called_once()

    def test_returns_false_without_spn_credentials(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        assert _setup_sync("/Shared/base", "c", "s", "w", "X") is False


class TestSetupSyncAuth:
    """SPN auth: extract a bearer from SPN creds, never use a PAT; strip PAT
    during the SDK call and restore it; keep SPN vars in the main process."""

    def test_spn_token_extracted_and_spn_vars_preserved(self, monkeypatch):
        _spn_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_TOKEN", "old-pat")
        mock_mlflow = _mock_mlflow()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "databricks.sdk": _mock_sdk()}):
            with patch(
                "src.services.otel_tracing.mlflow_setup._build_uc_trace_location",
                return_value=MagicMock(),
            ):
                ok = _setup_sync("/Shared/base", "c", "s", "wh", "X")
        assert ok is True
        # Bearer from authenticate() replaces the PAT; SPN vars preserved.
        assert os.environ.get("DATABRICKS_TOKEN") == "tok"
        assert os.environ.get("DATABRICKS_CLIENT_ID") == "cid"
        assert os.environ.get("DATABRICKS_CLIENT_SECRET") == "csecret"
        mock_mlflow.set_tracking_uri.assert_called_with("databricks")

    def test_pat_stripped_during_sdk_call_then_restored(self, monkeypatch):
        _spn_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_API_KEY", "key1")
        captured = {}
        sdk = _mock_sdk()

        def _auth():
            captured["api_key_present"] = "DATABRICKS_API_KEY" in os.environ
            return {"Authorization": "Bearer tok"}

        sdk.WorkspaceClient.return_value.config.authenticate.side_effect = _auth
        with patch.dict(sys.modules, {"mlflow": _mock_mlflow(), "databricks.sdk": sdk}):
            with patch(
                "src.services.otel_tracing.mlflow_setup._build_uc_trace_location",
                return_value=None,
            ):
                ok = _setup_sync("/Shared/base", "c", "s", None, "X")
        assert ok is True
        assert captured["api_key_present"] is False  # stripped during the call
        assert os.environ.get("DATABRICKS_API_KEY") == "key1"  # restored after

    def test_unexpected_auth_header_returns_false(self, monkeypatch):
        _spn_env(monkeypatch)
        sdk = _mock_sdk()
        sdk.WorkspaceClient.return_value.config.authenticate.return_value = {
            "Authorization": "Basic nope"
        }
        with patch.dict(sys.modules, {"mlflow": _mock_mlflow(), "databricks.sdk": sdk}):
            assert _setup_sync("/Shared/base", "c", "s", "wh", "X") is False

    def test_workspace_url_without_http_prefix_gets_https(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csecret")
        with patch.dict(sys.modules, {"mlflow": _mock_mlflow(), "databricks.sdk": _mock_sdk()}):
            with patch(
                "src.services.otel_tracing.mlflow_setup._build_uc_trace_location",
                return_value=None,
            ):
                ok = _setup_sync("/Shared/base", "c", "s", None, "X")
        assert ok is True
        assert os.environ.get("DATABRICKS_HOST") == "https://example.cloud.databricks.com"


class TestParentSetupMemoization:
    """Perf regression (W1.1): the full setup (2 DB reads + SPN OAuth mint +
    set_experiment HTTP call) used to run on EVERY dispatch and EVERY
    agent/task/crew generation call. Repeats for the group that owns the
    process-global binding must be free until the TTL or an invalidation."""

    def _ctx(self, group_id):
        ctx = MagicMock()
        ctx.primary_group_id = group_id
        return ctx

    def _enabled_stack(self, setup_result=True):
        svc = MagicMock()
        svc.is_enabled = AsyncMock(return_value=True)
        dbx = MagicMock()
        dbx.get_databricks_config = AsyncMock(return_value=None)
        setup = MagicMock(return_value=setup_result)
        return svc, dbx, setup

    @pytest.mark.asyncio
    async def test_repeat_call_same_group_skips_full_setup(self):
        svc, dbx, setup = self._enabled_stack()
        with (
            patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)),
            patch("src.services.databricks_service.DatabricksService", MagicMock(return_value=dbx)),
            patch("src.services.otel_tracing.mlflow_parent_setup._setup_sync", setup),
        ):
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is True
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is True
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is True

        setup.assert_called_once()          # OAuth + set_experiment ran once
        svc.is_enabled.assert_awaited_once()  # DB read ran once

    @pytest.mark.asyncio
    async def test_different_group_rebinds(self):
        svc, dbx, setup = self._enabled_stack()
        with (
            patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)),
            patch("src.services.databricks_service.DatabricksService", MagicMock(return_value=dbx)),
            patch("src.services.otel_tracing.mlflow_parent_setup._setup_sync", setup),
        ):
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is True
            # A different group must NOT reuse g1's binding — MLflow experiment
            # binding is process-global, so its traces would land in g1's experiment.
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g2")) is True

        assert setup.call_count == 2

    @pytest.mark.asyncio
    async def test_disabled_workspace_result_is_cached(self):
        svc = MagicMock()
        svc.is_enabled = AsyncMock(return_value=False)
        with patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)):
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is False
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is False

        svc.is_enabled.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_setup_backs_off_instead_of_reminting_oauth(self):
        svc, dbx, setup = self._enabled_stack(setup_result=False)
        with (
            patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)),
            patch("src.services.databricks_service.DatabricksService", MagicMock(return_value=dbx)),
            patch("src.services.otel_tracing.mlflow_parent_setup._setup_sync", setup),
        ):
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is False
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is False

        setup.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalidate_forces_full_setup(self):
        svc, dbx, setup = self._enabled_stack()
        with (
            patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)),
            patch("src.services.databricks_service.DatabricksService", MagicMock(return_value=dbx)),
            patch("src.services.otel_tracing.mlflow_parent_setup._setup_sync", setup),
        ):
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is True
            invalidate_parent_mlflow_cache()  # e.g. config toggled in the UI
            assert await configure_parent_mlflow_tracing(MagicMock(), self._ctx("g1")) is True

        assert setup.call_count == 2


class TestConfigureParentMlflowTracing:
    @pytest.mark.asyncio
    async def test_returns_false_when_mlflow_disabled(self):
        svc = MagicMock()
        svc.is_enabled = AsyncMock(return_value=False)
        with patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)):
            ok = await configure_parent_mlflow_tracing(MagicMock(), None, label="X")
        assert ok is False

    @pytest.mark.asyncio
    async def test_passes_resolved_config_to_setup(self):
        svc = MagicMock()
        svc.is_enabled = AsyncMock(return_value=True)
        cfg = MagicMock()
        cfg.mlflow_experiment_name = "kasal-crew-execution-traces"
        cfg.catalog = "nemotemo_catalog"
        cfg.db_schema = "kasal"
        cfg.warehouse_id = "wh-1"
        dbx = MagicMock()
        dbx.get_databricks_config = AsyncMock(return_value=cfg)

        captured = {}

        def _fake_setup(exp_name, uc_catalog, uc_schema, warehouse_id, label):
            captured.update(
                exp_name=exp_name, uc_catalog=uc_catalog,
                uc_schema=uc_schema, warehouse_id=warehouse_id, label=label,
            )
            return True

        with (
            patch("src.services.mlflow_service.MLflowService", MagicMock(return_value=svc)),
            patch("src.services.databricks_service.DatabricksService", MagicMock(return_value=dbx)),
            patch("src.services.otel_tracing.mlflow_parent_setup._setup_sync", side_effect=_fake_setup),
        ):
            ok = await configure_parent_mlflow_tracing(MagicMock(), None, label="CrewGeneration")

        assert ok is True
        # Experiment name normalized with /Shared/ prefix; -uc suffix is applied
        # inside _setup_sync (so the base is forwarded here).
        assert captured["exp_name"] == "/Shared/kasal-crew-execution-traces"
        assert captured["uc_catalog"] == "nemotemo_catalog"
        assert captured["uc_schema"] == "kasal"
        assert captured["warehouse_id"] == "wh-1"
        assert captured["label"] == "CrewGeneration"
