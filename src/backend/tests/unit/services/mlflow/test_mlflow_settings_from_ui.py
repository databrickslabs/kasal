"""MLflow settings come from Configuration → MLflow, not environment variables.

Covers the judge model (was MLFLOW_EVAL_JUDGE_MODEL / GEPA_JUDGE_MODEL) and the
Advanced settings (were MLFLOW_EVAL_MAX_ROWS / GEPA_JUDGE_SAMPLES).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from src.schemas.mlflow import MLflowSettingsUpdate
from src.services.mlflow import service as mlflow_service_mod
from src.services.mlflow.service import MLflowService


def _service(judge=None, advanced=None):
    svc = MLflowService(MagicMock(), group_id="group-1")
    svc.repo = MagicMock()
    svc.repo.get_evaluation_judge_model = AsyncMock(return_value=judge)
    svc.repo.get_advanced = AsyncMock(
        return_value=advanced
        or {"evaluation_max_rows": None, "optimization_judge_samples": None}
    )
    return svc


def _installation(monkeypatch, hosted, default_model=""):
    installation = MagicMock(hosted=hosted, default_model=default_model)
    monkeypatch.setattr(
        mlflow_service_mod.DatabricksAppInstallation,
        "from_env",
        classmethod(lambda cls: installation),
    )


class TestConfiguredJudgeModel:
    @pytest.mark.asyncio
    async def test_configured_judge_wins(self, monkeypatch):
        _installation(monkeypatch, hosted=True, default_model="installed-model")
        monkeypatch.setenv("MLFLOW_EVAL_JUDGE_MODEL", "env-judge")
        assert await _service("ui-judge").configured_judge_model() == "ui-judge"

    @pytest.mark.asyncio
    async def test_inside_apps_the_installed_model_is_the_default(self, monkeypatch):
        _installation(monkeypatch, hosted=True, default_model="installed-model")
        assert await _service(None).configured_judge_model() == "installed-model"

    @pytest.mark.asyncio
    async def test_outside_apps_nothing_configured_is_none_env_ignored(
        self, monkeypatch
    ):
        _installation(monkeypatch, hosted=False)
        monkeypatch.setenv("MLFLOW_EVAL_JUDGE_MODEL", "env-judge")
        monkeypatch.setenv("GEPA_JUDGE_MODEL", "env-judge")
        assert await _service(None).configured_judge_model() is None


class TestAdvancedSettings:
    @pytest.mark.asyncio
    async def test_defaults_fill_unset_values(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_EVAL_MAX_ROWS", "3")
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "7")
        assert await _service().advanced_settings() == {
            "evaluation_max_rows": mlflow_service_mod.DEFAULT_EVALUATION_MAX_ROWS,
            "optimization_judge_samples": (
                mlflow_service_mod.DEFAULT_OPTIMIZATION_JUDGE_SAMPLES
            ),
        }

    @pytest.mark.asyncio
    async def test_stored_values_are_used(self):
        svc = _service(
            advanced={"evaluation_max_rows": 50, "optimization_judge_samples": 1}
        )
        assert await svc.advanced_settings() == {
            "evaluation_max_rows": 50,
            "optimization_judge_samples": 1,
        }


class TestSettingsUpdateSchema:
    def test_omitted_advanced_fields_are_not_sent(self):
        sent = MLflowSettingsUpdate(enabled=True).model_dump(exclude_unset=True)
        assert "evaluation_max_rows" not in sent
        assert "optimization_judge_samples" not in sent

    def test_explicit_null_resets_to_default(self):
        sent = MLflowSettingsUpdate(evaluation_max_rows=None).model_dump(
            exclude_unset=True
        )
        assert sent == {"evaluation_max_rows": None}

    @pytest.mark.parametrize(
        "field,value",
        [
            ("evaluation_max_rows", 0),
            ("evaluation_max_rows", 10001),
            ("optimization_judge_samples", 0),
            ("optimization_judge_samples", 10),
        ],
    )
    def test_out_of_range_values_are_rejected(self, field, value):
        with pytest.raises(ValidationError):
            MLflowSettingsUpdate(**{field: value})


class TestLocalTrackingServer:
    """Configuration → MLflow local server (was MCP_SERVER_ENABLED +
    MLFLOW_TRACKING_URI at launch, which the UI could not set)."""

    def _svc(self, monkeypatch, stored, hosted=False):
        monkeypatch.setattr(mlflow_service_mod, "is_databricks_app", lambda: hosted)
        svc = _service()
        svc.repo.get_local_tracking_uri = AsyncMock(return_value=stored)
        svc.repo.set_local_tracking_uri = AsyncMock(return_value=True)
        return svc

    @pytest.mark.asyncio
    async def test_configured_server_is_used(self, monkeypatch):
        svc = self._svc(monkeypatch, "http://127.0.0.1:5555/")
        assert await svc.configured_local_uri() == "http://127.0.0.1:5555"

    @pytest.mark.asyncio
    async def test_nothing_configured_means_no_local_server(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
        monkeypatch.setenv("MCP_SERVER_ENABLED", "true")
        svc = self._svc(monkeypatch, None)
        assert await svc.configured_local_uri() is None

    @pytest.mark.asyncio
    async def test_never_inside_databricks_apps(self, monkeypatch):
        svc = self._svc(monkeypatch, "http://127.0.0.1:5555", hosted=True)
        assert await svc.configured_local_uri() is None
        with pytest.raises(ValueError, match="Databricks Apps"):
            await svc._set_local_tracking_uri("http://127.0.0.1:5555")

    @pytest.mark.asyncio
    async def test_save_validates_and_clears(self, monkeypatch):
        svc = self._svc(monkeypatch, None)
        with pytest.raises(ValueError, match="http"):
            await svc._set_local_tracking_uri("file:///tmp/mlruns")
        await svc._set_local_tracking_uri(" http://127.0.0.1:5555 ")
        svc.repo.set_local_tracking_uri.assert_awaited_with(
            "http://127.0.0.1:5555", group_id="group-1"
        )
        await svc._set_local_tracking_uri("")
        svc.repo.set_local_tracking_uri.assert_awaited_with(None, group_id="group-1")
