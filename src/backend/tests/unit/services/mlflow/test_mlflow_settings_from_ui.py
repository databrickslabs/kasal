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
            svc._validated_local_tracking_uri("http://127.0.0.1:5555")

    def test_validation_normalizes_and_clears(self, monkeypatch):
        svc = self._svc(monkeypatch, None)
        with pytest.raises(ValueError, match="http"):
            svc._validated_local_tracking_uri("file:///tmp/mlruns")
        assert (
            svc._validated_local_tracking_uri(" http://127.0.0.1:5555/ ")
            == "http://127.0.0.1:5555"
        )
        assert svc._validated_local_tracking_uri("") == ""

    @pytest.mark.parametrize(
        "uri",
        [
            "http://127.0.0.1:5555",
            "http://localhost:5000",
            "http://LOCALHOST:5000",
            "http://[::1]:5555",
            "http://127.0.0.2:8080",
            "https://mlflow.example.com",
            "https://mlflow.example.com:8443/base",
        ],
    )
    def test_accepted_servers(self, monkeypatch, uri):
        svc = self._svc(monkeypatch, None)
        assert svc._validated_local_tracking_uri(uri) == uri

    @pytest.mark.parametrize(
        "uri, reason",
        [
            ("http://mlflow.example.com", "Plain http"),
            ("http://10.0.0.5:5000", "Plain http"),
            ("http://localhost.example.com", "Plain http"),
            ("http://0.0.0.0:5000", "Plain http"),
            ("https://user:pw@mlflow.example.com", "credentials"),
            ("http://token@127.0.0.1:5555", "credentials"),
            ("ftp://127.0.0.1", "http"),
            ("databricks", "http"),
            ("https://", "host"),
            ("http://127.0.0.1:notaport", "Invalid"),
        ],
    )
    def test_rejected_servers(self, monkeypatch, uri, reason):
        svc = self._svc(monkeypatch, None)
        with pytest.raises(ValueError, match=reason):
            svc._validated_local_tracking_uri(uri)

    @pytest.mark.asyncio
    async def test_a_stored_value_that_breaks_the_rule_is_ignored(self, monkeypatch):
        """Saved before the rule existed (or written straight to the DB): it
        must not keep redirecting traces."""
        svc = self._svc(monkeypatch, "http://attacker.example.com:5000")
        assert await svc.configured_local_uri() is None


class TestUpdateSettingsIsAllOrNothing:
    """A rejected field must leave the stored settings untouched — the repository
    used to commit per setter, so earlier fields were saved before a 400."""

    def _svc(self, monkeypatch):
        monkeypatch.setattr(mlflow_service_mod, "is_databricks_app", lambda: False)
        session = MagicMock()
        session.commit = AsyncMock()
        svc = MLflowService(session, group_id="group-1")
        svc.repo = MagicMock()
        for setter in (
            "set_enabled",
            "set_evaluation_enabled",
            "set_experiment_name",
            "set_evaluation_judge_model",
            "set_advanced",
            "set_local_tracking_uri",
        ):
            setattr(svc.repo, setter, AsyncMock(return_value=True))
        svc._ensure_experiment_created = AsyncMock()
        svc.get_settings = AsyncMock(return_value={"saved": True})
        return svc, session

    @pytest.mark.asyncio
    async def test_invalid_uri_writes_nothing(self, monkeypatch):
        svc, session = self._svc(monkeypatch)
        with pytest.raises(ValueError):
            await svc.update_settings(
                evaluation_enabled=True,
                experiment_name="exp",
                local_tracking_uri="http://evil.example.com",
            )
        svc.repo.set_evaluation_enabled.assert_not_awaited()
        svc.repo.set_experiment_name.assert_not_awaited()
        svc.repo.set_local_tracking_uri.assert_not_awaited()
        session.commit.assert_not_awaited()
        svc._ensure_experiment_created.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_valid_update_commits_once_before_provisioning(self, monkeypatch):
        svc, session = self._svc(monkeypatch)
        order: list = []
        session.commit.side_effect = lambda: order.append("commit")
        svc._ensure_experiment_created.side_effect = lambda: order.append("provision")
        monkeypatch.setattr(svc, "_invalidate_parent_setup", lambda: None)
        out = await svc.update_settings(
            enabled=True, local_tracking_uri=" http://127.0.0.1:5555/ "
        )
        assert out == {"saved": True}
        svc.repo.set_local_tracking_uri.assert_awaited_once_with(
            "http://127.0.0.1:5555", group_id="group-1"
        )
        session.commit.assert_awaited_once()
        assert order == ["commit", "provision"]

    @pytest.mark.asyncio
    async def test_empty_uri_clears(self, monkeypatch):
        svc, _ = self._svc(monkeypatch)
        await svc.update_settings(local_tracking_uri="")
        svc.repo.set_local_tracking_uri.assert_awaited_once_with(
            None, group_id="group-1"
        )
        svc._ensure_experiment_created.assert_not_awaited()
