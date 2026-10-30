"""The local MLflow experiment: configured name, created or restored.

An experiment deleted in the MLflow UI used to switch tracing off for good
("Cannot set a deleted experiment ... as the active experiment"), and runs
traced to the per-teamspace default instead of the configured experiment.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.mlflow import local, mlflow_setup


class TestEnsureExperiment:
    def _client(self, experiment):
        client = MagicMock()
        client.get_experiment_by_name.return_value = experiment
        client.create_experiment.return_value = "7"
        return client

    def test_missing_experiment_is_created(self):
        client = self._client(None)
        with patch("mlflow.tracking.MlflowClient", return_value=client) as cls:
            assert local.ensure_experiment("http://127.0.0.1:5555", "kasal") == "7"
        cls.assert_called_once_with(tracking_uri="http://127.0.0.1:5555")
        client.create_experiment.assert_called_once_with("kasal")

    def test_deleted_experiment_is_restored(self):
        deleted = SimpleNamespace(experiment_id="1", lifecycle_stage="deleted")
        client = self._client(deleted)
        with patch("mlflow.tracking.MlflowClient", return_value=client):
            assert local.ensure_experiment("http://127.0.0.1:5555", "kasal") == "1"
        client.restore_experiment.assert_called_once_with("1")
        client.create_experiment.assert_not_called()

    def test_active_experiment_is_reused(self):
        active = SimpleNamespace(experiment_id="3", lifecycle_stage="active")
        client = self._client(active)
        with patch("mlflow.tracking.MlflowClient", return_value=client):
            assert local.ensure_experiment("http://127.0.0.1:5555", "kasal") == "3"
        client.restore_experiment.assert_not_called()


class TestSubprocessUsesTheConfiguredExperiment:
    def test_tracing_binds_the_configured_experiment(self, monkeypatch):
        monkeypatch.setattr(local, "is_reachable", lambda uri, timeout=2.0: True)
        ensure = MagicMock(return_value="5")
        monkeypatch.setattr(local, "ensure_experiment", ensure)
        fake_mlflow = MagicMock()
        with patch.dict("sys.modules", {"mlflow": fake_mlflow}):
            result = mlflow_setup._setup_local_mlflow(
                uri="http://127.0.0.1:5555",
                experiment="kasal",
                execution_id="e1",
                group_id="g1",
                alog=MagicMock(),
            )
        ensure.assert_called_once_with("http://127.0.0.1:5555", "kasal")
        fake_mlflow.set_experiment.assert_called_once_with(experiment_id="5")
        assert result.tracing_ready and result.experiment_name == "kasal"


class TestLocalExperimentName:
    @pytest.mark.asyncio
    async def test_local_server_uses_the_plain_name(self, monkeypatch):
        """Judges and GEPA watched "/Shared/kasal" while traces went to "kasal"."""
        from src.services.mlflow import service as service_mod
        from src.services.mlflow.service import MLflowService

        monkeypatch.setattr(
            service_mod.DatabricksAppInstallation,
            "from_env",
            classmethod(lambda cls: MagicMock(hosted=False, output_volume=None)),
        )
        svc = MLflowService(MagicMock(), group_id="g1")
        svc.repo = MagicMock()
        svc.repo.get_experiment_name = AsyncMock(return_value="kasal")
        svc._teamspace_name = AsyncMock(return_value="Team")
        svc._configured_workspace_url = AsyncMock(return_value=None)
        assert await svc.configured_crew_traces_experiment() == "kasal"
