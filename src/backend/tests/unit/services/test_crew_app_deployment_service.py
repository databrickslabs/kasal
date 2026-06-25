"""
Unit tests for CrewAppDeploymentService (one-click Databricks Apps deploy).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.crew_export import AppDeploymentConfig
from src.services.crew_app_deployment_service import (
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SUCCEEDED,
    CrewAppDeploymentService,
)
from src.utils.user_context import GroupContext


@pytest.fixture(autouse=True)
def clear_registry():
    CrewAppDeploymentService._deployments.clear()
    yield
    CrewAppDeploymentService._deployments.clear()


@pytest.fixture
def service():
    return CrewAppDeploymentService(session=MagicMock())


@pytest.fixture
def obo_context():
    return GroupContext(
        group_ids=["g1"], group_email="user@example.com", access_token="tok-123"
    )


def _export_result():
    return {
        "files": [
            {
                "path": "app.yaml",
                "content": "command: [uv, run, start-app]",
                "type": "yaml",
            },
            {"path": "agent_server/agent.py", "content": "x = 1\n", "type": "python"},
        ],
        "metadata": {"app_name": "research-crew"},
    }


class TestStartDeployment:
    @pytest.mark.asyncio
    async def test_falls_back_when_no_obo_token(self, service):
        """With no OBO token, deploy proceeds via the PAT/SPN fallback chain."""
        ctx = GroupContext(group_ids=["g1"], group_email="u@e.com", access_token=None)
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        mock_get = AsyncMock(return_value=MagicMock())
        with (
            patch(
                "src.services.crew_app_deployment_service.get_workspace_client",
                new=mock_get,
            ),
            patch.object(CrewAppDeploymentService, "_run_deployment", new=AsyncMock()),
        ):
            resp = await service.start_deployment("crew-1", AppDeploymentConfig(), ctx)

        assert resp.status == STATUS_PENDING
        # Fallback path: called with no user token but the group_id for PAT lookup.
        assert mock_get.await_args.kwargs.get("user_token") is None
        assert mock_get.await_args.kwargs.get("group_id") == "g1"

    @pytest.mark.asyncio
    async def test_registers_pending_and_returns_id(self, service, obo_context):
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with (
            patch(
                "src.services.crew_app_deployment_service.get_workspace_client",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch.object(CrewAppDeploymentService, "_run_deployment", new=AsyncMock()),
        ):
            resp = await service.start_deployment(
                "crew-1", AppDeploymentConfig(app_name="My App"), obo_context
            )

        assert resp.status == STATUS_PENDING
        assert resp.app_name == "my-app"  # normalized
        assert resp.crew_id == "crew-1"
        status = service.get_status(resp.deployment_id)
        assert status is not None and status.status == STATUS_PENDING

    @pytest.mark.asyncio
    async def test_threads_model_catalog_schema_experiment(self, service, obo_context):
        """Deploy-screen selections are threaded into the export options."""
        captured = {}

        async def fake_export(*, crew_id, export_format, options, group_context):
            captured["options"] = options
            return _export_result()

        service.export_service.export_crew = fake_export
        cfg = AppDeploymentConfig(
            model="databricks-claude-sonnet-4-5",
            catalog="main",
            schema_name="agents",
            experiment_name="news-exp",
        )
        with (
            patch(
                "src.services.crew_app_deployment_service.get_workspace_client",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch.object(
                CrewAppDeploymentService,
                "_resolve_experiment",
                return_value="exp-123",
            ),
            patch.object(CrewAppDeploymentService, "_run_deployment", new=AsyncMock()),
        ):
            await service.start_deployment("crew-1", cfg, obo_context)

        opts = captured["options"]
        assert opts.model_override == "databricks-claude-sonnet-4-5"
        assert opts.databricks_catalog == "main"
        assert opts.databricks_schema == "agents"
        assert opts.experiment_id == "exp-123"

    @pytest.mark.asyncio
    async def test_resolve_experiment_creates_under_user(self, service):
        client = MagicMock()
        client.current_user.me.return_value.user_name = "u@e.com"
        client.experiments.create_experiment.return_value.experiment_id = "exp-9"
        exp_id = CrewAppDeploymentService._resolve_experiment(client, "my-exp")
        assert exp_id == "exp-9"
        # Bare name is namespaced under the user.
        created_name = client.experiments.create_experiment.call_args.kwargs["name"]
        assert created_name == "/Users/u@e.com/my-exp"

    @pytest.mark.asyncio
    async def test_defaults_app_name_from_metadata(self, service, obo_context):
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with (
            patch(
                "src.services.crew_app_deployment_service.get_workspace_client",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch.object(CrewAppDeploymentService, "_run_deployment", new=AsyncMock()),
        ):
            resp = await service.start_deployment(
                "crew-1", AppDeploymentConfig(), obo_context
            )
        assert resp.app_name == "research-crew"

    @pytest.mark.asyncio
    async def test_auth_failure_raises(self, service, obo_context):
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with patch(
            "src.services.crew_app_deployment_service.get_workspace_client",
            new=AsyncMock(return_value=None),
        ):
            with pytest.raises(PermissionError):
                await service.start_deployment(
                    "crew-1", AppDeploymentConfig(), obo_context
                )


class TestDeployBlocking:
    def _mock_client(self, app_exists=False):
        client = MagicMock()
        client.current_user.me.return_value.user_name = "user@example.com"
        # apps.get is called: (1) existence check, (2) config read-back in
        # _configure_app, (3) final URL fetch. The existence check raises when the
        # app doesn't exist yet.
        readback = MagicMock(resources=[])
        url_app = MagicMock(url="https://research-crew.databricksapps.com")
        existence = MagicMock() if app_exists else Exception("not found")
        client.apps.get.side_effect = [existence, readback, url_app]
        waiter = MagicMock()
        waiter.result.return_value.deployment_id = "dep-1"
        client.apps.deploy.return_value = waiter
        client.config.host = "https://workspace.databricks.com"
        return client

    def _register(self, service, deployment_id="d-1", app_name="research-crew"):
        service._deployments[deployment_id] = {
            "deployment_id": deployment_id,
            "crew_id": "crew-1",
            "app_name": app_name,
            "status": STATUS_PENDING,
            "step": "QUEUED",
            "message": "queued",
            "app_url": None,
            "error": None,
        }

    def test_creates_uploads_deploys_starts(self, service):
        self._register(service)
        client = self._mock_client(app_exists=False)
        files = [
            {"path": "app.yaml", "content": "c", "type": "yaml"},
            {"path": "agent_server/agent.py", "content": "x=1", "type": "python"},
        ]

        service._deploy_blocking("d-1", client, "research-crew", files)

        # Created (did not exist), uploaded every file, deployed, started.
        client.apps.create_and_wait.assert_called_once()
        assert client.workspace.upload.call_count == len(files)
        assert client.workspace.mkdirs.called
        client.apps.deploy.assert_called_once()
        client.apps.start.assert_called_once_with("research-crew")

        # OAuth scopes are set via a single clean apps.update (not the get() result).
        client.apps.update.assert_called_once()
        scopes = client.apps.update.call_args.kwargs["app"].user_api_scopes
        assert "genie" in scopes and "vector-search" in scopes and "sql" in scopes

        status = service.get_status("d-1")
        assert status.status == STATUS_SUCCEEDED
        assert status.app_url == "https://research-crew.databricksapps.com"

    def test_configures_experiment_and_otel_telemetry(self, service):
        self._register(service)
        client = self._mock_client(app_exists=False)  # get: [not found, url_app]

        service._deploy_blocking(
            "d-1",
            client,
            "research-crew",
            [{"path": "app.yaml", "content": "c"}],
            "exp-77",
            "main",
            "agents",
        )

        client.apps.update.assert_called_once()
        app = client.apps.update.call_args.kwargs["app"]
        # Experiment attached as a resource (SP can write traces).
        exp_res = [r for r in (app.resources or []) if r.name == "experiment"]
        assert exp_res and exp_res[0].experiment.experiment_id == "exp-77"
        # OTel -> Unity Catalog telemetry destination from catalog/schema.
        dests = app.telemetry_export_destinations
        assert dests and dests[0].unity_catalog.logs_table == "main.agents.otel_logs"
        assert dests[0].unity_catalog.traces_table == "main.agents.otel_spans"
        assert service.get_status("d-1").status == STATUS_SUCCEEDED

    def test_skips_create_when_app_exists(self, service):
        self._register(service)
        client = self._mock_client(app_exists=True)
        service._deploy_blocking(
            "d-1", client, "research-crew", [{"path": "app.yaml", "content": "c"}]
        )
        client.apps.create_and_wait.assert_not_called()
        assert service.get_status("d-1").status == STATUS_SUCCEEDED

    @pytest.mark.asyncio
    async def test_run_deployment_records_failure(self, service):
        self._register(service)
        with patch.object(
            service, "_deploy_blocking", side_effect=RuntimeError("boom")
        ):
            await service._run_deployment("d-1", MagicMock(), "research-crew", [])
        status = service.get_status("d-1")
        assert status.status == STATUS_FAILED
        assert "boom" in (status.error or "")


class TestNormalizeAppName:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("My Crew!! 2025", "my-crew-2025"),
            ("123abc", "agent-123abc"),
            (None, None),
            ("", None),
        ],
    )
    def test_normalize(self, raw, expected):
        assert CrewAppDeploymentService._normalize_app_name(raw) == expected
