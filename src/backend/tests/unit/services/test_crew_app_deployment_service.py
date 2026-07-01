"""
Unit tests for CrewAppDeploymentService (one-click Databricks Apps deploy).
"""

from contextlib import contextmanager, nullcontext
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


@contextmanager
def _patch_deploy_pat(pat_groups, capture=None):
    """Patch the deploy's DIRECT PAT-resolution chain (bypasses get_auth_context).

    Deploy looks the PAT up straight from the DB on the request session, then
    builds a WorkspaceClient(auth_type="pat"). This patches every step:
      - ApiKeysService.find_by_name → returns a fake DATABRICKS_API_KEY only for
        the group_ids in ``pat_groups``;
      - EncryptionUtils.decrypt_value → returns a fake token;
      - the workspace-host lookup and _clean_environment;
      - WorkspaceClient (so no real SDK client is built).

    ``capture`` (optional dict) receives ``groups`` = the group_ids tried, in order.
    """
    tried = capture if capture is not None else {}
    tried.setdefault("groups", [])

    class _FakeApiKeysService:
        def __init__(self, session, group_id=None):
            self.group_id = group_id
            tried["groups"].append(group_id)

        async def find_by_name(self, name):
            if name == "DATABRICKS_API_KEY" and self.group_id in pat_groups:
                key = MagicMock()
                key.encrypted_value = f"enc::{self.group_id}"
                return key
            return None

    dbx = MagicMock()
    dbx._load_config = AsyncMock(return_value=True)
    dbx._workspace_host = "https://ws.example.com"

    with (
        patch("src.services.api_keys_service.ApiKeysService", _FakeApiKeysService),
        patch(
            "src.utils.encryption_utils.EncryptionUtils.decrypt_value",
            side_effect=lambda v: f"pat-token-{v}",
        ),
        patch("src.utils.databricks_auth._databricks_auth", dbx),
        patch("src.utils.databricks_auth._clean_environment", lambda: nullcontext()),
        patch("databricks.sdk.WorkspaceClient", MagicMock()),
        patch.object(CrewAppDeploymentService, "_run_deployment", new=AsyncMock()),
    ):
        yield tried


class TestStartDeployment:
    @pytest.mark.asyncio
    async def test_resolves_pat_from_request_group(self, service, obo_context):
        """Deploy resolves a PAT directly from the DB for the request's group and
        never forwards the OBO token."""
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with _patch_deploy_pat({"g1"}) as tried:
            resp = await service.start_deployment(
                "crew-1", AppDeploymentConfig(), obo_context
            )
        assert resp.status == STATUS_PENDING
        assert "g1" in tried["groups"]

    @pytest.mark.asyncio
    async def test_raises_when_no_pat_configured(self, service, obo_context):
        """No PAT under any of the user's groups → clear PAT-required error
        (deploy never falls back to OBO/SPN)."""
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with _patch_deploy_pat(set()):
            with pytest.raises(PermissionError, match="PAT"):
                await service.start_deployment(
                    "crew-1", AppDeploymentConfig(), obo_context
                )

    @pytest.mark.asyncio
    async def test_finds_pat_under_a_non_primary_group(self, service):
        """The PAT may be configured under ANY of the user's groups, not just the
        primary (group_ids[0]). Here only g2 has one — deploy still succeeds."""
        ctx = GroupContext(
            group_ids=["g1", "g2"], group_email="u@e.com", access_token=None
        )
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with _patch_deploy_pat({"g2"}) as tried:
            resp = await service.start_deployment("crew-1", AppDeploymentConfig(), ctx)
        assert resp.status == STATUS_PENDING
        # g1 tried first (no PAT), then g2 (found) — stops before the personal group.
        assert tried["groups"] == ["g1", "g2"]

    @pytest.mark.asyncio
    async def test_finds_pat_under_personal_workspace_with_strict_isolation(self, service):
        """With strict isolation, a request scoped to a shared workspace carries
        ONLY that group_id. A PAT under the user's PERSONAL workspace must still be
        found — deploy adds the personal workspace id (derived from the email)."""
        ctx = GroupContext(
            group_ids=["shared_ws"], group_email="alice@acme.com", access_token=None
        )
        personal_gid = "user_alice_acme_com"
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with _patch_deploy_pat({personal_gid}) as tried:
            resp = await service.start_deployment("crew-1", AppDeploymentConfig(), ctx)
        assert resp.status == STATUS_PENDING
        assert tried["groups"] == ["shared_ws", personal_gid]

    @pytest.mark.asyncio
    async def test_registers_pending_and_returns_id(self, service, obo_context):
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with _patch_deploy_pat({"g1"}):
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
            warehouse_id="wh-1",
        )
        with _patch_deploy_pat({"g1"}):
            await service.start_deployment("crew-1", cfg, obo_context)

        opts = captured["options"]
        assert opts.model_override == "databricks-claude-sonnet-4-5"
        assert opts.databricks_catalog == "main"
        assert opts.databricks_schema == "agents"
        assert opts.databricks_warehouse_id == "wh-1"
        # The app creates the experiment itself — the deploy just passes the name.
        assert opts.mlflow_experiment_name == "news-exp"

    @pytest.mark.asyncio
    async def test_experiment_name_defaults_to_app_name_when_blank(
        self, service, obo_context
    ):
        """With no experiment_name, the app's experiment defaults to the app name."""
        captured = {}

        async def fake_export(*, crew_id, export_format, options, group_context):
            captured["options"] = options
            return _export_result()

        service.export_service.export_crew = fake_export
        cfg = AppDeploymentConfig(app_name="oracle")  # no experiment_name
        with _patch_deploy_pat({"g1"}):
            await service.start_deployment("crew-1", cfg, obo_context)
        assert captured["options"].mlflow_experiment_name == "oracle"

    @pytest.mark.asyncio
    async def test_defaults_app_name_from_metadata(self, service, obo_context):
        service.export_service.export_crew = AsyncMock(return_value=_export_result())
        with _patch_deploy_pat({"g1"}):
            resp = await service.start_deployment(
                "crew-1", AppDeploymentConfig(), obo_context
            )
        assert resp.app_name == "research-crew"


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

    def test_configures_otel_telemetry_no_experiment_resource(self, service):
        self._register(service)
        client = self._mock_client(app_exists=False)  # get: [not found, url_app]

        service._deploy_blocking(
            "d-1",
            client,
            "research-crew",
            [{"path": "app.yaml", "content": "c"}],
            "main",
            "agents",
        )

        client.apps.update.assert_called_once()
        app = client.apps.update.call_args.kwargs["app"]
        # The experiment is NOT attached as a resource — the app owns/creates its
        # own UC-bound experiment (UC binding is creation-only).
        exp_res = [r for r in (app.resources or []) if r.name == "experiment"]
        assert not exp_res
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

    def test_creates_new_lakebase_and_attaches_resource(self, service):
        self._register(service)
        client = self._mock_client(app_exists=False)
        # Instance does not exist yet -> _ensure_lakebase_instance creates it.
        client.database.get_database_instance.side_effect = Exception("not found")

        service._deploy_blocking(
            "d-1",
            client,
            "research-crew",
            [{"path": "app.yaml", "content": "c"}],
            "",  # catalog
            "",  # schema
            "my-lakebase",  # lakebase_instance
            True,  # create_lakebase
        )

        client.database.create_database_instance_and_wait.assert_called_once()
        app = client.apps.update.call_args.kwargs["app"]
        db_res = [r for r in (app.resources or []) if r.name == "database"]
        assert db_res and db_res[0].database.instance_name == "my-lakebase"
        assert service.get_status("d-1").status == STATUS_SUCCEEDED

    def test_attaches_existing_lakebase_without_creating(self, service):
        self._register(service)
        client = self._mock_client(app_exists=False)

        service._deploy_blocking(
            "d-1",
            client,
            "research-crew",
            [{"path": "app.yaml", "content": "c"}],
            "",  # catalog
            "",  # schema
            "existing-lb",  # lakebase_instance
            False,  # create_lakebase -> attach only
        )

        client.database.create_database_instance_and_wait.assert_not_called()
        app = client.apps.update.call_args.kwargs["app"]
        db_res = [r for r in (app.resources or []) if r.name == "database"]
        assert db_res and db_res[0].database.instance_name == "existing-lb"
        assert service.get_status("d-1").status == STATUS_SUCCEEDED


    def test_grants_explicit_uc_trace_permissions_to_app_sp(self, service):
        """The app SP gets EXPLICIT CREATE TABLE/MODIFY/SELECT (+USE) — ALL_PRIVILEGES
        is not sufficient for UC trace tables."""
        self._register(service)
        client = MagicMock()
        client.apps.get.return_value.service_principal_client_id = "sp-abc"

        service._grant_trace_permissions(
            "d-1", client, "research-crew", "nemotemo_catalog", "kasal", "wh-1"
        )

        stmts = [
            c.kwargs["statement"]
            for c in client.statement_execution.execute_statement.call_args_list
        ]
        assert any("USE CATALOG" in s and "sp-abc" in s for s in stmts)
        assert any(
            "CREATE TABLE" in s and "MODIFY" in s and "SELECT" in s and "sp-abc" in s
            for s in stmts
        )
        # Run via the configured SQL warehouse.
        assert (
            client.statement_execution.execute_statement.call_args.kwargs["warehouse_id"]
            == "wh-1"
        )

    def test_grant_trace_permissions_is_best_effort(self, service):
        """A grant failure (e.g. deployer doesn't own the schema) must not fail the
        deploy — it records a hint and continues."""
        self._register(service)
        client = MagicMock()
        client.apps.get.return_value.service_principal_client_id = "sp-1"
        client.statement_execution.execute_statement.side_effect = Exception(
            "no grant authority"
        )
        service._grant_trace_permissions("d-1", client, "research-crew", "c", "s", "wh")
        assert "Could not grant" in (service.get_status("d-1").message or "")

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


class TestListLakebaseInstances:
    @pytest.mark.asyncio
    async def test_lists_instances(self, service, obo_context):
        inst = MagicMock()
        inst.name = "lb-1"
        inst.state = MagicMock(value="AVAILABLE")
        inst.capacity = "CU_1"
        client = MagicMock()
        client.database.list_database_instances.return_value = [inst]
        with patch.object(
            service, "_get_deploy_client", new=AsyncMock(return_value=client)
        ):
            result = await service.list_lakebase_instances(group_context=obo_context)
        assert result == [{"name": "lb-1", "state": "AVAILABLE", "capacity": "CU_1"}]

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self, service, obo_context):
        client = MagicMock()
        client.database.list_database_instances.side_effect = Exception("boom")
        with patch.object(
            service, "_get_deploy_client", new=AsyncMock(return_value=client)
        ):
            result = await service.list_lakebase_instances(group_context=obo_context)
        assert result == []

    @pytest.mark.asyncio
    async def test_raises_when_auth_fails(self, service, obo_context):
        # No PAT configured → _get_deploy_client raises; list surfaces it.
        with patch.object(
            service,
            "_get_deploy_client",
            new=AsyncMock(side_effect=PermissionError("PAT required")),
        ):
            with pytest.raises(PermissionError):
                await service.list_lakebase_instances(group_context=obo_context)


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
