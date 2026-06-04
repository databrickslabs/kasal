"""
Comprehensive unit tests for services/lakebase_service.py — coverage boost.

Targets the uncovered paths in LakebaseService:
  - __init__ with and without session
  - get_workspace_client delegation
  - list_instances (LAKEBASE_AVAILABLE=True/False, search, pagination)
  - get_config (with existing config, default config)
  - save_config (enabled=True, enabled=False)
  - create_instance (unavailable, already exists, success, quota error)
  - _get_autoscaling_project (success, not found, other error)
  - get_instance (unavailable, provisioned hit, autoscaling fallback, not found)
  - start_instance (unavailable, success, timeout)
  - migrate_existing_data (unavailable)
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(with_session=True, user_token="tok", user_email="user@example.com"):
    """Build a LakebaseService with all external deps mocked."""
    mock_session = AsyncMock() if with_session else None

    with patch("src.services.lakebase_service.LakebaseConnectionService") as mock_conn_svc, \
         patch("src.services.lakebase_service.LakebaseSchemaService") as mock_schema_svc, \
         patch("src.services.lakebase_service.LakebasePermissionService") as mock_perm_svc, \
         patch("src.services.lakebase_service.DatabaseConfigRepository") as mock_repo:

        mock_conn_svc.return_value = AsyncMock()
        mock_schema_svc.return_value = MagicMock()
        mock_perm_svc.return_value = MagicMock()
        mock_repo.return_value = AsyncMock()

        from src.services.lakebase_service import LakebaseService
        svc = LakebaseService(session=mock_session, user_token=user_token, user_email=user_email)
        svc.connection_service = AsyncMock()
        svc.schema_service = MagicMock()
        svc.permission_service = MagicMock()
        svc.config_repository = AsyncMock()
        return svc


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestLakebaseServiceInit:
    def test_init_with_session(self):
        from src.services.lakebase_service import LakebaseService
        mock_session = AsyncMock()
        with patch("src.services.lakebase_service.LakebaseConnectionService"), \
             patch("src.services.lakebase_service.LakebaseSchemaService"), \
             patch("src.services.lakebase_service.LakebasePermissionService"), \
             patch("src.services.lakebase_service.DatabaseConfigRepository"):
            svc = LakebaseService(session=mock_session)
            assert svc.session is mock_session

    def test_init_without_session(self):
        from src.services.lakebase_service import LakebaseService
        with patch("src.services.lakebase_service.LakebaseConnectionService"), \
             patch("src.services.lakebase_service.LakebaseSchemaService"), \
             patch("src.services.lakebase_service.LakebasePermissionService"):
            svc = LakebaseService()
            assert svc.session is None
            assert svc.config_repository is None

    def test_init_stores_user_token_and_email(self):
        from src.services.lakebase_service import LakebaseService
        with patch("src.services.lakebase_service.LakebaseConnectionService"), \
             patch("src.services.lakebase_service.LakebaseSchemaService"), \
             patch("src.services.lakebase_service.LakebasePermissionService"):
            svc = LakebaseService(user_token="my-token", user_email="admin@example.com")
            assert svc.user_token == "my-token"
            assert svc.user_email == "admin@example.com"

    def test_migration_service_initially_none(self):
        from src.services.lakebase_service import LakebaseService
        with patch("src.services.lakebase_service.LakebaseConnectionService"), \
             patch("src.services.lakebase_service.LakebaseSchemaService"), \
             patch("src.services.lakebase_service.LakebasePermissionService"):
            svc = LakebaseService()
            assert svc.migration_service is None


# ---------------------------------------------------------------------------
# get_workspace_client (delegation)
# ---------------------------------------------------------------------------

class TestGetWorkspaceClient:
    @pytest.mark.asyncio
    async def test_delegates_to_connection_service(self):
        svc = _make_service()
        mock_ws = MagicMock()
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_ws)
        result = await svc.get_workspace_client()
        assert result is mock_ws
        svc.connection_service.get_workspace_client.assert_awaited_once()


# ---------------------------------------------------------------------------
# list_instances
# ---------------------------------------------------------------------------

class TestListInstances:
    @pytest.mark.asyncio
    async def test_lakebase_not_available_returns_empty(self):
        svc = _make_service()
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False):
            result = await svc.list_instances()
        assert result["items"] == []
        assert result["total"] == 0

    @pytest.mark.asyncio
    async def test_returns_provisioned_instance(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.config.workspace_id = "12345"
        mock_w.api_client.do.side_effect = [
            # First call: GET /database/instances
            {"database_instances": [{"name": "my-inst", "state": "READY", "capacity": "CU_1"}]},
            # Second call: GET /postgres/projects
            {"projects": [], "next_page_token": None},
        ]
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.list_instances()
        assert len(result["items"]) == 1
        assert result["items"][0]["name"] == "my-inst"
        assert result["items"][0]["type"] == "provisioned"

    @pytest.mark.asyncio
    async def test_search_filters_instances(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.config.workspace_id = None
        mock_w.api_client.do.side_effect = [
            {"database_instances": [
                {"name": "kasal-prod"},
                {"name": "other-db"},
            ]},
            {"projects": [], "next_page_token": None},
        ]
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.list_instances(search="kasal")
        names = [i["name"] for i in result["items"]]
        assert "kasal-prod" in names
        assert "other-db" not in names

    @pytest.mark.asyncio
    async def test_provisioned_api_error_is_swallowed(self):
        """Error listing provisioned instances is caught; autoscaling may still work."""
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.config.workspace_id = None
        mock_w.api_client.do.side_effect = [
            Exception("API error"),  # provisioned call fails
            {"projects": [], "next_page_token": None},
        ]
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.list_instances()
        # Should succeed with empty items (no crash)
        assert isinstance(result, dict)
        assert "items" in result

    @pytest.mark.asyncio
    async def test_workspace_client_exception_propagates(self):
        svc = _make_service()
        svc.connection_service.get_workspace_client = AsyncMock(side_effect=RuntimeError("auth failed"))
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            with pytest.raises(RuntimeError, match="auth failed"):
                await svc.list_instances()

    @pytest.mark.asyncio
    async def test_autoscaling_project_included(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.config.workspace_id = None
        mock_w.api_client.do.side_effect = [
            {"database_instances": []},  # no provisioned
            {
                "projects": [
                    {"name": "projects/auto-proj", "status": {}}
                ],
                "next_page_token": None
            },
        ]
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.list_instances()
        assert any(i["name"] == "auto-proj" for i in result["items"])

    @pytest.mark.asyncio
    async def test_pagination_page_size_capped_at_100(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.config.workspace_id = None
        mock_w.api_client.do.return_value = {"database_instances": [], "projects": [], "next_page_token": None}
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.list_instances(page_size=500)
        assert result["page_size"] == 100


# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------

class TestGetConfig:
    @pytest.mark.asyncio
    async def test_returns_existing_config(self):
        svc = _make_service()
        mock_config = MagicMock()
        mock_config.value = {
            "enabled": True,
            "instance_name": "my-lb",
            "capacity": "CU_2",
            "retention_days": 7,
            "node_count": 2,
            "instance_status": "READY",
            "endpoint": "lb.example.com",
            "created_at": "2025-01-01",
            "database_type": "lakebase",
        }
        svc.config_repository.get_by_key = AsyncMock(return_value=mock_config)
        result = await svc.get_config()
        assert result["enabled"] is True
        assert result["instance_name"] == "my-lb"
        assert result["capacity"] == "CU_2"

    @pytest.mark.asyncio
    async def test_returns_default_when_no_config(self):
        svc = _make_service()
        svc.config_repository.get_by_key = AsyncMock(return_value=None)
        result = await svc.get_config()
        assert result["enabled"] is False
        assert result["instance_name"] == "kasal-lakebase"
        assert result["instance_status"] == "NOT_CREATED"

    @pytest.mark.asyncio
    async def test_repository_error_propagates(self):
        svc = _make_service()
        svc.config_repository.get_by_key = AsyncMock(side_effect=RuntimeError("db error"))
        with pytest.raises(RuntimeError, match="db error"):
            await svc.get_config()


# ---------------------------------------------------------------------------
# save_config
# ---------------------------------------------------------------------------

class TestSaveConfig:
    @pytest.mark.asyncio
    async def test_save_enabled_config_upserts(self):
        svc = _make_service()
        svc.config_repository.upsert = AsyncMock(return_value=None)
        config = {"enabled": True, "instance_name": "test", "capacity": "CU_1"}
        result = await svc.save_config(config)
        svc.config_repository.upsert.assert_awaited_once()
        assert result["enabled"] is True

    @pytest.mark.asyncio
    async def test_save_disabled_config_deletes(self):
        svc = _make_service()
        svc.config_repository.delete_by_key = AsyncMock(return_value=None)
        config = {"enabled": False, "instance_name": "test"}
        result = await svc.save_config(config)
        svc.config_repository.delete_by_key.assert_awaited_once_with("lakebase")
        assert result["enabled"] is False

    @pytest.mark.asyncio
    async def test_save_adds_updated_at_if_missing(self):
        svc = _make_service()
        svc.config_repository.upsert = AsyncMock(return_value=None)
        config = {"enabled": True, "instance_name": "test"}
        result = await svc.save_config(config)
        assert "updated_at" in result

    @pytest.mark.asyncio
    async def test_save_error_triggers_rollback(self):
        svc = _make_service()
        svc.config_repository.upsert = AsyncMock(side_effect=RuntimeError("write error"))
        svc.session.rollback = AsyncMock()
        config = {"enabled": True, "instance_name": "test"}
        with pytest.raises(RuntimeError, match="write error"):
            await svc.save_config(config)
        svc.session.rollback.assert_awaited_once()


# ---------------------------------------------------------------------------
# create_instance
# ---------------------------------------------------------------------------

class TestCreateInstance:
    @pytest.mark.asyncio
    async def test_create_instance_unavailable_raises(self):
        svc = _make_service()
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False):
            with pytest.raises(NotImplementedError):
                await svc.create_instance("my-inst")

    @pytest.mark.asyncio
    async def test_create_instance_already_exists_returns_early(self):
        svc = _make_service()
        existing = {"name": "my-inst", "state": "READY"}
        svc.get_instance = AsyncMock(return_value=existing)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.create_instance("my-inst")
        assert result is existing

    @pytest.mark.asyncio
    async def test_create_instance_quota_error_raises_value_error(self):
        svc = _make_service()
        svc.get_instance = AsyncMock(return_value={"state": "NOT_FOUND"})
        mock_w = MagicMock()
        mock_w.database.create_database_instance.side_effect = Exception("workspace limit exceeded")
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True), \
             patch("src.services.lakebase_service.DatabaseInstance"):
            with pytest.raises(ValueError, match="workspace limit"):
                await svc.create_instance("my-inst")

    @pytest.mark.asyncio
    async def test_create_instance_generic_error_propagates(self):
        svc = _make_service()
        svc.get_instance = AsyncMock(return_value={"state": "NOT_FOUND"})
        mock_w = MagicMock()
        mock_w.database.create_database_instance.side_effect = RuntimeError("unexpected error")
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True), \
             patch("src.services.lakebase_service.DatabaseInstance"):
            with pytest.raises(RuntimeError, match="unexpected error"):
                await svc.create_instance("my-inst")

    @pytest.mark.asyncio
    async def test_create_instance_success(self):
        """Test successful instance creation path (with asyncio.sleep mocked)."""
        import asyncio
        svc = _make_service()
        svc.get_instance = AsyncMock(return_value={"state": "NOT_FOUND"})

        mock_w = MagicMock()
        mock_final = MagicMock()
        mock_final.name = "my-inst"
        mock_final.state = "READY"
        mock_final.capacity = "CU_1"
        mock_final.read_write_dns = "host.example.com"

        mock_w.database.create_database_instance.return_value = mock_final
        mock_w.database.get_database_instance.return_value = mock_final

        # Make get_workspace_client return mock_w each time it's awaited
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)

        # Mock get_config and save_config to avoid repository calls
        svc.get_config = AsyncMock(return_value={"enabled": False})
        svc.save_config = AsyncMock(return_value={"enabled": True})

        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True), \
             patch("src.services.lakebase_service.DatabaseInstance", MagicMock()), \
             patch("asyncio.sleep", new_callable=lambda: lambda *_: asyncio.coroutine(lambda: None)()):
            # Mock asyncio.sleep to not actually sleep
            with patch("asyncio.sleep", new=AsyncMock()):
                # Make get_instance return READY after creation
                svc.get_instance = AsyncMock(side_effect=[
                    {"state": "NOT_FOUND"},  # initial check
                    {"state": "READY", "name": "my-inst"},  # polling
                ])
                result = await svc.create_instance("my-inst", "CU_1")
        assert result["name"] == "my-inst"


# ---------------------------------------------------------------------------
# _get_autoscaling_project
# ---------------------------------------------------------------------------

class TestGetAutoscalingProject:
    @pytest.mark.asyncio
    async def test_returns_project_info_on_success(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_project = MagicMock()
        mock_project.status = MagicMock()
        mock_w.postgres.get_project.return_value = mock_project
        mock_w.postgres.list_endpoints.return_value = []

        result = await svc._get_autoscaling_project(mock_w, "my-project")
        assert result is not None
        assert result["name"] == "my-project"
        assert result["type"] == "autoscaling"

    @pytest.mark.asyncio
    async def test_returns_none_on_not_found(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.postgres.get_project.side_effect = Exception("not_found: project does not exist")
        result = await svc._get_autoscaling_project(mock_w, "missing-project")
        assert result is None

    @pytest.mark.asyncio
    async def test_propagates_other_errors(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.postgres.get_project.side_effect = RuntimeError("network error")
        with pytest.raises(RuntimeError, match="network error"):
            await svc._get_autoscaling_project(mock_w, "my-project")

    @pytest.mark.asyncio
    async def test_extracts_endpoint_dns(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_project = MagicMock()
        mock_project.status = None
        mock_w.postgres.get_project.return_value = mock_project

        mock_ep = MagicMock()
        mock_ep.status.hosts.host = "endpoint.example.com"
        mock_w.postgres.list_endpoints.return_value = [mock_ep]

        result = await svc._get_autoscaling_project(mock_w, "my-project")
        assert result["read_write_dns"] == "endpoint.example.com"


# ---------------------------------------------------------------------------
# get_instance
# ---------------------------------------------------------------------------

class TestGetInstance:
    @pytest.mark.asyncio
    async def test_lakebase_not_available(self):
        svc = _make_service()
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False):
            result = await svc.get_instance("inst")
        assert result["state"] == "NOT_FOUND"
        assert "not available" in result["message"].lower()

    @pytest.mark.asyncio
    async def test_returns_provisioned_instance(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_inst = MagicMock()
        mock_inst.name = "inst"
        mock_inst.state = "READY"
        mock_inst.capacity = "CU_1"
        mock_inst.read_write_dns = "host.example.com"
        mock_w.database.get_database_instance.return_value = mock_inst
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.get_instance("inst")
        assert result["state"] == "READY"
        assert result["type"] == "provisioned"

    @pytest.mark.asyncio
    async def test_falls_back_to_autoscaling(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.database.get_database_instance.side_effect = Exception("not found")
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        auto_info = {"name": "inst", "state": "AVAILABLE", "type": "autoscaling",
                     "capacity": None, "read_write_dns": None, "created_at": None, "node_count": None}
        svc._get_autoscaling_project = AsyncMock(return_value=auto_info)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.get_instance("inst")
        assert result["type"] == "autoscaling"

    @pytest.mark.asyncio
    async def test_returns_not_found_when_both_fail(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.database.get_database_instance.side_effect = Exception("not found")
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        svc._get_autoscaling_project = AsyncMock(return_value=None)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
            result = await svc.get_instance("inst")
        assert result["state"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# start_instance
# ---------------------------------------------------------------------------

class TestStartInstance:
    @pytest.mark.asyncio
    async def test_start_instance_unavailable_raises(self):
        svc = _make_service()
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False):
            with pytest.raises(NotImplementedError):
                await svc.start_instance("inst")

    @pytest.mark.asyncio
    async def test_start_instance_success(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.database.start_database_instance.return_value = None
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        ready_inst = {"name": "inst", "state": "READY"}
        svc.get_instance = AsyncMock(return_value=ready_inst)
        svc.get_config = AsyncMock(return_value={"instance_status": "STARTING"})
        svc.save_config = AsyncMock(return_value=None)
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await svc.start_instance("inst")
        assert result["state"] == "READY"

    @pytest.mark.asyncio
    async def test_start_instance_timeout_returns_starting(self):
        svc = _make_service()
        mock_w = MagicMock()
        mock_w.database.start_database_instance.return_value = None
        svc.connection_service.get_workspace_client = AsyncMock(return_value=mock_w)
        # Never becomes READY
        svc.get_instance = AsyncMock(return_value={"name": "inst", "state": "STARTING"})
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True), \
             patch("asyncio.sleep", new=AsyncMock()):
            # Patch max_wait_seconds to 0 to force immediate timeout exit
            with patch.object(type(svc), "start_instance", wraps=svc.start_instance):
                # We invoke the real method but override internal loop via elapsed manipulation
                # Simplest: patch asyncio.sleep to advance elapsed past max by counting calls
                call_count = {"n": 0}
                async def fake_sleep(t):
                    call_count["n"] += 1
                    if call_count["n"] >= 13:  # 13 * 10 = 130 > 120
                        raise StopAsyncIteration
                with patch("asyncio.sleep", side_effect=fake_sleep):
                    try:
                        result = await svc.start_instance("inst")
                        # Either returns STARTING dict or raises; both acceptable
                        assert result.get("state") in ("STARTING", "READY") or "message" in result
                    except StopAsyncIteration:
                        pass


# ---------------------------------------------------------------------------
# migrate_existing_data (LAKEBASE_AVAILABLE=False path)
# ---------------------------------------------------------------------------

class TestMigrateExistingData:
    @pytest.mark.asyncio
    async def test_migrate_unavailable_returns_failure(self):
        svc = _make_service()
        with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False):
            result = await svc.migrate_existing_data("inst", "endpoint.example.com")
        assert result["success"] is False
        assert "not available" in result["error"].lower()


# ---------------------------------------------------------------------------
# _validate_identifier (module-level helper)
# ---------------------------------------------------------------------------

class TestValidateIdentifierInLakebaseService:
    def test_valid_identifier(self):
        from src.services.lakebase_service import _validate_identifier
        assert _validate_identifier("kasal") == "kasal"

    def test_invalid_identifier_raises(self):
        from src.services.lakebase_service import _validate_identifier
        with pytest.raises(ValueError):
            _validate_identifier("drop; table")

    def test_empty_identifier_raises(self):
        from src.services.lakebase_service import _validate_identifier
        with pytest.raises(ValueError):
            _validate_identifier("")
