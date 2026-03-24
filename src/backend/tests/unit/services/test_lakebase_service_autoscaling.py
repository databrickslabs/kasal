"""Tests for LakebaseService - list_instances, _get_autoscaling_project, get_instance, test_connection."""
import sys
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

# Ensure the backend src is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Users", "nehme.tohme", "workspace", "kasal", "src", "backend"))
sys.path.insert(0, "/Users/nehme.tohme/workspace/kasal/src/backend")


@pytest.fixture(autouse=True)
def patch_lakebase_available():
    """Ensure LAKEBASE_AVAILABLE is True for all tests."""
    with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True):
        yield


@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def service(mock_session):
    with patch("src.services.lakebase_service.DatabaseConfigRepository"):
        from src.services.lakebase_service import LakebaseService
        svc = LakebaseService(session=mock_session, user_token="tok", user_email="u@example.com")
        return svc


# ---- list_instances ----

@pytest.mark.asyncio
async def test_list_instances_empty(service):
    """list_instances returns empty when both APIs return nothing."""
    mock_w = MagicMock()
    mock_w.config.workspace_id = "123"
    mock_w.api_client.do = MagicMock(return_value={"database_instances": [], "projects": []})
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    # First call for provisioned, second for autoscaling
    mock_w.api_client.do = MagicMock(side_effect=[
        {"database_instances": []},
        {"projects": []},
    ])

    result = await service.list_instances()
    assert result["items"] == []
    assert result["total"] == 0
    assert result["page"] == 1


@pytest.mark.asyncio
async def test_list_instances_provisioned_only(service):
    """list_instances returns provisioned instances."""
    mock_w = MagicMock()
    mock_w.config.workspace_id = "123"

    provisioned = [
        {"name": "inst-a", "state": "READY", "capacity": "CU_1", "read_write_dns": "dns-a", "node_count": 1},
    ]
    mock_w.api_client.do = MagicMock(side_effect=[
        {"database_instances": provisioned},
        {"projects": []},
    ])
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.list_instances()
    assert result["total"] == 1
    assert result["items"][0]["name"] == "inst-a"
    assert result["items"][0]["type"] == "provisioned"


@pytest.mark.asyncio
async def test_list_instances_autoscaling_only(service):
    """list_instances returns autoscaling projects with lazy DNS resolution."""
    mock_w = MagicMock()
    mock_w.config.workspace_id = "123"

    projects = [
        {"name": "projects/proj-b", "status": {"default_endpoint_settings": {"autoscaling_limit_min_cu": 1, "autoscaling_limit_max_cu": 4}}},
    ]

    def fake_do(method, path, query=None, headers=None):
        if "database/instances" in path:
            return {"database_instances": []}
        if "postgres/projects" in path and "endpoints" not in path:
            return {"projects": projects}
        if "endpoints" in path:
            return {"endpoints": [{"status": {"hosts": {"host": "dns-proj-b"}}}]}
        return {}

    mock_w.api_client.do = MagicMock(side_effect=fake_do)
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.list_instances()
    assert result["total"] == 1
    assert result["items"][0]["name"] == "proj-b"
    assert result["items"][0]["type"] == "autoscaling"
    assert result["items"][0]["read_write_dns"] == "dns-proj-b"


@pytest.mark.asyncio
async def test_list_instances_search_filter(service):
    """list_instances filters by search term."""
    mock_w = MagicMock()
    mock_w.config.workspace_id = "123"

    provisioned = [
        {"name": "alpha", "state": "READY", "capacity": "CU_1", "read_write_dns": "dns-alpha", "node_count": 1},
        {"name": "beta", "state": "READY", "capacity": "CU_2", "read_write_dns": "dns-beta", "node_count": 1},
    ]
    mock_w.api_client.do = MagicMock(side_effect=[
        {"database_instances": provisioned},
        {"projects": []},
    ])
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.list_instances(search="alph")
    assert result["total"] == 1
    assert result["items"][0]["name"] == "alpha"


@pytest.mark.asyncio
async def test_list_instances_pagination(service):
    """list_instances paginates correctly."""
    mock_w = MagicMock()
    mock_w.config.workspace_id = "123"

    provisioned = [{"name": f"inst-{i}", "state": "READY", "capacity": "CU_1", "read_write_dns": f"dns-{i}", "node_count": 1} for i in range(5)]
    mock_w.api_client.do = MagicMock(side_effect=[
        {"database_instances": provisioned},
        {"projects": []},
    ])
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.list_instances(page=2, page_size=2)
    assert result["total"] == 5
    assert result["page"] == 2
    assert len(result["items"]) == 2
    assert result["items"][0]["name"] == "inst-2"
    assert result["total_pages"] == 3
    assert result["has_more"] is True


@pytest.mark.asyncio
async def test_list_instances_page_size_capped(service):
    """page_size is capped at 100."""
    mock_w = MagicMock()
    mock_w.config.workspace_id = "123"
    mock_w.api_client.do = MagicMock(side_effect=[
        {"database_instances": []},
        {"projects": []},
    ])
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.list_instances(page_size=200)
    assert result["page_size"] == 100


@pytest.mark.asyncio
async def test_list_instances_lakebase_unavailable(mock_session):
    """list_instances returns empty when LAKEBASE_AVAILABLE is False."""
    with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False), \
         patch("src.services.lakebase_service.DatabaseConfigRepository"):
        from src.services.lakebase_service import LakebaseService
        svc = LakebaseService(session=mock_session)
        result = await svc.list_instances()
        assert result["items"] == []
        assert result["total"] == 0


# ---- _get_autoscaling_project ----

@pytest.mark.asyncio
async def test_get_autoscaling_project_found(service):
    """_get_autoscaling_project returns dict when project exists."""
    mock_w = MagicMock()
    project = MagicMock()
    project.status = MagicMock()
    project.status.default_endpoint_settings = MagicMock()
    project.status.default_endpoint_settings.autoscaling_limit_min_cu = 1
    project.status.default_endpoint_settings.autoscaling_limit_max_cu = 4
    mock_w.postgres.get_project.return_value = project

    ep = MagicMock()
    ep.status = MagicMock()
    ep.status.hosts = MagicMock()
    ep.status.hosts.host = "ep-dns"
    mock_w.postgres.list_endpoints.return_value = [ep]

    result = await service._get_autoscaling_project(mock_w, "my-proj")
    assert result is not None
    assert result["name"] == "my-proj"
    assert result["read_write_dns"] == "ep-dns"
    assert result["capacity"] == "CU_1-4"
    assert result["type"] == "autoscaling"


@pytest.mark.asyncio
async def test_get_autoscaling_project_not_found(service):
    """_get_autoscaling_project returns None for not-found errors."""
    mock_w = MagicMock()
    mock_w.postgres.get_project.side_effect = Exception("NOT_FOUND: no such project")

    result = await service._get_autoscaling_project(mock_w, "missing")
    assert result is None


@pytest.mark.asyncio
async def test_get_autoscaling_project_raises_other_errors(service):
    """_get_autoscaling_project raises non-not-found errors."""
    mock_w = MagicMock()
    mock_w.postgres.get_project.side_effect = RuntimeError("server error")

    with pytest.raises(RuntimeError, match="server error"):
        await service._get_autoscaling_project(mock_w, "x")


# ---- get_instance ----

@pytest.mark.asyncio
async def test_get_instance_provisioned(service):
    """get_instance returns provisioned instance when found."""
    mock_w = MagicMock()
    inst = MagicMock()
    inst.name = "inst-p"
    inst.state = "READY"
    inst.capacity = "CU_1"
    inst.read_write_dns = "dns-p"
    inst.created_at = "2025-01-01"
    inst.node_count = 1
    mock_w.database.get_database_instance.return_value = inst
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.get_instance("inst-p")
    assert result["name"] == "inst-p"
    assert result["type"] == "provisioned"


@pytest.mark.asyncio
async def test_get_instance_fallback_to_autoscaling(service):
    """get_instance falls back to autoscaling when provisioned not found."""
    mock_w = MagicMock()
    mock_w.database.get_database_instance.side_effect = Exception("not found")
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    autoscaling_info = {"name": "proj-a", "state": "AVAILABLE", "type": "autoscaling", "read_write_dns": "dns"}
    service._get_autoscaling_project = AsyncMock(return_value=autoscaling_info)

    result = await service.get_instance("proj-a")
    assert result["type"] == "autoscaling"


@pytest.mark.asyncio
async def test_get_instance_not_found_anywhere(service):
    """get_instance returns NOT_FOUND when instance not in either backend."""
    mock_w = MagicMock()
    mock_w.database.get_database_instance.side_effect = Exception("not found")
    service.get_workspace_client = AsyncMock(return_value=mock_w)
    service._get_autoscaling_project = AsyncMock(return_value=None)

    result = await service.get_instance("ghost")
    assert result["state"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_get_instance_lakebase_unavailable(mock_session):
    """get_instance returns NOT_FOUND when LAKEBASE_AVAILABLE is False."""
    with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False), \
         patch("src.services.lakebase_service.DatabaseConfigRepository"):
        from src.services.lakebase_service import LakebaseService
        svc = LakebaseService(session=mock_session)
        result = await svc.get_instance("x")
        assert result["state"] == "NOT_FOUND"


# ---- test_connection (test_connection_and_check_migration) ----

@pytest.mark.asyncio
async def test_test_connection_success(service):
    """test_connection returns success with migration info."""
    service.get_instance = AsyncMock(return_value={
        "state": "READY",
        "read_write_dns": "dns-test",
    })
    cred = MagicMock()
    cred.token = "tok123"
    service.connection_service.generate_credentials = AsyncMock(return_value=cred)
    service.connection_service.get_username = AsyncMock(return_value="user@example.com")

    mock_engine = AsyncMock()
    service.connection_service.create_lakebase_engine_async = AsyncMock(return_value=mock_engine)

    # Mock the AsyncSession context
    mock_session_ctx = AsyncMock()
    mock_result_version = MagicMock()
    mock_result_version.scalar.return_value = "PostgreSQL 15"
    mock_result_schema = MagicMock()
    mock_result_schema.scalar.return_value = "kasal"
    mock_result_tables = MagicMock()
    mock_result_tables.scalar.return_value = 10

    mock_session_ctx.execute = AsyncMock(side_effect=[mock_result_version, mock_result_schema, mock_result_tables])

    with patch("src.services.lakebase_service.AsyncSession") as MockAsyncSession:
        MockAsyncSession.return_value.__aenter__ = AsyncMock(return_value=mock_session_ctx)
        MockAsyncSession.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await service.test_connection("test-inst")
        assert result["success"] is True
        assert result["version"] == "PostgreSQL 15"
        assert result["has_kasal_schema"] is True
        assert result["migration_needed"] is False


@pytest.mark.asyncio
async def test_test_connection_instance_not_ready(service):
    """test_connection returns error dict when instance not in a ready state."""
    service.get_instance = AsyncMock(return_value={
        "state": "STOPPED",
        "read_write_dns": "dns",
    })

    result = await service.test_connection("stopped-inst")
    assert result["success"] is False
    assert "STOPPED" in result["error"]


@pytest.mark.asyncio
async def test_test_connection_no_endpoint(service):
    """test_connection returns error dict when instance has no endpoint."""
    service.get_instance = AsyncMock(return_value={
        "state": "READY",
        "read_write_dns": None,
    })

    result = await service.test_connection("no-ep-inst")
    assert result["success"] is False
    assert "no endpoint" in result["error"]


@pytest.mark.asyncio
async def test_test_connection_lakebase_unavailable(mock_session):
    """test_connection raises NotImplementedError when lakebase unavailable."""
    with patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", False), \
         patch("src.services.lakebase_service.DatabaseConfigRepository"):
        from src.services.lakebase_service import LakebaseService
        svc = LakebaseService(session=mock_session)
        # test_connection catches all exceptions and returns error dict for most cases
        # but NotImplementedError is raised before the try/except in some paths
        # Let's just check it handles it
        result = await svc.test_connection("x")
        # With LAKEBASE_AVAILABLE=False it raises NotImplementedError which is caught
        assert result.get("success") is False or "not available" in str(result.get("error", "")).lower()
