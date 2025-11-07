import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from fastapi import HTTPException

from src.api.database_management_router import (
    export_database,
    import_database,
    list_backups,
    get_database_info,
    debug_permissions,
    debug_headers,
    check_database_management_permission,
    get_lakebase_config,
    save_lakebase_config,
    create_lakebase_instance,
    get_lakebase_instance,
    check_lakebase_tables,
    migrate_to_lakebase,
    start_lakebase_instance,
    get_lakebase_workspace_info,
    enable_lakebase_without_migration,
)
import importlib
from src.schemas.database_management import (
    ExportRequest,
    ImportRequest,
    ListBackupsRequest,
)


class Ctx:
    def __init__(self, user_role=None, primary_group_id="g1", group_email="u@x", access_token="tok", is_system_admin=False):
        self.user_role = user_role
        self.primary_group_id = primary_group_id
        self.group_email = group_email
        self.access_token = access_token
        # Add current_user attribute for system admin checks
        self.current_user = type('obj', (object,), {'is_system_admin': is_system_admin})()


@pytest.mark.asyncio
async def test_export_import_permissions_and_success():
    svc = AsyncMock()
    ctx_user = Ctx(user_role="user", is_system_admin=False)
    ctx_system_admin = Ctx(user_role="admin", is_system_admin=True)

    # Non-system-admin export -> 403
    with pytest.raises(HTTPException) as ei:
        await export_database(ExportRequest(), service=svc, group_context=ctx_user)
    assert ei.value.status_code == 403

    # System admin export -> success
    svc.export_to_volume = AsyncMock(return_value={"success": True, "backup_filename": "b.db"})
    out = await export_database(ExportRequest(), service=svc, group_context=ctx_system_admin)
    assert out.success is True

    # Non-system-admin import -> 403
    with pytest.raises(HTTPException):
        await import_database(ImportRequest(catalog="c", schema="s", volume_name="v", backup_filename="b.db"), service=svc, group_context=ctx_user)

    # System admin import -> success
    svc.import_from_volume = AsyncMock(return_value={"success": True, "imported_from": "v/b.db"})
    out2 = await import_database(ImportRequest(catalog="c", schema="s", volume_name="v", backup_filename="b.db"), service=svc, group_context=ctx_system_admin)
    assert out2.success is True


@pytest.mark.asyncio
async def test_list_info_and_permission_checks():
    svc = AsyncMock()
    ctx = Ctx(user_role="admin", is_system_admin=True)

    # list_backups
    svc.list_backups = AsyncMock(return_value={"success": True, "backups": []})
    out = await list_backups(ListBackupsRequest(), service=svc, group_context=ctx)
    assert out.success is True

    # get_database_info
    svc.get_database_info = AsyncMock(return_value={"success": True, "database_type": "sqlite"})
    out2 = await get_database_info(service=svc, group_context=ctx)
    assert out2.database_type == "sqlite"

    # check_database_management_permission returns raw dict
    svc.check_user_permission = AsyncMock(return_value={"has_permission": True, "is_databricks_apps": False})
    out3 = await check_database_management_permission(service=svc, session=AsyncMock(), group_context=ctx)
    assert out3["has_permission"] is True


@pytest.mark.asyncio
async def test_debug_permissions_and_headers_minimal():
    # No env set, unified auth likely missing -> returns missing configuration quickly
    ctx = Ctx()
    out = await debug_permissions(session=AsyncMock(), group_context=ctx)
    assert "error" in out

    # debug_headers with minimal request
    req = SimpleNamespace(headers={})
    out2 = await debug_headers(request=req, group_context=ctx)
    assert "all_headers" in out2 and "environment" in out2


@pytest.mark.asyncio
async def test_lakebase_endpoints_success_and_validations():
    svc = AsyncMock()

    # Config get/save
    svc.get_config = AsyncMock(return_value={"enabled": False})
    out = await get_lakebase_config(service=svc)
    assert out["enabled"] is False

    svc.save_config = AsyncMock(return_value={"enabled": True})
    out2 = await save_lakebase_config({"enabled": True}, service=svc)
    assert out2["enabled"] is True

    # Create instance
    svc.create_instance = AsyncMock(return_value={"instance_name": "kasal-lakebase"})
    out3 = await create_lakebase_instance({"instance_name": "kasal-lakebase"}, service=svc)
    assert out3["instance_name"] == "kasal-lakebase"

    # Get instance
    svc.get_instance = AsyncMock(return_value={"name": "kasal-lakebase"})
    out4 = await get_lakebase_instance("kasal-lakebase", service=svc)
    assert out4["name"] == "kasal-lakebase"

    # Check tables
    svc.check_lakebase_tables = AsyncMock(return_value={"tables": {}})
    out5 = await check_lakebase_tables(service=svc)
    assert "tables" in out5

    # Migrate: missing endpoint -> function wraps into 500
    with pytest.raises(HTTPException) as ei:
        await migrate_to_lakebase({"instance_name": "kasal-lakebase"}, service=svc)
    assert ei.value.status_code == 500

    # Start instance: missing instance_name -> 400
    with pytest.raises(HTTPException):
        await start_lakebase_instance({}, service=svc)

    # Start instance: success
    svc.start_instance = AsyncMock(return_value={"status": "running"})
    out6 = await start_lakebase_instance({"instance_name": "kasal-lakebase"}, service=svc)
    assert out6["status"] == "running"

    # Test connection GET/POST via module to avoid pytest name collision
    m = importlib.import_module('src.api.database_management_router')
    svc.test_connection = AsyncMock(return_value={"ok": True})
    out7 = await m.test_lakebase_connection_get("kasal-lakebase", service=svc)
    assert out7["ok"] is True

    out8 = await m.test_lakebase_connection({"instance_name": "kasal-lakebase"}, service=svc)
    assert out8["ok"] is True

    # Workspace info
    svc.get_workspace_info = AsyncMock(return_value={"workspace_url": "https://x", "organization_id": "o"})
    out9 = await get_lakebase_workspace_info(service=svc)
    assert out9["workspace_url"].startswith("https://")

    # Enable without migration: validation and success
    with pytest.raises(HTTPException):
        await enable_lakebase_without_migration({"instance_name": "x"}, service=svc)

    svc.enable_lakebase = AsyncMock(return_value={"enabled": True})
    out10 = await enable_lakebase_without_migration({"instance_name": "x", "endpoint": "https://e"}, service=svc)
    assert out10["enabled"] is True

