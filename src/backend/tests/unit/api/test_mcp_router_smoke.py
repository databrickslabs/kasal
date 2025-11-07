import pytest
from unittest.mock import AsyncMock
from fastapi import HTTPException

from src.api.mcp_router import (
    get_mcp_servers,
    get_enabled_mcp_servers,
    get_global_mcp_servers,
    get_mcp_server,
    create_mcp_server,
    update_mcp_server,
    delete_mcp_server,
    toggle_mcp_server_enabled,
    toggle_mcp_server_global_enabled,
    enable_mcp_server_for_workspace,
    get_mcp_settings,
    update_mcp_settings,
)
from src.schemas.mcp import MCPServerCreate, MCPServerUpdate, MCPSettingsUpdate, MCPTestConnectionRequest
import importlib


class Ctx:
    def __init__(self, user_role=None, primary_group_id=None):
        self.user_role = user_role
        self.primary_group_id = primary_group_id


@pytest.mark.asyncio
async def test_mcp_list_endpoints():
    svc = AsyncMock()
    ctx = Ctx()

    from types import SimpleNamespace
    svc.get_all_servers_effective = AsyncMock(return_value=SimpleNamespace(count=0, servers=[]))
    out = await get_mcp_servers(service=svc, group_context=ctx)
    assert out.count == 0

    svc.get_enabled_servers = AsyncMock(return_value=SimpleNamespace(count=1, servers=[1]))
    out2 = await get_enabled_mcp_servers(service=svc, group_context=ctx)
    assert out2.count == 1

    svc.get_global_servers = AsyncMock(return_value=SimpleNamespace(count=2, servers=[1, 2]))
    out3 = await get_global_mcp_servers(service=svc, group_context=ctx)
    assert out3.count == 2


@pytest.mark.asyncio
async def test_get_mcp_server_404():
    svc = AsyncMock()
    with pytest.raises(HTTPException) as ei:
        svc.get_server_by_id = AsyncMock(side_effect=HTTPException(status_code=404, detail="nf"))
        await get_mcp_server(123, service=svc, group_context=Ctx())
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_create_update_delete_permissions_and_success():
    svc = AsyncMock()
    # Create forbidden for non-admin
    with pytest.raises(HTTPException) as ei:
        await create_mcp_server(MCPServerCreate(name="n", server_url="u", api_key="k"), service=svc, group_context=Ctx(user_role="user"))
    assert ei.value.status_code == 403

    # Create success for admin; scoped by group
    from types import SimpleNamespace
    svc.create_server = AsyncMock(return_value=SimpleNamespace(id=1, name="n"))
    out = await create_mcp_server(MCPServerCreate(name="n", server_url="u", api_key="k"), service=svc, group_context=Ctx(user_role="admin", primary_group_id="g1"))
    assert out.id == 1

    # Update forbidden for non-admin
    with pytest.raises(HTTPException) as ei2:
        await update_mcp_server(1, MCPServerUpdate(name="x"), service=svc, group_context=Ctx(user_role="user"))
    assert ei2.value.status_code == 403

    # Update success for admin
    svc.update_server = AsyncMock(return_value={"id": 1, "name": "x"})
    out2 = await update_mcp_server(1, MCPServerUpdate(name="x"), service=svc, group_context=Ctx(user_role="admin"))
    assert out2["name"] == "x"

    # Delete forbidden for non-admin
    with pytest.raises(HTTPException) as ei3:
        await delete_mcp_server(1, service=svc, group_context=Ctx(user_role="user"))
    assert ei3.value.status_code == 403

    # Delete success for admin (no return)
    svc.delete_server = AsyncMock(return_value=None)
    out3 = await delete_mcp_server(1, service=svc, group_context=Ctx(user_role="admin"))
    assert out3 is None


@pytest.mark.asyncio
async def test_toggle_and_global_toggle_permissions_and_success():
    svc = AsyncMock()
    # Non-admin forbidden
    with pytest.raises(HTTPException):
        await toggle_mcp_server_enabled(1, service=svc, group_context=Ctx(user_role="user"))
    with pytest.raises(HTTPException):
        await toggle_mcp_server_global_enabled(1, service=svc, group_context=Ctx(user_role="user"))

    # Admin success
    from types import SimpleNamespace
    svc.toggle_server_enabled = AsyncMock(return_value=SimpleNamespace(enabled=True))
    out = await toggle_mcp_server_enabled(1, service=svc, group_context=Ctx(user_role="admin"))
    assert out.enabled is True

    svc.toggle_server_global_enabled = AsyncMock(return_value=SimpleNamespace(enabled=False))
    out2 = await toggle_mcp_server_global_enabled(1, service=svc, group_context=Ctx(user_role="admin"))
    assert out2.enabled is False


@pytest.mark.asyncio
async def test_enable_for_workspace_admin_and_requires_group():
    svc = AsyncMock()
    # Requires admin
    with pytest.raises(HTTPException) as ei:
        await enable_mcp_server_for_workspace(1, service=svc, group_context=Ctx(user_role="user", primary_group_id="g1"))
    assert ei.value.status_code == 403

    # Requires group id
    with pytest.raises(HTTPException) as ei2:
        await enable_mcp_server_for_workspace(1, service=svc, group_context=Ctx(user_role="admin", primary_group_id=None))
    assert ei2.value.status_code == 400

    # Admin with group works
    svc.enable_server_for_group = AsyncMock(return_value={"id": 1, "group_id": "g1"})
    out = await enable_mcp_server_for_workspace(1, service=svc, group_context=Ctx(user_role="admin", primary_group_id="g1"))
    assert out["group_id"] == "g1"


@pytest.mark.asyncio
async def test_test_connection_admin_success_and_error_path():
    svc = AsyncMock()
    # Non-admin forbidden
    m = importlib.import_module('src.api.mcp_router')
    with pytest.raises(HTTPException):
        await m.test_mcp_connection(MCPTestConnectionRequest(server_url="u", api_key="k"), service=svc, group_context=Ctx(user_role="user"))

    # Admin success
    from types import SimpleNamespace
    svc.test_connection = AsyncMock(return_value=SimpleNamespace(success=True, message="ok"))
    out = await m.test_mcp_connection(MCPTestConnectionRequest(server_url="u", api_key="k"), service=svc, group_context=Ctx(user_role="admin"))
    assert out.success is True

    # Admin error -> function returns structured error (no raise)
    svc.test_connection = AsyncMock(side_effect=Exception("boom"))
    out2 = await m.test_mcp_connection(MCPTestConnectionRequest(server_url="u", api_key="k"), service=svc, group_context=Ctx(user_role="admin"))
    assert out2.success is False and "Error testing connection" in out2.message


@pytest.mark.asyncio
async def test_settings_get_and_update_permissions():
    svc = AsyncMock()
    # Get settings
    from types import SimpleNamespace
    svc.get_settings = AsyncMock(return_value=SimpleNamespace(global_enabled=True))
    out = await get_mcp_settings(service=svc, group_context=Ctx())
    assert out.global_enabled is True

    # Update forbidden for non-admin
    with pytest.raises(HTTPException):
        await update_mcp_settings(MCPSettingsUpdate(global_enabled=False), service=svc, group_context=Ctx(user_role="user"))

    # Update success for admin
    from types import SimpleNamespace
    svc.update_settings = AsyncMock(return_value=SimpleNamespace(global_enabled=False))
    out2 = await update_mcp_settings(MCPSettingsUpdate(global_enabled=False), service=svc, group_context=Ctx(user_role="admin"))
    assert out2.global_enabled is False

