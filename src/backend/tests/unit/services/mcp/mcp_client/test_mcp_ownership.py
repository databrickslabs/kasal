"""MCP rows are owned. A numeric id is not an authorization (audit F02/F03).

A route dependency checks the caller's ROLE; the service checks the TARGET —
whose row it is and what the caller may do to it. Another workspace's row
reads as not found; a base row changes for a system admin only; and the API
never returns a stored key, to anyone.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.api.mcp_router import _is_global_admin
from src.core.exceptions import ForbiddenError, NotFoundError
from src.schemas.mcp import MCPServerUpdate
from src.services.mcp.mcp_client.service import MCPService


def _row(id=1, group_id="ws-a", encrypted_api_key="enc"):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=id,
        name=f"server-{id}",
        group_id=group_id,
        encrypted_api_key=encrypted_api_key,
        server_url="https://example.com",
        server_type="sse",
        auth_type="api_key",
        enabled=True,
        global_enabled=False,
        timeout_seconds=30,
        max_retries=3,
        model_mapping_enabled=False,
        rate_limit=60,
        additional_config={},
        created_at=now,
        updated_at=now,
    )


def _caller(group="ws-a", system_admin=False):
    return SimpleNamespace(
        primary_group_id=group,
        user_role="admin",
        current_user=SimpleNamespace(is_system_admin=system_admin),
    )


def _svc(row):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.get = AsyncMock(return_value=row)
    svc.server_repository.update = AsyncMock(return_value=row)
    svc.server_repository.delete = AsyncMock()
    svc.server_repository.toggle_enabled = AsyncMock(return_value=row)
    svc.server_repository.toggle_global_enabled = AsyncMock(return_value=row)
    return svc


class TestAnotherWorkspacesRowIsNotThere:
    @pytest.mark.asyncio
    async def test_read(self):
        svc = _svc(_row(group_id="ws-b"))
        with pytest.raises(NotFoundError):
            await svc.get_server_by_id(1, _caller("ws-a"))

    @pytest.mark.asyncio
    async def test_update_delete_and_toggles(self):
        svc = _svc(_row(group_id="ws-b"))
        caller = _caller("ws-a")
        with pytest.raises(NotFoundError):
            await svc.update_server(1, MCPServerUpdate(name="hijacked"), caller)
        with pytest.raises(NotFoundError):
            await svc.delete_server(1, caller)
        with pytest.raises(NotFoundError):
            await svc.toggle_server_enabled(1, caller)
        with pytest.raises(NotFoundError):
            await svc.toggle_server_global_enabled(1, caller)
        svc.server_repository.update.assert_not_awaited()
        svc.server_repository.delete.assert_not_awaited()
        svc.server_repository.toggle_enabled.assert_not_awaited()
        svc.server_repository.toggle_global_enabled.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_even_for_a_system_admin_in_another_workspace(self):
        # A system admin acts on base rows and on the workspace they are in;
        # another workspace's rows are reached by switching to that workspace.
        svc = _svc(_row(group_id="ws-b"))
        with pytest.raises(NotFoundError):
            await svc.update_server(
                1, MCPServerUpdate(name="x"), _caller("ws-a", system_admin=True)
            )


class TestTheOwnWorkspaceRow:
    @pytest.mark.asyncio
    async def test_reads_with_the_key_masked(self):
        svc = _svc(_row(group_id="ws-a", encrypted_api_key="enc"))
        out = await svc.get_server_by_id(1, _caller("ws-a"))
        assert out.api_key == ""
        assert out.has_api_key is True

    @pytest.mark.asyncio
    async def test_changes_for_its_workspace_admin(self):
        svc = _svc(_row(group_id="ws-a"))
        caller = _caller("ws-a")
        out = await svc.update_server(1, MCPServerUpdate(name="renamed"), caller)
        assert out.api_key == ""
        assert await svc.delete_server(1, caller) is True
        assert (await svc.toggle_server_enabled(1, caller)).enabled is True


class TestBaseRowsAreEveryWorkspaces:
    @pytest.mark.asyncio
    async def test_any_workspace_reads_them_masked(self):
        svc = _svc(_row(group_id=None, encrypted_api_key="enc"))
        out = await svc.get_server_by_id(1, _caller("ws-a"))
        assert out.group_id is None and out.api_key == "" and out.has_api_key

    @pytest.mark.asyncio
    async def test_a_workspace_admin_cannot_change_them(self):
        svc = _svc(_row(group_id=None))
        caller = _caller("ws-a")
        for call in (
            lambda: svc.update_server(1, MCPServerUpdate(name="x"), caller),
            lambda: svc.delete_server(1, caller),
            lambda: svc.toggle_server_enabled(1, caller),
            lambda: svc.toggle_server_global_enabled(1, caller),
            lambda: svc.set_global_availability(1, False, caller),
        ):
            with pytest.raises(ForbiddenError):
                await call()
        svc.server_repository.update.assert_not_awaited()
        svc.server_repository.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_system_admin_can(self):
        svc = _svc(_row(group_id=None))
        caller = _caller("ws-a", system_admin=True)
        await svc.update_server(1, MCPServerUpdate(name="x"), caller)
        assert await svc.delete_server(1, caller) is True
        await svc.set_global_availability(1, False, caller)
        svc.server_repository.update.assert_awaited()


class TestTheGlobalGate:
    def test_an_effective_workspace_admin_is_not_a_system_admin(self):
        assert _is_global_admin(_caller("ws-a")) is False
        assert (
            _is_global_admin(
                SimpleNamespace(
                    user_role="admin", highest_role="admin", current_user=None
                )
            )
            is False
        )
        assert _is_global_admin(None) is False

    def test_a_system_admin_is(self):
        assert _is_global_admin(_caller("ws-a", system_admin=True)) is True
