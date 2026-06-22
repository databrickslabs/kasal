"""
Service tests for the GLOBAL + per-workspace override behavior:
  - get_base_servers / create_global_server / set_global_availability (system admin)
  - set_server_enabled_for_group (per-workspace toggle / override)
  - enable_server_for_group no longer disables the base row
"""

import pytest
from types import SimpleNamespace
from datetime import datetime
from unittest.mock import AsyncMock

from src.services.mcp_service import MCPService
from src.schemas.mcp import MCPServerCreate
from src.core.exceptions import BadRequestError, NotFoundError


def mk_server(id=1, name="s1", group_id=None, enabled=True, encrypted_api_key=None):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=id,
        name=name,
        group_id=group_id,
        encrypted_api_key=encrypted_api_key,
        server_url="https://example.com/mcp",
        server_type="streamable",
        auth_type="databricks_spn",
        enabled=enabled,
        global_enabled=False,
        timeout_seconds=30,
        max_retries=3,
        model_mapping_enabled=False,
        rate_limit=60,
        additional_config={},
        created_at=now,
        updated_at=now,
    )


def _svc():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    return svc


# --- get_base_servers -------------------------------------------------------


@pytest.mark.asyncio
async def test_get_base_servers_masks_api_key():
    svc = _svc()
    svc.server_repository.find_all_base = AsyncMock(
        return_value=[
            mk_server(id=1, name="a", group_id=None),
            mk_server(id=2, name="b", group_id=None),
        ]
    )
    out = await svc.get_base_servers()
    assert out.count == 2
    assert all(s.api_key == "" for s in out.servers)


# --- create_global_server ---------------------------------------------------


@pytest.mark.asyncio
async def test_create_global_server_is_unscoped(monkeypatch):
    svc = _svc()
    from src.services import mcp_service as module

    monkeypatch.setattr(
        module.EncryptionUtils, "encrypt_value", lambda v: "enc", raising=True
    )
    svc.server_repository.find_by_name = AsyncMock(return_value=None)
    svc.server_repository.create = AsyncMock(
        return_value=mk_server(id=9, name="g", group_id=None)
    )

    await svc.create_global_server(
        MCPServerCreate(name="g", server_url="https://x", api_key="k")
    )

    created_payload = svc.server_repository.create.call_args.args[0]
    # Global servers are base rows: no group_id is set.
    assert created_payload.get("group_id") is None


# --- set_global_availability ------------------------------------------------


@pytest.mark.asyncio
async def test_set_global_availability_updates_base_enabled():
    svc = _svc()
    base = mk_server(id=5, name="g", group_id=None, enabled=True)
    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.update = AsyncMock(
        return_value=mk_server(id=5, name="g", group_id=None, enabled=False)
    )

    out = await svc.set_global_availability(5, False)

    svc.server_repository.update.assert_awaited_once_with(5, {"enabled": False})
    assert out.id == 5


@pytest.mark.asyncio
async def test_set_global_availability_rejects_group_row():
    svc = _svc()
    svc.server_repository.get = AsyncMock(
        return_value=mk_server(id=5, name="g", group_id="ws1")
    )
    with pytest.raises(BadRequestError):
        await svc.set_global_availability(5, True)


# --- set_server_enabled_for_group -------------------------------------------


@pytest.mark.asyncio
async def test_set_enabled_for_group_flips_own_group_row():
    svc = _svc()
    row = mk_server(id=7, name="x", group_id="ws1", enabled=True)
    svc.server_repository.get = AsyncMock(return_value=row)
    svc.server_repository.update = AsyncMock(
        return_value=mk_server(id=7, name="x", group_id="ws1", enabled=False)
    )

    out = await svc.set_server_enabled_for_group(7, "ws1", False)

    svc.server_repository.update.assert_awaited_once_with(7, {"enabled": False})
    assert out.id == 7


@pytest.mark.asyncio
async def test_set_enabled_for_group_creates_override_for_base_without_touching_base():
    svc = _svc()
    base = mk_server(id=6, name="x", group_id=None, enabled=True)
    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=None)
    svc.server_repository.create = AsyncMock(
        return_value=mk_server(id=20, name="x", group_id="ws1", enabled=False)
    )

    out = await svc.set_server_enabled_for_group(6, "ws1", False)

    payload = svc.server_repository.create.call_args.args[0]
    assert payload["group_id"] == "ws1" and payload["enabled"] is False
    # The base row is never mutated (no update call).
    svc.server_repository.update.assert_not_awaited()
    assert out.id == 20


@pytest.mark.asyncio
async def test_set_enabled_for_group_updates_existing_override():
    svc = _svc()
    base = mk_server(id=6, name="x", group_id=None, enabled=True)
    existing = mk_server(id=21, name="x", group_id="ws1", enabled=True)
    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=existing)
    svc.server_repository.update = AsyncMock(
        return_value=mk_server(id=21, name="x", group_id="ws1", enabled=False)
    )

    out = await svc.set_server_enabled_for_group(6, "ws1", False)

    assert svc.server_repository.update.call_args.args[0] == 21
    assert out.id == 21


@pytest.mark.asyncio
async def test_set_enabled_for_group_rejects_other_group_row():
    svc = _svc()
    svc.server_repository.get = AsyncMock(
        return_value=mk_server(id=8, name="x", group_id="other")
    )
    with pytest.raises(NotFoundError):
        await svc.set_server_enabled_for_group(8, "ws1", False)


@pytest.mark.asyncio
async def test_set_enabled_for_group_requires_group():
    svc = _svc()
    with pytest.raises(BadRequestError):
        await svc.set_server_enabled_for_group(1, "", True)


@pytest.mark.asyncio
async def test_set_enabled_for_group_missing_server():
    svc = _svc()
    svc.server_repository.get = AsyncMock(return_value=None)
    with pytest.raises(NotFoundError):
        await svc.set_server_enabled_for_group(99, "ws1", True)


# --- enable_server_for_group no longer disables the base --------------------


@pytest.mark.asyncio
async def test_enable_server_for_group_does_not_disable_base():
    svc = _svc()
    base = mk_server(id=6, name="x", group_id=None, enabled=True)
    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=None)
    svc.server_repository.create = AsyncMock(
        return_value=mk_server(id=30, name="x", group_id="ws1", enabled=True)
    )

    out = await svc.enable_server_for_group(6, "ws1")

    # Creates the (enabled) override; the base row is left untouched.
    payload = svc.server_repository.create.call_args.args[0]
    assert payload["group_id"] == "ws1" and payload["enabled"] is True
    svc.server_repository.update.assert_not_awaited()
    assert out.id == 30
