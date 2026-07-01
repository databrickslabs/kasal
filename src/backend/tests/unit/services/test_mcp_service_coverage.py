"""
Additional coverage tests for mcp_service.py targeting uncovered lines.
Missing: get_server_by_id, create_server, update_server, delete_server,
toggle_server_enabled, toggle_server_global_enabled, get_effective_servers,
get_settings error path, update_settings error path, get_servers_by_names
decrypt error, enable_server_for_group error paths.
"""
import asyncio
import pytest
from types import SimpleNamespace
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.mcp_service import MCPService
from src.schemas.mcp import (
    MCPServerCreate, MCPServerUpdate, MCPTestConnectionRequest,
    MCPSettingsUpdate
)
from src.core.exceptions import NotFoundError, ConflictError, KasalError


def mk_server(id=1, name="s1", group_id=None, encrypted_api_key=None,
              server_url="https://example.com", server_type="sse", auth_type="api_key",
              enabled=True, global_enabled=False, timeout_seconds=30, max_retries=3,
              model_mapping_enabled=False, rate_limit=60, additional_config=None):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=id, name=name, group_id=group_id, encrypted_api_key=encrypted_api_key,
        server_url=server_url, server_type=server_type, auth_type=auth_type,
        enabled=enabled, global_enabled=global_enabled, timeout_seconds=timeout_seconds,
        max_retries=max_retries, model_mapping_enabled=model_mapping_enabled, rate_limit=rate_limit,
        additional_config=additional_config or {}, created_at=now, updated_at=now
    )


def mk_settings(id=1, global_enabled=True, individual_enabled=True):
    now = datetime.utcnow()
    return SimpleNamespace(id=id, global_enabled=global_enabled, individual_enabled=individual_enabled,
                           created_at=now, updated_at=now)


# ---------------------------------------------------------------------------
# get_server_by_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_server_by_id_not_found():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.get = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await svc.get_server_by_id(999)


@pytest.mark.asyncio
async def test_get_server_by_id_found_no_api_key(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, name="test", encrypted_api_key=None)
    svc.server_repository.get = AsyncMock(return_value=server)

    result = await svc.get_server_by_id(1)
    assert result.name == "test"
    assert result.api_key == ""


@pytest.mark.asyncio
async def test_get_server_by_id_found_with_api_key(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, name="test", encrypted_api_key="enc123")
    svc.server_repository.get = AsyncMock(return_value=server)

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "decrypted")

    result = await svc.get_server_by_id(1)
    assert result.api_key == "decrypted"


@pytest.mark.asyncio
async def test_get_server_by_id_decrypt_error(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, name="test", encrypted_api_key="bad")
    svc.server_repository.get = AsyncMock(return_value=server)

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value",
                        lambda v: (_ for _ in ()).throw(Exception("decrypt fail")))

    result = await svc.get_server_by_id(1)
    assert result.api_key == ""


# ---------------------------------------------------------------------------
# create_server
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_server_conflict(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1, name="existing")
    svc.server_repository.find_by_name = AsyncMock(return_value=existing)

    server_data = MCPServerCreate(
        name="existing",
        server_url="https://example.com",
        server_type="sse",
        auth_type="api_key",
        api_key=""
    )
    with pytest.raises(ConflictError):
        await svc.create_server(server_data)


@pytest.mark.asyncio
async def test_create_server_success(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.find_by_name = AsyncMock(return_value=None)

    created = mk_server(id=2, name="new")
    svc.server_repository.create = AsyncMock(return_value=created)

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "encrypt_value", lambda v: "enc")

    server_data = MCPServerCreate(
        name="new",
        server_url="https://example.com",
        server_type="sse",
        auth_type="api_key",
        api_key="mykey"
    )
    result = await svc.create_server(server_data)
    assert result.name == "new"
    assert result.api_key == "mykey"


@pytest.mark.asyncio
async def test_create_server_with_group_id(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.find_by_name = AsyncMock(return_value=None)

    created = mk_server(id=3, name="grouped", group_id="g1")
    svc.server_repository.create = AsyncMock(return_value=created)

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "encrypt_value", lambda v: "enc")

    server_data = MCPServerCreate(
        name="grouped",
        server_url="https://example.com",
        server_type="sse",
        auth_type="api_key",
        api_key=""
    )
    result = await svc.create_server(server_data, group_id="g1")
    assert result.name == "grouped"


@pytest.mark.asyncio
async def test_create_server_generic_exception(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.find_by_name = AsyncMock(return_value=None)
    svc.server_repository.create = AsyncMock(side_effect=RuntimeError("DB error"))

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "encrypt_value", lambda v: "enc")

    server_data = MCPServerCreate(
        name="failing",
        server_url="https://example.com",
        server_type="sse",
        auth_type="api_key",
        api_key=""
    )
    with pytest.raises(KasalError):
        await svc.create_server(server_data)


# ---------------------------------------------------------------------------
# update_server
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_server_not_found():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.get = AsyncMock(return_value=None)

    update_data = MCPServerUpdate(name="updated")
    with pytest.raises(NotFoundError):
        await svc.update_server(999, update_data)


@pytest.mark.asyncio
async def test_update_server_success_no_api_key(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1, name="old")
    updated = mk_server(id=1, name="updated", encrypted_api_key=None)
    svc.server_repository.get = AsyncMock(return_value=existing)
    svc.server_repository.update = AsyncMock(return_value=updated)

    update_data = MCPServerUpdate(name="updated", api_key="")
    result = await svc.update_server(1, update_data)
    assert result.name == "updated"


@pytest.mark.asyncio
async def test_update_server_with_new_api_key(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1, name="old")
    updated = mk_server(id=1, name="old", encrypted_api_key="newenc")
    svc.server_repository.get = AsyncMock(return_value=existing)
    svc.server_repository.update = AsyncMock(return_value=updated)

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "encrypt_value", lambda v: "newenc")
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "decrypted_new")

    update_data = MCPServerUpdate(name="old", api_key="new_plain_key")
    result = await svc.update_server(1, update_data)
    assert result.api_key == "decrypted_new"


@pytest.mark.asyncio
async def test_update_server_decrypt_error(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1, name="old")
    updated = mk_server(id=1, name="old", encrypted_api_key="broken")
    svc.server_repository.get = AsyncMock(return_value=existing)
    svc.server_repository.update = AsyncMock(return_value=updated)

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "encrypt_value", lambda v: "broken")
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value",
                        lambda v: (_ for _ in ()).throw(Exception("decrypt fail")))

    update_data = MCPServerUpdate(name="old", api_key="trigger_encrypt")
    result = await svc.update_server(1, update_data)
    assert result.api_key == ""


@pytest.mark.asyncio
async def test_update_server_exception():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1, name="old")
    svc.server_repository.get = AsyncMock(return_value=existing)
    svc.server_repository.update = AsyncMock(side_effect=RuntimeError("DB down"))

    update_data = MCPServerUpdate(name="updated")
    with pytest.raises(KasalError):
        await svc.update_server(1, update_data)


# ---------------------------------------------------------------------------
# delete_server
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_server_not_found():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.get = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await svc.delete_server(999)


@pytest.mark.asyncio
async def test_delete_server_success():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1)
    svc.server_repository.get = AsyncMock(return_value=existing)
    svc.server_repository.delete = AsyncMock()

    result = await svc.delete_server(1)
    assert result is True


@pytest.mark.asyncio
async def test_delete_server_exception():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    existing = mk_server(id=1)
    svc.server_repository.get = AsyncMock(return_value=existing)
    svc.server_repository.delete = AsyncMock(side_effect=RuntimeError("DB error"))

    with pytest.raises(KasalError):
        await svc.delete_server(1)


# ---------------------------------------------------------------------------
# toggle_server_enabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_toggle_server_enabled_not_found():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.toggle_enabled = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await svc.toggle_server_enabled(999)


@pytest.mark.asyncio
async def test_toggle_server_enabled_success():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, enabled=True)
    svc.server_repository.toggle_enabled = AsyncMock(return_value=server)

    result = await svc.toggle_server_enabled(1)
    assert result.enabled is True
    assert "enabled" in result.message


@pytest.mark.asyncio
async def test_toggle_server_disabled():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, enabled=False)
    svc.server_repository.toggle_enabled = AsyncMock(return_value=server)

    result = await svc.toggle_server_enabled(1)
    assert result.enabled is False
    assert "disabled" in result.message


@pytest.mark.asyncio
async def test_toggle_server_enabled_exception():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.toggle_enabled = AsyncMock(side_effect=RuntimeError("DB error"))

    with pytest.raises(KasalError):
        await svc.toggle_server_enabled(1)


# ---------------------------------------------------------------------------
# toggle_server_global_enabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_toggle_global_not_found():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.toggle_global_enabled = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await svc.toggle_server_global_enabled(999)


@pytest.mark.asyncio
async def test_toggle_global_enabled():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, global_enabled=True)
    svc.server_repository.toggle_global_enabled = AsyncMock(return_value=server)

    result = await svc.toggle_server_global_enabled(1)
    assert result.enabled is True
    assert "globally enabled" in result.message


@pytest.mark.asyncio
async def test_toggle_global_disabled():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, global_enabled=False)
    svc.server_repository.toggle_global_enabled = AsyncMock(return_value=server)

    result = await svc.toggle_server_global_enabled(1)
    assert result.enabled is False
    assert "globally disabled" in result.message


@pytest.mark.asyncio
async def test_toggle_global_exception():
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    svc.server_repository.toggle_global_enabled = AsyncMock(side_effect=RuntimeError("DB error"))

    with pytest.raises(KasalError):
        await svc.toggle_server_global_enabled(1)


# ---------------------------------------------------------------------------
# get_effective_servers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_effective_servers(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "dec")

    global_server = mk_server(id=1, name="global_srv", global_enabled=True, encrypted_api_key="enc")
    explicit_server = mk_server(id=2, name="explicit_srv", encrypted_api_key="enc")

    svc.server_repository.find_global_enabled = AsyncMock(return_value=[global_server])
    svc.server_repository.find_by_names = AsyncMock(return_value=[global_server, explicit_server])

    results = await svc.get_effective_servers(["explicit_srv"])
    assert len(results) == 2


@pytest.mark.asyncio
async def test_get_effective_servers_empty_explicit(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "dec")

    global_server = mk_server(id=1, name="global_srv", global_enabled=True)
    svc.server_repository.find_global_enabled = AsyncMock(return_value=[global_server])
    svc.server_repository.find_by_names = AsyncMock(return_value=[global_server])

    results = await svc.get_effective_servers([])
    assert len(results) == 1


# ---------------------------------------------------------------------------
# get_servers_by_names - empty list
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_servers_by_names_empty():
    svc = MCPService(session=SimpleNamespace())
    result = await svc.get_servers_by_names([])
    assert result == []


@pytest.mark.asyncio
async def test_get_servers_by_names_group_aware_empty():
    svc = MCPService(session=SimpleNamespace())
    result = await svc.get_servers_by_names_group_aware([], group_id="g1")
    assert result == []


@pytest.mark.asyncio
async def test_get_servers_by_names_decrypt_error(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, name="s1", encrypted_api_key="broken_enc")
    svc.server_repository.find_by_names = AsyncMock(return_value=[server])

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value",
                        lambda v: (_ for _ in ()).throw(Exception("fail")))

    result = await svc.get_servers_by_names(["s1"])
    assert result[0].api_key == ""


@pytest.mark.asyncio
async def test_get_servers_by_names_group_aware_decrypt_error(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    server = mk_server(id=1, name="s1", group_id="g1", encrypted_api_key="broken")
    svc.server_repository.find_by_names_group_scope = AsyncMock(return_value=[server])

    import src.services.mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value",
                        lambda v: (_ for _ in ()).throw(Exception("fail")))

    result = await svc.get_servers_by_names_group_aware(["s1"], group_id="g1")
    assert result[0].api_key == ""


# ---------------------------------------------------------------------------
# get_settings / update_settings error paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_settings_error():
    svc = MCPService(session=SimpleNamespace())
    svc.settings_repository = AsyncMock()
    svc.settings_repository.get_settings = AsyncMock(side_effect=RuntimeError("DB error"))

    with pytest.raises(KasalError):
        await svc.get_settings()


@pytest.mark.asyncio
async def test_update_settings_error():
    svc = MCPService(session=SimpleNamespace())
    svc.settings_repository = AsyncMock()
    svc.settings_repository.get_settings = AsyncMock(side_effect=RuntimeError("DB error"))

    settings_data = MCPSettingsUpdate(global_enabled=True, individual_enabled=False)
    with pytest.raises(KasalError):
        await svc.update_settings(settings_data)


@pytest.mark.asyncio
async def test_update_settings_success():
    svc = MCPService(session=SimpleNamespace())
    svc.settings_repository = AsyncMock()
    s = mk_settings(id=1, global_enabled=True)
    svc.settings_repository.get_settings = AsyncMock(return_value=s)
    updated = mk_settings(id=1, global_enabled=False)
    svc.settings_repository.update = AsyncMock(return_value=updated)

    settings_data = MCPSettingsUpdate(global_enabled=False, individual_enabled=True)
    result = await svc.update_settings(settings_data)
    assert result.global_enabled is False


# ---------------------------------------------------------------------------
# enable_server_for_group - exception in disable base
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enable_server_for_group_disable_base_exception(monkeypatch):
    """Test that exception in disabling base server is swallowed (create path)."""
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    base = mk_server(id=6, name="x", group_id=None, encrypted_api_key=None)
    created = mk_server(id=8, name="x", group_id="g1", encrypted_api_key=None)

    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=None)
    svc.server_repository.create = AsyncMock(return_value=created)
    # update call to disable base raises
    svc.server_repository.update = AsyncMock(side_effect=RuntimeError("disable failed"))

    result = await svc.enable_server_for_group(6, "g1")
    assert result.id == 8


@pytest.mark.asyncio
async def test_enable_server_for_group_update_existing_disable_exception(monkeypatch):
    """Test that exception in disabling base is swallowed (update existing path)."""
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    base = mk_server(id=6, name="x", group_id=None, encrypted_api_key=None)
    existing = mk_server(id=7, name="x", group_id="g1", encrypted_api_key=None)

    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=existing)
    # First update returns existing, second raises
    svc.server_repository.update = AsyncMock(side_effect=[existing, RuntimeError("disable failed")])

    result = await svc.enable_server_for_group(6, "g1")
    assert result.id == 7


# ---------------------------------------------------------------------------
# test_connection routing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_test_connection_sse_type():
    svc = MCPService(session=SimpleNamespace())
    req = MCPTestConnectionRequest(server_url="https://example.com", api_key="k", server_type="sse")

    with patch.object(svc, "_test_sse_connection", new_callable=AsyncMock) as mock_sse:
        from src.schemas.mcp import MCPTestConnectionResponse
        mock_sse.return_value = MCPTestConnectionResponse(success=True, message="ok")
        result = await svc.test_connection(req)
    assert result.success is True


@pytest.mark.asyncio
async def test_test_connection_streamable_type():
    svc = MCPService(session=SimpleNamespace())
    req = MCPTestConnectionRequest(server_url="https://example.com", api_key="k", server_type="streamable")

    with patch.object(svc, "_test_streamable_connection", new_callable=AsyncMock) as mock_stream:
        from src.schemas.mcp import MCPTestConnectionResponse
        mock_stream.return_value = MCPTestConnectionResponse(success=True, message="ok")
        result = await svc.test_connection(req)
    assert result.success is True


# ---------------------------------------------------------------------------
# get_all_servers_effective — OPT-IN workspace model
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_all_servers_effective_inherited_base_shown_disabled():
    """An enabled base server (group_id=None) a workspace hasn't opted into is
    reported with enabled=False so the workspace toggle starts off."""
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    base = mk_server(id=1, name="global_a", group_id=None, enabled=True)
    svc.server_repository.list_for_group_scope = AsyncMock(return_value=[base])

    resp = await svc.get_all_servers_effective("ws1")
    assert resp.count == 1
    assert resp.servers[0].name == "global_a"
    assert resp.servers[0].enabled is False


@pytest.mark.asyncio
async def test_get_all_servers_effective_workspace_override_shown_enabled():
    """A workspace's own enabled override is reported as enabled."""
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    base = mk_server(id=1, name="global_a", group_id=None, enabled=True)
    override = mk_server(id=2, name="global_a", group_id="ws1", enabled=True)
    svc.server_repository.list_for_group_scope = AsyncMock(return_value=[base, override])

    resp = await svc.get_all_servers_effective("ws1")
    assert resp.count == 1
    assert resp.servers[0].group_id == "ws1"
    assert resp.servers[0].enabled is True


@pytest.mark.asyncio
async def test_get_all_servers_effective_enabled_only_hides_inherited_base():
    """Non-admin callers (enabled_only) don't see a not-opted-in inherited base."""
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    base = mk_server(id=1, name="global_a", group_id=None, enabled=True)
    svc.server_repository.list_for_group_scope = AsyncMock(return_value=[base])

    resp = await svc.get_all_servers_effective("ws1", enabled_only=True)
    assert resp.count == 0


@pytest.mark.asyncio
async def test_get_all_servers_effective_no_group_keeps_base_enabled():
    """With no workspace context (global admin), base enabled state is preserved."""
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()
    base = mk_server(id=1, name="global_a", group_id=None, enabled=True)
    svc.server_repository.list_for_group_scope = AsyncMock(return_value=[base])

    resp = await svc.get_all_servers_effective(None)
    assert resp.count == 1
    assert resp.servers[0].enabled is True
