import pytest
from types import SimpleNamespace
from datetime import datetime
from unittest.mock import AsyncMock

from src.services.mcp_service import MCPService
from src.schemas.mcp import MCPTestConnectionRequest


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


@pytest.mark.asyncio
async def test_get_all_and_effective_and_enabled_and_global_lists(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    # get_all_servers masks api_key
    s1 = mk_server(id=1, name="a", group_id=None)
    s2 = mk_server(id=2, name="a", group_id="g1")
    svc.server_repository.list = AsyncMock(return_value=[s1, s2])
    out = await svc.get_all_servers()
    assert out.count == 2 and out.servers[0].api_key == ""

    # get_all_servers_effective dedups by name preferring group-specific
    svc.server_repository.list_for_group_scope = AsyncMock(return_value=[s1, s2])
    eff = await svc.get_all_servers_effective(group_id="g1")
    assert eff.count == 1 and eff.servers[0].group_id == "g1"

    # get_enabled_servers and get_global_servers
    svc.server_repository.find_enabled = AsyncMock(return_value=[s1])
    en = await svc.get_enabled_servers()
    assert en.count == 1 and en.servers[0].name == "a"
    svc.server_repository.find_global_enabled = AsyncMock(return_value=[s1])
    gl = await svc.get_global_servers()
    assert gl.count == 1


@pytest.mark.asyncio
async def test_get_servers_by_names_and_group_aware(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    # decrypt happy path
    from src.services import mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "dec", raising=True)

    s1 = mk_server(id=1, name="a", group_id=None, encrypted_api_key="enc")
    s2 = mk_server(id=2, name="b", group_id=None, encrypted_api_key=None)
    svc.server_repository.find_by_names = AsyncMock(return_value=[s1, s2])
    out = await svc.get_servers_by_names(["a", "b"])
    assert [o.api_key for o in out] == ["dec", ""]

    # group aware dedup + decrypt
    s1g = mk_server(id=3, name="a", group_id="g1", encrypted_api_key="encg")
    svc.server_repository.find_by_names_group_scope = AsyncMock(return_value=[s1, s1g])
    out2 = await svc.get_servers_by_names_group_aware(["a"], group_id="g1")
    assert len(out2) == 1 and out2[0].group_id == "g1" and out2[0].api_key == "dec"


@pytest.mark.asyncio
async def test_enable_server_for_group_paths(monkeypatch):
    svc = MCPService(session=SimpleNamespace())
    svc.server_repository = AsyncMock()

    # not found
    svc.server_repository.get = AsyncMock(return_value=None)
    with pytest.raises(Exception) as ei:
        await svc.enable_server_for_group(99, "g1")
    assert getattr(ei.value, "status_code", None) == 404

    # already group-scoped: update and decrypt
    base_g = mk_server(id=5, name="x", group_id="g1", encrypted_api_key="enc")
    svc.server_repository.get = AsyncMock(return_value=base_g)
    svc.server_repository.update = AsyncMock(return_value=base_g)
    from src.services import mcp_service as module
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "decg", raising=True)
    out = await svc.enable_server_for_group(5, "g1")
    assert out.api_key == "decg" and out.enabled is True

    # existing group override by name -> update existing, disable base
    base = mk_server(id=6, name="x", group_id=None, encrypted_api_key="enc")
    svc.server_repository.get = AsyncMock(return_value=base)
    existing = mk_server(id=7, name="x", group_id="g1", encrypted_api_key="enc2")
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=existing)
    svc.server_repository.update = AsyncMock(side_effect=[existing, base])
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "dec2", raising=True)
    out2 = await svc.enable_server_for_group(6, "g1")
    assert out2.id == 7 and out2.api_key == "dec2"

    # no existing -> create and disable base
    svc.server_repository.get = AsyncMock(return_value=base)
    svc.server_repository.find_by_name_and_group = AsyncMock(return_value=None)
    created = mk_server(id=8, name="x", group_id="g1", encrypted_api_key="enc3")
    svc.server_repository.create = AsyncMock(return_value=created)
    svc.server_repository.update = AsyncMock(return_value=base)
    monkeypatch.setattr(module.EncryptionUtils, "decrypt_value", lambda v: "dec3", raising=True)
    out3 = await svc.enable_server_for_group(6, "g1")
    assert out3.id == 8 and out3.api_key == "dec3"


@pytest.mark.asyncio
async def test_settings_and_test_connection_shim():
    svc = MCPService(session=SimpleNamespace())
    svc.settings_repository = AsyncMock()

    # get settings
    s = mk_settings(id=1, global_enabled=True)
    svc.settings_repository.get_settings = AsyncMock(return_value=s)
    out = await svc.get_settings()
    assert out.global_enabled is True

    # update settings
    svc.settings_repository.update = AsyncMock(return_value=s)
    out2 = await svc.update_settings(SimpleNamespace(model_dump=lambda: {"global_enabled": False}))
    assert out2.global_enabled is True

    # test_connection for unsupported type (no network)
    req = MCPTestConnectionRequest(server_url="https://example.com", api_key="k", server_type="unknown")
    res = await svc.test_connection(req)
    assert res.success is False and "Unsupported" in res.message

