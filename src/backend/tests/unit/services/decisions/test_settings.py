from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.config.settings import settings
from src.core.exceptions import BadRequestError, ForbiddenError
from src.schemas.decision_config import DecisionConfigUpdate
from src.services.decisions.settings import DecisionSettingsService


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(settings, "JEV_API_BASE", "https://example.com")
    instance = DecisionSettingsService(AsyncMock(), "workspace-a")
    instance.repository = AsyncMock()
    instance.repository.get.return_value = None
    instance.api_keys = AsyncMock()
    instance.api_keys.find_by_name.return_value = None
    return instance


@pytest.mark.asyncio
async def test_no_row_defaults_off_without_key(service):
    config = await service.get()
    assert not config.enabled and not config.api_key_configured
    assert await service.credential() is None
    service.repository.get.assert_awaited_with("workspace-a")


@pytest.mark.asyncio
async def test_toggle_only_persists_flag_and_never_updates_keys(service):
    service.api_keys.find_by_name.return_value = SimpleNamespace(
        encrypted_value="ciphertext"
    )
    result = await service.save(DecisionConfigUpdate(enabled=True))
    service.repository.save.assert_awaited_once_with("workspace-a", True)
    assert result.api_key_configured and "ciphertext" not in result.model_dump_json()
    await service.save(DecisionConfigUpdate(enabled=False))
    service.repository.save.assert_awaited_with("workspace-a", False)
    service.api_keys.create_api_key.assert_not_called()
    service.api_keys.update_api_key.assert_not_called()


@pytest.mark.asyncio
async def test_cannot_enable_without_existing_api_key(service):
    with pytest.raises(BadRequestError):
        await service.save(DecisionConfigUpdate(enabled=True))
    service.repository.save.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_reads_key_on_its_own_session_only_when_enabled(service):
    """The key is read through the injected ApiKeysService, on this session.

    ``ApiKeysService.get_provider_api_key`` opens a second session of its own,
    so it must not be used here (two sessions per decision call).
    """
    with (
        patch(
            "src.services.settings.api_keys.ApiKeysService.get_provider_api_key",
            new_callable=AsyncMock,
        ) as second_session_lookup,
        patch(
            "src.services.decisions.settings.EncryptionUtils.decrypt_value",
            return_value="secret",
        ) as decrypt,
    ):
        service.repository.get.return_value = SimpleNamespace(enabled=False)
        assert await service.credential() is None
        service.api_keys.find_by_name.assert_not_awaited()

        service.repository.get.return_value = SimpleNamespace(enabled=True)
        service.api_keys.find_by_name.return_value = SimpleNamespace(
            encrypted_value="ciphertext"
        )
        assert await service.credential() == "secret"
        service.api_keys.find_by_name.assert_awaited_with("JEV_API_KEY")
        decrypt.assert_called_once_with("ciphertext")
        second_session_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_credential_is_none_without_key_or_when_decrypt_fails(service, caplog):
    service.repository.get.return_value = SimpleNamespace(enabled=True)
    service.api_keys.find_by_name.return_value = None
    assert await service.credential() is None

    service.api_keys.find_by_name.return_value = SimpleNamespace(
        encrypted_value="ciphertext"
    )
    with patch(
        "src.services.decisions.settings.EncryptionUtils.decrypt_value",
        side_effect=ValueError("ciphertext"),
    ):
        assert await service.credential() is None
    assert "ciphertext" not in caplog.text


@pytest.mark.asyncio
async def test_cannot_enable_when_endpoint_not_configured(service, monkeypatch):
    monkeypatch.setattr(settings, "JEV_API_BASE", "")
    service.api_keys.find_by_name.return_value = SimpleNamespace(
        encrypted_value="ciphertext"
    )
    with pytest.raises(BadRequestError, match="JEV_API_BASE"):
        await service.save(DecisionConfigUpdate(enabled=True))
    service.repository.save.assert_not_awaited()
    # Turning it OFF must always work.
    await service.save(DecisionConfigUpdate(enabled=False))
    service.repository.save.assert_awaited_once_with("workspace-a", False)


@pytest.mark.asyncio
async def test_route_enforces_workspace_admin():
    from src.api.decision_config_router import update_config

    with (
        patch("src.api.decision_config_router.is_workspace_admin", return_value=False),
        patch("src.api.decision_config_router.DecisionSettingsService") as service,
    ):
        with pytest.raises(ForbiddenError):
            await update_config(DecisionConfigUpdate(enabled=False), Mock(), Mock())
        service.assert_not_called()


@pytest.mark.asyncio
async def test_repository_reads_exact_workspace_without_global_fallback():
    from src.models.decision_config import DecisionConfig
    from src.repositories.decision_config_repository import DecisionConfigRepository

    session = AsyncMock()
    session.get.return_value = None
    assert await DecisionConfigRepository(session).get("workspace-b") is None
    session.get.assert_awaited_once_with(DecisionConfig, "workspace-b")
