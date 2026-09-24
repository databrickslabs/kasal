from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.exceptions import BadRequestError, ForbiddenError
from src.schemas.decision_config import DecisionConfigUpdate
from src.services.decisions.settings import DecisionSettingsService


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setenv("JEV_API_BASE", "https://example.com")
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
async def test_runtime_uses_existing_provider_key_service_only_when_enabled(service):
    with patch(
        "src.services.settings.api_keys.ApiKeysService.get_provider_api_key",
        new_callable=AsyncMock,
    ) as keys:
        service.repository.get.return_value = SimpleNamespace(enabled=False)
        assert await service.credential() is None
        keys.assert_not_awaited()
        service.repository.get.return_value = SimpleNamespace(enabled=True)
        keys.return_value = "secret"
        assert await service.credential() == "secret"
        keys.assert_awaited_once_with("jev", group_id="workspace-a")


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
