"""Unit tests for UIConfigService (per-workspace Predefined UI configuration)."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.ui_config_service import UIConfigService
from src.schemas.ui_config import UIConfigUpdate
from src.models.ui_config import UIConfig


@pytest.mark.asyncio
async def test_get_config_returns_enabled_default_when_missing():
    """A workspace that never configured UI gets an ENABLED default — output
    formatting is owned by the UI-document emission, so UI rendering is on
    unless an admin explicitly disables it."""
    session = AsyncMock()
    session.add = MagicMock()
    with patch("src.services.ui_config_service.UIConfigRepository") as Repo:
        repo = AsyncMock()
        Repo.return_value = repo
        repo.get_for_group = AsyncMock(return_value=None)

        svc = UIConfigService(session, group_id="g1")
        out = await svc.get_config()

    assert out.enabled is True
    assert out.group_id == "g1"
    assert out.catalog_type == "minimal"


@pytest.mark.asyncio
async def test_get_config_returns_existing_row():
    session = AsyncMock()
    session.add = MagicMock()
    with patch("src.services.ui_config_service.UIConfigRepository") as Repo:
        repo = AsyncMock()
        Repo.return_value = repo
        cfg = UIConfig(
            id=5, group_id="g1", enabled=True, catalog_type="basic",
            catalog_json=None, style_json='{"accent":"#fff"}',
        )
        repo.get_for_group = AsyncMock(return_value=cfg)

        svc = UIConfigService(session, group_id="g1")
        out = await svc.get_config()

    assert out.id == 5
    assert out.enabled is True
    assert out.catalog_type == "basic"


@pytest.mark.asyncio
async def test_update_config_creates_when_missing():
    session = AsyncMock()
    session.add = MagicMock()
    with patch("src.services.ui_config_service.UIConfigRepository") as Repo:
        repo = AsyncMock()
        Repo.return_value = repo
        repo.get_for_group = AsyncMock(return_value=None)

        svc = UIConfigService(session, group_id="g1")
        body = UIConfigUpdate(
            enabled=True, catalog_type="custom",
            catalog_json='{"x":1}', style_json='{"accent":"#000"}',
        )
        out = await svc.update_config(body, created_by_email="a@b.com")

    session.add.assert_called_once()
    session.commit.assert_awaited()
    session.refresh.assert_awaited()
    assert out.enabled is True
    assert out.catalog_type == "custom"


@pytest.mark.asyncio
async def test_update_config_updates_existing_row():
    session = AsyncMock()
    session.add = MagicMock()
    with patch("src.services.ui_config_service.UIConfigRepository") as Repo:
        repo = AsyncMock()
        Repo.return_value = repo
        existing = UIConfig(id=3, group_id="g1", enabled=False, catalog_type="minimal")
        repo.get_for_group = AsyncMock(return_value=existing)

        svc = UIConfigService(session, group_id="g1")
        body = UIConfigUpdate(enabled=True, catalog_type="basic", catalog_json=None, style_json=None)
        out = await svc.update_config(body)

    session.add.assert_not_called()  # row already existed
    assert existing.enabled is True
    assert existing.catalog_type == "basic"
    assert out.id == 3
