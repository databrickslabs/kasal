"""Unit tests for the ui_config_router handlers (workspace-admin gated PUT)."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.api.ui_config_router import get_ui_config, update_ui_config, get_ui_config_service
from src.schemas.ui_config import UIConfigUpdate, UIConfigResponse
from src.core.exceptions import ForbiddenError


@pytest.mark.asyncio
async def test_get_ui_config_delegates_to_service():
    svc = MagicMock()
    svc.get_config = AsyncMock(return_value=UIConfigResponse(enabled=True))
    out = await get_ui_config(svc)
    assert out.enabled is True
    svc.get_config.assert_awaited()


@pytest.mark.asyncio
async def test_update_ui_config_allows_workspace_admin():
    svc = MagicMock()
    svc.update_config = AsyncMock(return_value=UIConfigResponse(enabled=True))
    ctx = MagicMock(group_email="admin@x.com")
    with patch("src.api.ui_config_router.is_workspace_admin", return_value=True):
        out = await update_ui_config(UIConfigUpdate(enabled=True), svc, ctx)
    assert out.enabled is True
    svc.update_config.assert_awaited_once()
    # created_by_email is threaded from the group context
    assert svc.update_config.await_args.kwargs["created_by_email"] == "admin@x.com"


@pytest.mark.asyncio
async def test_update_ui_config_blocks_non_admin():
    svc = MagicMock()
    svc.update_config = AsyncMock()
    ctx = MagicMock(group_email="user@x.com")
    with patch("src.api.ui_config_router.is_workspace_admin", return_value=False):
        with pytest.raises(ForbiddenError):
            await update_ui_config(UIConfigUpdate(enabled=True), svc, ctx)
    svc.update_config.assert_not_awaited()


def test_get_ui_config_service_uses_group_context():
    session = MagicMock()
    ctx = MagicMock(primary_group_id="g1")
    svc = get_ui_config_service(session, ctx)
    assert svc.group_id == "g1"


def test_get_ui_config_service_handles_missing_context():
    session = MagicMock()
    svc = get_ui_config_service(session, None)
    assert svc.group_id is None
