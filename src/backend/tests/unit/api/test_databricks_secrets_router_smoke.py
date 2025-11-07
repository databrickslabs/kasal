import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace

from src.api.databricks_secrets_router import (
    get_databricks_secrets,
    create_databricks_secret,
    update_databricks_secret,
    delete_databricks_secret,
)
from src.schemas.databricks_secret import SecretCreate, SecretUpdate


class Ctx:
    def __init__(self, user_role='admin', primary_group_id='g1'):
        self.user_role = user_role
        self.primary_group_id = primary_group_id


@pytest.mark.asyncio
async def test_list_returns_empty_when_not_configured():
    ctx = Ctx()
    svc = AsyncMock()
    # No config
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=None)
    out = await get_databricks_secrets(group_context=ctx, service=svc)
    assert out == []


@pytest.mark.asyncio
async def test_create_secret_bad_config_400():
    ctx = Ctx()
    svc = AsyncMock()
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await create_databricks_secret(SecretCreate(name='n', value='v'), group_context=ctx, service=svc)


@pytest.mark.asyncio
async def test_update_secret_success_and_delete_404():
    ctx = Ctx()
    svc = AsyncMock()
    cfg = SimpleNamespace(is_enabled=True, workspace_url='https://w', secret_scope='sc')
    svc.databricks_service.get_databricks_config = AsyncMock(return_value=cfg)

    # Update success
    svc.set_databricks_secret_value = AsyncMock(return_value=True)
    out = await update_databricks_secret('n', SecretUpdate(value='v'), group_context=ctx, service=svc)
    assert out['name'] == 'n' and out['scope'] == 'sc'

    # Delete 404 when not found
    svc.delete_databricks_secret = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_databricks_secret('n', group_context=ctx, service=svc)

