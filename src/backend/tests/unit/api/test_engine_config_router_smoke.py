import pytest
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException

from src.api.engine_config_router import (
    get_engine_configs,
    get_enabled_engine_configs,
    get_engine_config,
    create_engine_config,
    toggle_engine_config,
    update_config_value,
    get_crewai_flow_enabled,
    set_crewai_flow_enabled,
)
from src.schemas.engine_config import EngineConfigCreate, EngineConfigToggleUpdate, EngineConfigValueUpdate, CrewAIFlowConfigUpdate


class Ctx:
    def __init__(self, user_role=None, primary_group_id='g1', is_system_admin=False):
        self.user_role = user_role
        self.primary_group_id = primary_group_id
        # Add current_user attribute for system admin checks
        self.current_user = type('obj', (object,), {'is_system_admin': is_system_admin})()


@pytest.mark.asyncio
async def test_list_endpoints():
    service = AsyncMock()
    group_ctx = Ctx()
    with patch('src.api.engine_config_router.EngineConfigService') as svc_cls:
        svc = AsyncMock()
        item = {
            'engine_name': 'crewai',
            'engine_type': 'workflow',
            'config_key': 'flow_enabled',
            'config_value': 'true',
            'enabled': True,
            'description': None,
            'id': 1,
            'created_at': __import__('datetime').datetime.utcnow(),
            'updated_at': __import__('datetime').datetime.utcnow(),
        }
        svc.find_all = AsyncMock(return_value=[item])
        svc.find_enabled_configs = AsyncMock(return_value=[item])
        svc_cls.return_value = svc
        out = await get_engine_configs(service=svc, group_context=group_ctx)
        assert out.count == 1
        out2 = await get_enabled_engine_configs(service=svc, group_context=group_ctx)
        assert out2.count == 1


@pytest.mark.asyncio
async def test_get_engine_config_found_and_not_found():
    service = AsyncMock()
    group_ctx = Ctx()
    svc = AsyncMock()
    svc.find_by_engine_name = AsyncMock(return_value={'engine_name': 'e1'})
    # Found path
    out = await get_engine_config('e1', service=svc, group_context=group_ctx)
    assert out['engine_name'] == 'e1'
    # Not found path
    svc.find_by_engine_name = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as ei:
        await get_engine_config('e2', service=svc, group_context=group_ctx)
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_create_toggle_update_value_permission_and_404s(monkeypatch):
    group_ctx = Ctx(user_role='admin')

    svc = AsyncMock()
    # Create
    created_item = {
        'engine_name': 'e', 'engine_type': 'llm', 'config_key': 'k', 'config_value': 'v', 'enabled': True,
        'description': None, 'id': 1,
        'created_at': __import__('datetime').datetime.utcnow(),
        'updated_at': __import__('datetime').datetime.utcnow(),
    }
    svc.create_engine_config = AsyncMock(return_value=created_item)
    created = await create_engine_config(EngineConfigCreate(engine_name='e', engine_type='llm', config_key='k', config_value='v'), service=svc, group_context=group_ctx)
    assert created['engine_name'] == 'e'

    # Toggle 404
    svc.toggle_engine_enabled = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as ei:
        await toggle_engine_config('e', EngineConfigToggleUpdate(enabled=True), service=svc, group_context=group_ctx)
    assert ei.value.status_code == 404

    # Update value 404
    svc.update_config_value = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as ei2:
        await update_config_value('e', 'k', EngineConfigValueUpdate(config_value='x'), service=svc, group_context=group_ctx)
    assert ei2.value.status_code == 404


@pytest.mark.asyncio
async def test_crewai_toggles():
    # System admin required for engine configuration endpoints
    group_ctx = Ctx(is_system_admin=True)
    svc = AsyncMock()
    svc.get_crewai_flow_enabled = AsyncMock(return_value=True)
    resp = await get_crewai_flow_enabled(service=svc, group_context=group_ctx)
    assert resp['flow_enabled'] is True

    svc.set_crewai_flow_enabled = AsyncMock(return_value=True)
    out = await set_crewai_flow_enabled(CrewAIFlowConfigUpdate(flow_enabled=False), service=svc, group_context=group_ctx)
    assert out['success'] is True

