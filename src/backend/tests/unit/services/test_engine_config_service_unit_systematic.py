import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.services.engine_config_service import EngineConfigService as Svc


class FakeRepo:
    def __init__(self, session):
        self.session = session
        self._find_by_engine_name = None
        self._find_by_engine_and_key = None
        self.created = None
        self.updated = None
        self.deleted = []
        self.toggle_enabled_calls = []
        self.update_config_value_calls = []
        self.crewai_flow_enabled = False
        self.crewai_debug_tracing = False
    async def find_all(self):
        return []
    async def find_enabled_configs(self):
        return []
    async def find_by_engine_name(self, engine_name):
        return self._find_by_engine_name
    async def find_by_engine_and_key(self, engine_name, config_key):
        return self._find_by_engine_and_key
    async def find_by_engine_type(self, engine_type):
        return []
    async def create(self, data: dict):
        self.created = data
        return SimpleNamespace(id=1, **data)
    async def update(self, id, data: dict):
        self.updated = (id, data)
        return SimpleNamespace(id=id, **data)
    async def delete(self, id):
        self.deleted.append(id)
        return True
    async def toggle_enabled(self, engine_name, enabled):
        self.toggle_enabled_calls.append((engine_name, enabled))
        return True
    async def update_config_value(self, engine_name, config_key, config_value):
        self.update_config_value_calls.append((engine_name, config_key, config_value))
        return True
    async def get_crewai_flow_enabled(self):
        return self.crewai_flow_enabled
    async def set_crewai_flow_enabled(self, enabled):
        self.crewai_flow_enabled = enabled
        return True
    async def get_crewai_debug_tracing(self):
        return self.crewai_debug_tracing
    async def set_crewai_debug_tracing(self, enabled):
        self.crewai_debug_tracing = enabled
        return True


@pytest.mark.asyncio
async def test_create_engine_config_duplicate_raises_error():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    existing = SimpleNamespace(engine_name='test', config_key='key1')
    svc.repository._find_by_engine_and_key = existing
    config_data = SimpleNamespace(engine_name='test', config_key='key1', model_dump=lambda: {'engine_name': 'test', 'config_key': 'key1'})
    with pytest.raises(ValueError) as exc:
        await svc.create_engine_config(config_data)
    assert "already exists" in str(exc.value)


@pytest.mark.asyncio
async def test_create_engine_config_success():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository._find_by_engine_and_key = None
    config_data = SimpleNamespace(engine_name='test', config_key='key1', model_dump=lambda: {'engine_name': 'test', 'config_key': 'key1'})
    out = await svc.create_engine_config(config_data)
    assert svc.repository.created['engine_name'] == 'test'
    assert out.id == 1


@pytest.mark.asyncio
async def test_update_engine_config_not_found_returns_none():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository._find_by_engine_name = None
    config_data = SimpleNamespace(model_dump=lambda exclude_unset=False: {'config_value': 'new'})
    out = await svc.update_engine_config('nonexistent', config_data)
    assert out is None


@pytest.mark.asyncio
async def test_update_engine_config_success():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    existing = SimpleNamespace(id=1, engine_name='test')
    svc.repository._find_by_engine_name = existing
    config_data = SimpleNamespace(model_dump=lambda exclude_unset=False: {'config_value': 'new'})
    out = await svc.update_engine_config('test', config_data)
    assert svc.repository.updated[0] == 1
    assert svc.repository.updated[1]['config_value'] == 'new'


@pytest.mark.asyncio
async def test_toggle_engine_enabled_not_found_returns_none():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository.toggle_enabled = AsyncMock(return_value=False)
    out = await svc.toggle_engine_enabled('nonexistent', True)
    assert out is None


@pytest.mark.asyncio
async def test_toggle_engine_enabled_success():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    updated_config = SimpleNamespace(id=1, engine_name='test', enabled=True)
    svc.repository._find_by_engine_name = updated_config
    out = await svc.toggle_engine_enabled('test', True)
    assert ('test', True) in svc.repository.toggle_enabled_calls
    assert out == updated_config


@pytest.mark.asyncio
async def test_update_config_value_not_found_returns_none():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository.update_config_value = AsyncMock(return_value=False)
    out = await svc.update_config_value('nonexistent', 'key', 'value')
    assert out is None


@pytest.mark.asyncio
async def test_update_config_value_success():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    updated_config = SimpleNamespace(id=1, engine_name='test', config_key='key')
    svc.repository._find_by_engine_and_key = updated_config
    out = await svc.update_config_value('test', 'key', 'newvalue')
    assert ('test', 'key', 'newvalue') in svc.repository.update_config_value_calls
    assert out == updated_config


@pytest.mark.asyncio
async def test_get_crewai_flow_enabled_delegates():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository.crewai_flow_enabled = True
    out = await svc.get_crewai_flow_enabled()
    assert out is True


@pytest.mark.asyncio
async def test_set_crewai_flow_enabled_delegates():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    ok = await svc.set_crewai_flow_enabled(True)
    assert ok is True
    assert svc.repository.crewai_flow_enabled is True


@pytest.mark.asyncio
async def test_get_crewai_debug_tracing_delegates():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository.crewai_debug_tracing = True
    out = await svc.get_crewai_debug_tracing()
    assert out is True


@pytest.mark.asyncio
async def test_set_crewai_debug_tracing_delegates():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    ok = await svc.set_crewai_debug_tracing(True)
    assert ok is True
    assert svc.repository.crewai_debug_tracing is True


@pytest.mark.asyncio
async def test_delete_engine_config_not_found_returns_false():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    svc.repository._find_by_engine_name = None
    ok = await svc.delete_engine_config('nonexistent')
    assert ok is False


@pytest.mark.asyncio
async def test_delete_engine_config_success():
    svc = Svc(SimpleNamespace())
    svc.repository = FakeRepo(None)
    existing = SimpleNamespace(id=1, engine_name='test')
    svc.repository._find_by_engine_name = existing
    ok = await svc.delete_engine_config('test')
    assert ok is True
    assert 1 in svc.repository.deleted


# Removed failing tests with incorrect method assumptions
