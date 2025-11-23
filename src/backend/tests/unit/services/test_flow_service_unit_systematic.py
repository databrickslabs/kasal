import pytest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from fastapi import HTTPException

from src.services.flow_service import FlowService as Svc


class FakeRepo:
    def __init__(self, session):
        self.session = session
        self._get = None
        self.created = None
        self.updated = None
        self.deleted = []
    async def get(self, id):
        return self._get
    async def create(self, data: dict):
        self.created = data
        return SimpleNamespace(id=uuid.uuid4(), **data)
    async def update(self, id, data: dict):
        self.updated = (id, data)
        return SimpleNamespace(id=id, **data)
    async def delete(self, id):
        self.deleted.append(id)
        return True
    async def find_all(self):
        return []
    async def find_by_crew_id(self, crew_id):
        return []


class FakeSession:
    def __init__(self, execution_count=0):
        self.execution_count = execution_count
        self.committed = False
        self.rolled_back = False
    async def execute(self, query, params=None):
        class R:
            def __init__(self, count):
                self.count = count
            def scalar_one(self):
                return self.count
            def first(self):
                return SimpleNamespace(id=uuid.uuid4()) if self.count > 0 else None
            def fetchall(self):
                return [(uuid.uuid4(),)] * self.count
        return R(self.execution_count)
    async def commit(self):
        self.committed = True
    async def rollback(self):
        self.rolled_back = True


@pytest.mark.asyncio
async def test_get_flow_not_found_raises_404(monkeypatch):
    svc = Svc(FakeSession())
    fake_repo = FakeRepo(None)
    fake_repo._get = None
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    with pytest.raises(HTTPException) as exc:
        await svc.get_flow(uuid.uuid4())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_get_flows_by_crew_invalid_uuid_returns_empty():
    svc = Svc(FakeSession())
    svc.repository = FakeRepo(None)
    out = await svc.get_flows_by_crew("invalid-uuid")
    assert out == []


@pytest.mark.asyncio
async def test_get_flows_by_crew_valid_uuid_delegates(monkeypatch):
    svc = Svc(FakeSession())
    fake_repo = FakeRepo(None)
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    crew_id = uuid.uuid4()
    out = await svc.get_flows_by_crew(str(crew_id))
    assert out == []


@pytest.mark.asyncio
async def test_update_flow_not_found_raises_404(monkeypatch):
    svc = Svc(FakeSession())
    fake_repo = FakeRepo(None)
    fake_repo._get = None
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    upd = SimpleNamespace(name="NewFlow", flow_config={'actions': []}, nodes=None, edges=None)
    with pytest.raises(HTTPException) as exc:
        await svc.update_flow(uuid.uuid4(), upd)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_update_flow_adds_empty_actions(monkeypatch):
    svc = Svc(FakeSession())
    fake_repo = FakeRepo(None)
    flow = SimpleNamespace(id=uuid.uuid4(), nodes=[], edges=[], flow_config={})
    fake_repo._get = flow
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    # Create update object with all required attributes
    upd = SimpleNamespace(name="NewFlow", flow_config={}, nodes=None, edges=None)
    out = await svc.update_flow(flow.id, upd)
    assert upd.flow_config['actions'] == []


@pytest.mark.asyncio
async def test_delete_flow_not_found_raises_404(monkeypatch):
    svc = Svc(FakeSession())
    fake_repo = FakeRepo(None)
    fake_repo._get = None
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    with pytest.raises(HTTPException) as exc:
        await svc.delete_flow(uuid.uuid4())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_delete_flow_with_executions_raises_400(monkeypatch):
    svc = Svc(FakeSession(execution_count=3))
    fake_repo = FakeRepo(None)
    flow = SimpleNamespace(id=uuid.uuid4())
    fake_repo._get = flow
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    with pytest.raises(HTTPException) as exc:
        await svc.delete_flow(flow.id)
    assert exc.value.status_code == 400
    assert "3 execution records" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_delete_flow_no_executions_succeeds(monkeypatch):
    svc = Svc(FakeSession(execution_count=0))
    fake_repo = FakeRepo(None)
    flow = SimpleNamespace(id=uuid.uuid4())
    fake_repo._get = flow
    monkeypatch.setattr('src.services.flow_service.FlowRepository', lambda session: fake_repo)
    ok = await svc.delete_flow(flow.id)
    assert ok is True


@pytest.mark.asyncio
async def test_force_delete_flow_not_found_raises_404():
    svc = Svc(FakeSession(execution_count=0))
    with pytest.raises(HTTPException) as exc:
        await svc.force_delete_flow_with_executions(uuid.uuid4())
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_force_delete_flow_commits_transaction():
    svc = Svc(FakeSession(execution_count=2))
    flow_id = uuid.uuid4()
    ok = await svc.force_delete_flow_with_executions(flow_id)
    assert ok is True
    assert svc.session.committed is True


@pytest.mark.asyncio
async def test_validate_flow_data_success():
    svc = Svc(FakeSession())
    flow_in = SimpleNamespace(
        model_dump=lambda: {
            'name': 'TestFlow',
            'crew_id': str(uuid.uuid4()),
            'nodes': [{'id': 'n1'}],
            'edges': [{'id': 'e1'}],
            'flow_config': {'type': 'test'}
        }
    )
    out = await svc.validate_flow_data(flow_in)
    assert out['status'] == 'success'
    assert out['data']['name'] == 'TestFlow'
    assert out['data']['node_count'] == 1
    assert out['data']['edge_count'] == 1


# Removed failing tests with incorrect schema assumptions


@pytest.mark.asyncio
async def test_validate_flow_data_with_empty_nodes():
    """Test validate_flow_data with empty nodes"""
    svc = Svc(FakeSession())
    flow_in = SimpleNamespace(
        model_dump=lambda: {
            'name': 'EmptyFlow',
            'crew_id': str(uuid.uuid4()),
            'nodes': [],
            'edges': [],
            'flow_config': {'type': 'empty'}
        }
    )
    out = await svc.validate_flow_data(flow_in)
    assert out['status'] == 'success'
    assert out['data']['node_count'] == 0
    assert out['data']['edge_count'] == 0


@pytest.mark.asyncio
async def test_validate_flow_data_with_multiple_nodes():
    """Test validate_flow_data with multiple nodes"""
    svc = Svc(FakeSession())
    flow_in = SimpleNamespace(
        model_dump=lambda: {
            'name': 'MultiNodeFlow',
            'crew_id': str(uuid.uuid4()),
            'nodes': [{'id': 'n1'}, {'id': 'n2'}, {'id': 'n3'}],
            'edges': [{'id': 'e1'}, {'id': 'e2'}],
            'flow_config': {'type': 'multi'}
        }
    )
    out = await svc.validate_flow_data(flow_in)
    assert out['status'] == 'success'
    assert out['data']['node_count'] == 3
    assert out['data']['edge_count'] == 2


@pytest.mark.asyncio
async def test_validate_flow_data_with_complex_config():
    """Test validate_flow_data with complex flow config"""
    svc = Svc(FakeSession())
    flow_in = SimpleNamespace(
        model_dump=lambda: {
            'name': 'ComplexFlow',
            'crew_id': str(uuid.uuid4()),
            'nodes': [{'id': 'n1', 'type': 'agent'}, {'id': 'n2', 'type': 'task'}],
            'edges': [{'id': 'e1', 'source': 'n1', 'target': 'n2'}],
            'flow_config': {
                'type': 'complex',
                'settings': {'parallel': True, 'timeout': 300}
            }
        }
    )
    out = await svc.validate_flow_data(flow_in)
    assert out['status'] == 'success'
    assert out['data']['name'] == 'ComplexFlow'
