import pytest
from types import SimpleNamespace
from pydantic import BaseModel

from src.services.task_service import TaskService as Svc


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
        return SimpleNamespace(id='t1', **data)
    async def update(self, id, data: dict):
        self.updated = (id, data)
        return SimpleNamespace(id=id, **data)
    async def delete(self, id):
        self.deleted.append(id)
        return True
    async def find_all(self):
        return []


class FakeSession:
    async def execute(self, stmt):
        class R:
            def scalars(self):
                class S: 
                    def all(self):
                        return []
                return S()
        return R()


class TaskUpdateModel(BaseModel):
    name: str | None = None
    agent_id: str | None = None
    tool_configs: dict | None = None

    def model_dump(self, exclude_none=False):
        d = super().model_dump()
        if exclude_none:
            return {k: v for k, v in d.items() if v is not None}
        return d


@pytest.mark.asyncio
async def test_get_with_group_check_blocks_other_group(monkeypatch):
    from src.services import task_service as module
    module.TaskRepository = FakeRepo

    svc = Svc(SimpleNamespace(), repository_class=FakeRepo)
    task = SimpleNamespace(id='t1', group_id='g2')
    svc.repository._get = task

    gc = SimpleNamespace(group_ids=['g1'])
    assert await svc.get_with_group_check('t1', gc) is None


@pytest.mark.asyncio
async def test_create_converts_empty_agent_id(monkeypatch):
    from src.services import task_service as module
    module.TaskRepository = FakeRepo

    svc = Svc(SimpleNamespace(), repository_class=FakeRepo)
    obj = SimpleNamespace(model_dump=lambda: {'name': 'N', 'agent_id': ''})
    out = await svc.create(obj)
    assert svc.repository.created['agent_id'] is None
    assert out.id == 't1'


@pytest.mark.asyncio
async def test_update_with_partial_data_exclude_none_and_empty_agent_to_none(monkeypatch):
    from src.services import task_service as module
    module.TaskRepository = FakeRepo

    svc = Svc(SimpleNamespace(), repository_class=FakeRepo)
    svc.repository._get = SimpleNamespace(id='t1', group_id='g1')
    upd = TaskUpdateModel(name=None, agent_id="", tool_configs={'a': 1})
    gc = SimpleNamespace(group_ids=['g1'])
    out = await svc.update_with_group_check('t1', upd, gc)
    assert out.id == 't1'
    # Verify update_data excludes None and converts empty agent_id
    _, data = svc.repository.updated
    assert 'name' not in data
    assert data['agent_id'] is None
    assert data['tool_configs'] == {'a': 1}


@pytest.mark.asyncio
async def test_delete_with_group_check_requires_ownership(monkeypatch):
    from src.services import task_service as module
    module.TaskRepository = FakeRepo

    svc = Svc(SimpleNamespace(), repository_class=FakeRepo)
    svc.repository._get = SimpleNamespace(id='t1', group_id='g2')
    gc = SimpleNamespace(group_ids=['g1'])
    ok = await svc.delete_with_group_check('t1', gc)
    assert ok is False


@pytest.mark.asyncio
async def test_create_with_group_sets_fields(monkeypatch):
    from src.services import task_service as module
    module.TaskRepository = FakeRepo

    svc = Svc(FakeSession(), repository_class=FakeRepo)
    obj = SimpleNamespace(model_dump=lambda: {'name': 'N', 'agent_id': ''})
    gc = SimpleNamespace(primary_group_id='g1', group_email='u@x')
    out = await svc.create_with_group(obj, gc)
    assert svc.repository.created['group_id'] == 'g1'
    assert svc.repository.created['created_by_email'] == 'u@x'
    assert svc.repository.created['agent_id'] is None
    assert out.id == 't1'

