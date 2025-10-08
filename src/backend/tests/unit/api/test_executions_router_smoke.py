import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime

from src.api.executions_router import (
    create_execution,
    health_check,
    list_executions,
    get_execution_status,
    generate_execution_name,
    stop_execution,
    force_stop_execution,
    get_execution_status_simple,
)
from src.schemas.execution import CrewConfig, ExecutionNameGenerationRequest, StopExecutionRequest, StopType


class Ctx:
    def __init__(self, user_role="user", primary_group_id="g1", group_email="u@x"):
        self.user_role = user_role
        self.primary_group_id = primary_group_id
        self.group_email = group_email
        self.group_ids = [primary_group_id]
        self.access_token = "t"


class FakeResult:
    def __init__(self, value=None, tasks=None):
        self._value = value
        self._tasks = tasks or []
    def scalar_one_or_none(self):
        return self._value
    def scalars(self):
        class _S:
            def __init__(self, tasks):
                self._tasks = tasks
            def all(self):
                return self._tasks
        return _S(self._tasks)


class FakeDB:
    def __init__(self, first=None, tasks=None):
        self._first = first
        self._tasks = tasks or []
        self._calls = 0
    async def execute(self, stmt):
        self._calls += 1
        if self._calls == 1:
            return FakeResult(self._first)
        return FakeResult(tasks=self._tasks)


@pytest.mark.asyncio
async def test_create_execution_success_and_invalid_flow_id(monkeypatch):
    # Success path without flow_id
    svc = AsyncMock()
    svc.create_execution = AsyncMock(return_value={"execution_id": "e1", "status": "created", "run_name": "r"})
    ctx = Ctx(user_role="admin")
    cfg = CrewConfig(agents_yaml={"a": {}}, tasks_yaml={"t": {}}, inputs={})
    out = await create_execution(cfg, background_tasks=SimpleNamespace(add_task=lambda *a, **k: None), service=svc, group_context=ctx)
    assert out.execution_id == "e1"

    # Invalid flow_id -> 400
    cfg2 = CrewConfig(agents_yaml={"a": {}}, tasks_yaml={"t": {}}, inputs={}, flow_id="not-a-uuid")
    with pytest.raises(Exception) as ei:
        await create_execution(cfg2, background_tasks=SimpleNamespace(add_task=lambda *a, **k: None), service=svc, group_context=ctx)
    assert "Invalid flow_id" in str(ei.value)


@pytest.mark.asyncio
async def test_health_and_generate_name():
    assert (await health_check())["status"] == "healthy"
    svc = AsyncMock()
    svc.generate_execution_name = AsyncMock(return_value=SimpleNamespace(name="Nice Run"))
    req = ExecutionNameGenerationRequest(agents_yaml={}, tasks_yaml={})
    out = await generate_execution_name(req, service=svc, group_context=Ctx())
    assert out.name == "Nice Run"


@pytest.mark.asyncio
async def test_list_executions_minimal_db():
    # Provide a fake session that satisfies repository calls
    class FakeSession:
        def __init__(self):
            self.calls = 0
        async def execute(self, stmt):
            self.calls += 1
            class _R:
                def __init__(self, calls):
                    self._calls = calls
                def scalar_one(self):
                    return 0
                def scalar(self):
                    return 0
                def scalars(self):
                    class _S:
                        def all(self_non):
                            return []
                    return _S()
            return _R(self.calls)
    out = await list_executions(group_context=Ctx(), db=FakeSession())
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_stop_execution_permissions_and_success(monkeypatch):
    # 403 for non-admin/editor
    with pytest.raises(Exception):
        await stop_execution("e1", StopExecutionRequest(), service=AsyncMock(), group_context=Ctx(user_role="user"), db=SimpleNamespace())

    # Success path for admin with running execution
    class DummyExec:
        def __init__(self):
            self.status = "RUNNING"
            self.result = {"x": 1}
    db = FakeDB(first=DummyExec())

    svc = AsyncMock()
    svc.stop_execution = AsyncMock(return_value={
        "execution_id": "e1", "status": "stopping", "message": "ok", "partial_results": {}})

    out = await stop_execution("e1", StopExecutionRequest(), service=svc, group_context=Ctx(user_role="admin"), db=db)
    assert out.status in ("stopping", "RUNNING")


@pytest.mark.asyncio
async def test_force_stop_execution_returns_error_on_db_commit():
    # Provide minimal DB object; expect server to wrap error into HTTPException 500
    db = FakeDB(first=SimpleNamespace(status="RUNNING", result={}))
    with pytest.raises(Exception):
        await force_stop_execution("e1", group_context=Ctx(user_role="admin"), db=db)


@pytest.mark.asyncio
async def test_get_execution_status_simple_with_progress():
    # First execute returns execution, second returns tasks
    class Task:
        def __init__(self, status, task_id):
            self.status = status
            self.task_id = task_id
    exec_obj = SimpleNamespace(status="RUNNING", is_stopping=False, stopped_at=None, stop_reason=None)
    db = FakeDB(first=exec_obj, tasks=[Task("completed", "t1"), Task("running", "t2")])

    out = await get_execution_status_simple("e1", group_context=Ctx(), db=db)
    assert out.status in ("RUNNING", "STOPPING")
    assert out.progress["total_tasks"] == 2

