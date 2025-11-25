import pytest
from types import SimpleNamespace

from src.services.execution_status_service import ExecutionStatusService as Svc


@pytest.mark.asyncio
async def test_update_status_invalid_job_id_returns_false():
    ok = await Svc.update_status(job_id=None, status="RUNNING", message="m")
    assert ok is False


@pytest.mark.asyncio
async def test_update_status_not_found_returns_false(monkeypatch):
    # Patch ExecutionRepository in the module to a fake that returns None
    from src.services import execution_status_service as module

    class FakeRepo:
        def __init__(self, session):
            self.session = session
        async def get_execution_by_job_id(self, job_id: str):
            return None

    async def fake_exec(op):
        # Execute the provided operation with a fake session
        return await op(SimpleNamespace(
            flush=lambda: None, commit=lambda: None, rollback=lambda: None
        ))

    monkeypatch.setattr(module, "ExecutionRepository", FakeRepo, raising=True)
    monkeypatch.setattr(module, "execute_db_operation_with_fresh_engine", fake_exec, raising=True)

    ok = await Svc.update_status(job_id="jid", status="RUNNING", message="m")
    assert ok is False


@pytest.mark.asyncio
async def test_update_status_success_with_result_and_terminal_status(monkeypatch):
    from src.services import execution_status_service as module

    updated_calls = {}

    class Record(SimpleNamespace):
        id: int = 42

    class FakeRepo:
        def __init__(self, session):
            self.session = session
        async def get_execution_by_job_id(self, job_id: str):
            return Record(id=42)
        async def update_execution(self, execution_id: int, data: dict):
            # capture call
            updated_calls["args"] = (execution_id, data)
            return True

    class FakeSession(SimpleNamespace):
        async def flush(self):
            return None
        async def commit(self):
            return None
        async def rollback(self):
            return None

    async def fake_exec(op):
        return await op(FakeSession())

    monkeypatch.setattr(module, "ExecutionRepository", FakeRepo, raising=True)
    monkeypatch.setattr(module, "execute_db_operation_with_fresh_engine", fake_exec, raising=True)

    ok = await Svc.update_status(job_id="jid", status="COMPLETED", message="done", result={"x": 1})
    assert ok is True
    # Verify we passed the integer id and included result and completed_at
    eid, data = updated_calls["args"]
    assert eid == 42
    assert data["status"] == "COMPLETED"
    assert data["error"] == "done"
    assert data["result"] == {"x": 1}
    assert "completed_at" in data


@pytest.mark.asyncio
async def test_update_mlflow_trace_id_paths(monkeypatch):
    from src.services import execution_status_service as module

    class Record(SimpleNamespace):
        id: int = 7

    class FakeRepo:
        def __init__(self, session):
            self.session = session
        async def get_execution_by_job_id(self, job_id: str):
            return Record(id=7)
        async def update_execution(self, execution_id: int, data: dict):
            return True

    class FakeSession(SimpleNamespace):
        async def flush(self):
            return None
        async def commit(self):
            return None
        async def rollback(self):
            return None

    async def fake_exec(op):
        return await op(FakeSession())

    monkeypatch.setattr(module, "ExecutionRepository", FakeRepo, raising=True)
    monkeypatch.setattr(module, "execute_db_operation_with_fresh_engine", fake_exec, raising=True)

    # Invalid args
    assert await Svc.update_mlflow_trace_id(job_id=None, trace_id="t1") is False
    assert await Svc.update_mlflow_trace_id(job_id="j1", trace_id=None) is False

    # Success path
    assert await Svc.update_mlflow_trace_id(job_id="j1", trace_id="t1") is True


@pytest.mark.asyncio
async def test_update_mlflow_evaluation_run_id_paths(monkeypatch):
    from src.services import execution_status_service as module

    class Record(SimpleNamespace):
        id: int = 9

    class FakeRepo:
        def __init__(self, session):
            self.session = session
        async def get_execution_by_job_id(self, job_id: str):
            return Record(id=9)
        async def update_execution(self, execution_id: int, data: dict):
            return True

    class FakeSession(SimpleNamespace):
        async def commit(self):
            return None

    monkeypatch.setattr(module, "ExecutionRepository", FakeRepo, raising=True)

    ok = await Svc.update_mlflow_evaluation_run_id(FakeSession(), job_id="j1", evaluation_run_id="er1")
    assert ok is True

