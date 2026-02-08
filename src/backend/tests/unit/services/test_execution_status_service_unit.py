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


# ---------------------------------------------------------------------------
# _broadcast_execution_created tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broadcast_execution_created_builds_correct_event(monkeypatch):
    """Test that _broadcast_execution_created builds and broadcasts correct SSE event."""
    from src.services import execution_status_service as module

    captured = {}

    async def fake_broadcast(job_id, event):
        captured["job_id"] = job_id
        captured["event"] = event
        return 1

    monkeypatch.setattr(module.sse_manager, "broadcast_to_job", fake_broadcast)

    execution_data = {
        "job_id": "job-abc",
        "status": "RUNNING",
        "run_name": "my-run",
        "execution_type": "crew",
        "created_at": "2025-01-15T10:00:00",
        "group_id": "grp-1",
        "planning": True,
    }

    await Svc._broadcast_execution_created(execution_data)

    assert captured["job_id"] == "job-abc"
    event = captured["event"]
    assert event.event == "execution_update"
    assert event.id == "job-abc_created"
    assert event.data["job_id"] == "job-abc"
    assert event.data["status"] == "RUNNING"
    assert event.data["run_name"] == "my-run"
    assert event.data["execution_type"] == "crew"
    assert event.data["created_at"] == "2025-01-15T10:00:00"
    assert event.data["group_id"] == "grp-1"
    assert event.data["planning"] is True
    assert "updated_at" in event.data


@pytest.mark.asyncio
async def test_broadcast_execution_created_handles_datetime_created_at(monkeypatch):
    """Test created_at as datetime object is converted via isoformat()."""
    from datetime import datetime as dt
    from src.services import execution_status_service as module

    captured = {}

    async def fake_broadcast(job_id, event):
        captured["event"] = event
        return 1

    monkeypatch.setattr(module.sse_manager, "broadcast_to_job", fake_broadcast)

    ts = dt(2025, 6, 1, 12, 30, 0)
    execution_data = {
        "job_id": "job-dt",
        "created_at": ts,
    }

    await Svc._broadcast_execution_created(execution_data)

    assert captured["event"].data["created_at"] == ts.isoformat()


@pytest.mark.asyncio
async def test_broadcast_execution_created_handles_missing_created_at(monkeypatch):
    """Test missing created_at falls back to now()."""
    from src.services import execution_status_service as module

    captured = {}

    async def fake_broadcast(job_id, event):
        captured["event"] = event
        return 1

    monkeypatch.setattr(module.sse_manager, "broadcast_to_job", fake_broadcast)

    execution_data = {
        "job_id": "job-no-ts",
        # created_at deliberately omitted
    }

    await Svc._broadcast_execution_created(execution_data)

    created_at = captured["event"].data["created_at"]
    # Should be an ISO-format string produced by datetime.now().isoformat()
    assert isinstance(created_at, str)
    assert "T" in created_at  # basic ISO-format check


@pytest.mark.asyncio
async def test_broadcast_execution_created_swallows_sse_errors(monkeypatch):
    """Test that SSE broadcast errors are caught and don't propagate."""
    from src.services import execution_status_service as module

    async def boom(job_id, event):
        raise RuntimeError("SSE connection lost")

    monkeypatch.setattr(module.sse_manager, "broadcast_to_job", boom)

    execution_data = {
        "job_id": "job-err",
        "created_at": "2025-01-01T00:00:00",
    }

    # Must not raise
    await Svc._broadcast_execution_created(execution_data)

