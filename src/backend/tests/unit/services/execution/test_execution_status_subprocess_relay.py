"""Regression guard: a status change made INSIDE the crew subprocess must reach
the parent so it can be broadcast over SSE.

The subprocess has its own SSE manager with no clients attached, so it used to
just skip the broadcast entirely. A tool-approval gate therefore announced
WAITING_FOR_APPROVAL (over the separate ``hitl_request`` frame) but announced
NOTHING when the human's decision flipped the run back to RUNNING — the badge
sat on "Awaiting Approval" forever while the run had actually continued. It only
showed up for runs the client was not separately polling (e.g. a crew launched
by prompt optimization); a canvas run masked it because its own status poll
refreshed the badge anyway.
"""
import pytest
from types import SimpleNamespace


def _fake_db(monkeypatch, module):
    """Wire update_status onto an in-memory row so it reaches the announce step."""

    class FakeRepo:
        def __init__(self, session):
            self.session = session

        async def get_execution_by_job_id(self, job_id: str):
            return SimpleNamespace(id=7, job_id=job_id)

        async def update_execution(self, execution_id, data):
            # `data` already carries completed_at for terminal statuses.
            fields = {"group_id": "user_dev_localhost", "completed_at": None}
            fields.update(data)
            return SimpleNamespace(**fields)

    async def fake_exec(op):
        return await op(
            SimpleNamespace(
                flush=_anoop, commit=_anoop, rollback=_anoop
            )
        )

    async def _noop_broadcast(*a, **k):
        return 0

    monkeypatch.setattr(module, "ExecutionRepository", FakeRepo, raising=True)
    monkeypatch.setattr(module, "execute_db_operation_with_fresh_engine", fake_exec, raising=True)
    monkeypatch.setattr(module, "execute_db_operation_smart", fake_exec, raising=True)
    monkeypatch.setattr(module.sse_manager, "broadcast_to_job", _noop_broadcast, raising=True)


async def _anoop(*a, **k):
    return None


class _CapturingWriter:
    """Stands in for the subprocess end of the execution event pipe."""

    def __init__(self):
        self.frames = []

    def _put(self, frame):
        self.frames.append(frame)


@pytest.mark.asyncio
async def test_subprocess_status_change_is_relayed_to_the_parent(monkeypatch):
    from src.services.execution import status as module
    from src.services.execution import event_pipe as execution_event_pipe

    _fake_db(monkeypatch, module)
    writer = _CapturingWriter()
    monkeypatch.setattr(execution_event_pipe, "_active_writer", writer, raising=False)
    monkeypatch.setenv("CREW_SUBPROCESS_MODE", "true")

    # The gate opens, then the human's decision sends the run back to RUNNING.
    await module.ExecutionStatusService.update_status(
        job_id="job-1", status="WAITING_FOR_APPROVAL", message="gate open"
    )
    await module.ExecutionStatusService.update_status(
        job_id="job-1", status="RUNNING", message="Approval decided — run continuing"
    )

    relayed = [(f["kind"], f["status"]) for f in writer.frames]
    # The RUNNING transition is the one that used to vanish.
    assert ("execution_update", "RUNNING") in relayed
    assert ("execution_update", "WAITING_FOR_APPROVAL") in relayed
    assert all(f["job_id"] == "job-1" for f in writer.frames)


@pytest.mark.asyncio
async def test_relayed_frame_omits_the_result_blob(monkeypatch):
    """Terminal results can be large and the pipe drops frames when full, so the
    payload stays small — clients fetch the result over REST."""
    from src.services.execution import status as module
    from src.services.execution import event_pipe as execution_event_pipe

    _fake_db(monkeypatch, module)
    writer = _CapturingWriter()
    monkeypatch.setattr(execution_event_pipe, "_active_writer", writer, raising=False)
    monkeypatch.setenv("CREW_SUBPROCESS_MODE", "true")

    await module.ExecutionStatusService.update_status(
        job_id="job-2",
        status="COMPLETED",
        message="done",
        result={"huge": "x" * 10_000},
    )

    assert writer.frames, "a terminal status must still be announced"
    assert all("result" not in f for f in writer.frames)


@pytest.mark.asyncio
async def test_missing_pipe_writer_does_not_fail_the_status_update(monkeypatch):
    """No writer (or a broken pipe) must never turn a successful DB update into
    a failed one — the status is already committed at that point."""
    from src.services.execution import status as module
    from src.services.execution import event_pipe as execution_event_pipe

    _fake_db(monkeypatch, module)
    monkeypatch.setattr(execution_event_pipe, "_active_writer", None, raising=False)
    monkeypatch.setenv("CREW_SUBPROCESS_MODE", "true")

    ok = await module.ExecutionStatusService.update_status(
        job_id="job-3", status="RUNNING", message="m"
    )
    assert ok is True


@pytest.mark.asyncio
async def test_parent_process_still_broadcasts_directly(monkeypatch):
    """Outside the subprocess nothing changes: the event goes straight to SSE
    and no pipe frame is written."""
    from src.services.execution import status as module
    from src.services.execution import event_pipe as execution_event_pipe

    _fake_db(monkeypatch, module)
    writer = _CapturingWriter()
    monkeypatch.setattr(execution_event_pipe, "_active_writer", writer, raising=False)
    monkeypatch.delenv("CREW_SUBPROCESS_MODE", raising=False)

    sent = []

    async def capture(job_id, event, **kwargs):
        sent.append((job_id, event.event))
        return 1

    monkeypatch.setattr(module.sse_manager, "broadcast_to_job", capture, raising=True)

    await module.ExecutionStatusService.update_status(
        job_id="job-4", status="RUNNING", message="m"
    )

    assert ("job-4", "execution_update") in sent
    assert writer.frames == []
