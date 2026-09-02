"""A skill draft as a run: the run record, the per-call trace rows, and the
terminal status — all best-effort, none able to fail the draft."""

import asyncio

from src.services.execution import generation_run
from src.services.execution.service import ExecutionService
from src.services.skills import draft_run


class _Group:
    primary_group_id = "g1"
    group_ids = ["g1"]
    group_email = "dev@example.com"


def test_run_name_is_the_request_or_the_conversation():
    assert draft_run.run_name("a skill for   release notes", 0) == (
        "Skill draft: a skill for release notes"
    )
    assert draft_run.run_name("", 12) == "Skill draft from conversation (12 turns)"
    assert len(draft_run.run_name("x" * 200, 0)) <= len("Skill draft: ") + 80


def test_open_run_needs_a_session_and_records_the_draft_as_a_running_agent_run(
    monkeypatch,
):
    assert (
        asyncio.run(
            draft_run.open_run(
                None,
                request="r",
                transcript_turns=0,
                model=None,
                group_context=_Group(),
            )
        )
        is None
    )

    seen = {}

    async def create_run_record(session, **kwargs):
        seen.update(kwargs, session=session)

    monkeypatch.setattr(ExecutionService, "create_run_record", create_run_record)
    job_id = asyncio.run(
        draft_run.open_run(
            "SESSION",
            request="a skill",
            transcript_turns=3,
            model="m",
            group_context=_Group(),
        )
    )
    assert job_id and seen["job_id"] == job_id and seen["session"] == "SESSION"
    assert seen["status"] == "RUNNING" and seen["execution_type"] == "agent"
    assert seen["group_id"] == "g1" and seen["group_email"] == "dev@example.com"
    assert seen["trigger_type"] == draft_run.TRIGGER_TYPE
    assert seen["inputs"]["mode"] == "capture" and seen["inputs"]["model"] == "m"


def test_open_run_failure_leaves_the_draft_without_a_run(monkeypatch):
    async def create_run_record(session, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(ExecutionService, "create_run_record", create_run_record)
    assert (
        asyncio.run(
            draft_run.open_run(
                "S", request="r", transcript_turns=0, model=None, group_context=_Group()
            )
        )
        is None
    )


def test_record_call_writes_a_request_row_and_a_response_row(monkeypatch):
    written = []

    async def write_rows(job_id, rows, **kwargs):
        written.append((job_id, rows, kwargs))

    monkeypatch.setattr(generation_run, "write_rows", write_rows)
    asyncio.run(
        draft_run.record_call(
            "job-1",
            attempt=2,
            model="m",
            prompt="[user]\nhi",
            response='{"name": "x"}',
            duration_ms=12.345,
            group_context=_Group(),
        )
    )
    job_id, rows, kwargs = written[0]
    assert job_id == "job-1" and kwargs["fallback_source"] == "Skills"
    assert [r[0] for r in rows] == ["llm_call", "llm_response"]
    assert rows[0][2] == "[user]\nhi" and rows[0][3]["attempt"] == 2
    assert rows[1][2] == '{"name": "x"}' and rows[1][3]["duration_ms"] == 12.35

    written.clear()
    asyncio.run(
        draft_run.record_call(
            None,
            attempt=1,
            model=None,
            prompt="p",
            response="r",
            duration_ms=1,
            group_context=None,
        )
    )
    assert written == []  # no run, nothing to attribute the rows to


def test_close_run_marks_completed_with_the_draft_or_failed_with_the_reason(
    monkeypatch,
):
    calls = []

    async def update_status(job_id, status, message, result=None, **kwargs):
        calls.append((job_id, status, message, result))
        return True

    monkeypatch.setattr(
        generation_run.ExecutionStatusService, "update_status", update_status
    )
    asyncio.run(
        draft_run.close_run(
            "job-1",
            result={
                "name": "n",
                "valid": True,
                "model": "m",
                "attempts": 1,
                "body": "big",
            },
        )
    )
    asyncio.run(draft_run.close_run("job-2", error="boom"))
    asyncio.run(draft_run.close_run(None, error="ignored"))
    assert calls[0][:3] == ("job-1", "COMPLETED", "Skill drafted")
    assert calls[0][3]["name"] == "n" and "body" not in calls[0][3]
    assert calls[1] == ("job-2", "FAILED", "boom", None)
    assert len(calls) == 2
