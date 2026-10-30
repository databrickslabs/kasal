"""A skill draft as a run: the run record and the terminal status — all best-effort, none able to fail the draft."""

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


def test_no_rows_are_hand_written_the_bridge_owns_the_trace():
    # The draft's LLM calls reach the trace through the event bus and the
    # scoped OTelEventBridge; a second writer here would double every row.
    assert not hasattr(draft_run, "record_call")


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
    # The whole draft rides along for a caller polling the run for its answer.
    assert calls[0][3][draft_run.RESULT_KEY]["body"] == "big"
    assert calls[1] == ("job-2", "FAILED", "boom", None)
    assert len(calls) == 2
