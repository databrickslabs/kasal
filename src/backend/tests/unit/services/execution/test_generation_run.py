"""A one-off generation call as a run: record, per-call rows, terminal status —
all best-effort, none able to fail the answer."""

import asyncio

from src.services.execution import generation_run
from src.services.execution.service import ExecutionService


class _Group:
    primary_group_id = "g1"
    group_email = "dev@example.com"


def test_open_run_needs_a_session_and_stamps_the_run(monkeypatch):
    assert (
        asyncio.run(
            generation_run.open_run(
                None, run_name="r", inputs={}, trigger_type="t", group_context=_Group()
            )
        )
        is None
    )
    seen = {}

    async def create_run_record(session, **kwargs):
        seen.update(kwargs, session=session)

    monkeypatch.setattr(ExecutionService, "create_run_record", create_run_record)
    job_id = asyncio.run(
        generation_run.open_run(
            "S",
            run_name="Slide refine: x",
            inputs={"a": 1},
            trigger_type="slide_refine",
            group_context=_Group(),
        )
    )
    assert job_id and seen["job_id"] == job_id and seen["session"] == "S"
    assert seen["status"] == "RUNNING" and seen["execution_type"] == "agent"
    assert seen["group_id"] == "g1" and seen["trigger_type"] == "slide_refine"


def test_open_run_failure_leaves_the_caller_without_a_run(monkeypatch):
    async def create_run_record(session, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(ExecutionService, "create_run_record", create_run_record)
    assert (
        asyncio.run(
            generation_run.open_run(
                "S", run_name="r", inputs={}, trigger_type="t", group_context=_Group()
            )
        )
        is None
    )


def test_record_call_writes_a_pair_under_the_source_lane(monkeypatch):
    written = []

    async def write_rows(job_id, rows, **kwargs):
        written.append((job_id, rows, kwargs))

    monkeypatch.setattr(generation_run, "write_rows", write_rows)
    asyncio.run(
        generation_run.record_call(
            "j",
            source="Decks",
            context="slide refine",
            attempt=1,
            model="m",
            prompt="p",
            response="r",
            duration_ms=3.456,
            group_context=_Group(),
        )
    )
    job_id, rows, kwargs = written[0]
    assert (
        kwargs["fallback_source"] == "Decks"
        and kwargs["fallback_context"] == "slide refine"
    )
    assert [r[0] for r in rows] == ["llm_call", "llm_response"]
    assert rows[0][1] == "kasal.decks.llm_call" and rows[1][3]["duration_ms"] == 3.46
    written.clear()
    asyncio.run(
        generation_run.record_call(
            None,
            source="Decks",
            context="c",
            attempt=1,
            model=None,
            prompt="p",
            response="r",
            duration_ms=1,
            group_context=None,
        )
    )
    assert written == []


def test_close_run_completes_or_fails(monkeypatch):
    calls = []

    async def update_status(job_id, status, message, result=None, **kwargs):
        calls.append((job_id, status, message, result))
        return True

    monkeypatch.setattr(
        generation_run.ExecutionStatusService, "update_status", update_status
    )
    asyncio.run(
        generation_run.close_run("j1", message="Slide refined", result={"a": 1})
    )
    asyncio.run(generation_run.close_run("j2", error="boom"))
    asyncio.run(generation_run.close_run(None, error="ignored"))
    assert calls == [
        ("j1", "COMPLETED", "Slide refined", {"a": 1}),
        ("j2", "FAILED", "boom", None),
    ]
