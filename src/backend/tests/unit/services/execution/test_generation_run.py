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


def test_builder_trace_streams_requests_answers_and_isolates_concurrent_turns(
    monkeypatch,
):
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from src.core.events.bus import event_bus
    from src.core.events.types import LLMCallCompletedEvent, LLMCallStartedEvent
    from src.services.otel_tracing import generation_scope

    exporters = {}

    def exporter(job_id, group_context):
        exporters[job_id] = InMemorySpanExporter()
        return exporters[job_id]

    monkeypatch.setattr(generation_scope, "KasalDBSpanExporter", exporter)

    async def turn(job_id):
        async with generation_scope.generation_trace(job_id, _Group(), "Builder"):

            def emit_request():
                event_bus.emit(
                    None,
                    LLMCallStartedEvent(
                        model="model", messages=[{"role": "user", "content": job_id}]
                    ),
                )

            await asyncio.to_thread(emit_request)
            # The request is visible BEFORE an answer arrives.
            assert any(
                s.attributes.get("kasal.event_type") == "llm_call"
                for s in exporters[job_id].get_finished_spans()
            )
            await asyncio.sleep(0)
            event_bus.emit(
                None,
                LLMCallCompletedEvent(
                    call_type="llm_call", model="model", response=f"answer-{job_id}"
                ),
            )

    async def run():
        await asyncio.gather(turn("one"), turn("two"))

    asyncio.run(run())
    for job_id, exp in exporters.items():
        spans = exp.get_finished_spans()
        calls = [s for s in spans if s.attributes.get("kasal.event_type") == "llm_call"]
        answers = [
            s for s in spans if s.attributes.get("kasal.event_type") == "llm_response"
        ]
        assert len(calls) == len(answers) == 1
        assert calls[0].attributes["kasal.extra.prompt"] == job_id
        assert answers[0].attributes["kasal.output_content"] == f"answer-{job_id}"
        count = len(spans)
        event_bus.emit(
            None, LLMCallCompletedEvent(call_type="llm_call", response="unrelated")
        )
        assert len(exp.get_finished_spans()) == count


def test_builder_trace_records_failures_and_removes_subscriptions(monkeypatch):
    import pytest
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from src.core.events.bus import event_bus
    from src.services.otel_tracing import generation_scope

    exp = InMemorySpanExporter()
    monkeypatch.setattr(generation_scope, "KasalDBSpanExporter", lambda *args: exp)
    count = sum(len(handlers) for handlers in event_bus._handlers.values())

    async def run():
        async with generation_scope.generation_trace(
            "failed", _Group(), "Flow Builder"
        ):
            raise ValueError("Invalid routing")

    with pytest.raises(ValueError, match="Invalid routing"):
        asyncio.run(run())
    assert any(
        s.attributes.get("kasal.event_type") == "task_failed"
        for s in exp.get_finished_spans()
    )
    assert sum(len(handlers) for handlers in event_bus._handlers.values()) == count


def test_generation_scope_persists_live_requests_and_final_answers(
    tmp_path, monkeypatch
):
    """Exercise the real bridge -> exporter -> repository path on an isolated DB."""
    from contextlib import asynccontextmanager

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    from src.core.events.bus import event_bus
    from src.core.events.types import LLMCallCompletedEvent, LLMCallStartedEvent
    from src.db import all_models  # noqa: F401 -- resolve ORM relationships
    from src.db import session as sessions
    from src.models.execution_trace import ExecutionTrace
    from src.services.otel_tracing.generation_scope import generation_trace

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'trace.db'}", poolclass=NullPool
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def isolated():
        async with factory() as session:
            yield session

    monkeypatch.setattr(sessions, "routed_scoped_session", isolated)

    async def rows():
        async with engine.connect() as conn:
            return (
                await conn.execute(
                    text(
                        "SELECT event_type, output, group_id, event_context FROM execution_trace ORDER BY id"
                    )
                )
            ).all()

    async def run():
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "CREATE TABLE executionhistory (id INTEGER PRIMARY KEY, job_id TEXT UNIQUE)"
                )
            )
            await conn.execute(
                text("INSERT INTO executionhistory (job_id) VALUES ('draft')")
            )
            await conn.run_sync(ExecutionTrace.__table__.create)
        async with generation_trace(
            "draft", _Group(), "Agent Builder", "Create the crew plan"
        ):
            event_bus.emit(
                None,
                LLMCallStartedEvent(
                    model="model",
                    messages=[{"role": "user", "content": "Create a crew plan"}],
                ),
            )
            for _ in range(100):
                if any(row[0] == "llm_call" for row in await rows()):
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError(
                    "Request was not persisted while generation was live"
                )
            event_bus.emit(
                None,
                LLMCallCompletedEvent(
                    call_type="llm_call",
                    model="model",
                    response="The complete crew plan",
                ),
            )
        saved = await rows()
        assert [row[0] for row in saved] == [
            "task_started",
            "llm_call",
            "llm_response",
            "task_completed",
        ]
        assert all(row[2] == "g1" for row in saved)
        assert all(row[3] == "Create the crew plan" for row in saved)
        assert "The complete crew plan" in saved[2][1]
        await engine.dispose()

    asyncio.run(run())


def test_traced_dispatch_waits_for_all_progressive_work_and_returns_recovery_payload(
    monkeypatch,
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from src.core.events.bus import event_context
    from src.services.generation.crew import dispatch

    group = SimpleNamespace(primary_group_id="team", group_ids=["team"])
    terminal = SimpleNamespace(
        event="generation_complete",
        data={"agents": [{"id": "agent"}], "tasks": [{"id": "task"}]},
    )
    monkeypatch.setattr(dispatch, "streaming_request_for", lambda *args: "request")
    monkeypatch.setattr(dispatch.sse_manager, "register_job_owner", lambda *args: None)
    monkeypatch.setattr(
        dispatch.sse_manager, "get_terminal_event", lambda *args, **kwargs: terminal
    )
    service = SimpleNamespace(create_crew_progressive=AsyncMock())

    async def run():
        with event_context(generation_job_id="design"):
            return await dispatch.dispatch_progressive(
                service, None, "prompt", [], group, False
            )

    result = asyncio.run(run())
    service.create_crew_progressive.assert_awaited_once_with(
        "request", group, "design", mlflow_enabled=False
    )
    assert result["completed"] is True
    assert result["generation_id"] == "design"
    assert result["generated_crew"] == terminal.data


def test_progressive_failure_fails_the_traced_parent_turn(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import pytest

    from src.core.events.bus import event_context
    from src.services.generation.crew import dispatch

    group = SimpleNamespace(primary_group_id="team", group_ids=["team"])
    monkeypatch.setattr(dispatch, "streaming_request_for", lambda *args: "request")
    monkeypatch.setattr(dispatch.sse_manager, "register_job_owner", lambda *args: None)
    monkeypatch.setattr(
        dispatch.sse_manager,
        "get_terminal_event",
        lambda *args, **kwargs: SimpleNamespace(
            event="generation_failed", data={"error": "Planning failed"}
        ),
    )

    async def run():
        with event_context(generation_job_id="design"):
            await dispatch.dispatch_progressive(
                SimpleNamespace(create_crew_progressive=AsyncMock()),
                None,
                "prompt",
                [],
                group,
                False,
            )

    with pytest.raises(ValueError, match="Planning failed"):
        asyncio.run(run())
