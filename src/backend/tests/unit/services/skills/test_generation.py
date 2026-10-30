"""Tests for SkillGenerationService: one focused LLM call, JSON out, validated
before it returns, one retry with the validator's own errors."""

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager

from src.services.skills import generation


class _Group:
    primary_group_id = "g1"
    group_ids = ["g1"]


def _install(monkeypatch, replies, template_text="SYSTEM TEMPLATE"):
    """Stub the LLM (sequential replies) and the template lookup; record calls."""
    calls = []

    async def completion(**kwargs):
        calls.append(kwargs)
        reply = replies.pop(0)
        # The service asks for the served model (a tuple return), like the
        # real LLMManager.completion does under ``with_served_model``.
        return (reply, "served-model") if kwargs.get("with_served_model") else reply

    async def template(name, gc):
        return template_text

    monkeypatch.setattr(generation.LLMManager, "completion", completion)
    monkeypatch.setattr(
        generation.TemplateService, "get_effective_template_content", template
    )
    return calls


GOOD = json.dumps(
    {
        "name": "writing-release-notes",
        "description": "Use when drafting release notes. Trigger when the user mentions releases.",
        "body": "# Writing release notes\n\n## When to use this skill\nAny release.\n\n## 1. Lead with what changed\nBecause readers skim.\n",
    }
)


def test_blank_page_draft_is_validated_and_uses_the_template(monkeypatch):
    calls = _install(monkeypatch, [GOOD])
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill for release notes", _Group())
    )
    assert out["valid"] is True and out["errors"] == []
    assert out["name"] == "writing-release-notes"
    assert calls[0]["messages"][0] == {"role": "system", "content": "SYSTEM TEMPLATE"}
    assert calls[0]["messages"][1]["content"].startswith("MODE: blank page")
    assert calls[0]["extra_headers"]  # Databricks telemetry header present


def test_capture_mode_feeds_the_transcript(monkeypatch):
    calls = _install(monkeypatch, [GOOD])
    transcript = [
        {"role": "user", "content": "write release notes"},
        {"role": "assistant", "content": "here"},
        {"role": "user", "content": "no — lead with what changed, not who did it"},
        {"role": "system", "content": "ignored"},
    ]
    asyncio.run(
        generation.SkillGenerationService.draft(
            "save this as a skill", _Group(), transcript=transcript, model="m-1"
        )
    )
    user = calls[0]["messages"][1]["content"]
    assert user.startswith("MODE: capture")
    assert "lead with what changed" in user
    assert "ignored" not in user
    assert calls[0]["model"] == "m-1"


def test_invalid_draft_is_retried_once_with_the_validator_errors(monkeypatch):
    bad = json.dumps({"name": "Not Kebab", "description": "d", "body": "b"})
    calls = _install(monkeypatch, [bad, GOOD])
    out = asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert out["valid"] is True
    assert len(calls) == 2
    retry = calls[1]["messages"][-1]["content"]
    assert retry.startswith("That draft failed validation")


def test_second_failure_is_returned_with_errors_not_raised(monkeypatch):
    bad = json.dumps({"name": "Not Kebab", "description": "d", "body": "b"})
    _install(monkeypatch, [bad, bad])
    out = asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert out["valid"] is False
    assert out["errors"]
    assert out["name"] == "Not Kebab"  # the card shows the draft + the errors


def test_unparseable_reply_becomes_an_invalid_empty_draft(monkeypatch):
    _install(monkeypatch, ["not json at all", "still not json"])
    out = asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert out["valid"] is False
    assert out["name"] == ""


def test_template_failure_falls_back_to_the_seed(monkeypatch):
    calls = _install(monkeypatch, [GOOD])

    async def boom(name, gc):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        generation.TemplateService, "get_effective_template_content", boom
    )
    asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert "Return ONLY a JSON object" in calls[0]["messages"][0]["content"]


def test_reports_the_served_model_and_the_attempt_count(monkeypatch):
    _install(monkeypatch, [GOOD])
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill", _Group(), model="picker-key")
    )
    assert out["model"] == "served-model" and out["attempts"] == 1

    _install(monkeypatch, [json.dumps({"name": "Bad Name!"}), GOOD])
    out = asyncio.run(generation.SkillGenerationService.draft("a skill", _Group()))
    assert out["valid"] is True and out["attempts"] == 2


def test_served_name_drops_the_none_substitution_suffix():
    assert generation._served_name("gpt-x (for 'None')", None) == "gpt-x"
    assert generation._served_name("gpt-x (for 'k')", "k") == "gpt-x (for 'k')"
    assert generation._served_name(None, "k") == "k"
    assert generation._served_name("", None) is None


def test_with_a_session_the_draft_is_a_run_traced_through_the_bridge(monkeypatch):
    """No hand-written rows: the run is opened, each attempt runs inside the
    run's generation trace (the retry under its own step), then closed."""
    _install(monkeypatch, [json.dumps({"name": "Bad Name!"}), GOOD])
    events = []

    async def open_run(session, **kwargs):
        events.append(("open", session, kwargs["transcript_turns"]))
        return "job-9"

    async def close_run(job_id, **kwargs):
        events.append(("close", job_id, kwargs.get("result", {}).get("valid")))

    @asynccontextmanager
    async def trace(job_id, group_context, label, step):
        events.append(("trace", job_id, group_context.primary_group_id, label, step))
        yield
        events.append(("trace-end", job_id))

    @contextmanager
    def step(label):
        events.append(("step", label))
        yield

    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)
    monkeypatch.setattr(generation, "generation_trace", trace)
    monkeypatch.setattr(generation, "generation_step", step)
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill", _Group(), session="S")
    )
    assert out["job_id"] == "job-9" and out["attempts"] == 2
    assert events == [
        ("open", "S", 0),
        ("trace", "job-9", "g1", "Skills", "Draft the skill"),
        ("step", "Fix validation errors"),
        ("trace-end", "job-9"),
        ("close", "job-9", True),
    ]
    assert not hasattr(generation.draft_run, "record_call")


def test_a_given_job_id_is_used_instead_of_opening_a_run(monkeypatch):
    _install(monkeypatch, [GOOD])
    traced = []

    async def open_run(session, **kwargs):
        raise AssertionError("the run is already open")

    async def close_run(job_id, **kwargs):
        traced.append(("close", job_id))

    @asynccontextmanager
    async def trace(job_id, *args):
        traced.append(("trace", job_id))
        yield

    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)
    monkeypatch.setattr(generation, "generation_trace", trace)
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill", _Group(), job_id="pre")
    )
    assert out["job_id"] == "pre"
    assert traced == [("trace", "pre"), ("close", "pre")]


def test_without_a_run_the_draft_is_not_traced(monkeypatch):
    _install(monkeypatch, [GOOD])

    def trace(*args):
        raise AssertionError("no run, nothing to trace under")

    monkeypatch.setattr(generation, "generation_trace", trace)
    out = asyncio.run(generation.SkillGenerationService.draft("a skill", _Group()))
    assert out["job_id"] is None and out["valid"] is True


def test_an_llm_failure_fails_the_run_and_still_raises(monkeypatch):
    async def completion(**kwargs):
        raise RuntimeError("endpoint down")

    async def template(name, gc):
        return "T"

    monkeypatch.setattr(generation.LLMManager, "completion", completion)
    monkeypatch.setattr(
        generation.TemplateService, "get_effective_template_content", template
    )
    closed = []

    async def open_run(session, **kwargs):
        return "job-x"

    async def close_run(job_id, **kwargs):
        closed.append((job_id, kwargs.get("error")))

    @asynccontextmanager
    async def trace(*args):
        yield

    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)
    monkeypatch.setattr(generation, "generation_trace", trace)
    try:
        asyncio.run(
            generation.SkillGenerationService.draft("a skill", _Group(), session="S")
        )
    except RuntimeError as exc:
        assert "endpoint down" in str(exc)
    else:
        raise AssertionError("expected the LLM failure to propagate")
    assert closed == [("job-x", "endpoint down")]


# ── The real path: LLM events -> scoped OTelEventBridge -> exporter ──────────


class _TracedGroup(_Group):
    group_email = "dev@example.com"


def _emitting_completion(replies):
    """A completion that behaves like the transport: it emits the LLM events on
    the bus from the LLM executor thread (contextvars copied, as
    ``_run_llm_blocking`` does) and answers with the next reply."""
    from src.core.events.bus import event_bus
    from src.core.events.types import LLMCallCompletedEvent, LLMCallStartedEvent

    async def completion(**kwargs):
        reply = replies.pop(0)

        def call():
            event_bus.emit(
                None,
                LLMCallStartedEvent(model="served", messages=kwargs["messages"]),
            )
            event_bus.emit(
                None,
                LLMCallCompletedEvent(
                    call_type="llm_call",
                    model="served",
                    response=reply,
                    usage={"prompt_tokens": 11, "completion_tokens": 7},
                ),
            )
            return reply

        return await asyncio.to_thread(call), "served"

    return completion


def test_the_draft_llm_calls_become_the_runs_trace_spans(monkeypatch):
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from src.core.events.bus import event_bus
    from src.core.events.types import LLMCallStartedEvent
    from src.services.otel_tracing import generation_scope

    exporters = {}

    def exporter(job_id, group_context):
        exporters[job_id] = (InMemorySpanExporter(), group_context)
        return exporters[job_id][0]

    async def open_run(session, **kwargs):
        return "draft-job"

    async def close_run(job_id, **kwargs):
        return None

    bad = json.dumps({"name": "Not Kebab", "description": "d", "body": "b"})
    monkeypatch.setattr(generation_scope, "KasalDBSpanExporter", exporter)
    monkeypatch.setattr(
        generation.LLMManager, "completion", _emitting_completion([bad, GOOD])
    )
    monkeypatch.setattr(
        generation.TemplateService,
        "get_effective_template_content",
        lambda *a: asyncio.sleep(0, result="SYSTEM"),
    )
    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)
    handlers_before = sum(len(h) for h in event_bus._handlers.values())

    out = asyncio.run(
        generation.SkillGenerationService.draft(
            "release notes", _TracedGroup(), session="S"
        )
    )

    assert out["valid"] is True and out["job_id"] == "draft-job"
    exp, group = exporters["draft-job"]
    assert group.primary_group_id == "g1"  # rows are written for this workspace
    spans = exp.get_finished_spans()
    kinds = [s.attributes.get("kasal.event_type") for s in spans]
    assert kinds.count("llm_call") == 2 and kinds.count("llm_response") == 2
    assert kinds[0] == "task_started" and kinds[-1] == "task_completed"
    request = next(
        s for s in spans if s.attributes.get("kasal.event_type") == "llm_call"
    )
    assert "release notes" in request.attributes["kasal.extra.prompt"]
    answers = [
        s for s in spans if s.attributes.get("kasal.event_type") == "llm_response"
    ]
    assert answers[-1].attributes["kasal.output_content"] == GOOD
    # The bridge is gone with the draft: a later LLM call is nobody's row.
    assert sum(len(h) for h in event_bus._handlers.values()) == handlers_before
    count = len(spans)
    event_bus.emit(None, LLMCallStartedEvent(model="m", messages=[]))
    assert len(exp.get_finished_spans()) == count


def test_the_draft_trace_is_persisted_for_its_workspace_only(tmp_path, monkeypatch):
    """Bridge -> KasalDBSpanExporter -> repository, on an isolated database."""
    from contextlib import asynccontextmanager as _acm

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    from src.db import all_models  # noqa: F401 -- resolve ORM relationships
    from src.db import session as sessions
    from src.models.execution_trace import ExecutionTrace

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'trace.db'}", poolclass=NullPool
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)

    @_acm
    async def isolated():
        async with factory() as session:
            yield session

    async def open_run(session, **kwargs):
        return "skill-job"

    async def close_run(job_id, **kwargs):
        return None

    monkeypatch.setattr(sessions, "routed_scoped_session", isolated)
    monkeypatch.setattr(
        generation.LLMManager, "completion", _emitting_completion([GOOD])
    )
    monkeypatch.setattr(
        generation.TemplateService,
        "get_effective_template_content",
        lambda *a: asyncio.sleep(0, result="SYSTEM"),
    )
    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)

    async def run():
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "CREATE TABLE executionhistory (id INTEGER PRIMARY KEY, job_id TEXT UNIQUE)"
                )
            )
            await conn.execute(
                text("INSERT INTO executionhistory (job_id) VALUES ('skill-job')")
            )
            await conn.run_sync(ExecutionTrace.__table__.create)
        await generation.SkillGenerationService.draft(
            "release notes", _TracedGroup(), session="S"
        )
        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT event_type, job_id, group_id, event_source, output "
                        "FROM execution_trace ORDER BY id"
                    )
                )
            ).all()
        await engine.dispose()
        return rows

    rows = asyncio.run(run())
    assert [r[0] for r in rows] == [
        "task_started",
        "llm_call",
        "llm_response",
        "task_completed",
    ]
    assert {r[1] for r in rows} == {"skill-job"}
    assert {r[2] for r in rows} == {"g1"}
    assert {r[3] for r in rows} == {"Skills"}
    assert "writing-release-notes" in rows[2][4]
