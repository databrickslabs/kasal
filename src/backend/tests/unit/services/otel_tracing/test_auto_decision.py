"""Decide, then call: the Auto decision and the chosen model's calls, linked.

The decision is recorded in the API process (an ``execution_trace`` row with OTel
ids); the run continues that trace. These follow it through every consumer: the
OTel spans the bridge emits, the rows the DB exporter writes from them, the
MLflow exporter's buffered trace, the chat path's hand-built rows, and a real
spawned interpreter that receives the decision in its payload.
"""

import json
import pathlib
import subprocess
import sys
from unittest.mock import AsyncMock, patch

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from src.core.events.bus import EventsBus
from src.core.events.types import (
    CrewKickoffStartedEvent,
    LLMCallCompletedEvent,
    LLMCallStartedEvent,
    LLMCallType,
)
from src.services.decisions.model_selection import ModelSelection, trace_row
from src.services.execution.config import auto_model
from src.services.otel_tracing.auto_decision import (
    PAYLOAD_KEY,
    AutoDecision,
    link_row,
)
from src.services.otel_tracing.db_exporter import KasalDBSpanExporter
from src.services.otel_tracing.event_bridge import OTelEventBridge
from src.services.otel_tracing.mlflow_exporter import KasalMLflowSpanExporter
from src.services.otel_tracing.otel_config import create_kasal_tracer_provider

BACKEND = pathlib.Path(__file__).resolve().parents[4]
CHOSEN = "claude-opus-5-5"
PROMPT = "summarise the secret plan"
KEY = "sk-or-secret"
PICK = ModelSelection(
    CHOSEN,
    "selected",
    42.0,
    connection="openrouter",
    candidates=3,
    picked="1",
    confidence=0.61,
)


def _record(job_id):
    """``run_freeze.record`` without its (fire-and-forget) row write."""
    with patch.object(auto_model, "_write_trace", new=AsyncMock()):
        auto_model.record_selection(PICK, job_id, None)


def _run(selection=PICK, payload=None):
    """One run's spans: the bridge on a fresh bus, as the subprocess wires it."""
    provider = create_kasal_tracer_provider(job_id="run-1")
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    bus = EventsBus()
    config = payload if payload is not None else {PAYLOAD_KEY: selection.trace()}
    bridge = OTelEventBridge(provider.get_tracer("t"), "run-1", None, config)
    bridge.register(bus)
    bus.emit(None, CrewKickoffStartedEvent(crew_name="c", inputs={}))
    bus.emit(
        None,
        LLMCallStartedEvent(
            model=CHOSEN, messages=[{"role": "user", "content": PROMPT}]
        ),
    )
    bus.emit(
        None,
        LLMCallCompletedEvent(
            model=CHOSEN, response="answer", call_type=LLMCallType.LLM_CALL
        ),
    )
    bus.emit(None, LLMCallStartedEvent(model="some-other-model", messages=[]))
    bridge.unregister()
    spans: dict = {}
    for span in exporter.get_finished_spans():
        spans.setdefault(span.name, span)  # the first of each: the chosen model's
    return spans, exporter


class TestTheOTelTrace:
    def test_the_decision_span_keeps_the_rows_ids(self):
        spans, _ = _run()
        decision = spans["kasal.decision.model_selection"]
        row = trace_row(PICK, "run-1", "ws")
        assert f"{decision.context.trace_id:016x}" == row["trace_id"]
        assert f"{decision.context.span_id:016x}" == row["span_id"]
        assert decision.parent is None

    def test_its_attributes_say_what_was_decided_and_nothing_else(self):
        spans, _ = _run()
        attributes = dict(spans["kasal.decision.model_selection"].attributes)
        assert attributes["kasal.decision.policy"] == "model_selection"
        assert attributes["kasal.decision.connection"] == "openrouter"
        assert attributes["kasal.decision.picked"] == "1"
        assert attributes["kasal.decision.model"] == CHOSEN
        assert attributes["kasal.decision.status"] == "selected"
        assert attributes["kasal.decision.candidates"] == 3
        assert attributes["kasal.decision.duration_ms"] == 42.0
        assert attributes["kasal.decision.confidence"] == 0.61
        assert attributes["kasal.output_content"].endswith(f"{CHOSEN} (0.61)")
        assert "kasal.decision.reason" not in attributes
        text = json.dumps(attributes)
        assert PROMPT not in text and KEY not in text

    def test_the_chosen_models_calls_join_the_decisions_trace(self):
        spans, _ = _run()
        decision = spans["kasal.decision.model_selection"]
        crew = spans["kasal.crew.kickoff"]
        call = spans["kasal.llm.call_started"]
        # The run's root continues the decision's trace, as its child.
        assert crew.parent.span_id == decision.context.span_id
        for span in (crew, call, spans["kasal.llm.call_completed"]):
            assert span.context.trace_id == decision.context.trace_id
            assert span.start_time >= decision.start_time
        assert call.attributes["kasal.auto.selected_model"] == CHOSEN
        assert call.attributes["kasal.auto.decision_span_id"] == PICK.span_id

    def test_another_models_call_is_not_labelled_with_the_pick(self):
        _, exporter = _run()
        other = [
            s
            for s in exporter.get_finished_spans()
            if s.name == "kasal.llm.call_started"
            and s.attributes.get("kasal.extra.model", "") != CHOSEN
            and "kasal.auto.selected_model" not in s.attributes
        ]
        assert len(other) == 1

    def test_a_run_without_auto_is_unchanged(self):
        spans, _ = _run(payload={})
        assert "kasal.decision.model_selection" not in spans
        assert spans["kasal.crew.kickoff"].parent is None
        assert (
            "kasal.auto.selected_model"
            not in spans["kasal.llm.call_started"].attributes
        )


class TestTheExecutionTraceRows:
    def test_rows_link_to_the_decision_row_and_it_is_not_written_twice(self):
        _, exporter = _run()
        writer = KasalDBSpanExporter("run-1")
        records = [writer._span_to_record(s) for s in exporter.get_finished_spans()]
        assert not any(r and r["event_type"] == "decision_evaluated" for r in records)
        decision_row = trace_row(PICK, "run-1", "ws")
        crew = next(r for r in records if r and r["event_type"] == "crew_started")
        call = next(r for r in records if r and r["event_type"] == "llm_call")
        assert crew["parent_span_id"] == decision_row["span_id"]
        assert call["trace_id"] == decision_row["trace_id"]
        assert call["trace_metadata"]["auto_selected_model"] == CHOSEN
        assert call["trace_metadata"]["auto_decision_span_id"] == PICK.span_id

    def test_the_chat_paths_rows_link_the_same_way(self):
        decision = AutoDecision.from_trace(PICK.trace())
        call = link_row(
            {
                "event_type": "llm_call",
                "output": {"input": CHOSEN},
                "trace_metadata": {},
            },
            decision,
        )
        assert (call["trace_id"], call["parent_span_id"]) == (
            PICK.trace_id,
            PICK.span_id,
        )
        assert call["trace_metadata"] == {
            "auto_selected_model": CHOSEN,
            "auto_decision_span_id": PICK.span_id,
        }
        tool = link_row({"event_type": "tool_run", "trace_metadata": {}}, decision)
        assert tool["trace_metadata"] == {} and tool["trace_id"] == PICK.trace_id
        assert link_row({"event_type": "llm_call"}, None) == {"event_type": "llm_call"}

    @pytest.mark.asyncio
    async def test_the_chat_path_finds_its_runs_decision(self):
        _record("chat-run")
        row = {"event_type": "llm_call", "output": {"input": CHOSEN}}
        row["trace_metadata"] = {}
        auto_model.link_run_row(row, "chat-run")
        assert row["parent_span_id"] == PICK.span_id


class TestMLflow:
    def test_the_buffered_trace_holds_both_spans_linked(self):
        _, exporter = _run()
        mlflow = KasalMLflowSpanExporter("run-1", mlflow_result=None)
        paired, instants = mlflow._pair_events(list(exporter.get_finished_spans()))
        decision = next(i for i in instants if i.event_type == "decision_evaluated")
        assert decision.attributes["kasal.decision.model"] == CHOSEN
        call = next(p for p in paired if p.event_type == "llm_call")
        assert call.attributes["kasal.auto.decision_span_id"] == PICK.span_id
        assert call.attributes["kasal.auto.selected_model"] == CHOSEN


class TestThePayload:
    @pytest.mark.asyncio
    async def test_the_engine_stamps_the_recorded_decision(self):
        _record("crew-run")
        payload: dict = {}
        auto_model.stamp_decision(payload, "crew-run")
        assert json.loads(json.dumps(payload))[PAYLOAD_KEY]["model"] == CHOSEN
        untouched: dict = {}
        auto_model.stamp_decision(untouched, "never-recorded")
        assert untouched == {}

    def test_a_malformed_payload_links_nothing(self):
        assert AutoDecision.from_payload({PAYLOAD_KEY: {"policy": "x"}}) is None
        broken = {**PICK.trace(), "span_id": "not-hex"}
        assert AutoDecision.from_payload({PAYLOAD_KEY: broken}) is None
        assert AutoDecision.from_payload(None) is None


CHILD = """
import json, sys
import src.services.agent_builder.process_executor  # the crew subprocess entry
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from src.core.events.bus import EventsBus
from src.core.events.types import LLMCallStartedEvent
from src.services.otel_tracing.event_bridge import OTelEventBridge
from src.services.otel_tracing.otel_config import create_kasal_tracer_provider

crew_config = json.load(sys.stdin)
decision = crew_config["auto_decision"]
assert crew_config["model"] == decision["model"], crew_config["model"]
provider = create_kasal_tracer_provider(job_id="run-1")
exporter = InMemorySpanExporter()
provider.add_span_processor(SimpleSpanProcessor(exporter))
bus = EventsBus()
OTelEventBridge(provider.get_tracer("t"), "run-1", None, crew_config).register(bus)
bus.emit(None, LLMCallStartedEvent(model=crew_config["model"], messages=[]))
spans = {s.name: s for s in exporter.get_finished_spans()}
root = spans["kasal.decision.model_selection"]
call = spans["kasal.llm.call_started"]
assert f"{root.context.span_id:016x}" == decision["span_id"]
assert call.context.trace_id == root.context.trace_id
assert call.attributes["kasal.auto.selected_model"] == decision["model"]
print("child-ok")
"""


@pytest.mark.asyncio
async def test_a_spawned_crew_interpreter_continues_the_decisions_trace():
    """The payload crosses a real process boundary and the child links to it."""
    _record("spawned-run")
    crew_config = {"model": CHOSEN, "agents": [], "tasks": []}
    auto_model.stamp_decision(crew_config, "spawned-run")
    child = subprocess.run(
        [sys.executable, "-c", CHILD],
        input=json.dumps(crew_config),
        capture_output=True,
        text=True,
        cwd=BACKEND,
        timeout=300,
    )
    assert "child-ok" in child.stdout, child.stderr[-3000:]
