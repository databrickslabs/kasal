"""Guardrail events must reach the trace timeline (engine → bus → OTel bridge).

Mirror of test_memory_trace_integration: real engine Task guardrail execution,
real event bus, real OTelEventBridge, in-memory OTel exporter. Asserts the span
names / event types that db_exporter and the frontend timeline consume — this
chain was completely dead after the migration (events never existed).
"""

from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from src.services.execution.runtime.task import Task
from src.services.execution.runtime.types import TaskOutput
from src.services.otel_tracing.event_bridge import OTelEventBridge


@pytest.fixture
def bus():
    from src.core.events import event_bus

    snapshot = {k: list(v) for k, v in event_bus._handlers.items()}
    yield event_bus
    event_bus._handlers = snapshot


@pytest.fixture
def spans(bus):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    bridge = OTelEventBridge(provider.get_tracer("test-bridge"), "job-1", None)
    bridge.register(bus)
    return exporter


def _span_names(exporter):
    return [span.name for span in exporter.get_finished_spans()]


class TestGuardrailSpans:
    def test_pass_produces_started_and_completed_spans(self, spans):
        task = Task(
            description="Collect Swiss news",
            expected_output="A report",
            name="collect news",
            guardrail=lambda output: (True, None),
        )
        output = TaskOutput(
            description="Collect Swiss news", agent="Reporter", raw="ok"
        )

        task._apply_guardrails(output, agent=MagicMock(), context=None, tools=None)

        names = _span_names(spans)
        assert "kasal.guardrail.started" in names
        assert "kasal.guardrail.completed" in names

        completed = next(
            s
            for s in spans.get_finished_spans()
            if s.name == "kasal.guardrail.completed"
        )
        attrs = dict(completed.attributes)
        # event_type the db_exporter maps and the frontend renders.
        assert attrs["kasal.event_type"] == "llm_guardrail"
        assert attrs["kasal.extra.success"] is True
        # Task attribution keeps the row under the validated task.
        assert attrs["kasal.extra.task_id"] == str(task.id)

    def test_exhausted_failure_produces_failed_span(self, spans):
        task = Task(
            description="d",
            expected_output="e",
            guardrail=lambda output: (False, "rejected"),
            max_retries=0,
        )
        output = TaskOutput(description="d", agent="A", raw="bad")

        with pytest.raises(ValueError):
            task._apply_guardrails(output, agent=MagicMock(), context=None, tools=None)

        names = _span_names(spans)
        assert "kasal.guardrail.failed" in names
        # success=False is carried by the completed span of the failing attempt.
        completed = next(
            s
            for s in spans.get_finished_spans()
            if s.name == "kasal.guardrail.completed"
        )
        assert dict(completed.attributes)["kasal.extra.success"] is False
        failed = next(
            s for s in spans.get_finished_spans() if s.name == "kasal.guardrail.failed"
        )
        attrs = dict(failed.attributes)
        assert attrs["kasal.event_type"] == "llm_guardrail"
        assert attrs["kasal.extra.retry_count"] == 0

    def test_bridge_subscribes_to_guardrail_events(self, bus, spans):
        """The migration's dangling subscriptions must resolve now."""
        from src.core.events import LLMGuardrailStartedEvent

        assert any(
            handlers
            for cls, handlers in bus._handlers.items()
            if cls is LLMGuardrailStartedEvent and handlers
        ), "bridge did not subscribe to LLMGuardrailStartedEvent"
