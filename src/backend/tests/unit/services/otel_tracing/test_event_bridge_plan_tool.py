"""One plan change is ONE trace row.

The ``todo`` tool fires the usual tool_usage pair around every call, and each
write also emits ``PlanUpdatedEvent`` — so a single plan update used to reach
the timeline as three rows: ``todo``, ``Plan Updated``, ``todo``. The bridge
absorbs the tool pair; the plan event is the row.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.otel_tracing.event_bridge import OTelEventBridge


def _bridge_with_tracer():
    tracer = MagicMock()
    span = MagicMock()
    tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=span)
    tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
    return OTelEventBridge(tracer=tracer, job_id="job-1"), tracer, span


class TestPlanToolSpansAreAbsorbed:
    def test_the_todo_tool_start_writes_no_span(self):
        bridge, tracer, _ = _bridge_with_tracer()

        bridge._emit_span(
            "kasal.tool.execute",
            "tool_usage",
            SimpleNamespace(tool_name="todo", tool_args="{'todos': []}"),
        )

        tracer.start_as_current_span.assert_not_called()

    def test_the_todo_tool_finish_writes_no_span(self):
        bridge, tracer, _ = _bridge_with_tracer()

        bridge._emit_span(
            "kasal.tool.complete",
            "tool_usage",
            SimpleNamespace(tool_name="todo", output="Plan updated."),
        )

        tracer.start_as_current_span.assert_not_called()

    def test_a_failing_todo_call_keeps_its_error_row(self):
        """The one case PlanUpdatedEvent never fires for."""
        bridge, tracer, _ = _bridge_with_tracer()

        bridge._emit_span(
            "kasal.tool.error",
            "tool_error",
            SimpleNamespace(tool_name="todo", error="'todos' must be a list"),
        )

        tracer.start_as_current_span.assert_called_once()

    def test_every_other_tool_is_untouched(self):
        bridge, tracer, _ = _bridge_with_tracer()

        bridge._emit_span(
            "kasal.tool.execute",
            "tool_usage",
            SimpleNamespace(tool_name="PerplexityTool"),
        )

        tracer.start_as_current_span.assert_called_once()

    def test_the_plan_event_still_carries_the_plan(self):
        """What the absorbed rows are traded for: items and counts, on one span."""
        bridge, _, span = _bridge_with_tracer()

        bridge._emit_span(
            "kasal.task.plan_updated",
            "plan_updated",
            SimpleNamespace(
                type="plan_updated",
                items=[{"id": "1", "content": "Scrape", "status": "in_progress"}],
                rendered="- [~] Scrape",
                total=1,
                pending=0,
                in_progress=1,
                completed=0,
                cancelled=0,
            ),
        )

        attrs = {call[0][0]: call[0][1] for call in span.set_attribute.call_args_list}
        assert attrs["kasal.extra.plan_total"] == 1
        assert attrs["kasal.extra.plan_in_progress"] == 1
        assert "Scrape" in attrs["kasal.extra.plan_items"]


class TestLivePipeAgrees:
    """The pipe must absorb the same rows, or the live view shows an orphan pill
    for a tool call that never lands in the database."""

    def test_the_pipe_drops_the_todo_tool_frame(self):
        from src.services.execution.event_pipe import EventPipeWriter

        writer = EventPipeWriter(queue=MagicMock(), execution_id="run-1")

        frame = writer._project_trace_frame(
            "tool_usage", SimpleNamespace(tool_name="todo")
        )

        assert frame is None

    def test_the_pipe_keeps_other_tool_frames(self):
        from src.services.execution.event_pipe import EventPipeWriter

        writer = EventPipeWriter(queue=MagicMock(), execution_id="run-1")

        frame = writer._project_trace_frame(
            "tool_usage", SimpleNamespace(tool_name="PerplexityTool")
        )

        assert frame is not None
        assert frame["trace_metadata"]["tool_name"] == "PerplexityTool"
