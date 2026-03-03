"""
Unit tests for null-guard paths in event_bridge.py.

Covers:
- _get_agent_name with None/missing attributes
- _get_task_name with None/missing attributes
- _get_tool_name with None/missing attributes
- _emit_span with event that has no agent/task/tool
"""

import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from src.services.otel_tracing.event_bridge import (
    _get_agent_name,
    _get_task_name,
    _get_tool_name,
    _get_output,
    _safe_str,
    OTelEventBridge,
)


class TestGetAgentNameNullGuards:
    """Test null guards in _get_agent_name."""

    def test_agent_role_on_event(self):
        """agent_role directly on event is used first."""
        event = SimpleNamespace(agent_role="Researcher")
        assert _get_agent_name(event) == "Researcher"

    def test_agent_object_role(self):
        """Falls back to event.agent.role."""
        event = SimpleNamespace(agent=SimpleNamespace(role="Analyst"))
        assert _get_agent_name(event) == "Analyst"

    def test_task_agent_role(self):
        """Falls back to event.task.agent.role."""
        event = SimpleNamespace(
            task=SimpleNamespace(agent=SimpleNamespace(role="Writer"))
        )
        assert _get_agent_name(event) == "Writer"

    def test_no_agent_info_returns_empty(self):
        """Returns empty string when no agent info available."""
        event = SimpleNamespace()
        assert _get_agent_name(event) == ""

    def test_agent_role_none(self):
        """agent_role=None falls through to agent.role."""
        event = SimpleNamespace(
            agent_role=None,
            agent=SimpleNamespace(role="Fallback"),
        )
        assert _get_agent_name(event) == "Fallback"

    def test_agent_with_no_role(self):
        """agent object exists but has no role attribute."""
        event = SimpleNamespace(agent=SimpleNamespace())
        assert _get_agent_name(event) == ""

    def test_agent_with_none_role(self):
        """agent.role is None."""
        event = SimpleNamespace(agent=SimpleNamespace(role=None))
        assert _get_agent_name(event) == ""

    def test_task_agent_none(self):
        """task exists but task.agent is None."""
        event = SimpleNamespace(task=SimpleNamespace(agent=None))
        assert _get_agent_name(event) == ""

    def test_task_agent_no_role(self):
        """task.agent exists but has no role."""
        event = SimpleNamespace(task=SimpleNamespace(agent=SimpleNamespace()))
        assert _get_agent_name(event) == ""


class TestGetTaskNameNullGuards:
    """Test null guards in _get_task_name."""

    def test_task_name_on_event(self):
        """task_name directly on event is used first."""
        event = SimpleNamespace(task_name="Research topic X")
        assert _get_task_name(event) == "Research topic X"

    def test_task_description(self):
        """Falls back to event.task.description."""
        event = SimpleNamespace(
            task=SimpleNamespace(description="Analyze data")
        )
        assert _get_task_name(event) == "Analyze data"

    def test_task_name_attr(self):
        """Falls back to event.task.name if no description."""
        event = SimpleNamespace(
            task=SimpleNamespace(name="WriteReport")
        )
        assert _get_task_name(event) == "WriteReport"

    def test_no_task_returns_empty(self):
        """Returns empty string when no task info available."""
        event = SimpleNamespace()
        assert _get_task_name(event) == ""

    def test_task_name_none_on_event(self):
        """task_name=None falls through to task.description."""
        event = SimpleNamespace(
            task_name=None,
            task=SimpleNamespace(description="Fallback desc"),
        )
        assert _get_task_name(event) == "Fallback desc"

    def test_task_with_no_desc_no_name(self):
        """task exists but has neither description nor name."""
        event = SimpleNamespace(task=SimpleNamespace())
        assert _get_task_name(event) == ""


class TestGetToolNameNullGuards:
    """Test null guards in _get_tool_name."""

    def test_tool_name_attr(self):
        """tool_name directly on event."""
        event = SimpleNamespace(tool_name="web_search")
        assert _get_tool_name(event) == "web_search"

    def test_tool_attr_fallback(self):
        """Falls back to event.tool."""
        event = SimpleNamespace(tool="calculator")
        assert _get_tool_name(event) == "calculator"

    def test_no_tool_returns_empty(self):
        """Returns empty string when no tool info available."""
        event = SimpleNamespace()
        assert _get_tool_name(event) == ""


class TestGetOutput:
    """Test _get_output extraction."""

    def test_output_attr(self):
        event = SimpleNamespace(output="result text")
        assert _get_output(event) == "result text"

    def test_result_attr(self):
        event = SimpleNamespace(result="result text")
        assert _get_output(event) == "result text"

    def test_no_output_returns_empty(self):
        event = SimpleNamespace()
        assert _get_output(event) == ""


class TestSafeStr:
    """Test _safe_str helper."""

    def test_none_returns_empty(self):
        assert _safe_str(None) == ""

    def test_truncation(self):
        long_str = "x" * 600
        result = _safe_str(long_str, max_len=500)
        assert len(result) == 500

    def test_short_string_unchanged(self):
        assert _safe_str("hello") == "hello"


class TestEmitSpanNullGuards:
    """Test _emit_span handles events with missing attributes gracefully."""

    def test_emit_span_with_empty_event(self):
        """_emit_span works with an event that has no attributes at all."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)

        bridge = OTelEventBridge(tracer=mock_tracer, job_id="job-1")

        # SimpleNamespace() has no attributes — should not raise
        event = SimpleNamespace()
        bridge._emit_span("test.span", "test_event", event)

        mock_tracer.start_as_current_span.assert_called_once_with("test.span")
        mock_span.set_attribute.assert_any_call("kasal.event_type", "test_event")

    def test_emit_span_captures_crew_name(self):
        """_emit_span captures crew_name from event and stores it for later spans."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)

        bridge = OTelEventBridge(tracer=mock_tracer, job_id="job-1")

        event = SimpleNamespace(crew_name="My Crew")
        bridge._emit_span("CrewAI.crew.kickoff", "crew_started", event)

        assert bridge._current_crew_name == "My Crew"

    def test_emit_span_error_event_sets_error_status(self):
        """_emit_span sets ERROR status on failed/error event types."""
        from opentelemetry.trace import StatusCode

        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)

        bridge = OTelEventBridge(tracer=mock_tracer, job_id="job-1")

        event = SimpleNamespace(error="something broke")
        bridge._emit_span("test.span", "task_failed", event)

        mock_span.set_status.assert_called_once()
        args = mock_span.set_status.call_args
        assert args[0][0] == StatusCode.ERROR

    def test_emit_span_exception_does_not_propagate(self):
        """If span creation fails, _emit_span logs error but doesn't propagate."""
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.side_effect = RuntimeError("tracer error")

        bridge = OTelEventBridge(tracer=mock_tracer, job_id="job-1")

        # Should not raise
        event = SimpleNamespace()
        bridge._emit_span("test.span", "test_event", event)
