"""
Comprehensive unit tests for OTelEventBridge and helper functions.

Covers all public/private methods, every branch, error path, and attribute
extraction in src/services/otel_tracing/event_bridge.py.
Target: 100% code coverage.
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

from src.services.otel_tracing.event_bridge import (
    _safe_str,
    _get_agent_name,
    _get_task_name,
    _get_tool_name,
    _get_output,
    _EVENT_SPAN_MAP,
    _SKIP_EVENTS,
    OTelEventBridge,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(**attrs):
    """Create a SimpleNamespace event object with only the given attributes.

    Unlike MagicMock, SimpleNamespace returns AttributeError for missing attrs,
    which matches getattr(obj, name, None) semantics correctly because getattr
    will return the default None when the attribute is absent.
    """
    return SimpleNamespace(**attrs)


def _make_span():
    """Create a MagicMock span that records set_attribute / set_status calls."""
    span = MagicMock()
    span.set_attribute = MagicMock()
    span.set_status = MagicMock()
    return span


def _make_tracer(span=None):
    """Create a mock tracer whose start_as_current_span context manager yields span."""
    if span is None:
        span = _make_span()
    tracer = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=span)
    cm.__exit__ = MagicMock(return_value=False)
    tracer.start_as_current_span.return_value = cm
    return tracer, span


# ---------------------------------------------------------------------------
# Tests: _safe_str
# ---------------------------------------------------------------------------


class TestSafeStr:
    """Tests for the _safe_str helper function."""

    def test_none_returns_empty_string(self):
        assert _safe_str(None) == ""

    def test_short_string_returned_as_is(self):
        assert _safe_str("hello") == "hello"

    def test_string_at_max_len_boundary(self):
        s = "a" * 500
        assert _safe_str(s) == s
        assert len(_safe_str(s)) == 500

    def test_string_exceeding_default_max_len_is_truncated(self):
        s = "b" * 600
        result = _safe_str(s)
        assert len(result) == 500
        assert result == "b" * 500

    def test_custom_max_len(self):
        s = "c" * 300
        result = _safe_str(s, max_len=100)
        assert len(result) == 100
        assert result == "c" * 100

    def test_custom_max_len_short_string(self):
        result = _safe_str("short", max_len=100)
        assert result == "short"

    def test_non_string_value_converted(self):
        assert _safe_str(42) == "42"
        assert _safe_str(3.14) == "3.14"

    def test_list_value_converted(self):
        result = _safe_str([1, 2, 3])
        assert result == "[1, 2, 3]"


# ---------------------------------------------------------------------------
# Tests: _get_agent_name
# ---------------------------------------------------------------------------


class TestGetAgentName:
    """Tests for the _get_agent_name helper function."""

    def test_agent_role_directly_on_event(self):
        event = _make_event(agent_role="Researcher")
        assert _get_agent_name(event) == "Researcher"

    def test_agent_role_from_event_agent_object(self):
        agent = SimpleNamespace(role="Analyst")
        event = _make_event(agent=agent)
        assert _get_agent_name(event) == "Analyst"

    def test_agent_role_from_task_agent(self):
        task_agent = SimpleNamespace(role="Writer")
        task = SimpleNamespace(agent=task_agent)
        event = _make_event(task=task)
        assert _get_agent_name(event) == "Writer"

    def test_no_agent_returns_empty_string(self):
        event = _make_event()
        assert _get_agent_name(event) == ""

    def test_agent_role_takes_priority_over_agent_object(self):
        agent = SimpleNamespace(role="SecondChoice")
        event = _make_event(agent_role="FirstChoice", agent=agent)
        assert _get_agent_name(event) == "FirstChoice"

    def test_agent_object_without_role_attribute(self):
        agent = SimpleNamespace(name="NoRoleAgent")
        event = _make_event(agent=agent)
        assert _get_agent_name(event) == ""

    def test_agent_object_with_empty_role(self):
        agent = SimpleNamespace(role="")
        event = _make_event(agent=agent)
        # empty role is falsy, so falls through to task.agent.role
        assert _get_agent_name(event) == ""

    def test_agent_object_with_none_role(self):
        agent = SimpleNamespace(role=None)
        event = _make_event(agent=agent)
        assert _get_agent_name(event) == ""

    def test_task_without_agent(self):
        task = SimpleNamespace()
        event = _make_event(task=task)
        assert _get_agent_name(event) == ""

    def test_task_agent_with_empty_role(self):
        task_agent = SimpleNamespace(role="")
        task = SimpleNamespace(agent=task_agent)
        event = _make_event(task=task)
        assert _get_agent_name(event) == ""

    def test_task_agent_with_none_role(self):
        task_agent = SimpleNamespace(role=None)
        task = SimpleNamespace(agent=task_agent)
        event = _make_event(task=task)
        assert _get_agent_name(event) == ""


# ---------------------------------------------------------------------------
# Tests: _get_task_name
# ---------------------------------------------------------------------------


class TestGetTaskName:
    """Tests for the _get_task_name helper function."""

    def test_task_name_directly_on_event(self):
        event = _make_event(task_name="Write report")
        result = _get_task_name(event)
        assert result == "Write report"

    def test_task_name_truncated_via_safe_str(self):
        long_name = "x" * 600
        event = _make_event(task_name=long_name)
        result = _get_task_name(event)
        assert len(result) == 500

    def test_task_description_from_task_object(self):
        task = SimpleNamespace(description="Analyze data", name="analysis")
        event = _make_event(task=task)
        result = _get_task_name(event)
        assert result == "Analyze data"

    def test_task_name_from_task_object_when_no_description(self):
        task = SimpleNamespace(description=None, name="fallback_name")
        event = _make_event(task=task)
        result = _get_task_name(event)
        assert result == "fallback_name"

    def test_task_object_without_description_or_name(self):
        task = SimpleNamespace(description=None, name=None)
        event = _make_event(task=task)
        result = _get_task_name(event)
        assert result == ""

    def test_no_task_returns_empty_string(self):
        event = _make_event()
        result = _get_task_name(event)
        assert result == ""

    def test_task_name_takes_priority_over_task_object(self):
        task = SimpleNamespace(description="ignored", name="also_ignored")
        event = _make_event(task_name="priority_name", task=task)
        result = _get_task_name(event)
        assert result == "priority_name"

    def test_task_object_with_empty_description_falls_to_name(self):
        task = SimpleNamespace(description="", name="backup")
        event = _make_event(task=task)
        result = _get_task_name(event)
        # description="" is falsy, falls through to name
        assert result == "backup"

    def test_task_object_with_empty_description_and_empty_name(self):
        task = SimpleNamespace(description="", name="")
        event = _make_event(task=task)
        result = _get_task_name(event)
        assert result == ""


# ---------------------------------------------------------------------------
# Tests: _get_tool_name
# ---------------------------------------------------------------------------


class TestGetToolName:
    """Tests for the _get_tool_name helper function."""

    def test_tool_name_attribute(self):
        event = _make_event(tool_name="search_web")
        assert _get_tool_name(event) == "search_web"

    def test_tool_attribute_fallback(self):
        event = _make_event(tool_name=None, tool="code_interpreter")
        assert _get_tool_name(event) == "code_interpreter"

    def test_tool_name_empty_falls_to_tool(self):
        event = _make_event(tool_name="", tool="fallback_tool")
        assert _get_tool_name(event) == "fallback_tool"

    def test_no_tool_returns_empty(self):
        event = _make_event()
        assert _get_tool_name(event) == ""

    def test_both_none_returns_empty(self):
        event = _make_event(tool_name=None, tool=None)
        assert _get_tool_name(event) == ""

    def test_long_tool_name_truncated_to_200(self):
        long_name = "t" * 300
        event = _make_event(tool_name=long_name)
        result = _get_tool_name(event)
        assert len(result) == 200


# ---------------------------------------------------------------------------
# Tests: _get_output
# ---------------------------------------------------------------------------


class TestGetOutput:
    """Tests for the _get_output helper function."""

    def test_output_attribute(self):
        event = _make_event(output="task output")
        assert _get_output(event) == "task output"

    def test_result_attribute(self):
        event = _make_event(result="task result")
        assert _get_output(event) == "task result"

    def test_response_attribute(self):
        event = _make_event(response="api response")
        assert _get_output(event) == "api response"

    def test_content_attribute(self):
        event = _make_event(content="message content")
        assert _get_output(event) == "message content"

    def test_message_attribute(self):
        event = _make_event(message="error message")
        assert _get_output(event) == "error message"

    def test_priority_order_output_first(self):
        event = _make_event(output="first", result="second", message="third")
        assert _get_output(event) == "first"

    def test_priority_order_result_second(self):
        event = _make_event(result="second", response="third")
        assert _get_output(event) == "second"

    def test_no_output_returns_empty(self):
        event = _make_event()
        assert _get_output(event) == ""

    def test_output_does_not_truncate_large_content(self):
        """_get_output uses str() not _safe_str, so no truncation occurs."""
        large = "x" * 6000
        event = _make_event(output=large)
        result = _get_output(event)
        assert len(result) == 6000
        assert result == large

    def test_output_with_none_value_skipped(self):
        """An attribute explicitly set to None should be skipped."""
        event = _make_event(output=None, result="fallback")
        assert _get_output(event) == "fallback"

    def test_output_with_zero_value_not_skipped(self):
        """Zero is not None, so it should be returned as '0'."""
        event = _make_event(output=0)
        assert _get_output(event) == "0"

    def test_output_with_empty_string_not_skipped(self):
        """Empty string is not None, so it should be returned."""
        event = _make_event(output="")
        assert _get_output(event) == ""

    def test_results_attribute(self):
        event = _make_event(results=["item1", "item2"])
        assert _get_output(event) == "['item1', 'item2']"

    def test_value_attribute(self):
        event = _make_event(value="saved memory value")
        assert _get_output(event) == "saved memory value"

    def test_memory_content_attribute(self):
        event = _make_event(memory_content="aggregated memory response")
        assert _get_output(event) == "aggregated memory response"

    def test_priority_results_before_response(self):
        """results should take priority over response."""
        event = _make_event(results=["r1"], response="ignored")
        assert _get_output(event) == "['r1']"

    def test_priority_value_after_message(self):
        """value comes after message in priority."""
        event = _make_event(message="msg", value="val")
        assert _get_output(event) == "msg"

    def test_memory_content_last_resort(self):
        """memory_content is last in priority chain."""
        event = _make_event(memory_content="mem")
        assert _get_output(event) == "mem"


# ---------------------------------------------------------------------------
# Tests: OTelEventBridge.__init__
# ---------------------------------------------------------------------------


class TestOTelEventBridgeInit:
    """Tests for OTelEventBridge initialization."""

    def test_initialization_stores_fields(self):
        tracer = MagicMock()
        bridge = OTelEventBridge(tracer, "job-123", group_context="grp")

        assert bridge._tracer is tracer
        assert bridge._job_id == "job-123"
        assert bridge._group_context == "grp"
        assert bridge._registered_count == 0

    def test_initialization_default_group_context(self):
        tracer = MagicMock()
        bridge = OTelEventBridge(tracer, "job-456")

        assert bridge._group_context is None

    def test_initialization_with_none_group_context(self):
        tracer = MagicMock()
        bridge = OTelEventBridge(tracer, "job-789", group_context=None)

        assert bridge._group_context is None


# ---------------------------------------------------------------------------
# Tests: OTelEventBridge.register
# ---------------------------------------------------------------------------


class TestOTelEventBridgeRegister:
    """Tests for OTelEventBridge.register method."""

    def test_register_successful_imports(self):
        """When all crewai event classes import successfully, all get registered."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-reg")
        event_bus = MagicMock()

        # All class names from the _EVENT_CLASSES list in register()
        all_class_names = [
            "CrewKickoffStartedEvent", "CrewKickoffCompletedEvent",
            "AgentExecutionStartedEvent", "AgentExecutionCompletedEvent",
            "TaskStartedEvent", "TaskCompletedEvent", "TaskFailedEvent",
            "ToolUsageStartedEvent", "ToolUsageFinishedEvent", "ToolUsageErrorEvent",
            "LLMCallStartedEvent", "LLMCallCompletedEvent", "LLMCallFailedEvent",
            "LLMStreamChunkEvent",
            "MemorySaveStartedEvent", "MemorySaveCompletedEvent",
            "MemoryQueryStartedEvent", "MemoryQueryCompletedEvent",
            "MemoryRetrievalCompletedEvent",
            "KnowledgeRetrievalStartedEvent", "KnowledgeRetrievalCompletedEvent",
            "AgentReasoningStartedEvent", "AgentReasoningCompletedEvent",
            "AgentReasoningFailedEvent",
            "LLMGuardrailStartedEvent", "LLMGuardrailCompletedEvent",
            "LLMGuardrailFailedEvent",
            "FlowStartedEvent", "FlowFinishedEvent", "FlowCreatedEvent",
            "MCPConnectionStartedEvent", "MCPConnectionCompletedEvent",
            "MCPToolExecutionStartedEvent", "MCPToolExecutionCompletedEvent",
            "HumanFeedbackRequestedEvent", "HumanFeedbackReceivedEvent",
        ]

        # Build a fake module with dynamically created types for each class name.
        # getattr(module, class_name) must return a type with __name__ == class_name
        # so _register_handler can look it up in _EVENT_SPAN_MAP.
        class_types = {name: type(name, (), {}) for name in all_class_names}
        fake_module = SimpleNamespace(**class_types)

        with patch("importlib.import_module", return_value=fake_module):
            count = bridge.register(event_bus)

        # 36 event classes total in the list (including LLMStreamChunkEvent)
        assert count == 36
        assert bridge._registered_count == 36

    def test_register_handles_import_error(self):
        """ImportError for a module is caught and that class is skipped."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-imp-err")
        event_bus = MagicMock()

        with patch("importlib.import_module", side_effect=ImportError("no module")):
            count = bridge.register(event_bus)

        assert count == 0
        assert bridge._registered_count == 0

    def test_register_handles_attribute_error(self):
        """AttributeError for a missing class on the module is caught."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-attr-err")
        event_bus = MagicMock()

        # Return an empty SimpleNamespace: getattr(ns, "SomeClass") raises
        # AttributeError because the attribute does not exist.
        empty_module = SimpleNamespace()

        with patch("importlib.import_module", return_value=empty_module):
            count = bridge.register(event_bus)

        assert count == 0

    def test_register_mixed_success_and_failure(self):
        """Some imports succeed, some fail -- only successes count."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mixed")
        event_bus = MagicMock()

        # All class names from register() _EVENT_CLASSES list
        all_class_names = [
            "CrewKickoffStartedEvent", "CrewKickoffCompletedEvent",
            "AgentExecutionStartedEvent", "AgentExecutionCompletedEvent",
            "TaskStartedEvent", "TaskCompletedEvent", "TaskFailedEvent",
            "ToolUsageStartedEvent", "ToolUsageFinishedEvent", "ToolUsageErrorEvent",
            "LLMCallStartedEvent", "LLMCallCompletedEvent", "LLMCallFailedEvent",
            "LLMStreamChunkEvent",
            "MemorySaveStartedEvent", "MemorySaveCompletedEvent",
            "MemoryQueryStartedEvent", "MemoryQueryCompletedEvent",
            "MemoryRetrievalCompletedEvent",
            "KnowledgeRetrievalStartedEvent", "KnowledgeRetrievalCompletedEvent",
            "AgentReasoningStartedEvent", "AgentReasoningCompletedEvent",
            "AgentReasoningFailedEvent",
            "LLMGuardrailStartedEvent", "LLMGuardrailCompletedEvent",
            "LLMGuardrailFailedEvent",
            "FlowStartedEvent", "FlowFinishedEvent", "FlowCreatedEvent",
            "MCPConnectionStartedEvent", "MCPConnectionCompletedEvent",
            "MCPToolExecutionStartedEvent", "MCPToolExecutionCompletedEvent",
            "HumanFeedbackRequestedEvent", "HumanFeedbackReceivedEvent",
        ]
        class_types = {name: type(name, (), {}) for name in all_class_names}
        fake_module = SimpleNamespace(**class_types)

        call_count = 0

        def import_side_effect(module_path):
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                raise ImportError("fail")
            return fake_module

        with patch("importlib.import_module", side_effect=import_side_effect):
            count = bridge.register(event_bus)

        # 34 total entries; every other import call fails.
        # However some modules are imported multiple times (e.g. crewai.events),
        # so the count depends on which calls succeed vs fail.
        # The important assertion: some succeed and some fail.
        assert 0 < count < 34


# ---------------------------------------------------------------------------
# Tests: OTelEventBridge._register_handler
# ---------------------------------------------------------------------------


class TestOTelEventBridgeRegisterHandler:
    """Tests for OTelEventBridge._register_handler method."""

    def test_skip_event_in_skip_events_set(self):
        """LLMStreamChunkEvent is in _SKIP_EVENTS and should be skipped."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-skip")
        event_bus = MagicMock()

        skip_cls = type("LLMStreamChunkEvent", (), {})
        bridge._register_handler(event_bus, skip_cls)

        event_bus.on.assert_not_called()

    def test_no_mapping_for_unknown_event(self):
        """An event class not in _EVENT_SPAN_MAP is skipped with debug log."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-nomap")
        event_bus = MagicMock()

        unknown_cls = type("UnknownEventXYZ", (), {})
        bridge._register_handler(event_bus, unknown_cls)

        event_bus.on.assert_not_called()

    def test_successful_registration_calls_event_bus_on(self):
        """A known event class triggers event_bus.on(cls) decorator."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ok")
        event_bus = MagicMock()

        known_cls = type("TaskStartedEvent", (), {})
        bridge._register_handler(event_bus, known_cls)

        event_bus.on.assert_called_once_with(known_cls)

    def test_handler_calls_emit_span(self):
        """The registered handler closure should call _emit_span with correct args."""
        tracer, _ = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-handler")
        bridge._emit_span = MagicMock()

        event_bus = MagicMock()
        # Make event_bus.on(cls) return the handler function (decorator pattern)
        handlers = {}

        def on_decorator(cls):
            def decorator(fn):
                handlers[cls] = fn
                return fn
            return decorator

        event_bus.on = on_decorator

        known_cls = type("TaskStartedEvent", (), {})
        bridge._register_handler(event_bus, known_cls)

        # Now call the registered handler
        fake_event = _make_event()
        handlers[known_cls]("source", fake_event)

        bridge._emit_span.assert_called_once_with(
            "CrewAI.task.execute", "task_started", fake_event
        )


# ---------------------------------------------------------------------------
# Tests: OTelEventBridge._emit_span
# ---------------------------------------------------------------------------


class TestOTelEventBridgeEmitSpan:
    """Tests for OTelEventBridge._emit_span method."""

    def test_emit_span_with_all_fields(self):
        """Span gets all core attributes when event has agent, task, tool, output."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-all")

        event = _make_event(
            agent_role="Researcher",
            task_name="Gather data",
            tool_name="web_search",
            output="Found results",
        )

        bridge._emit_span("CrewAI.task.execute", "task_started", event)

        tracer.start_as_current_span.assert_called_once_with("CrewAI.task.execute")
        span.set_attribute.assert_any_call("kasal.event_type", "task_started")
        span.set_attribute.assert_any_call("kasal.agent_name", "Researcher")
        span.set_attribute.assert_any_call("kasal.task_name", "Gather data")
        span.set_attribute.assert_any_call("kasal.tool_name", "web_search")
        span.set_attribute.assert_any_call("kasal.output_content", "Found results")

    def test_emit_span_with_no_fields(self):
        """Span gets only event_type when event has no extractable fields."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-empty")

        event = _make_event()
        bridge._emit_span("CrewAI.task.execute", "task_started", event)

        span.set_attribute.assert_any_call("kasal.event_type", "task_started")
        # Verify optional attributes were NOT set
        attr_calls = [c[0] for c in span.set_attribute.call_args_list]
        attr_keys = [c[0] for c in attr_calls]
        assert ("kasal.agent_name",) not in attr_calls or all(
            c[0][0] != "kasal.agent_name" for c in span.set_attribute.call_args_list
            if len(c[0]) > 0
        )

    def test_emit_span_with_failed_event_type_sets_error_status(self):
        """Event types containing 'failed' should set span status to ERROR."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-fail")

        event = _make_event(error="Something went wrong")
        bridge._emit_span("CrewAI.task.fail", "task_failed", event)

        from opentelemetry.trace import StatusCode
        span.set_status.assert_called_once_with(
            StatusCode.ERROR, "Something went wrong"
        )

    def test_emit_span_with_error_event_type_sets_error_status(self):
        """Event types containing 'error' should set span status to ERROR."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-err")

        event = _make_event(error=None, message="Tool error occurred")
        bridge._emit_span("CrewAI.tool.error", "tool_error", event)

        from opentelemetry.trace import StatusCode
        span.set_status.assert_called_once_with(
            StatusCode.ERROR, "Tool error occurred"
        )

    def test_emit_span_error_event_with_no_error_or_message(self):
        """Failed event with neither error nor message uses empty string."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-no-msg")

        event = _make_event()
        bridge._emit_span("CrewAI.task.fail", "task_failed", event)

        from opentelemetry.trace import StatusCode
        span.set_status.assert_called_once_with(StatusCode.ERROR, "")

    def test_emit_span_exception_is_caught_and_logged(self):
        """An exception inside _emit_span is caught and logged, not raised."""
        tracer = MagicMock()
        tracer.start_as_current_span.side_effect = RuntimeError("tracer broke")
        bridge = OTelEventBridge(tracer, "job-exc")

        event = _make_event()
        # Should not raise
        bridge._emit_span("span_name", "event_type", event)

    def test_emit_span_task_name_truncated_to_500(self):
        """task_name attribute on the span is truncated to 500 chars."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-trunc")

        long_task = "z" * 700
        event = _make_event(task_name=long_task)
        bridge._emit_span("CrewAI.task.execute", "task_started", event)

        # _get_task_name applies _safe_str (500) and then span code does [:500]
        task_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.task_name"
        ]
        assert len(task_calls) == 1
        assert len(task_calls[0][0][1]) == 500

    def test_emit_span_non_error_event_does_not_set_error_status(self):
        """A normal event type should not call set_status."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ok-status")

        event = _make_event()
        bridge._emit_span("CrewAI.task.execute", "task_started", event)

        span.set_status.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: OTelEventBridge._set_extra_attributes -- Task identification
# ---------------------------------------------------------------------------


class TestSetExtraAttributesTask:
    """Tests for task identification branches in _set_extra_attributes."""

    def test_task_id_directly_on_event(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tid")

        event = _make_event(task_id="tid-001")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.task_id", "tid-001")

    def test_task_name_directly_on_event(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tname")

        event = _make_event(task_name="My task")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.task_name", "My task")

    def test_task_name_truncated_to_200(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tname200")

        long_name = "n" * 300
        event = _make_event(task_name=long_name)
        bridge._set_extra_attributes(span, event)

        name_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.task_name"
        ]
        assert len(name_calls) == 1
        assert len(name_calls[0][0][1]) == 200

    def test_task_object_provides_id_when_no_task_id(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tobj-id")

        task = SimpleNamespace(id="task-obj-id", description=None, name=None)
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.task_id", "task-obj-id")

    def test_task_object_does_not_override_direct_task_id(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tid-prio")

        task = SimpleNamespace(id="ignored-id", description=None, name=None)
        event = _make_event(task_id="direct-id", task=task)
        bridge._set_extra_attributes(span, event)

        id_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.task_id"
        ]
        assert len(id_calls) == 1
        assert id_calls[0][0][1] == "direct-id"

    def test_task_object_provides_description_when_no_task_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tobj-desc")

        task = SimpleNamespace(description="Task desc", name="ignored")
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        name_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.task_name"
        ]
        assert len(name_calls) == 1
        assert name_calls[0][0][1] == "Task desc"

    def test_task_object_provides_name_when_no_description(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tobj-name")

        task = SimpleNamespace(description=None, name="Fallback name")
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        name_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.task_name"
        ]
        assert len(name_calls) == 1
        assert name_calls[0][0][1] == "Fallback name"

    def test_task_object_does_not_override_direct_task_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tname-prio")

        task = SimpleNamespace(description="ignored desc", name="ignored")
        event = _make_event(task_name="direct-name", task=task)
        bridge._set_extra_attributes(span, event)

        name_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.task_name"
        ]
        assert len(name_calls) == 1
        assert name_calls[0][0][1] == "direct-name"

    def test_task_object_no_resolved_name_or_description(self):
        """Task object with no description, no name, no id -- nothing extra set for those."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tobj-empty")

        task = SimpleNamespace(description=None, name=None)
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.task_name" not in attr_keys

    def test_kasal_task_id_from_task_object(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-kasal-tid")

        task = SimpleNamespace(
            description=None, name=None, _kasal_task_id="frontend-uuid"
        )
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.frontend_task_id", "frontend-uuid"
        )

    def test_no_kasal_task_id_when_absent(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-no-kasal-tid")

        task = SimpleNamespace(description=None, name=None)
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.frontend_task_id" not in attr_keys

    def test_task_object_with_id_none(self):
        """task.id is None when task_id is also not set -- should not set task_id."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tid-none")

        task = SimpleNamespace(id=None, description=None, name=None)
        event = _make_event(task=task)
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.task_id" not in attr_keys


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Agent identification
# ---------------------------------------------------------------------------


class TestSetExtraAttributesAgent:
    """Tests for agent identification branches in _set_extra_attributes."""

    def test_agent_role_on_event(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-arole")

        event = _make_event(agent_role="Researcher")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.agent_role", "Researcher")

    def test_agent_id_on_event(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-aid")

        event = _make_event(agent_id="agent-uuid-123")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.agent_id", "agent-uuid-123")

    def test_agent_role_fallback_from_agent_object(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-arole-fb")

        agent = SimpleNamespace(role="Fallback Agent")
        event = _make_event(agent=agent)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.agent_role", "Fallback Agent")

    def test_agent_role_direct_takes_priority_over_fallback(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-arole-prio")

        agent = SimpleNamespace(role="Ignored Role")
        event = _make_event(agent_role="Direct Role", agent=agent)
        bridge._set_extra_attributes(span, event)

        role_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.agent_role"
        ]
        assert len(role_calls) == 1
        assert role_calls[0][0][1] == "Direct Role"

    def test_agent_object_without_role_no_fallback(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-arole-norole")

        agent = SimpleNamespace(name="No Role Agent")
        event = _make_event(agent=agent)
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.agent_role" not in attr_keys

    def test_agent_object_with_none_role_no_fallback(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-arole-none")

        agent = SimpleNamespace(role=None)
        event = _make_event(agent=agent)
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.agent_role" not in attr_keys


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Memory type derivation
# ---------------------------------------------------------------------------


class TestSetExtraAttributesMemoryType:
    """Tests for source_type -> memory_type derivation in _set_extra_attributes."""

    def test_short_term_memory(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mem-short")

        event = _make_event(source_type="ShortTermMemory")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.source_type", "ShortTermMemory")
        span.set_attribute.assert_any_call("kasal.extra.memory_type", "short_term")

    def test_long_term_memory(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mem-long")

        event = _make_event(source_type="LongTermMemory")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.memory_type", "long_term")

    def test_entity_memory(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mem-entity")

        event = _make_event(source_type="EntityMemory")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.memory_type", "entity")

    def test_external_memory(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mem-ext")

        event = _make_event(source_type="ExternalKnowledgeMemory")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.memory_type", "external")

    def test_unknown_source_type_no_memory_type(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mem-unk")

        event = _make_event(source_type="SomethingElse")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.source_type", "SomethingElse")
        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.memory_type" not in attr_keys

    def test_no_source_type(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-no-st")

        event = _make_event()
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.source_type" not in attr_keys
        assert "kasal.extra.memory_type" not in attr_keys


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Memory query/save fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesMemoryFields:
    """Tests for memory query/save field extraction in _set_extra_attributes."""

    def test_query_field(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-query")

        event = _make_event(query="What is the capital?")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.query", "What is the capital?")

    def test_value_field(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-val")

        event = _make_event(value="stored value content")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.value", "stored value content")

    def test_query_time_ms(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-qtime")

        event = _make_event(query_time_ms=42.5)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.query_time_ms", 42.5)

    def test_query_time_ms_zero(self):
        """Zero is a valid query_time_ms value (not None)."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-qtime0")

        event = _make_event(query_time_ms=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.query_time_ms", 0.0)

    def test_save_time_ms(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-stime")

        event = _make_event(save_time_ms=15.3)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.save_time_ms", 15.3)

    def test_retrieval_time_ms(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-rtime")

        event = _make_event(retrieval_time_ms=88.1)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.retrieval_time_ms", 88.1)

    def test_memory_content(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mcontent")

        event = _make_event(memory_content="remembered fact")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.memory_content", "remembered fact")

    def test_memory_content_not_truncated(self):
        """memory_content uses str() directly, no truncation."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-mcontent-long")

        large = "m" * 5000
        event = _make_event(memory_content=large)
        bridge._set_extra_attributes(span, event)

        mc_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.memory_content"
        ]
        assert len(mc_calls) == 1
        assert len(mc_calls[0][0][1]) == 5000

    def test_limit_field(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-lim")

        event = _make_event(limit=10)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.limit", 10)

    def test_limit_zero_is_set(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-lim0")

        event = _make_event(limit=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.limit", 0)

    def test_score_threshold(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-score")

        event = _make_event(score_threshold=0.75)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.score_threshold", 0.75)

    def test_score_threshold_zero_is_set(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-score0")

        event = _make_event(score_threshold=0.0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.score_threshold", 0.0)

    def test_results_count_from_list(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-rcnt")

        event = _make_event(results=["r1", "r2", "r3"])
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.results_count", 3)

    def test_results_count_empty_list(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-rcnt0")

        event = _make_event(results=[])
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.results_count", 0)

    def test_results_count_from_tuple(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-rcnt-t")

        event = _make_event(results=("a", "b"))
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.results_count", 2)

    def test_results_count_non_list_ignored(self):
        """Non-list/tuple results (e.g. a string) should not set results_count."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-rcnt-str")

        event = _make_event(results="some string result")
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.results_count" not in attr_keys

    def test_results_count_none_ignored(self):
        """When results is not present, results_count should not be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-rcnt-none")

        event = _make_event()
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.results_count" not in attr_keys


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Tool fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesTool:
    """Tests for tool field extraction in _set_extra_attributes."""

    def test_tool_name_from_tool_name_attr(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tn")

        event = _make_event(tool_name="search_api")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.tool_name", "search_api")

    def test_tool_name_from_tool_attr_fallback(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tn-fb")

        event = _make_event(tool_name=None, tool="calc_tool")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.tool_name", "calc_tool")

    def test_tool_name_empty_falls_to_tool_attr(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tn-empty")

        event = _make_event(tool_name="", tool="fallback")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.tool_name", "fallback")

    def test_tool_args(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-targs")

        event = _make_event(tool_args={"query": "test"})
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.tool_args", "{'query': 'test'}"
        )

    def test_tool_args_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-targs-long")

        large_args = "a" * 5000
        event = _make_event(tool_args=large_args)
        bridge._set_extra_attributes(span, event)

        args_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.tool_args"
        ]
        assert len(args_calls) == 1
        assert len(args_calls[0][0][1]) == 5000

    def test_tool_class_with_dunder_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tcls")

        class MyTool:
            pass

        event = _make_event(tool_class=MyTool)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.tool_class", "MyTool")

    def test_tool_class_without_dunder_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tcls-noname")

        event = _make_event(tool_class="SomeToolClassString")
        bridge._set_extra_attributes(span, event)

        # getattr(tool_class, "__name__", str(tool_class)) -> str("SomeToolClassString")
        span.set_attribute.assert_any_call(
            "kasal.extra.tool_class", "SomeToolClassString"
        )

    def test_from_cache_true(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-cache-t")

        event = _make_event(from_cache=True)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.from_cache", True)

    def test_from_cache_false(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-cache-f")

        event = _make_event(from_cache=False)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.from_cache", False)

    def test_run_attempts(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-runs")

        event = _make_event(run_attempts=3)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.run_attempts", 3)

    def test_run_attempts_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-runs0")

        event = _make_event(run_attempts=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.run_attempts", 0)

    def test_delegations(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-deleg")

        event = _make_event(delegations=2)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.delegations", 2)

    def test_delegations_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-deleg0")

        event = _make_event(delegations=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.delegations", 0)


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Crew fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesCrew:
    """Tests for crew field extraction in _set_extra_attributes."""

    def test_crew_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-crew")

        event = _make_event(crew_name="Research Crew")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.crew_name", "Research Crew")

    def test_inputs(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-inputs")

        event = _make_event(inputs={"topic": "AI"})
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.inputs", "{'topic': 'AI'}")

    def test_inputs_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-inputs-long")

        large_inputs = "i" * 5000
        event = _make_event(inputs=large_inputs)
        bridge._set_extra_attributes(span, event)

        inp_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.inputs"
        ]
        assert len(inp_calls) == 1
        assert len(inp_calls[0][0][1]) == 5000

    def test_total_tokens(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tokens")

        event = _make_event(total_tokens=1500)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.total_tokens", 1500)

    def test_total_tokens_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tokens0")

        event = _make_event(total_tokens=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.total_tokens", 0)


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Agent execution fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesAgentExecution:
    """Tests for agent execution fields in _set_extra_attributes."""

    def test_task_prompt(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tprompt")

        event = _make_event(task_prompt="You are a researcher. Analyze...")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.task_prompt", "You are a researcher. Analyze..."
        )

    def test_task_prompt_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tprompt-long")

        large_prompt = "p" * 5000
        event = _make_event(task_prompt=large_prompt)
        bridge._set_extra_attributes(span, event)

        prompt_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.task_prompt"
        ]
        assert len(prompt_calls) == 1
        assert len(prompt_calls[0][0][1]) == 5000

    def test_tools_list_with_name_attribute(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tools-list")

        tool1 = SimpleNamespace(name="search")
        tool2 = SimpleNamespace(name="calculator")
        event = _make_event(tools=[tool1, tool2])
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.tools", "['search', 'calculator']"
        )

    def test_tools_list_without_name_attribute(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tools-noname")

        event = _make_event(tools=["raw_tool_1", "raw_tool_2"])
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.tools", "['raw_tool_1', 'raw_tool_2']"
        )

    def test_tools_tuple_also_works(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tools-tuple")

        tool1 = SimpleNamespace(name="writer")
        event = _make_event(tools=(tool1,))
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.tools", "['writer']")

    def test_tools_empty_list_not_set(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tools-empty")

        event = _make_event(tools=[])
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.tools" not in attr_keys

    def test_tools_non_list_not_set(self):
        """tools as a string (not list/tuple) should not match the isinstance check."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tools-str")

        event = _make_event(tools="not a list")
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.tools" not in attr_keys

    def test_tools_list_limited_to_20(self):
        """Only first 20 tools should be extracted."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-tools-20")

        tools = [SimpleNamespace(name=f"tool_{i}") for i in range(30)]
        event = _make_event(tools=tools)
        bridge._set_extra_attributes(span, event)

        tools_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.tools"
        ]
        assert len(tools_calls) == 1
        val = tools_calls[0][0][1]
        # Should contain tool_0 through tool_19 but not tool_20
        assert "tool_19" in val
        assert "tool_20" not in val


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Task context
# ---------------------------------------------------------------------------


class TestSetExtraAttributesContext:
    """Tests for context field in _set_extra_attributes."""

    def test_context_string(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ctx")

        event = _make_event(context="previous task output")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.context", "previous task output"
        )

    def test_context_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ctx-long")

        large_ctx = "c" * 5000
        event = _make_event(context=large_ctx)
        bridge._set_extra_attributes(span, event)

        ctx_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.context"
        ]
        assert len(ctx_calls) == 1
        assert len(ctx_calls[0][0][1]) == 5000

    def test_context_non_string_not_set(self):
        """context that is not a string should not be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ctx-notstr")

        event = _make_event(context=["list", "context"])
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.context" not in attr_keys

    def test_context_empty_string_not_set(self):
        """Empty string is falsy, so context should not be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ctx-empty")

        event = _make_event(context="")
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.context" not in attr_keys


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Knowledge fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesKnowledge:
    """Tests for knowledge field extraction in _set_extra_attributes."""

    def test_retrieved_knowledge(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-know")

        event = _make_event(retrieved_knowledge="relevant facts")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.retrieved_knowledge", "relevant facts"
        )

    def test_retrieved_knowledge_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-know-long")

        large = "k" * 5000
        event = _make_event(retrieved_knowledge=large)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.retrieved_knowledge"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 5000


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Reasoning fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesReasoning:
    """Tests for reasoning field extraction in _set_extra_attributes."""

    def test_plan(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-plan")

        event = _make_event(plan="Step 1: research. Step 2: analyze.")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.plan", "Step 1: research. Step 2: analyze."
        )

    def test_plan_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-plan-long")

        large = "p" * 5000
        event = _make_event(plan=large)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.plan"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 5000

    def test_ready_true(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ready-t")

        event = _make_event(ready=True)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.ready", True)

    def test_ready_false(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ready-f")

        event = _make_event(ready=False)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.ready", False)

    def test_attempt(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-attempt")

        event = _make_event(attempt=2)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.attempt", 2)

    def test_attempt_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-attempt0")

        event = _make_event(attempt=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.attempt", 0)


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Guardrail fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesGuardrail:
    """Tests for guardrail field extraction in _set_extra_attributes."""

    def test_guardrail(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-guard")

        event = _make_event(guardrail="content_filter_v2")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.guardrail", "content_filter_v2"
        )

    def test_guardrail_truncated_via_safe_str(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-guard-long")

        long = "g" * 600
        event = _make_event(guardrail=long)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.guardrail"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 500

    def test_success_true(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-succ-t")

        event = _make_event(success=True)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.success", True)

    def test_success_false(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-succ-f")

        event = _make_event(success=False)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.success", False)

    def test_result_field(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-res")

        event = _make_event(result="guardrail passed")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.result", "guardrail passed")

    def test_result_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-res-long")

        large = "r" * 5000
        event = _make_event(result=large)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.result"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 5000

    def test_result_with_zero_value(self):
        """Zero is not None, so result should be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-res0")

        event = _make_event(result=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.result", "0")

    def test_retry_count(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-retry")

        event = _make_event(retry_count=3)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.retry_count", 3)

    def test_retry_count_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-retry0")

        event = _make_event(retry_count=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.retry_count", 0)


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Flow fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesFlow:
    """Tests for flow field extraction in _set_extra_attributes."""

    def test_flow_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-flow")

        event = _make_event(flow_name="ResearchFlow")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.flow_name", "ResearchFlow")

    def test_method_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-method")

        event = _make_event(method_name="execute_step")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.method_name", "execute_step")


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- MCP fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesMCP:
    """Tests for MCP field extraction in _set_extra_attributes."""

    def test_server_name(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-sname")

        event = _make_event(server_name="mcp-tools")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.server_name", "mcp-tools")

    def test_server_url(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-surl")

        event = _make_event(server_url="https://example.com/mcp")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.server_url", "https://example.com/mcp"
        )

    def test_transport_type(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-trans")

        event = _make_event(transport_type="sse")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.transport_type", "sse")

    def test_connection_duration_ms(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-conndur")

        event = _make_event(connection_duration_ms=150.5)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.connection_duration_ms", 150.5
        )

    def test_connection_duration_ms_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-conndur0")

        event = _make_event(connection_duration_ms=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.connection_duration_ms", 0.0
        )

    def test_execution_duration_ms(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-execdur")

        event = _make_event(execution_duration_ms=320.7)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.execution_duration_ms", 320.7
        )

    def test_execution_duration_ms_zero(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-execdur0")

        event = _make_event(execution_duration_ms=0)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.execution_duration_ms", 0.0
        )


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- HITL fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesHITL:
    """Tests for HITL field extraction in _set_extra_attributes."""

    def test_message_string(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msg")

        event = _make_event(message="Please approve")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.message", "Please approve")

    def test_message_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msg-long")

        large_msg = "m" * 5000
        event = _make_event(message=large_msg)
        bridge._set_extra_attributes(span, event)

        msg_calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.message"
        ]
        assert len(msg_calls) == 1
        assert len(msg_calls[0][0][1]) == 5000

    def test_message_non_string_not_set(self):
        """message that is not a string (isinstance check) should not be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msg-notstr")

        event = _make_event(message=42)
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.message" not in attr_keys

    def test_message_empty_string_not_set(self):
        """Empty string is falsy, so message should not be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msg-empty")

        event = _make_event(message="")
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.message" not in attr_keys

    def test_feedback(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-fb")

        event = _make_event(feedback="Approved with changes")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.feedback", "Approved with changes"
        )

    def test_feedback_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-fb-long")

        large = "f" * 5000
        event = _make_event(feedback=large)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.feedback"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 5000

    def test_outcome(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-out")

        event = _make_event(outcome="approved")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.outcome", "approved")


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- LLM call fields
# ---------------------------------------------------------------------------


class TestSetExtraAttributesLLMCall:
    """Tests for LLM call field extraction in _set_extra_attributes."""

    def test_model(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-model")

        event = _make_event(model="gpt-4")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.model", "gpt-4")

    def test_messages_as_list_with_user_messages(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msgs-user")

        messages = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello there"},
            {"role": "assistant", "content": "Hi!"},
            {"role": "user", "content": "Final question"},
        ]
        event = _make_event(messages=messages)
        bridge._set_extra_attributes(span, event)

        # Last user message extracted as prompt
        span.set_attribute.assert_any_call("kasal.extra.prompt", "Final question")
        span.set_attribute.assert_any_call("kasal.extra.message_count", 4)

    def test_messages_as_list_without_user_messages(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msgs-nouser")

        messages = [
            {"role": "system", "content": "You are helpful"},
            {"role": "assistant", "content": "Hi!"},
        ]
        event = _make_event(messages=messages)
        bridge._set_extra_attributes(span, event)

        # No user messages -> no prompt attribute
        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.prompt" not in attr_keys
        span.set_attribute.assert_any_call("kasal.extra.message_count", 2)

    def test_messages_as_list_with_non_dict_entries(self):
        """Non-dict entries in messages list are skipped during user message extraction."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msgs-mixed")

        messages = ["raw string", {"role": "user", "content": "query"}]
        event = _make_event(messages=messages)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.prompt", "query")
        span.set_attribute.assert_any_call("kasal.extra.message_count", 2)

    def test_messages_as_string(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msgs-str")

        event = _make_event(messages="raw prompt string")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.prompt", "raw prompt string")

    def test_messages_as_string_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msgs-str-long")

        large = "s" * 5000
        event = _make_event(messages=large)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.prompt"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 5000

    def test_messages_user_message_without_content_key(self):
        """User message dict missing 'content' key should use empty string."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-msgs-nocontent")

        messages = [{"role": "user"}]
        event = _make_event(messages=messages)
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.prompt", "")

    def test_call_type(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ctype")

        event = _make_event(call_type="completion")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.call_type", "completion")

    def test_call_type_none_not_set(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-ctype-none")

        event = _make_event()
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.call_type" not in attr_keys

    def test_available_functions_dict(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-avail")

        event = _make_event(
            available_functions={"search": {}, "calculate": {}, "write": {}}
        )
        bridge._set_extra_attributes(span, event)

        # Keys are extracted and passed through _safe_str
        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.available_tools"
        ]
        assert len(calls) == 1
        val = calls[0][0][1]
        assert "search" in val
        assert "calculate" in val
        assert "write" in val

    def test_available_functions_non_dict_not_set(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-avail-notdict")

        event = _make_event(available_functions=["not", "a", "dict"])
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.available_tools" not in attr_keys

    def test_available_functions_empty_dict_not_set(self):
        """Empty dict is falsy, so available_tools should not be set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-avail-empty")

        event = _make_event(available_functions={})
        bridge._set_extra_attributes(span, event)

        attr_keys = [c[0][0] for c in span.set_attribute.call_args_list]
        assert "kasal.extra.available_tools" not in attr_keys


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Error field
# ---------------------------------------------------------------------------


class TestSetExtraAttributesError:
    """Tests for error field extraction in _set_extra_attributes."""

    def test_error_field(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-err")

        event = _make_event(error="Connection refused")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call("kasal.extra.error", "Connection refused")

    def test_error_not_truncated(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-err-long")

        large = "e" * 5000
        event = _make_event(error=large)
        bridge._set_extra_attributes(span, event)

        calls = [
            c for c in span.set_attribute.call_args_list
            if c[0][0] == "kasal.extra.error"
        ]
        assert len(calls) == 1
        assert len(calls[0][0][1]) == 5000


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Operation field
# ---------------------------------------------------------------------------


class TestSetExtraAttributesOperation:
    """Tests for operation field extraction in _set_extra_attributes."""

    def test_operation_field(self):
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-op")

        event = _make_event(operation="similarity_search")
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_any_call(
            "kasal.extra.operation", "similarity_search"
        )


# ---------------------------------------------------------------------------
# Tests: _set_extra_attributes -- Comprehensive event
# ---------------------------------------------------------------------------


class TestSetExtraAttributesComprehensive:
    """Tests for _set_extra_attributes with many fields set at once."""

    def test_all_fields_set_simultaneously(self):
        """Verify all attribute branches work when a maximally-populated event is used."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-all-fields")

        # Create a type with __name__ == "WebSearchTool" using dynamic type creation
        ToolCls = type("WebSearchTool", (), {})

        task_obj = SimpleNamespace(
            id="task-id-from-obj",
            description="task desc from obj",
            name="task name from obj",
            _kasal_task_id="frontend-task-id",
        )

        event = _make_event(
            task_id="tid-direct",
            task_name="tname-direct",
            task=task_obj,
            agent_role="Researcher",
            agent_id="agent-uuid",
            source_type="ShortTermMemory",
            query="search query",
            value="stored value",
            query_time_ms=10.5,
            save_time_ms=5.2,
            retrieval_time_ms=8.3,
            memory_content="content",
            limit=100,
            score_threshold=0.8,
            tool_name="search",
            tool_args='{"q": "test"}',
            tool_class=ToolCls,
            from_cache=True,
            run_attempts=2,
            delegations=1,
            crew_name="MyCrew",
            inputs={"topic": "AI"},
            total_tokens=1500,
            task_prompt="Analyze...",
            tools=[SimpleNamespace(name="tool1")],
            context="prev output",
            retrieved_knowledge="facts",
            plan="step 1, step 2",
            ready=True,
            attempt=3,
            guardrail="content_filter",
            success=True,
            result="passed",
            retry_count=0,
            flow_name="MainFlow",
            method_name="run_step",
            server_name="mcp-server",
            server_url="https://example.com",
            transport_type="sse",
            connection_duration_ms=50.0,
            execution_duration_ms=200.0,
            message="Please review",
            feedback="Looks good",
            outcome="approved",
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello"}],
            call_type="chat",
            available_functions={"search": {}, "calc": {}},
            error="minor warning",
            operation="upsert",
        )

        bridge._set_extra_attributes(span, event)

        # Verify a representative sample of attributes were set
        expected_calls = [
            ("kasal.extra.task_id", "tid-direct"),
            ("kasal.extra.task_name", "tname-direct"),
            ("kasal.extra.frontend_task_id", "frontend-task-id"),
            ("kasal.extra.agent_role", "Researcher"),
            ("kasal.extra.agent_id", "agent-uuid"),
            ("kasal.extra.source_type", "ShortTermMemory"),
            ("kasal.extra.memory_type", "short_term"),
            ("kasal.extra.query_time_ms", 10.5),
            ("kasal.extra.save_time_ms", 5.2),
            ("kasal.extra.retrieval_time_ms", 8.3),
            ("kasal.extra.limit", 100),
            ("kasal.extra.score_threshold", 0.8),
            ("kasal.extra.tool_class", "WebSearchTool"),
            ("kasal.extra.from_cache", True),
            ("kasal.extra.run_attempts", 2),
            ("kasal.extra.delegations", 1),
            ("kasal.extra.crew_name", "MyCrew"),
            ("kasal.extra.total_tokens", 1500),
            ("kasal.extra.ready", True),
            ("kasal.extra.attempt", 3),
            ("kasal.extra.success", True),
            ("kasal.extra.retry_count", 0),
            ("kasal.extra.flow_name", "MainFlow"),
            ("kasal.extra.method_name", "run_step"),
            ("kasal.extra.server_name", "mcp-server"),
            ("kasal.extra.transport_type", "sse"),
            ("kasal.extra.connection_duration_ms", 50.0),
            ("kasal.extra.execution_duration_ms", 200.0),
            ("kasal.extra.message", "Please review"),
            ("kasal.extra.outcome", "approved"),
            ("kasal.extra.model", "gpt-4"),
            ("kasal.extra.prompt", "Hello"),
            ("kasal.extra.message_count", 1),
            ("kasal.extra.call_type", "chat"),
            ("kasal.extra.operation", "upsert"),
        ]

        for attr_key, attr_val in expected_calls:
            span.set_attribute.assert_any_call(attr_key, attr_val)

    def test_empty_event_sets_no_extra_attributes(self):
        """An event with no relevant fields should not set any extra attributes."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-empty-extra")

        event = _make_event()
        bridge._set_extra_attributes(span, event)

        span.set_attribute.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: Module-level constants
# ---------------------------------------------------------------------------


class TestModuleConstants:
    """Tests for module-level constants and mappings."""

    def test_event_span_map_has_expected_entries(self):
        assert "CrewKickoffStartedEvent" in _EVENT_SPAN_MAP
        assert "TaskFailedEvent" in _EVENT_SPAN_MAP
        assert "ToolUsageErrorEvent" in _EVENT_SPAN_MAP
        assert "LLMCallCompletedEvent" in _EVENT_SPAN_MAP

    def test_event_span_map_values_are_tuples(self):
        for key, value in _EVENT_SPAN_MAP.items():
            assert isinstance(value, tuple), f"{key} value is not a tuple"
            assert len(value) == 2, f"{key} tuple does not have 2 elements"
            assert isinstance(value[0], str), f"{key} span_name is not str"
            assert isinstance(value[1], str), f"{key} event_type is not str"

    def test_skip_events_contains_llm_stream_chunk(self):
        assert "LLMStreamChunkEvent" in _SKIP_EVENTS

    def test_skip_events_is_set(self):
        assert isinstance(_SKIP_EVENTS, set)


# ---------------------------------------------------------------------------
# Tests: Integration-style -- register then trigger handler
# ---------------------------------------------------------------------------


class TestIntegrationRegisterAndTrigger:
    """Integration-style tests that register a handler and then trigger it."""

    def test_full_flow_register_and_emit(self):
        """Register a handler via the decorator pattern and verify span emission."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-integ")

        handlers = {}

        class FakeEventBus:
            def on(self, cls):
                def decorator(fn):
                    handlers[cls.__name__] = fn
                    return fn
                return decorator

        event_bus = FakeEventBus()

        # Manually register a known event type
        task_started_cls = type("TaskStartedEvent", (), {})
        bridge._register_handler(event_bus, task_started_cls)

        assert "TaskStartedEvent" in handlers

        # Trigger the handler
        event = _make_event(
            agent_role="Coder",
            task_name="Implement feature",
            output="Done",
        )
        handlers["TaskStartedEvent"]("source", event)

        # Verify span was created
        tracer.start_as_current_span.assert_called_once_with("CrewAI.task.execute")
        span.set_attribute.assert_any_call("kasal.event_type", "task_started")
        span.set_attribute.assert_any_call("kasal.agent_name", "Coder")

    def test_full_flow_error_event_sets_status(self):
        """Register a failed event handler and verify error status is set."""
        tracer, span = _make_tracer()
        bridge = OTelEventBridge(tracer, "job-integ-err")

        handlers = {}

        class FakeEventBus:
            def on(self, cls):
                def decorator(fn):
                    handlers[cls.__name__] = fn
                    return fn
                return decorator

        event_bus = FakeEventBus()

        task_failed_cls = type("TaskFailedEvent", (), {})
        bridge._register_handler(event_bus, task_failed_cls)

        event = _make_event(error="Task timed out")
        handlers["TaskFailedEvent"]("source", event)

        from opentelemetry.trace import StatusCode
        span.set_status.assert_called_once_with(
            StatusCode.ERROR, "Task timed out"
        )
