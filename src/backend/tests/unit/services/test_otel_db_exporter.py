"""
Comprehensive unit tests for KasalDBSpanExporter and helper functions.

Covers all public/private methods, every branch, error path, and attribute
extraction in src/services/otel_tracing/db_exporter.py.
Target: 100% code coverage.
"""

import contextlib
import json
import logging
from types import SimpleNamespace
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
    call,
    PropertyMock,
)
from uuid import UUID

import pytest

from src.services.otel_tracing.db_exporter import (
    UUIDEncoder,
    _span_to_hex,
    _extract_event_type,
    _extract_event_source,
    _extract_event_context,
    _safe_json_parse,
    _extract_output,
    _extract_trace_metadata,
    SPAN_NAME_MAP,
    KasalDBSpanExporter,
)
from opentelemetry.sdk.trace.export import SpanExportResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_span_context(span_id=1, trace_id=2):
    """Create a mock span context with span_id and trace_id."""
    ctx = SimpleNamespace(span_id=span_id, trace_id=trace_id)
    return ctx


def _make_status(status_code_name="OK"):
    """Create a mock status object with a status_code that has a .name attr."""
    code = SimpleNamespace(name=status_code_name)
    return SimpleNamespace(status_code=code)


def _make_readable_span(
    name="test.span",
    attributes=None,
    span_id=1,
    trace_id=2,
    parent_span_id=None,
    status_code_name="OK",
    start_time=1000000000,
    end_time=2000000000,
    has_status=True,
):
    """Build a mock ReadableSpan with configurable fields.

    Parameters
    ----------
    name : str
        Span name.
    attributes : dict or None
        Span attributes dict.  None means no attributes.
    span_id : int
        Integer span ID.
    trace_id : int
        Integer trace ID.
    parent_span_id : int or None
        If set, a parent SimpleNamespace with .span_id is created.
    status_code_name : str
        Name of the status code (e.g. "OK", "ERROR").
    start_time : int or None
        Span start time in nanoseconds.
    end_time : int or None
        Span end time in nanoseconds.
    has_status : bool
        If False, span.status is None.
    """
    span = MagicMock()
    span.name = name
    span.attributes = attributes
    span.context = _make_span_context(span_id=span_id, trace_id=trace_id)
    span.parent = (
        SimpleNamespace(span_id=parent_span_id) if parent_span_id else None
    )
    span.status = _make_status(status_code_name) if has_status else None
    span.start_time = start_time
    span.end_time = end_time
    return span


# ---------------------------------------------------------------------------
# Tests: UUIDEncoder
# ---------------------------------------------------------------------------


class TestUUIDEncoder:
    """Tests for the UUIDEncoder JSON encoder."""

    def test_uuid_serialized_to_string(self):
        test_uuid = UUID("12345678-1234-5678-9012-123456789012")
        result = json.dumps({"id": test_uuid}, cls=UUIDEncoder)
        parsed = json.loads(result)
        assert parsed["id"] == "12345678-1234-5678-9012-123456789012"

    def test_non_uuid_raises_type_error(self):
        """super().default() raises TypeError for non-serializable objects."""
        encoder = UUIDEncoder()
        with pytest.raises(TypeError):
            encoder.default(object())

    def test_standard_types_pass_through(self):
        """Standard JSON-serializable types work normally."""
        data = {"str": "hello", "int": 42, "float": 3.14, "bool": True, "none": None}
        result = json.dumps(data, cls=UUIDEncoder)
        parsed = json.loads(result)
        assert parsed == data

    def test_nested_uuid(self):
        test_uuid = UUID("abcdef12-3456-7890-abcd-ef1234567890")
        data = {"outer": {"inner_id": test_uuid}}
        result = json.dumps(data, cls=UUIDEncoder)
        parsed = json.loads(result)
        assert parsed["outer"]["inner_id"] == "abcdef12-3456-7890-abcd-ef1234567890"


# ---------------------------------------------------------------------------
# Tests: _span_to_hex
# ---------------------------------------------------------------------------


class TestSpanToHex:
    """Tests for the _span_to_hex helper function."""

    def test_zero(self):
        assert _span_to_hex(0) == "0000000000000000"

    def test_small_integer(self):
        assert _span_to_hex(255) == "00000000000000ff"

    def test_large_integer(self):
        assert _span_to_hex(0xDEADBEEFCAFE) == "0000deadbeefcafe"

    def test_max_16_hex_digits(self):
        val = 0xFFFFFFFFFFFFFFFF
        assert _span_to_hex(val) == "ffffffffffffffff"

    def test_one(self):
        assert _span_to_hex(1) == "0000000000000001"


# ---------------------------------------------------------------------------
# Tests: _extract_event_type
# ---------------------------------------------------------------------------


class TestExtractEventType:
    """Tests for _extract_event_type with all branching paths."""

    def test_explicit_kasal_event_type_attribute(self):
        span = _make_readable_span(
            name="anything",
            attributes={"kasal.event_type": "custom_event"},
        )
        assert _extract_event_type(span) == "custom_event"

    def test_span_name_exact_match_in_map(self):
        for span_name, expected_type in SPAN_NAME_MAP.items():
            span = _make_readable_span(name=span_name)
            assert _extract_event_type(span) == expected_type

    def test_span_name_startswith_match(self):
        """A span name that starts with a map key should match."""
        span = _make_readable_span(name="CrewAI.crew.kickoff.extra_suffix")
        assert _extract_event_type(span) == "crew_started"

    def test_execute_core_pattern(self):
        span = _make_readable_span(name="AgentName._execute_core")
        assert _extract_event_type(span) == "agent_execution"

    def test_execute_task_pattern(self):
        span = _make_readable_span(name="SomeAgent.execute_task")
        assert _extract_event_type(span) == "agent_execution"

    def test_kickoff_suffix(self):
        span = _make_readable_span(name="MyCrew.kickoff")
        assert _extract_event_type(span) == "crew_execution"

    def test_unknown_name_fallback_with_dots(self):
        span = _make_readable_span(name="some.custom.span")
        assert _extract_event_type(span) == "some_custom_span"

    def test_empty_name_returns_unknown(self):
        span = _make_readable_span(name="")
        assert _extract_event_type(span) == "unknown"

    def test_none_name_returns_unknown(self):
        span = _make_readable_span(name=None)
        # span.name is None; name = span.name or "" => ""
        assert _extract_event_type(span) == "unknown"

    def test_no_attributes(self):
        span = _make_readable_span(name="CrewAI.tool.execute", attributes=None)
        assert _extract_event_type(span) == "tool_usage"

    def test_explicit_event_type_takes_priority_over_span_name(self):
        span = _make_readable_span(
            name="CrewAI.crew.kickoff",
            attributes={"kasal.event_type": "override"},
        )
        assert _extract_event_type(span) == "override"


# ---------------------------------------------------------------------------
# Tests: _extract_event_source
# ---------------------------------------------------------------------------


class TestExtractEventSource:
    """Tests for _extract_event_source with all attribute fallbacks."""

    def test_crewai_agent_role(self):
        span = _make_readable_span(
            attributes={"crewai.agent.role": "Researcher"}
        )
        assert _extract_event_source(span) == "Researcher"

    def test_kasal_agent_name(self):
        span = _make_readable_span(
            attributes={"kasal.agent_name": "DataAnalyst"}
        )
        assert _extract_event_source(span) == "DataAnalyst"

    def test_agent_role_attribute(self):
        span = _make_readable_span(
            attributes={"agent.role": "Writer"}
        )
        assert _extract_event_source(span) == "Writer"

    def test_graph_node_id(self):
        span = _make_readable_span(
            attributes={"graph.node.id": "node-42"}
        )
        assert _extract_event_source(span) == "node-42"

    def test_crew_in_name(self):
        span = _make_readable_span(name="CrewAI.crew.kickoff", attributes={})
        assert _extract_event_source(span) == "crew"

    def test_flow_in_name(self):
        span = _make_readable_span(name="kasal.flow.started", attributes={})
        assert _extract_event_source(span) == "flow"

    def test_kickoff_suffix(self):
        span = _make_readable_span(name="MyCrew.kickoff", attributes={})
        assert _extract_event_source(span) == "crew"

    def test_system_fallback(self):
        span = _make_readable_span(name="random.span.name", attributes={})
        assert _extract_event_source(span) == "System"

    def test_no_attributes(self):
        span = _make_readable_span(name="unrelated", attributes=None)
        assert _extract_event_source(span) == "System"

    def test_empty_name_system_fallback(self):
        span = _make_readable_span(name="", attributes={})
        assert _extract_event_source(span) == "System"

    def test_priority_order_first_match_wins(self):
        """crewai.agent.role should win over kasal.agent_name."""
        span = _make_readable_span(
            attributes={
                "crewai.agent.role": "FirstMatch",
                "kasal.agent_name": "SecondMatch",
            }
        )
        assert _extract_event_source(span) == "FirstMatch"


# ---------------------------------------------------------------------------
# Tests: _extract_event_context
# ---------------------------------------------------------------------------


class TestExtractEventContext:
    """Tests for _extract_event_context with all attribute fallbacks."""

    def test_crewai_task_description(self):
        span = _make_readable_span(
            attributes={"crewai.task.description": "Research topic X"}
        )
        assert _extract_event_context(span) == "Research topic X"

    def test_truncation_to_500_chars(self):
        long_desc = "A" * 800
        span = _make_readable_span(
            attributes={"crewai.task.description": long_desc}
        )
        result = _extract_event_context(span)
        assert len(result) == 500
        assert result == "A" * 500

    def test_kasal_task_name(self):
        span = _make_readable_span(
            attributes={"kasal.task_name": "my_task"}
        )
        assert _extract_event_context(span) == "my_task"

    def test_task_description(self):
        span = _make_readable_span(
            attributes={"task.description": "Do something"}
        )
        assert _extract_event_context(span) == "Do something"

    def test_formatted_description(self):
        span = _make_readable_span(
            attributes={"formatted_description": "Formatted task"}
        )
        assert _extract_event_context(span) == "Formatted task"

    def test_tool_name_fallback(self):
        span = _make_readable_span(
            attributes={"tool.name": "SearchTool"}
        )
        assert _extract_event_context(span) == "tool:SearchTool"

    def test_crewai_tool_name_fallback(self):
        span = _make_readable_span(
            attributes={"crewai.tool.name": "WebSearch"}
        )
        assert _extract_event_context(span) == "tool:WebSearch"

    def test_kasal_tool_name_fallback(self):
        span = _make_readable_span(
            attributes={"kasal.tool_name": "MyTool"}
        )
        assert _extract_event_context(span) == "tool:MyTool"

    def test_span_name_fallback(self):
        span = _make_readable_span(name="fallback.span", attributes={})
        assert _extract_event_context(span) == "fallback.span"

    def test_empty_name_fallback(self):
        span = _make_readable_span(name="", attributes={})
        assert _extract_event_context(span) == ""

    def test_no_attributes(self):
        span = _make_readable_span(name="test.span", attributes=None)
        assert _extract_event_context(span) == "test.span"

    def test_priority_order(self):
        """crewai.task.description wins over tool.name."""
        span = _make_readable_span(
            attributes={
                "crewai.task.description": "Winner",
                "tool.name": "Loser",
            }
        )
        assert _extract_event_context(span) == "Winner"

    def test_formatted_description_truncated_in_event_context(self):
        """formatted_description is truncated to 500 in _extract_event_context."""
        long_val = "B" * 700
        span = _make_readable_span(
            attributes={"formatted_description": long_val}
        )
        result = _extract_event_context(span)
        assert len(result) == 500


# ---------------------------------------------------------------------------
# Tests: _safe_json_parse
# ---------------------------------------------------------------------------


class TestSafeJsonParse:
    """Tests for _safe_json_parse helper."""

    def test_valid_json_string(self):
        result = _safe_json_parse('{"key": "value"}')
        assert result == {"key": "value"}

    def test_valid_json_list(self):
        result = _safe_json_parse('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_invalid_json_string(self):
        result = _safe_json_parse("not json at all")
        assert result == "not json at all"

    def test_non_string_input_integer(self):
        result = _safe_json_parse(42)
        assert result == 42

    def test_non_string_input_list(self):
        data = [1, 2, 3]
        result = _safe_json_parse(data)
        assert result == [1, 2, 3]

    def test_non_string_input_none(self):
        result = _safe_json_parse(None)
        assert result is None

    def test_empty_string(self):
        """Empty string is not valid JSON; returned as-is."""
        result = _safe_json_parse("")
        assert result == ""

    def test_partial_json(self):
        result = _safe_json_parse('{"key": ')
        assert result == '{"key": '


# ---------------------------------------------------------------------------
# Tests: _extract_output
# ---------------------------------------------------------------------------


class TestExtractOutput:
    """Tests for _extract_output with all attribute branches."""

    def test_kasal_output_content(self):
        span = _make_readable_span(
            attributes={"kasal.output_content": "result data"}
        )
        result = _extract_output(span)
        assert result["content"] == "result data"

    def test_output_value_fallback(self):
        span = _make_readable_span(
            attributes={"output.value": "output val"}
        )
        result = _extract_output(span)
        assert result["content"] == "output val"

    def test_crewai_output_fallback(self):
        span = _make_readable_span(
            attributes={"crewai.output": "crewai result"}
        )
        result = _extract_output(span)
        assert result["content"] == "crewai result"

    def test_content_priority(self):
        """kasal.output_content takes priority over output.value."""
        span = _make_readable_span(
            attributes={
                "kasal.output_content": "kasal_wins",
                "output.value": "output_loses",
            }
        )
        result = _extract_output(span)
        assert result["content"] == "kasal_wins"

    def test_input_value(self):
        span = _make_readable_span(
            attributes={"input.value": "my input"}
        )
        result = _extract_output(span)
        assert result["input"] == "my input"

    def test_duration_ms_computed(self):
        span = _make_readable_span(
            start_time=1_000_000_000,
            end_time=2_500_000_000,
        )
        result = _extract_output(span)
        assert result["duration_ms"] == 1500.0

    def test_no_duration_when_no_start_time(self):
        span = _make_readable_span(start_time=None, end_time=2_000_000_000)
        result = _extract_output(span)
        assert "duration_ms" not in result

    def test_no_duration_when_no_end_time(self):
        span = _make_readable_span(start_time=1_000_000_000, end_time=None)
        result = _extract_output(span)
        assert "duration_ms" not in result

    def test_tool_name(self):
        span = _make_readable_span(
            attributes={"tool.name": "SearchTool"}
        )
        result = _extract_output(span)
        assert result["tool_name"] == "SearchTool"

    def test_tool_description_truncated_to_300(self):
        long_desc = "D" * 500
        span = _make_readable_span(
            attributes={"tool.description": long_desc}
        )
        result = _extract_output(span)
        assert len(result["tool_description"]) == 300

    def test_tool_description_short(self):
        span = _make_readable_span(
            attributes={"tool.description": "Short desc"}
        )
        result = _extract_output(span)
        assert result["tool_description"] == "Short desc"

    def test_memory_fields_long_term(self):
        span = _make_readable_span(
            attributes={
                "long_term_memory.save_time_ms": 50,
                "long_term_memory.query_time_ms": 30,
                "long_term_memory.source_type": "vector",
                "long_term_memory.agent_role": "Researcher",
            }
        )
        result = _extract_output(span)
        assert result["save_time_ms"] == 50
        assert result["query_time_ms"] == 30
        assert result["source_type"] == "vector"
        assert result["agent_role"] == "Researcher"

    def test_memory_fields_short_term(self):
        span = _make_readable_span(
            attributes={
                "short_term_memory.save_time_ms": 10,
                "short_term_memory.query_time_ms": 5,
            }
        )
        result = _extract_output(span)
        assert result["save_time_ms"] == 10
        assert result["query_time_ms"] == 5

    def test_extra_data_fields(self):
        span = _make_readable_span(
            attributes={
                "kasal.extra.model_name": "gpt-4",
                "kasal.extra.token_count": 150,
            }
        )
        result = _extract_output(span)
        assert result["extra_data"]["model_name"] == "gpt-4"
        assert result["extra_data"]["token_count"] == 150

    def test_empty_output_fallback_to_span_name(self):
        span = _make_readable_span(
            name="my.fallback.span",
            attributes={},
            start_time=None,
            end_time=None,
        )
        result = _extract_output(span)
        assert result == {"content": "my.fallback.span"}

    def test_no_attributes(self):
        span = _make_readable_span(
            name="fallback.name",
            attributes=None,
            start_time=None,
            end_time=None,
        )
        result = _extract_output(span)
        assert result == {"content": "fallback.name"}

    def test_memory_field_value_zero_is_included(self):
        """A value of 0 should still be included (val is not None)."""
        span = _make_readable_span(
            attributes={"long_term_memory.save_time_ms": 0}
        )
        result = _extract_output(span)
        assert result["save_time_ms"] == 0


# ---------------------------------------------------------------------------
# Tests: _extract_trace_metadata
# ---------------------------------------------------------------------------


class TestExtractTraceMetadata:
    """Tests for _extract_trace_metadata with all branches."""

    def test_kasal_extra_prefix(self):
        span = _make_readable_span(
            attributes={
                "kasal.extra.custom_field": "custom_value",
                "kasal.extra.another": 42,
            }
        )
        result = _extract_trace_metadata(span)
        assert result["custom_field"] == "custom_value"
        assert result["another"] == 42

    def test_kasal_extra_none_value_skipped(self):
        span = _make_readable_span(
            attributes={"kasal.extra.nope": None}
        )
        result = _extract_trace_metadata(span)
        assert "nope" not in result

    def test_crew_instrumentor_ids(self):
        span = _make_readable_span(
            attributes={
                "crew_key": "ck-123",
                "crew_id": "cid-456",
                "task_key": "tk-789",
                "task_id": "tid-012",
                "flow_id": "fid-345",
            }
        )
        result = _extract_trace_metadata(span)
        assert result["crew_key"] == "ck-123"
        assert result["crew_id"] == "cid-456"
        assert result["task_key"] == "tk-789"
        assert result["task_id"] == "tid-012"
        assert result["flow_id"] == "fid-345"

    def test_crew_id_not_overwritten_by_attrs_if_already_in_kasal_extra(self):
        """If kasal.extra.crew_id is set, attrs crew_id should NOT overwrite."""
        span = _make_readable_span(
            attributes={
                "kasal.extra.crew_id": "from-extra",
                "crew_id": "from-attrs",
            }
        )
        result = _extract_trace_metadata(span)
        assert result["crew_id"] == "from-extra"

    def test_openinference_span_kind(self):
        span = _make_readable_span(
            attributes={"openinference.span.kind": "AGENT"}
        )
        result = _extract_trace_metadata(span)
        assert result["span_kind"] == "AGENT"

    def test_graph_node_parent_id(self):
        span = _make_readable_span(
            attributes={"graph.node.parent_id": "parent-node-1"}
        )
        result = _extract_trace_metadata(span)
        assert result["parent_agent_role"] == "parent-node-1"

    def test_tool_parameters_json(self):
        span = _make_readable_span(
            attributes={"tool.parameters": '{"param1": "val1"}'}
        )
        result = _extract_trace_metadata(span)
        assert result["tool_parameters"] == {"param1": "val1"}

    def test_tool_parameters_non_json(self):
        span = _make_readable_span(
            attributes={"tool.parameters": "plain text"}
        )
        result = _extract_trace_metadata(span)
        assert result["tool_parameters"] == "plain text"

    def test_crew_agents_json(self):
        span = _make_readable_span(
            attributes={"crew_agents": '["agent1", "agent2"]'}
        )
        result = _extract_trace_metadata(span)
        assert result["crew_agents"] == ["agent1", "agent2"]

    def test_crew_tasks_json(self):
        span = _make_readable_span(
            attributes={"crew_tasks": '["task1"]'}
        )
        result = _extract_trace_metadata(span)
        assert result["crew_tasks"] == ["task1"]

    def test_crew_inputs_json(self):
        span = _make_readable_span(
            attributes={"crew_inputs": '{"topic": "AI"}'}
        )
        result = _extract_trace_metadata(span)
        assert result["crew_inputs"] == {"topic": "AI"}

    def test_flow_inputs_json(self):
        span = _make_readable_span(
            attributes={"flow_inputs": '{"step": 1}'}
        )
        result = _extract_trace_metadata(span)
        assert result["flow_inputs"] == {"step": 1}

    def test_formatted_description_not_truncated(self):
        """CRITICAL: formatted_description must NOT be truncated in metadata."""
        long_val = "X" * 2000
        span = _make_readable_span(
            attributes={"formatted_description": long_val}
        )
        result = _extract_trace_metadata(span)
        assert result["formatted_description"] == long_val
        assert len(result["formatted_description"]) == 2000

    def test_formatted_expected_output_not_truncated(self):
        """CRITICAL: formatted_expected_output must NOT be truncated in metadata."""
        long_val = "Y" * 1800
        span = _make_readable_span(
            attributes={"formatted_expected_output": long_val}
        )
        result = _extract_trace_metadata(span)
        assert result["formatted_expected_output"] == long_val
        assert len(result["formatted_expected_output"]) == 1800

    def test_no_attributes(self):
        span = _make_readable_span(attributes=None)
        result = _extract_trace_metadata(span)
        assert result == {}

    def test_empty_attributes(self):
        span = _make_readable_span(attributes={})
        result = _extract_trace_metadata(span)
        assert result == {}

    def test_flow_id_only(self):
        span = _make_readable_span(attributes={"flow_id": "f-100"})
        result = _extract_trace_metadata(span)
        assert result["flow_id"] == "f-100"

    def test_combined_metadata(self):
        """Multiple metadata sources combined into one dict."""
        span = _make_readable_span(
            attributes={
                "kasal.extra.custom": "val",
                "crew_key": "ck-1",
                "openinference.span.kind": "CHAIN",
                "graph.node.parent_id": "parent-x",
                "tool.parameters": '{"a": 1}',
                "crew_agents": '["a1"]',
                "formatted_description": "desc",
                "formatted_expected_output": "output",
            }
        )
        result = _extract_trace_metadata(span)
        assert result["custom"] == "val"
        assert result["crew_key"] == "ck-1"
        assert result["span_kind"] == "CHAIN"
        assert result["parent_agent_role"] == "parent-x"
        assert result["tool_parameters"] == {"a": 1}
        assert result["crew_agents"] == ["a1"]
        assert result["formatted_description"] == "desc"
        assert result["formatted_expected_output"] == "output"


# ---------------------------------------------------------------------------
# Tests: KasalDBSpanExporter.__init__
# ---------------------------------------------------------------------------


class TestKasalDBSpanExporterInit:
    """Tests for KasalDBSpanExporter constructor.

    The exporter uses request_scoped_session() + create_and_run_loop() in
    _write_batch, so __init__ just sets up the thread pool and state.
    """

    @patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor")
    def test_init_basic(self, mock_executor_cls):
        exporter = KasalDBSpanExporter(job_id="job-1", max_workers=3)

        assert exporter._job_id == "job-1"
        assert exporter._group_context is None
        assert exporter._total_exported == 0
        mock_executor_cls.assert_called_once_with(max_workers=3)

    @patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor")
    def test_init_with_group_context(self, mock_executor_cls):
        exporter = KasalDBSpanExporter(
            job_id="job-2",
            group_context=SimpleNamespace(primary_group_id="g1"),
        )

        assert exporter._job_id == "job-2"
        assert exporter._group_context is not None

    @patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor")
    @patch("sqlalchemy.orm.sessionmaker", create=True)
    @patch("sqlalchemy.create_engine", create=True)
    @patch("src.config.settings.settings")
    def test_init_default_max_workers(
        self,
        mock_settings,
        mock_create_engine,
        mock_session_factory,
        mock_executor_cls,
    ):
        mock_settings.DATABASE_URI = "sqlite+aiosqlite:///:memory:"
        KasalDBSpanExporter(job_id="job-3")
        mock_executor_cls.assert_called_once_with(max_workers=2)


# ---------------------------------------------------------------------------
# Tests: KasalDBSpanExporter.export
# ---------------------------------------------------------------------------


class TestKasalDBSpanExporterExport:
    """Tests for the export() method."""

    def _make_exporter(self):
        """Create an exporter with all init dependencies mocked out."""
        with patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor") as mock_exec_cls, \
             patch("sqlalchemy.orm.sessionmaker", create=True), \
             patch("sqlalchemy.create_engine", create=True), \
             patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///:memory:"
            mock_executor = MagicMock()
            mock_exec_cls.return_value = mock_executor
            exporter = KasalDBSpanExporter(job_id="test-job")
        exporter._executor = MagicMock()
        return exporter

    def test_successful_export_single_span(self):
        exporter = self._make_exporter()
        span = _make_readable_span(
            name="CrewAI.crew.kickoff",
            attributes={},
        )
        result = exporter.export([span])

        assert result == SpanExportResult.SUCCESS
        assert exporter._total_exported == 1
        exporter._executor.submit.assert_called_once()

    def test_successful_export_multiple_spans(self):
        exporter = self._make_exporter()
        spans = [
            _make_readable_span(name="CrewAI.crew.kickoff", attributes={}),
            _make_readable_span(name="CrewAI.task.execute", attributes={}),
            _make_readable_span(name="CrewAI.llm.call", attributes={}),
        ]
        result = exporter.export(spans)

        assert result == SpanExportResult.SUCCESS
        assert exporter._total_exported == 3
        exporter._executor.submit.assert_called_once()

    def test_span_to_record_exception_is_caught(self):
        exporter = self._make_exporter()
        bad_span = MagicMock()
        bad_span.name = "bad_span"
        # Make attributes access raise an exception when iterating
        type(bad_span).attributes = PropertyMock(side_effect=RuntimeError("boom"))

        result = exporter.export([bad_span])

        assert result == SpanExportResult.SUCCESS
        # No records produced, so warning emitted and no submit call
        exporter._executor.submit.assert_not_called()

    def test_empty_records_warning(self):
        exporter = self._make_exporter()
        # Patch _span_to_record to return None
        exporter._span_to_record = MagicMock(return_value=None)
        span = _make_readable_span()

        with patch("src.services.otel_tracing.db_exporter.logger") as mock_logger:
            result = exporter.export([span])

        assert result == SpanExportResult.SUCCESS
        exporter._executor.submit.assert_not_called()

    def test_export_increments_total_exported(self):
        exporter = self._make_exporter()
        span1 = _make_readable_span(name="CrewAI.llm.call", attributes={})
        span2 = _make_readable_span(name="CrewAI.llm.complete", attributes={})

        exporter.export([span1])
        assert exporter._total_exported == 1

        exporter.export([span2])
        assert exporter._total_exported == 2

    def test_export_empty_span_list(self):
        exporter = self._make_exporter()
        result = exporter.export([])
        assert result == SpanExportResult.SUCCESS
        exporter._executor.submit.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: KasalDBSpanExporter._span_to_record
# ---------------------------------------------------------------------------


class TestSpanToRecord:
    """Tests for the _span_to_record method."""

    def _make_exporter(self, group_context=None):
        with patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor"), \
             patch("sqlalchemy.orm.sessionmaker", create=True), \
             patch("sqlalchemy.create_engine", create=True), \
             patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///:memory:"
            exporter = KasalDBSpanExporter(
                job_id="job-abc",
                group_context=group_context,
            )
        return exporter

    def test_all_fields_populated(self):
        exporter = self._make_exporter()
        span = _make_readable_span(
            name="CrewAI.task.execute",
            attributes={
                "crewai.agent.role": "Researcher",
                "crewai.task.description": "Do research",
            },
            span_id=0xABCD,
            trace_id=0x1234,
            parent_span_id=0x5678,
            status_code_name="OK",
            start_time=1_000_000_000,
            end_time=3_000_000_000,
        )

        record = exporter._span_to_record(span)

        assert record["job_id"] == "job-abc"
        assert record["event_type"] == "task_started"
        assert record["event_source"] == "Researcher"
        assert record["event_context"] == "Do research"
        assert record["span_id"] == "000000000000abcd"
        assert record["trace_id"] == "0000000000001234"
        assert record["parent_span_id"] == "0000000000005678"
        assert record["span_name"] == "CrewAI.task.execute"
        assert record["status_code"] == "OK"
        assert record["duration_ms"] == 2000
        assert "output" in record
        assert "trace_metadata" in record

    def test_no_parent_span(self):
        exporter = self._make_exporter()
        span = _make_readable_span(parent_span_id=None)
        record = exporter._span_to_record(span)
        assert record["parent_span_id"] is None

    def test_with_parent_span(self):
        exporter = self._make_exporter()
        span = _make_readable_span(parent_span_id=99)
        record = exporter._span_to_record(span)
        assert record["parent_span_id"] == "0000000000000063"

    def test_no_status(self):
        exporter = self._make_exporter()
        span = _make_readable_span(has_status=False)
        record = exporter._span_to_record(span)
        assert record["status_code"] == "UNSET"

    def test_no_start_end_time(self):
        exporter = self._make_exporter()
        span = _make_readable_span(start_time=None, end_time=None)
        record = exporter._span_to_record(span)
        assert record["duration_ms"] is None

    def test_no_start_time_only(self):
        exporter = self._make_exporter()
        span = _make_readable_span(start_time=None, end_time=5_000_000_000)
        record = exporter._span_to_record(span)
        assert record["duration_ms"] is None

    def test_with_group_context(self):
        group_ctx = SimpleNamespace(
            primary_group_id="grp-42",
            group_email="test@example.com",
        )
        exporter = self._make_exporter(group_context=group_ctx)
        span = _make_readable_span()
        record = exporter._span_to_record(span)

        assert record["group_id"] == "grp-42"
        assert record["group_email"] == "test@example.com"

    def test_without_group_context(self):
        exporter = self._make_exporter(group_context=None)
        span = _make_readable_span()
        record = exporter._span_to_record(span)
        assert "group_id" not in record
        assert "group_email" not in record

    def test_group_context_missing_attributes(self):
        """If group_context lacks the expected attributes, getattr returns None."""
        group_ctx = SimpleNamespace()
        exporter = self._make_exporter(group_context=group_ctx)
        span = _make_readable_span()
        record = exporter._span_to_record(span)
        assert record["group_id"] is None
        assert record["group_email"] is None


# ---------------------------------------------------------------------------
# Tests: KasalDBSpanExporter._write_batch
# ---------------------------------------------------------------------------


class TestWriteBatch:
    """Tests for the _write_batch method (async DB writes via smart session).

    _write_batch uses request_scoped_session() + create_and_run_loop() to
    write traces to whichever DB is configured (local or Lakebase).
    """

    def _make_exporter(self):
        with patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor"):
            exporter = KasalDBSpanExporter(job_id="job-write")
        return exporter

    def _make_record(self, **overrides):
        """Build a minimal trace record dict."""
        defaults = {
            "job_id": "job-write",
            "event_source": "agent",
            "event_context": "task",
            "event_type": "task_started",
            "output": {"content": "result"},
            "trace_metadata": {},
            "span_id": "0000000000000001",
            "trace_id": "0000000000000002",
            "parent_span_id": None,
            "span_name": "test.span",
            "status_code": "OK",
            "duration_ms": 100,
        }
        defaults.update(overrides)
        return defaults

    def _mock_write_batch(self, exporter, records, mock_trace_cls=None, mock_logger=None):
        """Run _write_batch with mocked request_scoped_session and create_and_run_loop.

        Instead of actually running an event loop, we execute the async
        coroutine inline by using AsyncMock for the session.
        """
        import asyncio

        mock_session = AsyncMock()

        # create_and_run_loop receives a coroutine — execute it in a test loop
        def fake_create_and_run_loop(coro):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()

        # _write_batch uses local imports: `from src.db.session import request_scoped_session`
        # and `from src.utils.asyncio_utils import create_and_run_loop`.
        # Patch at the source module so the local import picks up the mock.
        patches = [
            patch(
                "src.db.session.request_scoped_session",
                return_value=AsyncMock(
                    __aenter__=AsyncMock(return_value=mock_session),
                    __aexit__=AsyncMock(return_value=False),
                ),
            ),
            patch(
                "src.utils.asyncio_utils.create_and_run_loop",
                side_effect=fake_create_and_run_loop,
            ),
        ]
        if mock_trace_cls is not None:
            patches.append(
                patch(
                    "src.models.execution_trace.ExecutionTrace",
                    mock_trace_cls,
                )
            )
        if mock_logger is not None:
            patches.append(
                patch("src.services.otel_tracing.db_exporter.logger", mock_logger)
            )

        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            exporter._write_batch(records)

        return mock_session

    def test_successful_write_calls_session_add_and_commit(self):
        """session.add is called for each record and commit is awaited."""
        exporter = self._make_exporter()
        records = [self._make_record()]

        mock_trace_cls = MagicMock(return_value=MagicMock())
        mock_session = self._mock_write_batch(exporter, records, mock_trace_cls=mock_trace_cls)

        mock_session.add.assert_called_once()
        mock_session.commit.assert_awaited_once()

    def test_session_exception_triggers_error_log(self):
        """An exception during session.commit logs error."""
        exporter = self._make_exporter()
        records = [self._make_record()]

        mock_trace_cls = MagicMock(return_value=MagicMock())
        mock_logger = MagicMock()

        mock_session = AsyncMock()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("commit failed"))

        import asyncio

        def fake_create_and_run_loop(coro):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()

        with patch(
            "src.db.session.request_scoped_session",
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_session),
                __aexit__=AsyncMock(return_value=False),
            ),
        ), patch(
            "src.utils.asyncio_utils.create_and_run_loop",
            side_effect=fake_create_and_run_loop,
        ), patch(
            "src.models.execution_trace.ExecutionTrace",
            mock_trace_cls,
        ), patch(
            "src.services.otel_tracing.db_exporter.logger",
            mock_logger,
        ):
            exporter._write_batch(records)

        mock_logger.error.assert_called()

    def test_write_successful_creates_execution_trace(self):
        """Successful write creates ExecutionTrace model instances."""
        exporter = self._make_exporter()
        records = [self._make_record()]

        mock_trace_instance = MagicMock()
        mock_trace_cls = MagicMock(return_value=mock_trace_instance)
        mock_session = self._mock_write_batch(exporter, records, mock_trace_cls=mock_trace_cls)

        mock_trace_cls.assert_called_once()
        call_kwargs = mock_trace_cls.call_args.kwargs
        assert call_kwargs["job_id"] == "job-write"
        assert call_kwargs["event_source"] == "agent"
        assert call_kwargs["event_context"] == "task"
        assert call_kwargs["event_type"] == "task_started"
        mock_session.add.assert_called_once_with(mock_trace_instance)
        mock_session.commit.assert_awaited_once()

    def test_write_record_exception_logs_error_but_continues(self):
        """Exception during individual record processing logs error and continues."""
        exporter = self._make_exporter()
        records = [self._make_record()]

        mock_trace_cls = MagicMock(side_effect=RuntimeError("model creation failed"))
        mock_logger = MagicMock()
        mock_session = self._mock_write_batch(
            exporter, records, mock_trace_cls=mock_trace_cls, mock_logger=mock_logger
        )

        mock_logger.error.assert_called()

    def test_write_empty_output(self):
        """Record with empty output should produce cleaned == {}."""
        exporter = self._make_exporter()
        records = [self._make_record(output={})]

        mock_trace_cls = MagicMock(return_value=MagicMock())
        self._mock_write_batch(exporter, records, mock_trace_cls=mock_trace_cls)

        call_kwargs = mock_trace_cls.call_args.kwargs
        assert call_kwargs["output"] == {}

    def test_write_zero_written_warning(self):
        """When all records fail, warning is logged about 0 written."""
        exporter = self._make_exporter()
        records = [self._make_record(), self._make_record()]

        mock_trace_cls = MagicMock(side_effect=RuntimeError("all fail"))
        mock_logger = MagicMock()
        self._mock_write_batch(
            exporter, records, mock_trace_cls=mock_trace_cls, mock_logger=mock_logger
        )

        warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
        assert any("0/2" in c for c in warning_calls)

    def test_write_partial_success(self):
        """First record succeeds, second fails."""
        exporter = self._make_exporter()
        records = [self._make_record(), self._make_record()]

        call_count = 0

        def trace_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("second record fails")
            return MagicMock()

        mock_trace_cls = MagicMock(side_effect=trace_side_effect)
        mock_session = self._mock_write_batch(exporter, records, mock_trace_cls=mock_trace_cls)

        mock_session.commit.assert_awaited_once()

    def test_write_batch_with_uuid_in_output(self):
        """UUIDEncoder should handle UUID objects in output dict."""
        exporter = self._make_exporter()
        test_uuid = UUID("abcdef12-3456-7890-abcd-ef1234567890")
        records = [self._make_record(output={"content": "test", "id": test_uuid})]

        mock_trace_cls = MagicMock(return_value=MagicMock())
        self._mock_write_batch(exporter, records, mock_trace_cls=mock_trace_cls)

        call_kwargs = mock_trace_cls.call_args.kwargs
        assert call_kwargs["output"]["id"] == "abcdef12-3456-7890-abcd-ef1234567890"

    def test_write_batch_record_fields_passed_correctly(self):
        """Verify all record fields are correctly passed to ExecutionTrace."""
        exporter = self._make_exporter()
        records = [self._make_record(
            group_id="grp-1",
            group_email="user@example.com",
        )]

        mock_trace_cls = MagicMock(return_value=MagicMock())
        self._mock_write_batch(exporter, records, mock_trace_cls=mock_trace_cls)

        call_kwargs = mock_trace_cls.call_args.kwargs
        assert call_kwargs["job_id"] == "job-write"
        assert call_kwargs["event_source"] == "agent"
        assert call_kwargs["event_context"] == "task"
        assert call_kwargs["event_type"] == "task_started"
        assert call_kwargs["trace_metadata"] == {}
        assert call_kwargs["span_id"] == "0000000000000001"
        assert call_kwargs["trace_id"] == "0000000000000002"
        assert call_kwargs["parent_span_id"] is None
        assert call_kwargs["span_name"] == "test.span"
        assert call_kwargs["status_code"] == "OK"
        assert call_kwargs["duration_ms"] == 100
        assert call_kwargs["group_id"] == "grp-1"
        assert call_kwargs["group_email"] == "user@example.com"

    def test_write_written_logs_info(self):
        """When at least one record is written, info log is emitted."""
        exporter = self._make_exporter()
        records = [self._make_record(), self._make_record()]

        mock_trace_cls = MagicMock(return_value=MagicMock())
        mock_logger = MagicMock()
        self._mock_write_batch(
            exporter, records, mock_trace_cls=mock_trace_cls, mock_logger=mock_logger
        )

        info_calls = [str(c) for c in mock_logger.info.call_args_list]
        assert any("2/2" in c for c in info_calls)


# ---------------------------------------------------------------------------
# Tests: KasalDBSpanExporter.shutdown
# ---------------------------------------------------------------------------


class TestShutdown:
    """Tests for the shutdown() method."""

    def _make_exporter(self):
        with patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor") as mock_exec_cls, \
             patch("sqlalchemy.orm.sessionmaker", create=True), \
             patch("sqlalchemy.create_engine", create=True), \
             patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///:memory:"
            exporter = KasalDBSpanExporter(job_id="job-shutdown")
        return exporter

    def test_shutdown_calls_executor_shutdown(self):
        exporter = self._make_exporter()
        mock_executor = MagicMock()
        exporter._executor = mock_executor

        exporter.shutdown()

        mock_executor.shutdown.assert_called_once_with(wait=True, cancel_futures=False)

    def test_shutdown_logs_total_exported(self):
        exporter = self._make_exporter()
        exporter._executor = MagicMock()
        exporter._total_exported = 42

        with patch("src.services.otel_tracing.db_exporter.logger") as mock_logger:
            exporter.shutdown()
            info_calls = [str(c) for c in mock_logger.info.call_args_list]
            assert any("42" in c for c in info_calls)

    def test_shutdown_waits_for_pending_writes(self):
        """Verify shutdown uses wait=True, cancel_futures=False so pending
        trace writes are not dropped."""
        exporter = self._make_exporter()
        mock_executor = MagicMock()
        exporter._executor = mock_executor
        mock_engine = MagicMock()
        exporter._sync_engine = mock_engine

        exporter.shutdown()

        # The executor.shutdown is called inside the _do_shutdown thread
        # with wait=True and cancel_futures=False
        mock_executor.shutdown.assert_called_once_with(
            wait=True, cancel_futures=False
        )

    def test_shutdown_timeout(self):
        """Verify that the 10-second timeout mechanism works: if the thread
        pool shutdown takes too long, the method logs a warning and returns
        instead of blocking forever."""
        exporter = self._make_exporter()
        mock_engine = MagicMock()
        exporter._sync_engine = mock_engine

        # Make executor.shutdown block indefinitely so the timeout fires
        import threading

        block_event = threading.Event()

        def blocking_shutdown(**kwargs):
            # Block until the test signals us (never, in this case)
            block_event.wait(timeout=30)

        mock_executor = MagicMock()
        mock_executor.shutdown.side_effect = blocking_shutdown
        exporter._executor = mock_executor

        with patch("src.services.otel_tracing.db_exporter.logger") as mock_logger:
            # Use a very short timeout to avoid slowing down tests.
            # We monkey-patch the timeout value inside the shutdown method
            # by patching threading.Event.wait to simulate timeout expiry.
            original_event_wait = threading.Event.wait

            def fast_wait(self_event, timeout=None):
                # The _do_shutdown thread's Event.set() call — let it pass
                # The main thread's shutdown_done.wait(timeout=10.0) — return False (timed out)
                # Distinguish: _do_shutdown sets the event; main thread waits on it.
                # We intercept waits with timeout=10.0 (the main wait) and return False.
                if timeout == 10.0:
                    return False  # Simulate timeout
                return original_event_wait(self_event, timeout=timeout)

            with patch.object(threading.Event, "wait", fast_wait):
                exporter.shutdown()

            # Verify that the timeout warning was logged
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any(
                "timed out" in c.lower() for c in warning_calls
            ), f"Expected timeout warning in: {warning_calls}"

        # Unblock the thread so it doesn't leak
        block_event.set()


# ---------------------------------------------------------------------------
# Tests: KasalDBSpanExporter.force_flush
# ---------------------------------------------------------------------------


class TestForceFlush:
    """Tests for the force_flush() method."""

    def _make_exporter(self):
        with patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor"), \
             patch("sqlalchemy.orm.sessionmaker", create=True), \
             patch("sqlalchemy.create_engine", create=True), \
             patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///:memory:"
            exporter = KasalDBSpanExporter(job_id="job-flush")
        return exporter

    def test_force_flush_returns_true(self):
        exporter = self._make_exporter()
        assert exporter.force_flush() is True

    def test_force_flush_with_custom_timeout(self):
        exporter = self._make_exporter()
        assert exporter.force_flush(timeout_millis=5000) is True

    def test_force_flush_default_timeout(self):
        exporter = self._make_exporter()
        assert exporter.force_flush(timeout_millis=30000) is True


# ---------------------------------------------------------------------------
# Tests: SPAN_NAME_MAP coverage
# ---------------------------------------------------------------------------


class TestSpanNameMap:
    """Verify SPAN_NAME_MAP constants are correct."""

    def test_map_is_not_empty(self):
        assert len(SPAN_NAME_MAP) > 0

    def test_all_values_are_strings(self):
        for key, val in SPAN_NAME_MAP.items():
            assert isinstance(key, str), f"Key {key} is not a string"
            assert isinstance(val, str), f"Value {val} for key {key} is not a string"

    def test_known_entries(self):
        assert SPAN_NAME_MAP["CrewAI.crew.kickoff"] == "crew_started"
        assert SPAN_NAME_MAP["kasal.flow.started"] == "flow_started"
        assert SPAN_NAME_MAP["kasal.hitl.feedback_requested"] == "hitl_feedback_requested"

    def test_all_crewai_entries_present(self):
        expected_crewai = [
            "CrewAI.crew.kickoff",
            "CrewAI.crew.complete",
            "CrewAI.task.execute",
            "CrewAI.task.complete",
            "CrewAI.task.fail",
            "CrewAI.agent.execute",
            "CrewAI.agent.complete",
            "CrewAI.tool.execute",
            "CrewAI.tool.complete",
            "CrewAI.tool.error",
            "CrewAI.llm.call",
            "CrewAI.llm.complete",
        ]
        for key in expected_crewai:
            assert key in SPAN_NAME_MAP


# ---------------------------------------------------------------------------
# Tests: Integration-level coverage for _extract_event_type edge cases
# ---------------------------------------------------------------------------


class TestExtractEventTypeEdgeCases:
    """Additional edge case tests for _extract_event_type."""

    def test_name_with_execute_core_and_other_text(self):
        span = _make_readable_span(name="MyAgent._execute_core.subtask")
        assert _extract_event_type(span) == "agent_execution"

    def test_name_with_execute_task_and_other_text(self):
        span = _make_readable_span(name="SomeModule.execute_task.step")
        assert _extract_event_type(span) == "agent_execution"

    def test_name_only_dots(self):
        span = _make_readable_span(name="a.b.c", attributes={})
        assert _extract_event_type(span) == "a_b_c"


# ---------------------------------------------------------------------------
# Tests: _extract_event_source edge cases
# ---------------------------------------------------------------------------


class TestExtractEventSourceEdgeCases:
    """Additional edge case tests for _extract_event_source."""

    def test_flow_in_name_case_insensitive(self):
        span = _make_readable_span(name="MyFlow.step1", attributes={})
        assert _extract_event_source(span) == "flow"

    def test_crew_in_name_case_insensitive(self):
        span = _make_readable_span(name="TestCrew.run", attributes={})
        assert _extract_event_source(span) == "crew"

    def test_kickoff_suffix_takes_lower_priority_than_crew_name(self):
        """If name contains 'crew' and ends with .kickoff, 'crew' branch hits first."""
        span = _make_readable_span(name="crew.kickoff", attributes={})
        # "crew" in name.lower() is True, so returns "crew" from that branch
        assert _extract_event_source(span) == "crew"

    def test_kickoff_suffix_without_crew_or_flow(self):
        """Name ends in .kickoff but doesn't contain 'crew' or 'flow'."""
        span = _make_readable_span(name="MyApp.kickoff", attributes={})
        assert _extract_event_source(span) == "crew"


# ---------------------------------------------------------------------------
# Tests: _extract_event_context edge cases
# ---------------------------------------------------------------------------


class TestExtractEventContextEdgeCases:
    """Additional edge cases for _extract_event_context."""

    def test_kasal_task_name_truncation(self):
        long_val = "Z" * 600
        span = _make_readable_span(
            attributes={"kasal.task_name": long_val}
        )
        result = _extract_event_context(span)
        assert len(result) == 500

    def test_task_description_truncation(self):
        long_val = "W" * 600
        span = _make_readable_span(
            attributes={"task.description": long_val}
        )
        result = _extract_event_context(span)
        assert len(result) == 500

    def test_none_span_name_returns_empty(self):
        span = _make_readable_span(name=None, attributes={})
        # span.name or "" => ""
        result = _extract_event_context(span)
        assert result == ""


# ---------------------------------------------------------------------------
# Tests: _extract_output edge cases
# ---------------------------------------------------------------------------


class TestExtractOutputEdgeCases:
    """Additional edge cases for _extract_output."""

    def test_output_value_without_kasal_output(self):
        """output.value used when kasal.output_content is not present."""
        span = _make_readable_span(
            attributes={"output.value": "from instrumentor"}
        )
        result = _extract_output(span)
        assert result["content"] == "from instrumentor"

    def test_crewai_output_lowest_priority(self):
        """crewai.output only used when others are absent."""
        span = _make_readable_span(
            attributes={"crewai.output": "crew result"}
        )
        result = _extract_output(span)
        assert result["content"] == "crew result"

    def test_no_content_keys(self):
        """When no content keys are present, no 'content' key in output."""
        span = _make_readable_span(
            attributes={"input.value": "just input"},
            start_time=None,
            end_time=None,
        )
        result = _extract_output(span)
        assert "content" not in result
        assert result["input"] == "just input"

    def test_short_term_memory_fields(self):
        span = _make_readable_span(
            attributes={
                "short_term_memory.source_type": "cache",
                "short_term_memory.agent_role": "Analyst",
            }
        )
        result = _extract_output(span)
        assert result["source_type"] == "cache"
        assert result["agent_role"] == "Analyst"

    def test_no_extra_data_when_no_kasal_extra(self):
        span = _make_readable_span(
            attributes={"kasal.output_content": "data"}
        )
        result = _extract_output(span)
        assert "extra_data" not in result

    def test_duration_precision(self):
        """Check duration_ms rounding to 2 decimal places."""
        # (end - start) / 1_000_000 = 1_333_333 / 1_000_000 = 1.333333
        # round(1.333333, 2) = 1.33
        span = _make_readable_span(
            start_time=1_000_000_000,
            end_time=1_001_333_333,
        )
        result = _extract_output(span)
        assert result["duration_ms"] == 1.33


# ---------------------------------------------------------------------------
# Tests: _extract_trace_metadata edge cases
# ---------------------------------------------------------------------------


class TestExtractTraceMetadataEdgeCases:
    """Additional edge cases for _extract_trace_metadata."""

    def test_tool_parameters_not_string(self):
        """Non-string tool.parameters passed through _safe_json_parse."""
        span = _make_readable_span(
            attributes={"tool.parameters": {"already": "dict"}}
        )
        result = _extract_trace_metadata(span)
        assert result["tool_parameters"] == {"already": "dict"}

    def test_crew_agents_non_json(self):
        span = _make_readable_span(
            attributes={"crew_agents": "not json"}
        )
        result = _extract_trace_metadata(span)
        assert result["crew_agents"] == "not json"

    def test_all_instrumentor_ids_absent(self):
        span = _make_readable_span(attributes={})
        result = _extract_trace_metadata(span)
        for key in ("crew_key", "crew_id", "task_key", "task_id", "flow_id"):
            assert key not in result

    def test_formatted_description_1500_chars_not_truncated(self):
        """Verify strings > 1500 chars are NOT truncated."""
        val = "M" * 1500
        span = _make_readable_span(
            attributes={"formatted_description": val}
        )
        result = _extract_trace_metadata(span)
        assert len(result["formatted_description"]) == 1500
        assert result["formatted_description"] == val

    def test_formatted_expected_output_1500_chars_not_truncated(self):
        """Verify strings > 1500 chars are NOT truncated."""
        val = "N" * 1500
        span = _make_readable_span(
            attributes={"formatted_expected_output": val}
        )
        result = _extract_trace_metadata(span)
        assert len(result["formatted_expected_output"]) == 1500
        assert result["formatted_expected_output"] == val

    def test_formatted_fields_3000_chars_not_truncated(self):
        """Verify extremely long strings (3000 chars) are NOT truncated."""
        long_desc = "P" * 3000
        long_output = "Q" * 3000
        span = _make_readable_span(
            attributes={
                "formatted_description": long_desc,
                "formatted_expected_output": long_output,
            }
        )
        result = _extract_trace_metadata(span)
        assert len(result["formatted_description"]) == 3000
        assert len(result["formatted_expected_output"]) == 3000

    def test_falsy_crew_key_not_added(self):
        """Empty string or 0 for crew_key should not be added."""
        span = _make_readable_span(attributes={"crew_key": ""})
        result = _extract_trace_metadata(span)
        assert "crew_key" not in result

    def test_kasal_extra_with_zero_value_included(self):
        """Zero is a valid value (not None), so it should be included."""
        span = _make_readable_span(
            attributes={"kasal.extra.count": 0}
        )
        result = _extract_trace_metadata(span)
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# Tests: Full integration flow (export -> _span_to_record -> _write_batch)
# ---------------------------------------------------------------------------


class TestExportIntegration:
    """Integration tests exercising the full export pipeline."""

    def _make_exporter(self, group_context=None):
        with patch("src.services.otel_tracing.db_exporter.ThreadPoolExecutor") as mock_exec_cls, \
             patch("sqlalchemy.ext.asyncio.async_sessionmaker", create=True), \
             patch("sqlalchemy.ext.asyncio.create_async_engine", create=True), \
             patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite+aiosqlite:///:memory:"
            exporter = KasalDBSpanExporter(
                job_id="integration-job",
                group_context=group_context,
            )
        exporter._executor = MagicMock()
        return exporter

    def test_full_pipeline_crew_kickoff(self):
        exporter = self._make_exporter()
        span = _make_readable_span(
            name="CrewAI.crew.kickoff",
            attributes={
                "crewai.agent.role": "Orchestrator",
                "crewai.task.description": "Manage the crew",
                "crew_key": "ck-1",
            },
            span_id=0xFF,
            trace_id=0xAA,
            start_time=1_000_000_000,
            end_time=5_000_000_000,
        )

        result = exporter.export([span])

        assert result == SpanExportResult.SUCCESS
        submit_call = exporter._executor.submit.call_args
        records = submit_call[0][1]
        assert len(records) == 1
        record = records[0]
        assert record["event_type"] == "crew_started"
        assert record["event_source"] == "Orchestrator"
        assert record["event_context"] == "Manage the crew"
        assert record["duration_ms"] == 4000

    def test_full_pipeline_with_group_context(self):
        group_ctx = SimpleNamespace(
            primary_group_id="grp-int",
            group_email="integration@test.com",
        )
        exporter = self._make_exporter(group_context=group_ctx)
        span = _make_readable_span(
            name="kasal.llm.call_started",
            attributes={"kasal.agent_name": "LLMAgent"},
        )

        exporter.export([span])

        submit_call = exporter._executor.submit.call_args
        records = submit_call[0][1]
        record = records[0]
        assert record["group_id"] == "grp-int"
        assert record["group_email"] == "integration@test.com"

    def test_full_pipeline_tool_span(self):
        exporter = self._make_exporter()
        span = _make_readable_span(
            name="CrewAI.tool.execute",
            attributes={
                "tool.name": "WebSearch",
                "tool.description": "Searches the web",
                "tool.parameters": '{"query": "AI trends"}',
            },
        )

        exporter.export([span])

        submit_call = exporter._executor.submit.call_args
        records = submit_call[0][1]
        record = records[0]
        assert record["event_type"] == "tool_usage"
        assert record["event_context"] == "tool:WebSearch"
        assert record["output"]["tool_name"] == "WebSearch"
        assert record["trace_metadata"]["tool_parameters"] == {"query": "AI trends"}
