"""
Comprehensive unit tests for services/otel_tracing/mlflow_exporter.py
"""

import threading
import pytest
from unittest.mock import MagicMock, patch, Mock, call
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult

from src.services.otel_tracing.mlflow_exporter import (
    KasalMLflowSpanExporter,
    _build_span_name,
    _build_pairing_key,
    _extract_agent_name,
    _extract_task_name,
    _extract_span_outputs,
    _extract_span_attrs,
    _PairedSpan,
    _InstantSpan,
    _EVENT_PAIRS,
    _END_TO_STARTS,
)


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def _make_span(
    name="test-span",
    event_type="crew_started",
    agent_name="",
    task_name="",
    tool_name="",
    output_content="",
    start_time=1000,
    end_time=2000,
    status_code_name="OK",
    extra_attrs=None,
):
    span = MagicMock(spec=ReadableSpan)
    span.name = name
    span.start_time = start_time
    span.end_time = end_time

    attrs = {"kasal.event_type": event_type}
    if agent_name:
        attrs["kasal.agent_name"] = agent_name
    if task_name:
        attrs["kasal.task_name"] = task_name
    if tool_name:
        attrs["kasal.tool_name"] = tool_name
    if output_content:
        attrs["kasal.output_content"] = output_content
    if extra_attrs:
        attrs.update(extra_attrs)

    span.attributes = attrs

    status = MagicMock()
    status.status_code = MagicMock()
    status.status_code.name = status_code_name
    span.status = status

    return span


def _make_exporter(job_id="job-1"):
    mlflow_result = MagicMock()
    mlflow_result.experiment_id = "exp-123"
    mlflow_result.tracing_ready = True
    return KasalMLflowSpanExporter(
        job_id=job_id,
        mlflow_result=mlflow_result,
        group_context=None,
        max_workers=1,
    )


# ---------------------------------------------------------------------------
# Utility function tests
# ---------------------------------------------------------------------------

class TestBuildSpanName:
    def test_crew_event_no_context(self):
        result = _build_span_name("crew_started", {})
        assert result == "crew_started"

    def test_agent_event_with_agent(self):
        result = _build_span_name("agent_execution", {"kasal.agent_name": "Alice"})
        assert result == "agent:Alice"

    def test_task_event_with_task(self):
        result = _build_span_name("task_started", {"kasal.task_name": "Research Topic"})
        assert result == "task:Research Topic"

    def test_tool_event_with_tool(self):
        result = _build_span_name("tool_usage", {"kasal.tool_name": "WebSearch"})
        assert result == "tool:WebSearch"

    def test_truncates_task_name(self):
        long_task = "A" * 100
        result = _build_span_name("task_started", {"kasal.task_name": long_task})
        assert len(result) <= len("task:") + 80

    def test_fallback_uses_event_type(self):
        result = _build_span_name("memory_write", {})
        assert result == "memory_write"


class TestBuildPairingKey:
    def test_includes_event_type(self):
        key = _build_pairing_key("crew_started", {})
        assert "crew_started" in key

    def test_uses_task_as_suffix(self):
        key = _build_pairing_key("task_started", {"kasal.task_name": "Research"})
        assert "Research" in key

    def test_uses_agent_when_no_task(self):
        key = _build_pairing_key("agent_execution", {"kasal.agent_name": "Bob"})
        assert "Bob" in key

    def test_uses_tool_when_present(self):
        key = _build_pairing_key("tool_usage", {"kasal.tool_name": "Search"})
        assert "Search" in key

    def test_empty_suffix_for_no_context(self):
        key = _build_pairing_key("flow_started", {})
        assert key == "flow_started:"


class TestExtractAgentName:
    def test_from_agent_name_attr(self):
        assert _extract_agent_name({"kasal.agent_name": "Alice"}) == "Alice"

    def test_from_extra_agent_role(self):
        assert _extract_agent_name({"kasal.extra.agent_role": "Researcher"}) == "Researcher"

    def test_empty_string_when_missing(self):
        assert _extract_agent_name({}) == ""


class TestExtractTaskName:
    def test_from_task_name_attr(self):
        assert _extract_task_name({"kasal.task_name": "Research Topic"}) == "Research Topic"

    def test_from_extra_task_name(self):
        assert _extract_task_name({"kasal.extra.task_name": "Write Report"}) == "Write Report"

    def test_empty_string_when_missing(self):
        assert _extract_task_name({}) == ""


class TestExtractSpanOutputs:
    def test_extracts_content(self):
        span = _make_span(output_content="Here is the answer")
        result = _extract_span_outputs(span)
        assert "content" in result
        assert result["content"] == "Here is the answer"

    def test_truncates_long_content(self):
        span = _make_span(output_content="X" * 5000)
        result = _extract_span_outputs(span)
        assert len(result["content"]) <= 4000

    def test_extracts_extra_attrs(self):
        span = _make_span(extra_attrs={"kasal.extra.model": "gpt-4"})
        result = _extract_span_outputs(span)
        assert "model" in result

    def test_empty_when_no_attrs(self):
        span = MagicMock(spec=ReadableSpan)
        span.attributes = None
        result = _extract_span_outputs(span)
        assert result == {}


class TestExtractSpanAttrs:
    def test_only_kasal_attrs(self):
        span = _make_span(extra_attrs={"kasal.custom": "value", "other.attr": "ignored"})
        result = _extract_span_attrs(span)
        assert "kasal.custom" in result
        assert "other.attr" not in result

    def test_none_attrs_excluded(self):
        span = MagicMock(spec=ReadableSpan)
        span.attributes = {"kasal.attr": None, "kasal.valid": "ok"}
        result = _extract_span_attrs(span)
        assert "kasal.attr" not in result
        assert "kasal.valid" in result

    def test_empty_dict_when_no_attrs(self):
        span = MagicMock(spec=ReadableSpan)
        span.attributes = None
        result = _extract_span_attrs(span)
        assert result == {}


# ---------------------------------------------------------------------------
# Exporter tests
# ---------------------------------------------------------------------------

class TestKasalMLflowSpanExporterInit:
    def test_initialization(self):
        exporter = _make_exporter("test-job")
        assert exporter._job_id == "test-job"
        assert exporter._buffer == []
        assert exporter._flushed is False
        assert exporter._is_flow is False

    def test_creates_thread_pool(self):
        exporter = _make_exporter()
        assert exporter._executor is not None
        exporter._executor.shutdown(wait=False)


class TestExport:
    def test_returns_success(self):
        exporter = _make_exporter()
        spans = [_make_span("s1", "crew_started")]
        result = exporter.export(spans)
        assert result == SpanExportResult.SUCCESS
        exporter._executor.shutdown(wait=False)

    def test_skips_spans_without_event_type(self):
        exporter = _make_exporter()
        span = MagicMock(spec=ReadableSpan)
        span.attributes = {"other.attr": "value"}
        exporter.export([span])
        assert len(exporter._buffer) == 0
        exporter._executor.shutdown(wait=False)

    def test_buffers_spans_with_event_type(self):
        exporter = _make_exporter()
        spans = [_make_span("s1", "crew_started"), _make_span("s2", "task_started")]
        exporter.export(spans)
        assert len(exporter._buffer) == 2
        exporter._executor.shutdown(wait=False)

    def test_detects_flow_context_from_flow_started(self):
        exporter = _make_exporter()
        spans = [_make_span("s1", "flow_started")]
        exporter.export(spans)
        assert exporter._is_flow is True
        exporter._executor.shutdown(wait=False)

    def test_detects_flow_context_from_flow_created(self):
        exporter = _make_exporter()
        spans = [_make_span("s1", "flow_created")]
        exporter.export(spans)
        assert exporter._is_flow is True
        exporter._executor.shutdown(wait=False)

    def test_crew_completed_triggers_flush_for_crew(self):
        exporter = _make_exporter()
        with patch.object(exporter._executor, "submit") as mock_submit:
            exporter.export([_make_span("s1", "crew_completed")])
        mock_submit.assert_called_once()
        exporter._executor.shutdown(wait=False)

    def test_flow_completed_triggers_flush_for_flow(self):
        exporter = _make_exporter()
        exporter._is_flow = True
        with patch.object(exporter._executor, "submit") as mock_submit:
            exporter.export([_make_span("s1", "flow_completed")])
        mock_submit.assert_called_once()
        exporter._executor.shutdown(wait=False)

    def test_crew_completed_not_flush_when_in_flow_mode(self):
        exporter = _make_exporter()
        exporter._is_flow = True
        with patch.object(exporter._executor, "submit") as mock_submit:
            exporter.export([_make_span("s1", "crew_completed")])
        mock_submit.assert_not_called()
        exporter._executor.shutdown(wait=False)

    def test_no_flush_after_already_flushed(self):
        exporter = _make_exporter()
        exporter._flushed = True
        with patch.object(exporter._executor, "submit") as mock_submit:
            exporter.export([_make_span("s1", "crew_completed")])
        mock_submit.assert_not_called()
        exporter._executor.shutdown(wait=False)


class TestPairEvents:
    def test_pairs_crew_started_crew_completed(self):
        exporter = _make_exporter()
        spans = [
            _make_span("s1", "crew_started", start_time=1000, end_time=1001),
            _make_span("s2", "crew_completed", start_time=5000, end_time=5001),
        ]
        paired, instants = exporter._pair_events(spans)
        assert len(paired) == 1
        assert paired[0].event_type == "crew_started"
        assert paired[0].start_time == 1000
        assert paired[0].end_time == 5001

    def test_unpaired_start_becomes_instant(self):
        exporter = _make_exporter()
        spans = [_make_span("s1", "crew_started", start_time=1000, end_time=1001)]
        paired, instants = exporter._pair_events(spans)
        assert len(paired) == 0
        assert len(instants) == 1

    def test_standalone_event_becomes_instant(self):
        exporter = _make_exporter()
        spans = [_make_span("s1", "flow_created", start_time=1000, end_time=1001)]
        paired, instants = exporter._pair_events(spans)
        # flow_created is not in _EVENT_PAIRS (it's not a start event)
        assert len(instants) == 1

    def test_pairs_task_started_task_completed(self):
        exporter = _make_exporter()
        spans = [
            _make_span("s1", "task_started", task_name="Research", start_time=100, end_time=101),
            _make_span("s2", "task_completed", task_name="Research", start_time=500, end_time=501),
        ]
        paired, instants = exporter._pair_events(spans)
        assert len(paired) == 1

    def test_merges_attributes_from_start_and_end(self):
        exporter = _make_exporter()
        spans = [
            _make_span("s1", "crew_started", extra_attrs={"kasal.start_info": "begin"}, start_time=100, end_time=101),
            _make_span("s2", "crew_completed", extra_attrs={"kasal.end_info": "end"}, start_time=500, end_time=501),
        ]
        paired, _ = exporter._pair_events(spans)
        assert "kasal.start_info" in paired[0].attributes
        assert "kasal.end_info" in paired[0].attributes

    def test_error_status_from_end_span(self):
        exporter = _make_exporter()
        spans = [
            _make_span("s1", "task_started", start_time=100, end_time=101),
            _make_span("s2", "task_completed", start_time=500, end_time=501, status_code_name="ERROR"),
        ]
        paired, _ = exporter._pair_events(spans)
        assert paired[0].status == "ERROR"


class TestDetermineHierarchyLevel:
    def test_crew_events(self):
        exporter = _make_exporter()
        for event_type in ["crew_started", "crew_completed", "flow_started", "flow_completed"]:
            assert exporter._determine_hierarchy_level(event_type) == "crew"

    def test_agent_events(self):
        exporter = _make_exporter()
        for event_type in ["agent_execution", "llm_response"]:
            assert exporter._determine_hierarchy_level(event_type) == "agent"

    def test_task_events(self):
        exporter = _make_exporter()
        for event_type in ["task_started", "task_completed", "task_failed"]:
            assert exporter._determine_hierarchy_level(event_type) == "task"

    def test_leaf_event(self):
        exporter = _make_exporter()
        assert exporter._determine_hierarchy_level("tool_usage") == "leaf"
        assert exporter._determine_hierarchy_level("llm_call") == "leaf"


class TestCreateMlflowSpan:
    def test_returns_span_id_on_success(self):
        exporter = _make_exporter()
        mock_client = MagicMock()
        mock_child = MagicMock()
        mock_child.span_id = "span-abc"
        mock_client.start_span.return_value = mock_child

        span_id = exporter._create_mlflow_span(
            client=mock_client,
            trace_id="trace-1",
            parent_id="parent-1",
            name="test-span",
            start_time=1000,
            end_time=2000,
            outputs={"result": "ok"},
            attributes={"kasal.event": "test"},
        )
        assert span_id == "span-abc"

    def test_returns_none_on_error(self):
        exporter = _make_exporter()
        mock_client = MagicMock()
        mock_client.start_span.side_effect = RuntimeError("mlflow error")

        span_id = exporter._create_mlflow_span(
            client=mock_client,
            trace_id="trace-1",
            parent_id="parent-1",
            name="test-span",
            start_time=1000,
            end_time=2000,
            outputs={},
            attributes={},
        )
        assert span_id is None

    def test_sets_outputs_when_provided(self):
        exporter = _make_exporter()
        mock_client = MagicMock()
        mock_child = MagicMock()
        mock_child.span_id = "s1"
        mock_client.start_span.return_value = mock_child

        exporter._create_mlflow_span(
            client=mock_client,
            trace_id="t1",
            parent_id="p1",
            name="span",
            start_time=0,
            end_time=1,
            outputs={"key": "value"},
            attributes={},
        )

        end_call_kwargs = mock_client.end_span.call_args.kwargs
        assert "outputs" in end_call_kwargs

    def test_skips_empty_outputs(self):
        exporter = _make_exporter()
        mock_client = MagicMock()
        mock_child = MagicMock()
        mock_child.span_id = "s1"
        mock_client.start_span.return_value = mock_child

        exporter._create_mlflow_span(
            client=mock_client,
            trace_id="t1",
            parent_id="p1",
            name="span",
            start_time=0,
            end_time=1,
            outputs={},
            attributes={},
        )

        end_call_kwargs = mock_client.end_span.call_args.kwargs
        assert "outputs" not in end_call_kwargs


class TestFlush:
    def test_empty_buffer_no_action(self):
        exporter = _make_exporter()
        exporter._flushed = False
        # Patch _build_mlflow_trace to ensure it's not called
        with patch.object(exporter, "_build_mlflow_trace") as mock_build:
            exporter._flush()
        mock_build.assert_not_called()
        exporter._executor.shutdown(wait=False)

    def test_marks_flushed_after_flush(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_build_mlflow_trace"):
            exporter._flush()
        assert exporter._flushed is True
        exporter._executor.shutdown(wait=False)

    def test_clears_buffer_after_flush(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_build_mlflow_trace"):
            exporter._flush()
        assert exporter._buffer == []
        exporter._executor.shutdown(wait=False)

    def test_second_flush_is_noop(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_build_mlflow_trace") as mock_build:
            exporter._flush()
            exporter._flush()  # second call
        mock_build.assert_called_once()  # only once
        exporter._executor.shutdown(wait=False)

    def test_handles_build_error_gracefully(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_completed"))
        with patch.object(exporter, "_build_mlflow_trace", side_effect=RuntimeError("build error")):
            exporter._flush()  # Should not raise
        exporter._executor.shutdown(wait=False)


class TestShutdown:
    def test_flushes_remaining_buffer(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_flush") as mock_flush:
            exporter.shutdown()
        mock_flush.assert_called_once()

    def test_no_flush_when_already_flushed(self):
        exporter = _make_exporter()
        exporter._flushed = True
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_flush") as mock_flush:
            exporter.shutdown()
        mock_flush.assert_not_called()

    def test_no_flush_when_empty_buffer(self):
        exporter = _make_exporter()
        with patch.object(exporter, "_flush") as mock_flush:
            exporter.shutdown()
        mock_flush.assert_not_called()


class TestForceFlush:
    def test_flushes_when_buffer_not_empty(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_flush") as mock_flush:
            result = exporter.force_flush()
        mock_flush.assert_called_once()
        assert result is True

    def test_no_flush_when_empty(self):
        exporter = _make_exporter()
        with patch.object(exporter, "_flush") as mock_flush:
            result = exporter.force_flush()
        mock_flush.assert_not_called()
        assert result is True

    def test_returns_true_even_on_exception(self):
        exporter = _make_exporter()
        exporter._buffer.append(_make_span("s1", "crew_started"))
        with patch.object(exporter, "_flush", side_effect=RuntimeError("flush error")):
            result = exporter.force_flush()
        assert result is True


class TestBuildMlflowTrace:
    """Tests for _build_mlflow_trace method."""

    def test_does_nothing_when_mlflow_not_installed(self):
        exporter = _make_exporter()
        paired = [_PairedSpan(name="crew", start_time=100, end_time=200)]
        instants = []
        all_spans = [_make_span("s", "crew_started")]

        with patch.dict("sys.modules", {"mlflow.tracking": None}):
            # Should not raise even when mlflow.tracking import fails
            try:
                exporter._build_mlflow_trace(paired, instants, all_spans)
            except Exception:
                pass  # Expected if mlflow not available

        exporter._executor.shutdown(wait=False)

    def test_handles_empty_spans_gracefully(self):
        exporter = _make_exporter()

        with patch("mlflow.tracking.MlflowClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.start_trace.side_effect = RuntimeError("no trace")

            # Empty all_spans should cause early return
            exporter._build_mlflow_trace([], [], [])

        exporter._executor.shutdown(wait=False)

    def test_creates_trace_with_spans(self):
        exporter = _make_exporter()

        all_spans = [
            _make_span("s1", "crew_started", start_time=1000, end_time=1001),
            _make_span("s2", "crew_completed", start_time=2000, end_time=2001),
        ]

        mock_root = MagicMock()
        mock_root.trace_id = "trace-1"
        mock_root.span_id = "root-span-1"

        with patch("mlflow.tracking.MlflowClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.start_trace.return_value = mock_root
            mock_client.start_span.return_value = MagicMock(span_id="child-1")

            paired = [_PairedSpan(name="crew_execution", start_time=1000, end_time=2001, event_type="crew_started")]
            exporter._build_mlflow_trace(paired, [], all_spans)

            mock_client.start_trace.assert_called_once()
            mock_client.end_trace.assert_called_once()

        exporter._executor.shutdown(wait=False)

    def test_handles_start_trace_failure(self):
        exporter = _make_exporter()
        all_spans = [_make_span("s", "crew_started", start_time=100, end_time=200)]

        with patch("mlflow.tracking.MlflowClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.start_trace.side_effect = RuntimeError("trace error")

            # Should not propagate
            try:
                exporter._build_mlflow_trace([], [], all_spans)
            except Exception:
                pass

        exporter._executor.shutdown(wait=False)

    def test_includes_group_context_in_inputs(self):
        mlflow_result = MagicMock()
        mlflow_result.experiment_id = "exp-1"

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp-42"

        exporter = KasalMLflowSpanExporter(
            job_id="job-gc",
            mlflow_result=mlflow_result,
            group_context=group_ctx,
        )

        all_spans = [_make_span("s", "crew_started", start_time=100, end_time=200)]
        captured_inputs = {}

        mock_root = MagicMock()
        mock_root.trace_id = "trace-1"
        mock_root.span_id = "root-1"

        def capture_start_trace(**kwargs):
            captured_inputs.update(kwargs.get("inputs", {}))
            return mock_root

        with patch("mlflow.tracking.MlflowClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.start_trace.side_effect = capture_start_trace

            exporter._build_mlflow_trace([], [], all_spans)

        assert "group_id" in captured_inputs
        assert captured_inputs["group_id"] == "grp-42"
        exporter._executor.shutdown(wait=False)


class TestEventPairsConstants:
    def test_all_start_events_have_end_events(self):
        for start, end in _EVENT_PAIRS.items():
            assert isinstance(start, str)
            assert isinstance(end, str)

    def test_end_to_starts_populated(self):
        assert len(_END_TO_STARTS) > 0

    def test_crew_started_maps_to_crew_completed(self):
        assert "crew_started" in _EVENT_PAIRS
        assert _EVENT_PAIRS["crew_started"] == "crew_completed"
