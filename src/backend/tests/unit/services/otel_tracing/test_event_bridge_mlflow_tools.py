"""Tests for bridging CrewAI tool calls into the active MLflow trace.

Native ``mlflow.crewai.autolog`` records crew/task/agent/LLM spans but not tool
calls, so MCP/Genie tool usage was missing from the UC trace. The bridge opens
an MLflow span on tool start and closes it on finish/error, nesting under the
active autolog span — and never orphans a root trace when no span is active.

It uses the IMPERATIVE MlflowClient.start_span/end_span API (explicit
parent_id), NOT the fluent context manager: the start and finish events run in
different OTel contexts, so the fluent manager's attach/detach(token) raises
"Token was created in a different Context" on exit.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.services.otel_tracing.event_bridge import OTelEventBridge


def _tool_bridge() -> OTelEventBridge:
    b = object.__new__(OTelEventBridge)
    b._job_id = "test"
    b._mlflow_tool_spans = []
    return b


def _mock_mlflow(active: bool = True):
    """Return (mlflow_module, mlflow.tracking_module, client) mocks."""
    m = MagicMock()
    parent = MagicMock()
    parent.trace_id = "tr-1"
    parent.span_id = "sp-parent"
    m.get_current_active_span.return_value = parent if active else None

    client = MagicMock()
    started = MagicMock()
    started.trace_id = "tr-1"
    started.span_id = "sp-tool"
    client.start_span.return_value = started

    tracking_mod = MagicMock()
    tracking_mod.MlflowClient.return_value = client
    return m, tracking_mod, client


def _patch(m, tracking_mod):
    return patch.dict(sys.modules, {"mlflow": m, "mlflow.tracking": tracking_mod})


class TestBridgeToolToMlflow:
    def test_start_opens_span_when_active(self):
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=True)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow(
                "CrewAI.tool.execute", "genie_query", "",
                SimpleNamespace(tool_args={"q": "x"}),
            )
        assert len(b._mlflow_tool_spans) == 1
        client.start_span.assert_called_once()
        kw = client.start_span.call_args.kwargs
        assert kw["span_type"] == "TOOL"
        assert kw["parent_id"] == "sp-parent"
        assert kw["trace_id"] == "tr-1"
        assert "args" in kw["inputs"]

    def test_start_skipped_without_active_span(self):
        """No active autolog span -> do not start (would orphan a root trace)."""
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=False)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow(
                "CrewAI.tool.execute", "genie_query", "", SimpleNamespace()
            )
        assert b._mlflow_tool_spans == []
        client.start_span.assert_not_called()

    def test_finish_closes_with_outputs_and_ok_status(self):
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=True)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "genie", "", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "genie", "RESULT", SimpleNamespace())
        assert b._mlflow_tool_spans == []
        client.end_span.assert_called_once()
        kw = client.end_span.call_args.kwargs
        assert kw["span_id"] == "sp-tool"
        assert kw["status"] == "OK"
        assert kw["outputs"] == {"result": "RESULT"}

    def test_error_sets_error_status(self):
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=True)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "genie", "", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.error", "genie", "", SimpleNamespace())
        assert client.end_span.call_args.kwargs["status"] == "ERROR"
        assert b._mlflow_tool_spans == []

    def test_non_tool_span_is_noop(self):
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=True)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow("CrewAI.crew.kickoff", "", "", SimpleNamespace())
        client.start_span.assert_not_called()
        assert b._mlflow_tool_spans == []

    def test_finish_with_empty_stack_is_safe(self):
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=True)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "genie", "r", SimpleNamespace())
        client.end_span.assert_not_called()  # nothing to close

    def test_nested_tools_pair_lifo(self):
        b = _tool_bridge()
        m, tracking_mod, client = _mock_mlflow(active=True)
        with _patch(m, tracking_mod):
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "outer", "", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "inner", "", SimpleNamespace())
            assert len(b._mlflow_tool_spans) == 2
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "inner", "ri", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "outer", "ro", SimpleNamespace())
        assert b._mlflow_tool_spans == []
        assert client.end_span.call_count == 2
