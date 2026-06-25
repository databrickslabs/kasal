"""Tests for bridging CrewAI tool calls into the active MLflow trace.

Native ``mlflow.crewai.autolog`` records crew/task/agent/LLM spans but not tool
calls, so MCP/Genie tool usage was missing from the UC trace. The bridge opens
an MLflow span on tool start and closes it on finish/error, nesting under the
active autolog span — and never orphans a root trace when no span is active.
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
    m = MagicMock()
    m.get_current_active_span.return_value = MagicMock() if active else None
    span = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = span
    m.start_span.return_value = cm
    return m, cm, span


class TestBridgeToolToMlflow:
    def test_start_opens_span_when_active(self):
        b = _tool_bridge()
        m, cm, span = _mock_mlflow(active=True)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow(
                "CrewAI.tool.execute", "genie_query", "",
                SimpleNamespace(tool_args={"q": "x"}),
            )
        assert len(b._mlflow_tool_spans) == 1
        m.start_span.assert_called_once()
        span.set_inputs.assert_called_once()

    def test_start_skipped_without_active_span(self):
        """No active autolog span -> do not start (would orphan a root trace)."""
        b = _tool_bridge()
        m, cm, span = _mock_mlflow(active=False)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow(
                "CrewAI.tool.execute", "genie_query", "", SimpleNamespace()
            )
        assert b._mlflow_tool_spans == []
        m.start_span.assert_not_called()

    def test_finish_closes_and_sets_outputs(self):
        b = _tool_bridge()
        m, cm, span = _mock_mlflow(active=True)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "genie", "", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "genie", "RESULT", SimpleNamespace())
        assert b._mlflow_tool_spans == []
        span.set_outputs.assert_called_once_with({"result": "RESULT"})
        cm.__exit__.assert_called_once()

    def test_error_sets_error_status_and_closes(self):
        b = _tool_bridge()
        m, cm, span = _mock_mlflow(active=True)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "genie", "", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.error", "genie", "", SimpleNamespace())
        span.set_status.assert_called_once_with("ERROR")
        cm.__exit__.assert_called_once()
        assert b._mlflow_tool_spans == []

    def test_non_tool_span_is_noop(self):
        b = _tool_bridge()
        m, _, _ = _mock_mlflow(active=True)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow("CrewAI.crew.kickoff", "", "", SimpleNamespace())
        m.start_span.assert_not_called()
        assert b._mlflow_tool_spans == []

    def test_finish_with_empty_stack_is_safe(self):
        b = _tool_bridge()
        m, cm, span = _mock_mlflow(active=True)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "genie", "r", SimpleNamespace())
        cm.__exit__.assert_not_called()  # nothing to close

    def test_nested_tools_pair_lifo(self):
        b = _tool_bridge()
        m, cm, span = _mock_mlflow(active=True)
        with patch.dict(sys.modules, {"mlflow": m}):
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "outer", "", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.execute", "inner", "", SimpleNamespace())
            assert len(b._mlflow_tool_spans) == 2
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "inner", "ri", SimpleNamespace())
            b._bridge_tool_to_mlflow("CrewAI.tool.complete", "outer", "ro", SimpleNamespace())
        assert b._mlflow_tool_spans == []
