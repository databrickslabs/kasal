"""Confidential-values mode on the UCMV reconciliation tool: value-free mode forced on."""

from unittest.mock import MagicMock, patch

from src.services.execution.runtime.executor import wrap_tool
from src.services.tools import tool_policies
from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool


POLICY = {"confidential_values": True}


def _stamped(tool):
    tool_policies.stamp_tool_policies(
        tool, tool_policies.extract_tool_policies(dict(POLICY))
    )
    return tool


def _finished_outputs(emit: MagicMock) -> list:
    """Outputs carried by the ToolUsageFinishedEvent(s) the engine emitted."""
    outputs = []
    for call in emit.call_args_list:
        event = call.args[1]
        if type(event).__name__ == "ToolUsageFinishedEvent":
            outputs.append(event.output)
    return outputs


def test_reconciliation_runs_in_forced_statistics_mode_and_keeps_its_verdicts():
    captured: dict = {}

    def fake_run(self, **kwargs):
        captured.update(kwargs)
        return '{"statistics_only": true, "measures": {"revenue": {"share_aligned": 0.97}}}'

    tool = UCMVReconciliationTool()
    with patch.object(UCMVReconciliationTool, "_run", fake_run):
        tool = _stamped(tool)
        with patch("src.services.execution.runtime.executor.event_bus.emit") as emit:
            output = wrap_tool(tool)(statistics_only=False, dry_run=False)

    assert "statistics_only" not in captured, "the run may not turn the mode off"
    assert "0.97" in output, "value-free mode is not redacted: the verdict stays"
    assert _finished_outputs(emit) == [output]
