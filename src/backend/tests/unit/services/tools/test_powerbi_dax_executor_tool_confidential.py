"""Confidential-values mode on the Power BI DAX executor, through the engine's wrap_tool."""

from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution.runtime.executor import wrap_tool
from src.services.tools import tool_policies
from src.services.tools.powerbi_dax_executor_tool import PowerBIDaxExecutorTool


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


DAX_ROWS = [
    {"[Plant].[Name]": "North", "[Measures].[Revenue]": 1234.5, "[Plant].[Count]": 12},
    {"[Plant].[Name]": "South", "[Measures].[Revenue]": 7.25, "[Plant].[Count]": 3},
]
DAX_COLUMNS = list(DAX_ROWS[0].keys())


def _dax_tool() -> PowerBIDaxExecutorTool:
    # workspace and dataset are teamspace config, not call arguments (the
    # executor reads them from its config; the args schema drops them).
    return _stamped(PowerBIDaxExecutorTool(workspace_id="ws", dataset_id="ds"))


def test_dax_executor_result_reaches_agent_and_trace_without_fractions():
    tool = _dax_tool()
    fake_query = AsyncMock(
        return_value={
            "success": True,
            "data": DAX_ROWS,
            "row_count": 2,
            "columns": DAX_COLUMNS,
            "error": None,
        }
    )
    with (
        patch(
            "src.services.tools.powerbi_dax_executor_tool.get_powerbi_access_token",
            AsyncMock(return_value="token"),
        ),
        patch.object(PowerBIDaxExecutorTool, "_execute_dax_query", fake_query),
        patch("src.services.execution.runtime.executor.event_bus.emit") as emit,
    ):
        output = wrap_tool(tool)(dax_query="EVALUATE x")

    assert "1234.5" not in output and "7.25" not in output
    assert output.splitlines()[0] == "| Name | Revenue | Count |"
    assert "| North | <withheld> | 12 |" in output
    assert "| South | <withheld> | 3 |" in output
    assert "2 rows" in output and "Revenue (fractional, withheld)" in output
    assert _finished_outputs(emit) == [output]


def test_dax_executor_error_is_scrubbed_at_the_boundary():
    tool = _dax_tool()
    fake_query = AsyncMock(
        return_value={
            "success": False,
            "data": [],
            "columns": [],
            "error": "column 7 at 12.5",
        }
    )
    with (
        patch(
            "src.services.tools.powerbi_dax_executor_tool.get_powerbi_access_token",
            AsyncMock(return_value="token"),
        ),
        patch.object(PowerBIDaxExecutorTool, "_execute_dax_query", fake_query),
        patch("src.services.execution.runtime.executor.event_bus.emit"),
    ):
        output = wrap_tool(tool)(dax_query="EVALUATE x")
    assert output == "DAX execution failed: column <n> at <n>"
