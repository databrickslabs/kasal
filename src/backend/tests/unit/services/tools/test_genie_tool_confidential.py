"""Confidential-values mode on the Genie tool, through the engine's wrap_tool."""

from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution.runtime.executor import wrap_tool
from src.services.tools import tool_policies
from src.services.tools.genie_tool import GenieTool


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


def _genie_with_answer(status: dict, result: object = None) -> GenieTool:
    tool = _stamped(GenieTool(tool_config={"spaceId": "space-1"}))
    tool._get_workspace_url = AsyncMock(return_value="https://example.com")
    tool._get_auth_headers = AsyncMock(return_value={"Authorization": "Bearer t"})
    tool._start_or_continue_conversation = AsyncMock(
        return_value={"conversation_id": "conv-1", "message_id": "msg-1"}
    )
    tool._get_message_status = AsyncMock(return_value=status)
    tool._get_query_result = AsyncMock(return_value=result)
    return tool


def test_genie_answer_with_numbers_is_replaced_by_a_notice_in_agent_and_trace():
    status = {
        "status": "COMPLETED",
        "attachments": [{"text": {"content": "Plant North revenue was 1234.5 in Q1"}}],
    }
    tool = _genie_with_answer(status)
    with patch("src.services.execution.runtime.executor.event_bus.emit") as emit:
        output = wrap_tool(tool)(question="What was revenue?")

    assert "1234.5" not in output
    assert "[answer withheld" in output
    assert _finished_outputs(emit) == [output]


def test_genie_failure_keeps_its_reason_with_numbers_scrubbed():
    status = {
        "status": "FAILED",
        "error": {"error": "column 7 is ambiguous at 12.5"},
    }
    tool = _genie_with_answer(status)
    with patch("src.services.execution.runtime.executor.event_bus.emit"):
        output = wrap_tool(tool)(question="q")
    assert "Genie error: column <n> is ambiguous at <n>" in output
    assert "12.5" not in output
