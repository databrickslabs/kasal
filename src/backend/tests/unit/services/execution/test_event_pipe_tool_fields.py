"""A row has two producers — this pipe live, the database on reload.

Any field only one of them sets is a row that changes when you refresh, which
is what "PerplexityTool (input) / PerplexityTool / PerplexityTool (output)
[cached]" was: the bare middle row is a piped frame.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.execution.event_pipe import EventPipeWriter


def _frame(event, event_type="tool_usage"):
    writer = EventPipeWriter(queue=MagicMock(), execution_id="run-1")
    return writer._project_trace_frame(event_type, event)


def test_the_started_frame_says_which_half_it_is():
    frame = _frame(
        SimpleNamespace(type="tool_usage_started", tool_name="PerplexityTool")
    )

    assert frame["trace_metadata"]["operation"] == "tool_started"


def test_the_finished_frame_says_which_half_it_is():
    frame = _frame(
        SimpleNamespace(
            type="tool_usage_finished", tool_name="PerplexityTool", output="x"
        )
    )

    assert frame["trace_metadata"]["operation"] == "tool_finished"


def test_a_replayed_call_is_badged_LIVE_not_only_after_a_refresh():
    frame = _frame(
        SimpleNamespace(
            type="tool_usage_finished",
            tool_name="PerplexityTool",
            output="x",
            from_cache=True,
        )
    )

    assert frame["trace_metadata"]["from_cache"] is True


def test_a_live_call_carries_no_badge():
    frame = _frame(
        SimpleNamespace(
            type="tool_usage_finished",
            tool_name="PerplexityTool",
            output="x",
            from_cache=False,
        )
    )

    assert frame["trace_metadata"].get("from_cache") in (None, False)


def test_a_non_tool_event_gets_no_operation():
    frame = _frame(
        SimpleNamespace(type="llm_call_started", model="gpt"), event_type="llm_call"
    )

    assert "operation" not in frame["trace_metadata"]
