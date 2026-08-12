"""A chat call has no Task, so position needs a bucket the recordings share.

Chat runs `Agent.kickoff_async` directly — no Task, so `task_key()` is empty.
Without a shared bucket the cassette can only match identical arguments, and a
re-asked question whose search the model rephrased pays again.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.services.execution.kernel.tool_replay import install_tool_replay_hook
from src.services.execution.runtime import unregister_tool_hooks
from src.services.execution.runtime.executor import wrap_tool
from src.services.trace.recordings import ToolRecording, canonical_args

TURN = "9f8e7d6c5b4a3210"


def _recording(args, task=TURN, output="RECORDED"):
    return ToolRecording(
        job_id="chat-1",
        tool_name="PerplexityTool",
        task_name=task,
        args_key=canonical_args(args),
        output=output,
        recorded_at=None,
    )


class _Tool:
    def __init__(self):
        self.name = "PerplexityTool"
        self.calls = []
        self._replay_policy = {"ttl_seconds": 3600, "scope": "group"}

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return "LIVE CALL"


@pytest.fixture
def install():
    hooks = []

    def _go(recordings, turn_key=TURN):
        uninstall = install_tool_replay_hook(
            "chat-2",
            SimpleNamespace(group_ids=["g"]),
            turn_key=turn_key,
            load=lambda *_a: recordings,
        )
        if uninstall:
            hooks.append(uninstall)
        return uninstall

    yield _go
    for uninstall in hooks:
        uninstall()


def test_the_same_question_replays_even_when_the_query_was_rephrased(install):
    """The case that made chat record for nothing: same question re-asked, the
    model wrote a different search, and the call went out again."""
    install([_recording({"query": "Lebanon news today"})])
    tool = _Tool()

    result = wrap_tool(tool, task=None)(query="latest Lebanese news 2026")

    assert result == "RECORDED"
    assert tool.calls == []


def test_a_DIFFERENT_question_does_not_take_this_one_s_recording(install):
    install([_recording({"query": "Lebanon news today"}, task="other-question")])
    tool = _Tool()

    result = wrap_tool(tool, task=None)(query="Swiss apartment prices")

    assert result == "LIVE CALL"


def test_identical_arguments_still_match_across_questions(install):
    """Exact arguments are exact wherever they were recorded."""
    install([_recording({"query": "same"}, task="other-question")])
    tool = _Tool()

    assert wrap_tool(tool, task=None)(query="same") == "RECORDED"


def test_a_task_when_there_is_one_still_wins(install):
    """Crew calls have a real Task; the turn key is only the fallback."""
    install([_recording({"query": "x"}, task="Research the market")])
    tool = _Tool()

    result = wrap_tool(tool, task=SimpleNamespace(name="Research the market"))(
        query="y"
    )

    assert result == "RECORDED"


def test_no_turn_key_and_no_task_means_exact_arguments_only(install):
    """Rows recorded before turn_key existed are filed under their run, which
    no live call can name."""
    install([_recording({"query": "x"}, task="run:chat-0")], turn_key="")
    tool = _Tool()

    assert wrap_tool(tool, task=None)(query="y") == "LIVE CALL"
