"""Replay must not depend on which run happened to be last.

The cassette used to keep only the most recent run's recordings, so one crew
run between two chat turns left the chat turn with a source it shares no bucket
with — nothing matched and the calls went out again.
"""

from types import SimpleNamespace

import pytest

from src.services.execution.kernel.tool_replay import install_tool_replay_hook
from src.services.execution.runtime.executor import wrap_tool
from src.services.trace.recordings import ToolRecording, canonical_args

QUESTION = "2d24cfd110d18445"


def _rec(job, task, args, output):
    return ToolRecording(
        job_id=job,
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

    def _go(recordings, turn_key=QUESTION):
        uninstall = install_tool_replay_hook(
            "now",
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


def test_an_unrelated_run_in_between_does_not_hide_the_recording(install):
    """Newest run first: a crew run that ran later must not shadow the chat
    recording of this very question."""
    install(
        [
            _rec("crew-later", "Research the market", {"query": "market"}, "CREW"),
            _rec("chat-earlier", QUESTION, {"query": "lebanon news"}, "CHAT"),
        ]
    )
    tool = _Tool()

    assert wrap_tool(tool, task=None)(query="latest lebanon headlines") == "CHAT"


def test_position_never_merges_two_runs(install):
    """Two runs of the same question: the second call must come from the SAME
    run as the first, not from whichever run has an unused call left."""
    install(
        [
            _rec("run-b", QUESTION, {"query": "b1"}, "B1"),
            _rec("run-b", QUESTION, {"query": "b2"}, "B2"),
            _rec("run-a", QUESTION, {"query": "a1"}, "A1"),
            _rec("run-a", QUESTION, {"query": "a2"}, "A2"),
        ]
    )
    tool = _Tool()
    call = wrap_tool(tool, task=None)

    assert [call(query="x"), call(query="y")] == ["B1", "B2"]


def test_the_third_call_falls_through_to_an_older_run(install):
    """Once a run's recordings are spent, an older run of the same question is
    better than paying for the call."""
    install(
        [
            _rec("run-b", QUESTION, {"query": "b1"}, "B1"),
            _rec("run-a", QUESTION, {"query": "a1"}, "A1"),
        ]
    )
    tool = _Tool()
    call = wrap_tool(tool, task=None)

    assert [call(query="x"), call(query="y")] == ["B1", "A1"]


def test_an_empty_bucket_cannot_match_by_position(install):
    """A call with no task and no turn key must not take someone else's slot."""
    install([_rec("run-a", "run:run-a", {"query": "a"}, "A")], turn_key="")
    tool = _Tool()

    assert wrap_tool(tool, task=None)(query="different") == "LIVE CALL"
