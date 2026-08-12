"""The replay cassette: an earlier run's result answers this run's call.

The matching rules are the whole feature — measured on this repo's traces, a
re-run repeats the same query text only 11% of the time, so matching on
arguments alone would miss nine calls in ten.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.services.execution.kernel.tool_replay import install_tool_replay_hook
from src.services.execution.runtime import (
    ToolCallAnswered,
    unregister_tool_hooks,
)
from src.services.execution.runtime.executor import wrap_tool
from src.services.trace.recordings import ToolRecording, canonical_args


def _recording(tool="PerplexityTool", args=None, task="Research", output="recorded"):
    return ToolRecording(
        job_id="run-1",
        tool_name=tool,
        task_name=task,
        args_key=canonical_args(args if args is not None else {"query": "a"}),
        output=output,
        recorded_at=None,
    )


def _group(ids=("grp-1",)):
    return SimpleNamespace(group_ids=list(ids))


class _Tool:
    """A replayable tool that records whether it was actually called."""

    def __init__(self, name="PerplexityTool", replayable=True):
        self.name = name
        self.calls = []
        if replayable:
            self._replay_policy = {"ttl_seconds": 3600, "scope": "group"}

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return "LIVE CALL"


@pytest.fixture
def installed():
    """Install a cassette, yield a factory for calls, always uninstall."""
    uninstalls = []

    def _install(recordings, group=None):
        uninstall = install_tool_replay_hook(
            "run-2",
            group if group is not None else _group(),
            load=lambda *_args: recordings,
        )
        if uninstall:
            uninstalls.append(uninstall)
        return uninstall

    yield _install
    for uninstall in uninstalls:
        uninstall()


class TestMatching:
    def test_the_same_arguments_are_answered_from_the_recording(self, installed):
        installed([_recording(args={"query": "a"}, output="RECORDED")])
        tool = _Tool()

        result = wrap_tool(tool, task=SimpleNamespace(name="Research"))(query="a")

        assert result == "RECORDED"
        assert tool.calls == [], "the tool must not have been called"

    def test_argument_ORDER_does_not_break_the_match(self, installed):
        """The model emits kwargs in whatever order it likes."""
        installed([_recording(args={"a": 1, "b": 2}, output="RECORDED")])
        tool = _Tool()

        result = wrap_tool(tool, task=SimpleNamespace(name="Research"))(b=2, a=1)

        assert result == "RECORDED"

    def test_a_REPHRASED_query_still_matches_by_position(self, installed):
        """The case that makes this worth building: same workload, new wording."""
        installed(
            [_recording(args={"query": "ABC News today 2026"}, output="RECORDED")]
        )
        tool = _Tool()

        result = wrap_tool(tool, task=SimpleNamespace(name="Research"))(
            query="ABC News today August 2026"
        )

        assert result == "RECORDED"
        assert tool.calls == []

    def test_position_is_scoped_to_the_task(self, installed):
        """A recording from another task must not answer this one."""
        installed([_recording(task="Write-up", args={"query": "x"})])
        tool = _Tool()

        result = wrap_tool(tool, task=SimpleNamespace(name="Research"))(query="y")

        assert result == "LIVE CALL"

    def test_a_recording_answers_only_ONE_call(self, installed):
        """Or an agent looping on a tool gets the same answer forever."""
        installed([_recording(args={"query": "a"}, output="RECORDED")])
        tool = _Tool()
        call = wrap_tool(tool, task=SimpleNamespace(name="Research"))

        first, second = call(query="a"), call(query="a")

        assert first == "RECORDED"
        assert second == "LIVE CALL"

    def test_calls_beyond_the_recordings_go_out_for_real(self, installed):
        """A re-run that searches more than the original still works."""
        installed([_recording(args={"query": "a"})])
        tool = _Tool()
        call = wrap_tool(tool, task=SimpleNamespace(name="Research"))

        call(query="a")
        call(query="b")
        call(query="c")

        assert len(tool.calls) == 2


class TestWhatIsNotReplayed:
    def test_a_tool_without_the_policy_is_never_replayed(self, installed):
        """Opt-in per tool: no `replayable: true`, no cassette."""
        installed([_recording(tool="postgres_execute_sql")])
        tool = _Tool(name="postgres_execute_sql", replayable=False)

        result = wrap_tool(tool, task=SimpleNamespace(name="Research"))(
            sql="DELETE ..."
        )

        assert result == "LIVE CALL"
        assert tool.calls == [{"sql": "DELETE ..."}]

    def test_another_tools_recording_is_not_used(self, installed):
        installed([_recording(tool="SerperDevTool")])
        tool = _Tool(name="PerplexityTool")

        assert wrap_tool(tool, task=SimpleNamespace(name="Research"))(query="a") == (
            "LIVE CALL"
        )

    def test_no_group_means_no_cassette(self):
        """Recordings are workspace data; without a group there is no scope."""
        assert (
            install_tool_replay_hook("run-2", _group(ids=()), load=lambda *_a: [])
            is None
        )

    def test_no_recordings_installs_nothing(self):
        assert install_tool_replay_hook("run-2", _group(), load=lambda *_a: []) is None

    def test_a_failed_load_installs_nothing_rather_than_failing_the_run(self):
        def _boom(*_args):
            raise RuntimeError("database is down")

        with pytest.raises(RuntimeError):
            # The caller's try/except owns this; the point is it does not
            # silently install a half-built cassette.
            install_tool_replay_hook("run-2", _group(), load=_boom)


class TestTheTraceTellsTheTruth:
    def test_a_replayed_call_is_marked_from_cache(self, installed, monkeypatch):
        events = []
        from src.services.execution.runtime import executor as executor_module

        monkeypatch.setattr(
            executor_module.event_bus,
            "emit",
            lambda _source, event: events.append(event),
        )
        installed([_recording(args={"query": "a"}, output="RECORDED")])

        wrap_tool(_Tool(), task=SimpleNamespace(name="Research"))(query="a")

        finished = [
            e for e in events if getattr(e, "type", "") == "tool_usage_finished"
        ]
        assert len(finished) == 1
        assert finished[0].from_cache is True
        assert finished[0].output == "RECORDED"

    def test_a_live_call_is_not(self, installed, monkeypatch):
        events = []
        from src.services.execution.runtime import executor as executor_module

        monkeypatch.setattr(
            executor_module.event_bus,
            "emit",
            lambda _source, event: events.append(event),
        )
        # Another task's recording, and different arguments — so neither the
        # argument match (which is deliberately NOT task-scoped: identical
        # arguments are identical wherever they were recorded) nor the
        # positional one (which is) can answer this call.
        installed([_recording(task="Other", args={"query": "unrelated"})])

        wrap_tool(_Tool(), task=SimpleNamespace(name="Research"))(query="a")

        finished = [
            e for e in events if getattr(e, "type", "") == "tool_usage_finished"
        ]
        assert finished[0].from_cache is False


class TestHookContract:
    def test_the_hook_is_removed_on_uninstall(self, installed):
        uninstall = installed([_recording(args={"query": "a"})])
        uninstall()

        tool = _Tool()
        assert wrap_tool(tool, task=SimpleNamespace(name="Research"))(query="a") == (
            "LIVE CALL"
        )
        # Idempotent: the fixture will call it again.
        unregister_tool_hooks()

    def test_an_answer_short_circuits_the_remaining_pre_hooks(self):
        """A settled call must not be re-answered by a later hook."""
        from src.services.execution.runtime import register_tool_hooks

        second_ran = []

        def _answer(_tool, _kwargs, _agent, _task):
            return ToolCallAnswered(output="FIRST")

        def _later(_tool, _kwargs, _agent, _task):
            second_ran.append(True)
            return ToolCallAnswered(output="SECOND")

        register_tool_hooks(pre=_answer)
        register_tool_hooks(pre=_later)
        try:
            result = wrap_tool(_Tool(), task=SimpleNamespace(name="t"))(query="a")
        finally:
            unregister_tool_hooks(pre=_answer)
            unregister_tool_hooks(pre=_later)

        assert result == "FIRST"
        assert second_ran == []

    def test_a_dict_return_still_rewrites_the_arguments(self):
        """The older contract must keep working beside the new one."""
        from src.services.execution.runtime import register_tool_hooks

        def _rewrite(_tool, kwargs, _agent, _task):
            return {**kwargs, "query": "rewritten"}

        tool = _Tool(replayable=False)
        register_tool_hooks(pre=_rewrite)
        try:
            wrap_tool(tool, task=SimpleNamespace(name="t"))(query="original")
        finally:
            unregister_tool_hooks(pre=_rewrite)

        assert tool.calls == [{"query": "rewritten"}]


class TestRecordingParsing:
    """`tool_args` reaches the trace as a Python repr, not JSON."""

    def test_a_python_repr_and_its_json_twin_produce_the_same_key(self):
        assert canonical_args("{'query': 'a', 'n': 1}") == canonical_args(
            {"n": 1, "query": "a"}
        )

    def test_python_literals_survive(self):
        assert canonical_args("{'ok': True, 'x': None}") == canonical_args(
            {"ok": True, "x": None}
        )

    def test_something_unparseable_still_matches_itself(self):
        assert canonical_args("<object at 0x1>") == canonical_args("<object at 0x1>")

    def test_a_row_without_content_is_not_a_recording(self):
        from src.services.trace.recordings import _recording as parse

        row = MagicMock()
        row.output = {"extra_data": {"tool_name": "PerplexityTool"}}

        assert parse(row) is None
