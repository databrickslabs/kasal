"""A tool whose result IS the answer ends the agent turn.

``result_as_answer`` has been a field on ``BaseTool`` since the engine was
vendored, is seeded per tool in ``seeds/tools.py`` (several already set it
True), and was plumbed through ``ToolFactory`` onto the instance — where
nothing read it. The tool-call loop now does.

Opt-in and off by default, because it bypasses the agent: the raw tool output
becomes the task's answer, with no chance to reason over it or combine it with
anything. Right for a tool that already produces the deliverable, wrong for a
search that is one input among several.
"""

from types import SimpleNamespace

from src.core.llm.transport.tool_rounds import (
    SUPERSEDED_RESULT,
    answers_directly,
    run_chat_round,
    run_responses_round,
)
from src.services.execution.runtime.executor import wrap_tool


def _calls(count: int) -> list[dict]:
    return [
        {"id": f"call{i}", "name": f"tool{i}", "arguments": "{}"} for i in range(count)
    ]


def _functions(final_index: int | None = None) -> dict:
    functions = {}
    for i in range(3):
        fn = lambda **_: f"tool{i} result"  # noqa: E731
        fn.result_as_answer = i == final_index
        functions[f"tool{i}"] = fn
    return functions


class TestDetection:
    def test_a_flagged_tool_is_detected(self):
        assert answers_directly(_functions(final_index=1), "tool1") is True

    def test_an_unflagged_tool_is_not(self):
        assert answers_directly(_functions(final_index=1), "tool0") is False

    def test_an_unknown_tool_is_not(self):
        assert answers_directly(_functions(), "nope") is False

    def test_no_tool_table_is_not(self):
        assert answers_directly(None, "tool0") is False


class TestShortCircuit:
    def test_the_flagged_result_becomes_the_answer(self):
        conversation: list[dict] = []
        outcome = run_chat_round(
            conversation,
            "thinking",
            _calls(2),
            lambda name, _a: f"{name} says hello",
            None,
            _functions(final_index=0),
        )
        assert outcome.final_answer == "tool0 says hello"

    def test_later_calls_in_the_batch_are_not_paid_for(self):
        """The whole value of checking per call rather than after the batch: a
        model that fans out to three slow searches must not pay for all three
        when the first one already answered."""
        executed: list[str] = []

        def execute(name, _arguments):
            executed.append(name)
            return f"{name} result"

        conversation: list[dict] = []
        run_chat_round(
            conversation, "t", _calls(3), execute, None, _functions(final_index=0)
        )
        assert executed == ["tool0"]

    def test_the_dropped_calls_are_still_answered(self):
        """A dangling tool_call with no result is a malformed conversation."""
        conversation: list[dict] = []
        run_chat_round(
            conversation,
            "t",
            _calls(3),
            lambda n, _a: f"{n} result",
            None,
            _functions(final_index=0),
        )
        results = [e for e in conversation if e.get("role") == "tool"]
        assert [e["tool_call_id"] for e in results] == ["call0", "call1", "call2"]
        assert [e["content"] for e in results][1:] == [
            SUPERSEDED_RESULT,
            SUPERSEDED_RESULT,
        ]

    def test_an_unflagged_batch_runs_to_completion(self):
        executed: list[str] = []
        conversation: list[dict] = []
        outcome = run_chat_round(
            conversation,
            "t",
            _calls(3),
            lambda n, _a: executed.append(n) or f"{n} result",
            None,
            _functions(),
        )
        assert outcome.final_answer is None
        assert executed == ["tool0", "tool1", "tool2"]

    def test_a_flag_later_in_the_batch_still_stops_the_rest(self):
        executed: list[str] = []
        conversation: list[dict] = []
        outcome = run_chat_round(
            conversation,
            "t",
            _calls(3),
            lambda n, _a: executed.append(n) or f"{n} result",
            None,
            _functions(final_index=1),
        )
        assert outcome.final_answer == "tool1 result"
        assert executed == ["tool0", "tool1"]

    def test_the_responses_shape_behaves_identically(self):
        conversation: list[dict] = []
        outcome = run_responses_round(
            conversation,
            _calls(2),
            lambda n, _a: f"{n} result",
            None,
            _functions(final_index=0),
        )
        assert outcome.final_answer == "tool0 result"
        assert [e["output"] for e in conversation] == [
            "tool0 result",
            SUPERSEDED_RESULT,
        ]

    def test_a_missing_tool_does_not_become_the_answer(self):
        """`execute` returning None means 'tool not found' — an error string,
        not a deliverable."""
        conversation: list[dict] = []
        outcome = run_chat_round(
            conversation, "t", _calls(1), lambda n, _a: None, None, _functions(0)
        )
        assert outcome.final_answer is None


class _Tool:
    def __init__(self, name, result_as_answer):
        self.name = name
        self.result_as_answer = result_as_answer

    def run(self, **kwargs):
        return "output"


class TestWrapperCarriesTheFlag:
    def test_the_flag_reaches_the_wrapper(self):
        """The transport only ever sees the wrapped callables, never the
        BaseTool instances, so the flag has to travel on the wrapper."""
        agent = SimpleNamespace(role="r")
        assert wrap_tool(_Tool("finisher", True), agent).result_as_answer is True
        assert wrap_tool(_Tool("searcher", False), agent).result_as_answer is False

    def test_a_tool_without_the_field_defaults_to_false(self):
        plain = SimpleNamespace(name="plain", run=lambda **_: "out")
        assert wrap_tool(plain, SimpleNamespace(role="r")).result_as_answer is False
