"""What happens when an agent's execution budget runs out.

Three behaviours, all learned from one deep-research trace: eleven sequential
``sonar-deep-research`` calls in a single round blew a 300s budget by roughly
4x, the overrun was only noticed at the top of the NEXT round, the error was
then retried three times (identical prompt, identical work), and the run ended
with nothing to show for ~30 minutes of searching.
"""

from types import SimpleNamespace

import pytest

from src.core.llm.transport import budget
from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
from src.core.llm.transport.tool_rounds import (
    SKIPPED_RESULT,
    run_chat_round,
    run_responses_round,
)


class _Clock:
    """Stands in for the ``time`` module inside budget.py."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def _calls(count: int) -> list[dict[str, object]]:
    return [
        {"id": f"call{i}", "name": f"tool{i}", "arguments": "{}"} for i in range(count)
    ]


class TestMidRoundBudget:
    """A round's tool calls are checked against the clock, not just the round."""

    def test_calls_run_until_the_clock_goes_then_are_skipped(self, monkeypatch):
        clock = _Clock()
        monkeypatch.setattr(budget, "time", clock)
        executed: list[str] = []

        def execute(name, _arguments):
            executed.append(name)
            clock.advance(10)
            return f"{name} ok"

        conversation: list[dict] = []
        outcome = run_chat_round(
            conversation, "thinking", _calls(3), execute, deadline=15
        )

        assert outcome.exhausted is True
        # tool2 never ran: the clock passed 15 while tool1 was running.
        assert executed == ["tool0", "tool1"]

    def test_a_round_inside_its_budget_is_untouched(self, monkeypatch):
        monkeypatch.setattr(budget, "time", _Clock())
        executed: list[str] = []
        conversation: list[dict] = []

        outcome = run_chat_round(
            conversation,
            "thinking",
            _calls(3),
            lambda name, _a: executed.append(name) or f"{name} ok",
            deadline=1000,
        )

        assert outcome.exhausted is False
        assert executed == ["tool0", "tool1", "tool2"]

    def test_no_deadline_never_aborts(self):
        conversation: list[dict] = []
        assert (
            run_chat_round(
                conversation, None, _calls(2), lambda n, _a: "ok", deadline=None
            ).exhausted
            is False
        )

    @pytest.mark.parametrize("shape", ["chat", "responses"])
    def test_skipped_calls_are_still_answered(self, monkeypatch, shape):
        """A dangling tool_call with no result is a malformed conversation that
        every provider rejects — so an abort must not simply stop appending."""
        clock = _Clock()
        monkeypatch.setattr(budget, "time", clock)
        conversation: list[dict] = []
        calls = _calls(3)

        def execute(name, _arguments):
            clock.advance(100)
            return f"{name} ok"

        if shape == "chat":
            run_chat_round(conversation, "t", calls, execute, deadline=1)
            results = [e for e in conversation if e.get("role") == "tool"]
            answered = [e["tool_call_id"] for e in results]
            contents = [e["content"] for e in results]
        else:
            run_responses_round(conversation, calls, execute, deadline=1)
            results = conversation
            answered = [e["call_id"] for e in results]
            contents = [e["output"] for e in results]

        assert answered == ["call0", "call1", "call2"]
        assert contents[1:] == [SKIPPED_RESULT, SKIPPED_RESULT]

    def test_the_chat_shape_records_the_assistant_turn(self):
        conversation: list[dict] = []
        run_chat_round(conversation, "thought", _calls(1), lambda n, _a: "ok", None)
        assert conversation[0]["role"] == "assistant"
        assert conversation[0]["content"] == "thought"
        assert conversation[0]["tool_calls"][0]["function"]["name"] == "tool0"


class _Message:
    def __init__(self, content, tool_calls):
        self.content = content
        self.tool_calls = tool_calls


class _Response:
    def __init__(self, content=None, tool_calls=()):
        self.choices = [SimpleNamespace(message=_Message(content, list(tool_calls)))]
        self.usage = None


def _tool_call(name="search"):
    return SimpleNamespace(
        id="call0", function=SimpleNamespace(name=name, arguments="{}")
    )


class _Client:
    """Returns queued responses; raises if asked for one more than queued."""

    def __init__(self, responses, fail_after=None):
        self._responses = list(responses)
        self._fail_after = fail_after
        self.params: list[dict] = []
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=self._create),
        )

    def _create(self, **params):
        self.params.append(params)
        if self._fail_after is not None and len(self.params) > self._fail_after:
            raise RuntimeError("provider is down")
        return self._responses.pop(0)


def _llm(client):
    llm = OpenAICompletion(model="test-model")
    llm._client = client
    return llm


def _agent(**overrides):
    return SimpleNamespace(
        **{
            "max_iter": 1,
            "max_execution_time": None,
            "run_deadline": None,
            **overrides,
        }
    )


class TestForcedFinalAnswer:
    """A spent budget yields the best answer available, not an exception."""

    def test_exhausted_rounds_produce_an_answer(self):
        client = _Client(
            [
                _Response(content=None, tool_calls=[_tool_call()]),
                _Response(content="Here is what I found so far."),
            ]
        )
        result = _llm(client).call(
            [{"role": "user", "content": "research this"}],
            tools=[{"type": "function", "function": {"name": "search"}}],
            available_functions={"search": lambda **_: "a result"},
            from_agent=_agent(max_iter=1),
        )
        assert result == "Here is what I found so far."

    def test_the_wrapup_call_is_offered_no_tools(self):
        """Otherwise it could open another round and spend the budget again."""
        client = _Client(
            [
                _Response(content=None, tool_calls=[_tool_call()]),
                _Response(content="done"),
            ]
        )
        _llm(client).call(
            [{"role": "user", "content": "go"}],
            tools=[{"type": "function", "function": {"name": "search"}}],
            available_functions={"search": lambda **_: "a result"},
            from_agent=_agent(max_iter=1),
        )
        assert "tools" not in client.params[-1]
        assert budget.FORCE_FINAL_ANSWER in client.params[-1]["messages"][-1]["content"]

    def test_a_failing_wrapup_still_raises_the_budget_error(self):
        client = _Client(
            [_Response(content=None, tool_calls=[_tool_call()])], fail_after=1
        )
        with pytest.raises(ExecutionBudgetExceededError):
            _llm(client).call(
                [{"role": "user", "content": "go"}],
                tools=[{"type": "function", "function": {"name": "search"}}],
                available_functions={"search": lambda **_: "a result"},
                from_agent=_agent(max_iter=1),
            )

    def test_an_empty_wrapup_still_raises_the_budget_error(self):
        client = _Client(
            [
                _Response(content=None, tool_calls=[_tool_call()]),
                _Response(content="   "),
            ]
        )
        with pytest.raises(ExecutionBudgetExceededError):
            _llm(client).call(
                [{"role": "user", "content": "go"}],
                tools=[{"type": "function", "function": {"name": "search"}}],
                available_functions={"search": lambda **_: "a result"},
                from_agent=_agent(max_iter=1),
            )

    def test_a_normal_answer_is_not_a_wrapup(self):
        """The happy path must not spend an extra call."""
        client = _Client([_Response(content="direct answer")])
        result = _llm(client).call(
            [{"role": "user", "content": "hi"}], from_agent=_agent(max_iter=5)
        )
        assert result == "direct answer"
        assert len(client.params) == 1
