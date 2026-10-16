"""A spent wall clock under ``delegate_tool_calls`` ends a task's turn with an answer.

With delegation on (the CrewAI harness), every ``call()`` is ONE round of a
loop the caller owns, so the transport's own wrap-up path is unreachable. A
deadline that had passed made round 0 raise ``ExecutionBudgetExceededError``,
and CrewAI turned that into a failed task — or, wrapped in ``RuntimeError``,
into a re-run of the whole task through ``max_retry_limit``.

Under delegation, for a TASK's turn, the transport now hands back the same
annotated partial the Kasal harness produces (``runtime/task.py``), as plain
text, so the caller's loop finishes normally. Everything else is unchanged: the
Kasal harness (no delegation) still gets the error and ``Task._run_agent``
degrades it; Chat's lone agent (no task) still gets it and ``turn_kickoff``
degrades it.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.llm.transport.budget import (
    TRUNCATED_MARKER,
    TURN_CAP_ATTR,
    last_assistant_text,
    resolve_execution_budget,
    turn_cap,
)
from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.exceptions import (
    ExecutionBudgetExceededError,
    ExecutionStoppedError,
)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search",
            "description": "s",
            "parameters": {"type": "object", "properties": {}},
        },
    }
]
TASK = SimpleNamespace(id="task-1", name="research", description="research this")
HISTORY = [
    {"role": "user", "content": "research this"},
    {"role": "assistant", "content": "Found the first two sources."},
    {"role": "tool", "tool_call_id": "c1", "content": "result"},
]


class _Agent:
    def __init__(self, run_deadline=None):
        self.role = "researcher"
        self.id = "agent-1"
        self.max_iter = 25
        self.max_execution_time = None
        self.run_deadline = run_deadline


def _llm(delegate: bool) -> OpenAICompletion:
    llm = OpenAICompletion(model="test-model", api_key="x")
    llm.delegate_tool_calls = delegate
    object.__setattr__(llm, "_client", MagicMock())
    return llm


def _past() -> float:
    return time.monotonic() - 1


def _turn(llm, deadline=None, task=TASK):
    return llm.call(
        list(HISTORY),
        tools=TOOLS,
        from_task=task,
        from_agent=_Agent(run_deadline=deadline),
    )


class TestDeadlinePassedAtRoundZero:
    def test_a_delegated_task_turn_returns_the_truncated_partial_as_text(self):
        answer = _turn(_llm(delegate=True), deadline=_past())
        assert isinstance(answer, str)
        assert TRUNCATED_MARKER in answer
        # The partial is what the model last wrote, kept above the banner.
        assert answer.startswith("Found the first two sources.")

    def test_no_request_is_made_once_the_deadline_has_passed(self):
        llm = _llm(delegate=True)
        _turn(llm, deadline=_past())
        llm._client.chat.completions.create.assert_not_called()

    def test_without_delegation_the_error_still_raises(self):
        """The Kasal harness degrades in ``Task._run_agent``; leave it the error."""
        with pytest.raises(ExecutionBudgetExceededError):
            _turn(_llm(delegate=False), deadline=_past())

    def test_without_a_task_the_error_still_raises(self):
        """Chat's lone agent: ``turn_kickoff`` degrades it, and marks the turn
        ``budget_exhausted`` so it is neither remembered nor composed."""
        with pytest.raises(ExecutionBudgetExceededError):
            _turn(_llm(delegate=True), deadline=_past(), task=None)

    def test_a_user_stop_still_raises_under_delegation(self, monkeypatch):
        """Stop ends the run; it is not a budget to degrade."""
        monkeypatch.setattr(
            "src.core.llm.transport.budget.stop_requested", lambda: True
        )
        with pytest.raises(ExecutionStoppedError):
            _turn(_llm(delegate=True))


class TestDeadlinePassedDuringTheRequest:
    def test_a_request_cut_off_by_the_deadline_degrades_too(self):
        """``bounded_params`` sizes the HTTP timeout to the time left, so the
        common way a deadline passes is a request timing out AT it. Raising
        there would hand CrewAI the same retry the round-0 check avoids."""
        llm = _llm(delegate=True)

        def _times_out(**kwargs):
            time.sleep(0.1)
            raise TimeoutError("Request timed out.")

        llm._client.chat.completions.create.side_effect = _times_out
        answer = _turn(llm, deadline=time.monotonic() + 0.05)
        assert TRUNCATED_MARKER in answer

    def test_an_error_before_the_deadline_still_raises(self):
        llm = _llm(delegate=True)
        llm._client.chat.completions.create.side_effect = RuntimeError("HTTP 500")
        with pytest.raises(RuntimeError, match="HTTP 500"):
            _turn(llm, deadline=time.monotonic() + 300)


class TestThePartialIsNeverThePlaceholder:
    def test_the_tool_call_placeholder_is_skipped(self):
        """Databricks' sanitizer writes "Calling tools." into every tool-call
        message CrewAI sends with no content. It is not an answer."""
        history = [
            {"role": "assistant", "content": "Two sources so far."},
            {"role": "assistant", "content": "Calling tools.", "tool_calls": [{}]},
            {"role": "tool", "tool_call_id": "c1", "content": "result"},
        ]
        assert last_assistant_text(history) == "Two sources so far."


class TestTheCarriedTurnCap:
    """The CrewAI harness keeps ``max_execution_time`` off ``crewai.Agent``."""

    def test_the_public_field_wins(self):
        assert turn_cap(SimpleNamespace(max_execution_time=30)) == 30

    def test_the_carried_value_is_read_when_the_field_is_empty(self):
        agent = SimpleNamespace(max_execution_time=None)
        setattr(agent, TURN_CAP_ATTR, 45)
        assert turn_cap(agent) == 45

    def test_no_cap_anywhere(self):
        assert turn_cap(SimpleNamespace()) is None

    def test_the_budget_still_applies_a_carried_cap(self):
        agent = SimpleNamespace(max_iter=5, max_execution_time=None, run_deadline=None)
        setattr(agent, TURN_CAP_ATTR, 600)
        _, deadline = resolve_execution_budget(agent)
        assert deadline is not None
        assert 590 < deadline - time.monotonic() <= 600
