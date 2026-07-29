"""The task-level output contract: structured output, guardrail retries, budgets.

Three defects are pinned here, each of which was invisible because the run still
"succeeded":

* ``output_json = True`` — a bool in a field typed ``type[BaseModel] | None``,
  which made ``Task(**task_args)`` raise and killed the crew build outright for
  Databricks and Gemini models.
* A guardrail retry patched only ``raw``, leaving ``.pydantic``/``.json_dict``
  holding the REJECTED parse and reverting ``raw`` from the JSON dump back to
  prose. The retry that was supposed to fix the output broke the contract.
* Exhausted retries and blown budgets raised, destroying every task that had
  already succeeded.
"""

from typing import Any

import pytest
from pydantic import BaseModel

from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
from src.services.execution.kernel.model_conversion_handler import (
    configure_output_json_approach,
)
from src.services.execution.runtime import Task
from src.services.execution.runtime.types import OutputFormat


class Answer(BaseModel):
    summary: str


class _Agent:
    """Minimal agent stand-in: returns queued answers, one per call."""

    role = "tester"
    llm = None

    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = []

    def execute_task(self, task, context=None, tools=None):
        self.calls.append(context)
        return self.answers.pop(0)


class _BudgetAgent:
    role = "tester"
    llm = None

    def __init__(self, partial=""):
        self.partial = partial

    def execute_task(self, task, context=None, tools=None):
        raise ExecutionBudgetExceededError("out of rounds", partial=self.partial)


def _reject_then_accept():
    """A guardrail that rejects the first answer it sees and accepts the next."""
    state = {"seen": 0}

    def guardrail(output):
        state["seen"] += 1
        if state["seen"] == 1:
            return (False, "needs more detail")
        return (True, output)

    return guardrail


class TestOutputJsonIsTheModelClass:
    def test_configure_sets_the_class_not_a_bool(self):
        args = {"description": "d", "expected_output": "e"}
        configure_output_json_approach(args, Answer)
        assert args["output_json"] is Answer

    def test_task_actually_constructs(self):
        """The test whose absence let the bool ship: the value has to survive
        ``Task(**task_args)``, not merely be set."""
        args = {"description": "d", "expected_output": "e"}
        configure_output_json_approach(args, Answer)
        task = Task(**args)
        assert task.output_json is Answer

    def test_a_bool_would_still_be_rejected(self):
        with pytest.raises(Exception):
            Task(description="d", expected_output="e", output_json=True)


class TestReshapeAfterRetry:
    def test_retry_result_is_reshaped_not_just_patched(self):
        agent = _Agent(['{"summary": "rejected"}', '{"summary": "accepted"}'])
        task = Task(
            description="d",
            expected_output="e",
            output_json=Answer,
            guardrail=_reject_then_accept(),
            max_retries=2,
        )
        output = task.execute_sync(agent)

        # raw is the JSON dump of the ACCEPTED answer, not the agent's unshaped
        # text and not the rejected parse.
        assert output.json_dict == {"summary": "accepted"}
        assert "accepted" in output.raw
        assert "rejected" not in output.raw
        assert output.output_format == OutputFormat.JSON

    def test_downstream_context_stays_json_across_a_retry(self):
        """``raw`` is what the next task receives. Before the fix it reverted to
        prose after any retry, so the JSON contract held only for tasks that
        passed first time."""
        agent = _Agent(['{"summary": "first"}', '{"summary": "second"}'])
        task = Task(
            description="d",
            expected_output="e",
            output_json=Answer,
            guardrail=_reject_then_accept(),
            max_retries=2,
        )
        output = task.execute_sync(agent)
        import json

        assert json.loads(output.raw) == {"summary": "second"}


class TestDegradeInsteadOfAbort:
    def test_exhausted_guardrail_raises_by_default(self):
        agent = _Agent(["nope", "nope", "nope", "nope"])
        task = Task(
            description="d",
            expected_output="e",
            guardrail=lambda output: (False, "never good enough"),
            max_retries=1,
        )
        with pytest.raises(ValueError, match="guardrail failed"):
            task.execute_sync(agent)

    def test_exhausted_guardrail_degrades_when_asked(self):
        agent = _Agent(["attempt one", "attempt two"])
        task = Task(
            description="d",
            expected_output="e",
            guardrail=lambda output: (False, "never good enough"),
            max_retries=1,
            guardrail_on_exhausted="degrade",
        )
        output = task.execute_sync(agent)
        assert "⚠️ Unverified" in output.raw
        assert "never good enough" in output.raw

    def test_budget_exhaustion_raises_by_default(self):
        task = Task(description="d", expected_output="e")
        with pytest.raises(ExecutionBudgetExceededError):
            task.execute_sync(_BudgetAgent())

    def test_budget_exhaustion_degrades_with_the_partial_answer(self):
        task = Task(description="d", expected_output="e", on_budget_exceeded="degrade")
        output = task.execute_sync(_BudgetAgent(partial="what I found so far"))
        assert "what I found so far" in output.raw
        assert "⚠️ Truncated" in output.raw
