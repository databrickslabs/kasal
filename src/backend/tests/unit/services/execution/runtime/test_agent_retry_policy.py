"""Which agent-turn failures are worth retrying, and which are not.

``run_agent`` retries any exception ``max_retry_limit + 1`` times. That is right
for a 429, a 503 or a dropped connection — send the same request again and it
may well work.

It is wrong for a spent execution budget, and this is where a deep-research run
burned three full rounds of ``sonar-deep-research`` calls to arrive at the
outcome the first attempt already had: ``messages`` is built once before the
loop and never touched inside it, and the deadline is recomputed fresh on every
``call_llm``, so each retry sends an identical prompt, gets an identical (slow)
tool fan-out, and blows an identical budget.
"""

from types import SimpleNamespace

import pytest

from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
from src.services.execution.runtime.executor import run_agent


class _FakeTool:
    """The smallest thing `build_tool_context` will wrap."""

    def __init__(self, name, fn):
        self.name = name
        self.description = "does something"
        # `tool_schema` asks the schema for its JSON form.
        self.args_schema = SimpleNamespace(model_json_schema=lambda: {"type": "object"})
        self._fn = fn
        self.result_as_answer = False

    def run(self, **kwargs):
        return self._fn(**kwargs)


class _LLM:
    """Counts calls and raises whatever it was given."""

    def __init__(self, error=None, answer="done"):
        self.error = error
        self.answer = answer
        self.calls = 0

    def call(self, messages, **_kwargs):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.answer


def _agent(llm, max_retry_limit=2):
    return SimpleNamespace(
        llm=llm,
        role="researcher",
        max_retry_limit=max_retry_limit,
        step_callback=None,
    )


def _run(agent):
    return run_agent(
        agent, "irrelevant", None, messages=[{"role": "user", "content": "go"}]
    )


class TestBudgetErrors:
    def test_a_spent_budget_is_not_retried(self):
        llm = _LLM(
            error=ExecutionBudgetExceededError("spent", partial="half an answer")
        )
        agent = _agent(llm, max_retry_limit=2)

        with pytest.raises(ExecutionBudgetExceededError):
            _run(agent)

        assert llm.calls == 1, "a blown budget must not be re-attempted"

    def test_the_partial_survives_for_the_degrade_path(self):
        """Task._run_agent reads exc.partial to keep the work when
        on_budget_exceeded is 'degrade'. Swallowing the error into the retry
        loop and re-raising the LAST attempt's would have discarded it."""
        error = ExecutionBudgetExceededError("spent", partial="half an answer")
        with pytest.raises(ExecutionBudgetExceededError) as raised:
            _run(_agent(_LLM(error=error)))
        assert raised.value.partial == "half an answer"


class TestATurnThatAlreadyRanTools:
    """A turn with side effects behind it is not replayed.

    The transport runs the whole tool-call loop inside ONE `call`, on its own
    copy of the conversation, so an exception thrown late in that loop discards
    every round it had completed. The retry then sends the ORIGINAL prompt: the
    model reproduces its opening line, calls the same tools again, and their
    side effects happen a second time.

    Measured on a real run: 43 `postgres_execute_sql` calls re-executed, and the
    same sentence painted into the chat 92 times.
    """

    def test_a_failure_after_a_tool_ran_is_not_retried(self):
        ran = {"count": 0}

        def a_tool(**_kwargs):
            ran["count"] += 1
            return "did something"

        class _ToolCallingLLM:
            def __init__(self):
                self.calls = 0

            def call(self, messages, available_functions=None, **_kwargs):
                self.calls += 1
                # The transport drives tools INSIDE the call, then fails.
                available_functions["a_tool"]()
                raise RuntimeError("provider hiccup")

        llm = _ToolCallingLLM()
        agent = _agent(llm, max_retry_limit=2)
        with pytest.raises(RuntimeError):
            run_agent(
                agent,
                "irrelevant",
                [_FakeTool("a_tool", a_tool)],
                messages=[{"role": "user", "content": "go"}],
            )

        assert llm.calls == 1, "the turn must not be replayed"
        assert ran["count"] == 1, "the tool's side effect must not happen twice"

    def test_a_failure_before_any_tool_ran_is_still_retried(self):
        """Nothing happened yet, so replaying costs only the call itself."""

        class _FailFast:
            def __init__(self):
                self.calls = 0

            def call(self, messages, **_kwargs):
                self.calls += 1
                raise RuntimeError("provider hiccup")

        llm = _FailFast()
        with pytest.raises(RuntimeError):
            run_agent(
                _agent(llm, max_retry_limit=2),
                "irrelevant",
                [_FakeTool("a_tool", lambda **_k: "unused")],
                messages=[{"role": "user", "content": "go"}],
            )
        assert llm.calls == 3


class TestErrorsThePromptWillReproduce:
    def test_malformed_tool_arguments_are_not_retried(self):
        """The model writes the same arguments again from the same prompt.

        Both Databricks and llama.cpp surface this as a 500 whose message names
        the parse failure — the run that prompted this test hit it nine times.
        """
        llm = _LLM(
            error=RuntimeError(
                "Error code: 500 - {'error': {'code': 500, 'message': "
                "'Failed to parse tool call arguments as JSON: ...'}}"
            )
        )
        with pytest.raises(RuntimeError):
            _run(_agent(llm, max_retry_limit=2))
        assert llm.calls == 1


class TestTransientErrors:
    def test_an_ordinary_failure_is_still_retried(self):
        llm = _LLM(error=RuntimeError("provider hiccup"))
        with pytest.raises(RuntimeError):
            _run(_agent(llm, max_retry_limit=2))
        assert llm.calls == 3, "max_retry_limit=2 means three attempts"

    def test_a_successful_call_is_made_once(self):
        llm = _LLM(answer="the answer")
        assert _run(_agent(llm)) == "the answer"
        assert llm.calls == 1
