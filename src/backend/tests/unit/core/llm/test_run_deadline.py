"""The run-level deadline, and why the per-call one was not enough.

``max_execution_time`` computes a FRESH deadline on every ``call()``. A six-task
crew therefore had an effective ceiling of six times the per-call cap, and the
guardrail retry loop — which re-enters ``agent.execute_task`` — multiplied it
again. ``Agent.run_deadline`` is one fixed point for the whole run, and the
transport takes whichever comes first.
"""

import time
from types import SimpleNamespace

from pydantic import BaseModel

from src.core.llm.transport.completion import OpenAICompletion
from src.services.execution.runtime import Agent as _AgentBase


def _budget(**agent_attrs):
    llm = OpenAICompletion(model="test-model")
    agent = SimpleNamespace(**agent_attrs)
    return llm._execution_budget(agent)


class _StubAgent(_AgentBase):
    """Agent is a Pydantic model, so a method cannot be patched by assignment."""

    def execute_task(self, task, context=None, tools=None):
        return "done"


class TestRounds:
    def test_max_iter_sets_the_round_cap(self):
        rounds, _ = _budget(max_iter=30, max_execution_time=None, run_deadline=None)
        assert rounds == 30

    def test_no_agent_keeps_the_engine_default(self):
        llm = OpenAICompletion(model="test-model")
        rounds, deadline = llm._execution_budget(None)
        assert rounds == 15 and deadline is None


class TestDeadlines:
    def test_per_call_only(self):
        _, deadline = _budget(max_iter=10, max_execution_time=600, run_deadline=None)
        assert deadline is not None
        assert 590 < deadline - time.monotonic() <= 600

    def test_run_deadline_only(self):
        run_deadline = time.monotonic() + 120
        _, deadline = _budget(
            max_iter=10, max_execution_time=None, run_deadline=run_deadline
        )
        assert deadline == run_deadline

    def test_the_earlier_deadline_wins_when_the_run_is_nearly_up(self):
        """The case that matters: an agent allowed 600s per call, 30s before the
        run's hour is up, must stop in 30s — not start a fresh 600s clock."""
        run_deadline = time.monotonic() + 30
        _, deadline = _budget(
            max_iter=30, max_execution_time=600, run_deadline=run_deadline
        )
        assert deadline == run_deadline

    def test_the_per_call_deadline_wins_when_it_is_tighter(self):
        run_deadline = time.monotonic() + 3600
        _, deadline = _budget(
            max_iter=30, max_execution_time=60, run_deadline=run_deadline
        )
        assert deadline < run_deadline

    def test_neither_means_no_clock(self):
        _, deadline = _budget(max_iter=10, max_execution_time=None, run_deadline=None)
        assert deadline is None


class TestCrewStampsTheDeadline:
    def test_kickoff_gives_every_agent_the_same_deadline(self):
        from src.services.execution.runtime import Crew, Task

        agents = [_StubAgent(role=f"r{n}", goal="g", backstory="b") for n in range(3)]
        crew = Crew(
            agents=agents,
            tasks=[Task(description="d", expected_output="e", agent=agents[0])],
            run_max_seconds=1800,
        )
        # Stamping happens at kickoff, not at build time, so the clock starts
        # when work does.
        assert all(agent.run_deadline is None for agent in agents)

        crew.kickoff()

        deadlines = {agent.run_deadline for agent in agents}
        assert len(deadlines) == 1
        assert deadlines.pop() is not None

    def test_no_run_max_seconds_leaves_agents_unbounded(self):
        from src.services.execution.runtime import Crew, Task

        agent = _StubAgent(role="r", goal="g", backstory="b")
        crew = Crew(
            agents=[agent],
            tasks=[Task(description="d", expected_output="e", agent=agent)],
        )
        crew.kickoff()
        assert agent.run_deadline is None


class TestStructuredOutputReachesBothApis:
    """A schema must be sent on the path the request actually takes.

    Chat completions take `response_format`; the Responses API takes
    `text.format` with the schema inline. Only the first was implemented, so
    setting `response_format` on a GPT-5/Codex model — the whole family that runs
    on the Responses API — was accepted and then silently dropped. A schema that
    never reaches the endpoint is indistinguishable from no schema, which is how
    a caller ends up trusting fields the model was never required to return: a
    4.3k-char prompt asked gpt-5-3-codex for scope/produces/needs_tools and it
    replied with none of them.
    """

    class _Shape(BaseModel):
        name: str
        needed: bool

    def test_the_responses_api_gets_the_schema(self):
        llm = OpenAICompletion(model="gpt-5-3-codex", api="responses")
        llm.response_format = self._Shape
        params = llm._prepare_responses_params([{"role": "user", "content": "x"}])
        fmt = params["text"]["format"]
        assert fmt["type"] == "json_schema"
        assert fmt["name"] == "_Shape"
        assert fmt["schema"]["required"] == ["name", "needed"]

    def test_strict_is_set_because_that_is_what_binds_the_fields(self):
        """Without strict, "required" is a suggestion."""
        llm = OpenAICompletion(model="gpt-5-3-codex", api="responses")
        llm.response_format = self._Shape
        params = llm._prepare_responses_params([{"role": "user", "content": "x"}])
        assert params["text"]["format"]["strict"] is True

    def test_chat_completions_still_use_their_own_envelope(self):
        """The two APIs disagree on shape; neither may be given the other's."""
        llm = OpenAICompletion(model="some-model")
        llm.response_format = self._Shape
        params = llm._prepare_completion_params([{"role": "user", "content": "x"}])
        assert params["response_format"]["type"] == "json_schema"
        assert params["response_format"]["json_schema"]["name"] == "_Shape"

    def test_no_schema_requested_sends_no_format(self):
        llm = OpenAICompletion(model="gpt-5-3-codex", api="responses")
        params = llm._prepare_responses_params([{"role": "user", "content": "x"}])
        assert "text" not in params

    def test_a_responses_shaped_dict_passes_through(self):
        llm = OpenAICompletion(model="gpt-5-3-codex", api="responses")
        llm.response_format = {"type": "json_object"}
        params = llm._prepare_responses_params([{"role": "user", "content": "x"}])
        assert params["text"]["format"] == {"type": "json_object"}

    def test_a_completions_shaped_dict_is_translated(self):
        """Callers copying the chat-completions form must not be silently
        ignored — that is the bug this whole class exists for."""
        llm = OpenAICompletion(model="gpt-5-3-codex", api="responses")
        llm.response_format = {
            "type": "json_schema",
            "json_schema": {"name": "Thing", "schema": {"type": "object"}},
        }
        params = llm._prepare_responses_params([{"role": "user", "content": "x"}])
        fmt = params["text"]["format"]
        assert fmt["name"] == "Thing" and fmt["schema"] == {"type": "object"}
