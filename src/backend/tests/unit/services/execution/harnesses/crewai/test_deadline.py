"""``max_execution_time`` has to STOP an agent, not just report on it.

Regression test for a production failure: one task, ``max_execution_time=30``,
still making LLM calls 145 seconds later, no timeout reported until the run
ended by itself.

CrewAI does honour the field — ``execute_task`` runs the agent in a
``ThreadPoolExecutor`` and calls ``future.result(timeout=...)``. But Python
cannot kill a thread and the enclosing ``with`` block joins on exit, so the loop
keeps going and the ``TimeoutError`` surfaces only once the agent finishes
anyway. It reports; it does not bound.

The Kasal harness stops the agent because the TRANSPORT enforces a deadline
inside its round loop. That deadline is rebuilt on every ``call()``, so it only
bounds a turn when one ``call()`` IS the turn — true under Kasal, false under
CrewAI, where the executor owns the tool loop and each call is one round.
``run_deadline`` is the only term that survives across calls, so the run scope
stamps it per turn.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses import binding_for, reset_for_tests
from src.services.tools.base import BaseTool as KasalBaseTool

# Small on purpose. These two tests are the only ones here that really sleep,
# and `backend/CLAUDE.md` is explicit about what a sleeping test costs a suite.
# The ratio is what matters, not the magnitudes: each round must outlast the cap
# so an unbounded agent visibly overruns it.
# An INTEGER: CrewAI's `max_execution_time` is typed `int | None` and rejects
# a fractional value outright.
CAP_SECONDS = 1
CALL_SECONDS = 0.6
MAX_ITER = 6


class _Args(BaseModel):
    q: str = Field(description="query")


class _Search(KasalBaseTool):
    name: str = "search"
    description: str = "Search"
    args_schema: type = _Args

    def _run(self, q: str) -> str:
        return "some result"


@pytest.fixture(autouse=True)
def _clean():
    reset_for_tests()
    yield
    reset_for_tests()


@pytest.fixture
def never_finishing_client():
    """A model that always asks for another tool call — an agent with no end."""

    def _tool_call(*args, **kwargs):
        time.sleep(CALL_SECONDS)
        function = SimpleNamespace(name="search", arguments='{"q":"x"}')
        message = SimpleNamespace(
            content=None,
            tool_calls=[SimpleNamespace(id="c", function=function)],
            reasoning_content=None,
        )
        return SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="tool_calls")],
            usage=SimpleNamespace(
                total_tokens=1,
                prompt_tokens=1,
                completion_tokens=0,
                prompt_tokens_details=None,
                completion_tokens_details=None,
            ),
        )

    client = MagicMock()
    client.chat.completions.create.side_effect = _tool_call
    return client


def _run_a_capped_agent(client, *, inside_run_scope: bool):
    """Run one capped agent; return (llm rounds made, seconds elapsed)."""
    harness = binding_for("crewai")
    transport = LLM(model="gpt-4o")
    agent = harness.build_agent(
        role="R",
        goal="g",
        backstory="b",
        llm=transport,
        tools=[_Search()],
        max_execution_time=CAP_SECONDS,
        max_iter=MAX_ITER,
    )
    task = harness.build_task(description="do it", expected_output="text", agent=agent)
    crew = harness.build_crew(
        agents=[agent], tasks=[task], process=harness.process("sequential")
    )

    before = client.chat.completions.create.call_count
    started = time.monotonic()
    with patch.object(type(transport), "client", property(lambda s: client)):
        try:
            if inside_run_scope:
                with harness.event_bridge():
                    crew.kickoff()
            else:
                crew.kickoff()
        except Exception:  # noqa: BLE001 — the timeout is expected either way
            pass
    return client.chat.completions.create.call_count - before, (
        time.monotonic() - started
    )


class TestTheCapBindsTheAgent:
    def test_the_agent_stops_at_the_cap(self, never_finishing_client):
        """The whole point: work STOPS, rather than being reported on later."""
        rounds, elapsed = _run_a_capped_agent(
            never_finishing_client, inside_run_scope=True
        )
        # One round may already be in flight when the deadline passes.
        assert rounds <= 3, f"{rounds} rounds against a {CAP_SECONDS}s cap"
        assert elapsed < CAP_SECONDS + CALL_SECONDS + 1

    def test_without_the_run_scope_the_cap_does_not_bind(self, never_finishing_client):
        """Pins the failure so it cannot come back unnoticed.

        This is CrewAI's own behaviour, and it is why the run scope exists: the
        agent runs to ``max_iter`` and the cap only surfaces at the end. On the
        real run that was 145 seconds against a 30-second cap.
        """
        rounds, elapsed = _run_a_capped_agent(
            never_finishing_client, inside_run_scope=False
        )
        assert rounds >= MAX_ITER - 1, "expected CrewAI alone to overrun the cap"
        assert elapsed > CAP_SECONDS * 2


class TestTheRunCeilingSurvivesTheTurn:
    def test_a_turn_deadline_is_restored_to_the_run_ceiling(self):
        """Otherwise task two starts already past task one's deadline."""
        from src.services.execution.harnesses.crewai.deadline import (
            RUN_DEADLINE_ATTR,
            _restore,
        )

        run_ceiling = time.monotonic() + 300
        agent = SimpleNamespace(run_deadline=time.monotonic() + 5)
        setattr(agent, RUN_DEADLINE_ATTR, run_ceiling)

        _restore(agent)
        assert agent.run_deadline == run_ceiling

    def test_the_earlier_of_the_two_wins(self):
        from src.services.execution.harnesses.crewai.deadline import (
            RUN_DEADLINE_ATTR,
            _combined_deadline,
        )

        far = time.monotonic() + 300
        agent = SimpleNamespace(max_execution_time=5)
        setattr(agent, RUN_DEADLINE_ATTR, far)
        # The turn cap is nearer, so it binds.
        assert _combined_deadline(agent) < far

        near = time.monotonic() + 1
        agent = SimpleNamespace(max_execution_time=600)
        setattr(agent, RUN_DEADLINE_ATTR, near)
        # The run ceiling is nearer, so it does.
        assert _combined_deadline(agent) == near

    def test_no_cap_anywhere_means_no_deadline(self):
        from src.services.execution.harnesses.crewai.deadline import _combined_deadline

        assert _combined_deadline(SimpleNamespace()) is None

    def test_the_crew_stamps_the_run_ceiling_at_kickoff(self):
        """Computed when work starts, not when the crew was assembled."""
        from src.services.execution.harnesses.crewai.deadline import RUN_DEADLINE_ATTR

        harness = binding_for("crewai")
        agent = harness.build_agent(
            role="R", goal="g", backstory="b", llm=LLM(model="gpt-4o")
        )
        task = harness.build_task(description="d", expected_output="o", agent=agent)
        crew = harness.build_crew(
            agents=[agent],
            tasks=[task],
            process=harness.process("sequential"),
            run_max_seconds=45,
        )

        assert getattr(agent, "run_deadline", None) is None
        crew._stamp_run_deadline()
        assert getattr(agent, RUN_DEADLINE_ATTR) == pytest.approx(agent.run_deadline)
        assert agent.run_deadline - time.monotonic() == pytest.approx(45, abs=2)
