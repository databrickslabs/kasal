"""Run limits on the CrewAI harness: how far a task may go, and how it ends.

Real ``crewai`` crews built through the binding, over Kasal's transport, with
only the HTTP client faked. Four things are pinned:

* a Databricks chat model keeps its tools past 8 tool results — the cap in
  ``DatabricksRetryLLM`` used to strip them whatever ``max_iter`` said;
* a spent wall clock ends the task with the "Truncated" partial answer, the run
  completes with that task marked degraded, and nothing runs twice — it used to
  fail the task, or re-run all of it (tools included) through
  ``max_retry_limit``;
* ``max_execution_time`` no longer reaches ``crewai.Agent`` (whose ThreadPool
  timeout is what failed the task) and still bounds the turn;
* a task whose every tool call failed is flagged, as it is on the Kasal harness.
"""

import itertools
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from src.core.llm.transport.budget import TRUNCATED_MARKER, TURN_CAP_ATTR
from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses import binding_for, reset_for_tests
from src.services.execution.harnesses.crewai.availability import crewai_symbols
from src.services.execution.harnesses.crewai.guardrails import DEGRADED_MARKER
from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM
from src.services.memory.run.persist import make_memory_output_sink
from src.services.tools.base import BaseTool as KasalBaseTool

_ids = itertools.count()
_USAGE = SimpleNamespace(
    total_tokens=1,
    prompt_tokens=1,
    completion_tokens=0,
    prompt_tokens_details=None,
    completion_tokens_details=None,
)


class _Args(BaseModel):
    q: str = Field(description="query")


class _Search(KasalBaseTool):
    name: str = "search"
    description: str = "Search"
    args_schema: type = _Args
    reply: str = "some result"
    runs: list = Field(default_factory=list)

    def _run(self, q: str) -> str:
        self.runs.append(threading.current_thread().name)
        return self.reply


def _tool_calls(*names):
    calls = [
        SimpleNamespace(
            id=f"call-{next(_ids)}",
            function=SimpleNamespace(name=name, arguments='{"q":"x"}'),
        )
        for name in names
    ]
    message = SimpleNamespace(content=None, tool_calls=calls, reasoning_content=None)
    return SimpleNamespace(
        choices=[SimpleNamespace(message=message, finish_reason="tool_calls")],
        usage=_USAGE,
    )


def _text(content):
    message = SimpleNamespace(content=content, tool_calls=None, reasoning_content=None)
    return SimpleNamespace(
        choices=[SimpleNamespace(message=message, finish_reason="stop")],
        usage=_USAGE,
    )


class _Client:
    """A model that wants ``tool_rounds`` searches, then answers.

    Honest about tools: offered none, it can only answer in text — which is
    what the cap forced, and how a real model behaves.
    """

    def __init__(self, tool_rounds=10_000, per_round=1, delay=0.0, final=None):
        self.tool_rounds, self.per_round, self.delay = tool_rounds, per_round, delay
        self.final = final or "finished after every search"
        self.requests = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **params):
        self.requests.append(bool(params.get("tools")))
        if self.delay:
            time.sleep(self.delay)
        if not params.get("tools"):
            return _text("forced answer without tools")
        if sum(self.requests) <= self.tool_rounds:
            return _tool_calls(*["search"] * self.per_round)
        return _text(self.final)


@pytest.fixture(autouse=True)
def _clean():
    reset_for_tests()
    yield
    reset_for_tests()


def _databricks_llm():
    with patch.object(DatabricksRetryLLM, "_get_crew_logger", return_value=MagicMock()):
        return DatabricksRetryLLM(model="databricks/databricks-claude-sonnet-4-6")


def _crew(
    transport,
    tool,
    *,
    tasks=1,
    sink=None,
    crew_kwargs=None,
    task_kwargs=None,
    **agent_kwargs,
):
    harness = binding_for("crewai")
    agent_kwargs.setdefault("max_iter", 40)
    agent = harness.build_agent(
        role="R", goal="g", backstory="b", llm=transport, tools=[tool], **agent_kwargs
    )
    built = [
        harness.build_task(
            description=f"do {i}",
            expected_output="text",
            agent=agent,
            **(task_kwargs or {}),
        )
        for i in range(tasks)
    ]
    crew_kwargs = crew_kwargs or {}
    crew = harness.build_crew(
        agents=[agent],
        tasks=built,
        process=harness.process("sequential"),
        **crew_kwargs,
    )
    if sink is not None:
        harness.wire_memory(crew, sink=sink)
    return harness, agent, built, crew


def _kickoff(harness, transport, client, crew):
    agent_class = crewai_symbols()["Agent"]
    original = agent_class.execute_task
    turns = []

    def counting(self, *args, **kwargs):
        turns.append(time.monotonic())
        return original(self, *args, **kwargs)

    with patch.object(type(transport), "client", property(lambda s: client)):
        with patch.object(agent_class, "execute_task", counting):
            with harness.event_bridge():
                result = crew.kickoff()
    return result, turns


class TestToolsArePastTheOldCap:
    def test_a_databricks_model_keeps_its_tools_after_eight_results(self):
        transport, tool = _databricks_llm(), _Search()
        client = _Client(tool_rounds=12)
        sink = make_memory_output_sink(MagicMock())
        assert sink is not None
        harness, _, _, crew = _crew(transport, tool, sink=sink)

        with patch("src.services.memory.run.persist.remember_async") as persisted:
            result, _ = _kickoff(harness, transport, client, crew)

        assert len(tool.runs) == 12
        assert all(client.requests), "a request went out without tools"
        assert result.raw == "finished after every search"
        # A clean answer still reaches memory.
        assert persisted.call_count == 1

    def test_parallel_calls_do_not_reach_the_cap_in_two_rounds(self):
        """Four calls per round used to hit 8 results after round two."""
        transport, tool = _databricks_llm(), _Search()
        client = _Client(tool_rounds=4, per_round=4)
        harness, _, _, crew = _crew(transport, tool)

        result, _ = _kickoff(harness, transport, client, crew)

        assert len(tool.runs) == 16
        assert result.raw == "finished after every search"


class TestASpentClockDegrades:
    CAP = 1  # an int: CrewAI's field rejects floats, and the kernel sends ints

    def test_the_turn_cap_ends_the_task_with_the_truncated_partial(self):
        transport, tool = _databricks_llm(), _Search()
        client = _Client(delay=0.25)
        persisted = []
        sink = make_memory_output_sink(MagicMock())
        assert sink is not None
        harness, _, tasks, crew = _crew(
            transport, tool, sink=sink, max_execution_time=self.CAP
        )

        started = time.monotonic()
        with patch(
            "src.services.memory.run.persist.remember_async",
            side_effect=lambda *a, **k: persisted.append(a),
        ):
            result, turns = _kickoff(harness, transport, client, crew)
        elapsed = time.monotonic() - started

        assert TRUNCATED_MARKER in result.raw
        assert tasks[0].output.degraded is True
        assert len(turns) == 1, "the task was re-run"
        # Stopped near the cap, not at max_iter (40 rounds x 0.25s). Loose on
        # purpose: timing under a loaded CI worker.
        assert elapsed < self.CAP + 3
        assert persisted == [], "a degraded answer was written to memory"

    def test_a_clean_guardrail_retry_is_not_marked_and_is_remembered(self):
        """Attempt one hits the turn cap, the guardrail asks again, and the
        retry (a new turn, with its own clock) answers in full. The mark the
        first attempt left must not follow the answer that replaced it."""
        transport, tool = _databricks_llm(), _Search()
        client = _Client(delay=0.25)

        def guardrail(output):  # unannotated: CrewAI validates annotations
            if TRUNCATED_MARKER in output.raw:
                client.tool_rounds = 0  # the retry answers at once
                return (False, "this was cut short; answer from what you have")
            return (True, output)

        sink = make_memory_output_sink(MagicMock())
        assert sink is not None
        harness, _, tasks, crew = _crew(
            transport,
            tool,
            sink=sink,
            max_execution_time=self.CAP,
            task_kwargs={"guardrail": guardrail, "guardrail_max_retries": 1},
        )

        with patch("src.services.memory.run.persist.remember_async") as remember:
            result, turns = _kickoff(harness, transport, client, crew)

        assert len(turns) == 2  # the truncated attempt and the retry
        assert result.raw == "finished after every search"
        assert getattr(tasks[0].output, "degraded", None) is not True
        remember.assert_called_once()

    def test_the_run_ceiling_degrades_every_later_task_without_a_retry(self):
        transport, tool = _databricks_llm(), _Search()
        client = _Client(delay=0.25)
        harness, _, tasks, crew = _crew(
            transport, tool, tasks=2, crew_kwargs={"run_max_seconds": self.CAP}
        )

        result, turns = _kickoff(harness, transport, client, crew)

        assert len(turns) == 2, "a task was re-run"
        assert all(t.output.degraded is True for t in tasks)
        assert all(TRUNCATED_MARKER in t.output.raw for t in tasks)
        # The second task met a spent ceiling: it made no request at all.
        assert len(client.requests) <= 5
        assert TRUNCATED_MARKER in result.raw


class TestTruncationIsAFactNotAPhrase:
    def test_quoting_an_upstream_banner_does_not_degrade_the_task(self):
        """A downstream task that repeats its truncated context has not itself
        run out of time; only the turn the transport ended is marked."""
        quoted = (
            "Upstream reported:\n"
            f"{TRUNCATED_MARKER}exceeded the execution budget (x). "
            "This answer is incomplete.\nHere is my own complete analysis."
        )
        transport, tool = LLM(model="gpt-4o"), _Search()
        harness, _, tasks, crew = _crew(transport, tool)

        _kickoff(harness, transport, _Client(tool_rounds=1, final=quoted), crew)

        assert TRUNCATED_MARKER in tasks[0].output.raw
        assert getattr(tasks[0].output, "degraded", None) is not True


class TestEveryHandlerDegrades:
    """``DatabricksResponsesLLM`` has its own ``call`` and raises at a spent
    deadline; the CrewAI LLM wrapper ends the task's turn the same way."""

    def _wrapped(self):
        from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
        from src.services.execution.harnesses.crewai.llm import build_kasal_backed_llm

        inner = LLM(model="gpt-4o")
        spent = ExecutionBudgetExceededError(
            "max_execution_time exceeded after 0 tool round(s)", partial="so far"
        )
        return build_kasal_backed_llm(inner), spent

    def test_a_task_turn_gets_the_truncated_partial(self):
        from src.core.llm.transport.budget import BUDGET_SPENT_ATTR

        wrapped, spent = self._wrapped()
        task = SimpleNamespace()

        def _raises(self, *args, **kwargs):  # a real method: it has a __self__
            raise spent

        with patch.object(type(wrapped.inner), "call", _raises):
            answer = wrapped.call([{"role": "user", "content": "x"}], from_task=task)
        assert answer.startswith("so far")
        assert TRUNCATED_MARKER in answer
        # Same wording as the transport's, and the task is told in structure.
        assert "max_execution_time reached for model gpt-4o" in answer
        assert "0 tool round" not in answer
        assert getattr(task, BUDGET_SPENT_ATTR)

    def test_a_call_with_no_task_still_raises(self):
        from src.core.llm.transport.exceptions import ExecutionBudgetExceededError

        wrapped, spent = self._wrapped()
        with patch.object(type(wrapped.inner), "call", side_effect=spent):
            with pytest.raises(ExecutionBudgetExceededError):
                wrapped.call([{"role": "user", "content": "x"}])


class TestMaxExecutionTimeStaysOffCrewAI:
    def test_the_field_is_not_set_on_the_crewai_agent(self):
        harness = binding_for("crewai")
        agent = harness.build_agent(
            role="R",
            goal="g",
            backstory="b",
            llm=LLM(model="gpt-4o"),
            max_execution_time=900,
        )
        assert agent.max_execution_time is None  # no ThreadPool timeout
        assert getattr(agent, TURN_CAP_ATTR) == 900

    def test_the_carried_cap_still_bounds_the_turn(self):
        from src.services.execution.harnesses.crewai.deadline import (
            _combined_deadline,
        )

        harness = binding_for("crewai")
        agent = harness.build_agent(
            role="R",
            goal="g",
            backstory="b",
            llm=LLM(model="gpt-4o"),
            max_execution_time=30,
        )
        deadline = _combined_deadline(agent)
        assert deadline is not None
        assert 25 < deadline - time.monotonic() <= 30

    def test_a_manager_agent_s_effort_cap_is_carried_too(self):
        harness = binding_for("crewai")
        worker = harness.build_agent(
            role="R", goal="g", backstory="b", llm=LLM(model="gpt-4o")
        )
        task = harness.build_task(description="d", expected_output="o", agent=None)
        crew = harness.build_crew(
            agents=[worker],
            tasks=[task],
            process=harness.process("hierarchical"),
            manager_llm=LLM(model="gpt-4o"),
            execution_effort={"tier": "medium"},
        )
        crew._create_manager_agent()
        assert crew.manager_agent.max_execution_time is None
        assert getattr(crew.manager_agent, TURN_CAP_ATTR) == 300

    def test_a_custom_manager_keeps_its_cap_without_an_effort(self):
        """``inputs.manager_agent`` is built by build_agent, which has already
        moved the cap off CrewAI's field; nothing may erase it afterwards."""
        harness = binding_for("crewai")
        worker = harness.build_agent(
            role="R", goal="g", backstory="b", llm=LLM(model="gpt-4o")
        )
        manager = harness.build_agent(
            role="M",
            goal="manage",
            backstory="b",
            llm=LLM(model="gpt-4o"),
            allow_delegation=True,
            max_execution_time=900,
        )
        task = harness.build_task(description="d", expected_output="o", agent=None)
        crew = harness.build_crew(
            agents=[worker],
            tasks=[task],
            process=harness.process("hierarchical"),
            manager_agent=manager,
        )
        crew._create_manager_agent()
        assert crew.manager_agent.max_execution_time is None
        assert getattr(crew.manager_agent, TURN_CAP_ATTR) == 900


class TestWhollyFailedToolsAreFlagged:
    def _failing(self):
        return _Search(reply="Error: service unavailable (503)")

    def _run(self, client):
        transport, tool = LLM(model="gpt-4o"), self._failing()
        seen = []
        harness, _, tasks, crew = _crew(
            transport,
            tool,
            sink=lambda task, output: seen.append(getattr(output, "degraded", None)),
        )
        _kickoff(harness, transport, client, crew)
        return tool, tasks[0].output, seen

    def test_the_output_is_marked_unverified_before_the_sinks_run(self):
        tool, output, seen = self._run(_Client(tool_rounds=2))
        assert len(tool.runs) == 2
        assert DEGRADED_MARKER in output.raw
        assert "search" in output.degradation_reason
        assert output.degraded is True
        assert seen == [True], "the memory sink saw an unflagged output"

    def test_the_ledger_reaches_crewai_s_parallel_tool_pool(self):
        tool, output, _ = self._run(_Client(tool_rounds=1, per_round=2))
        assert len(tool.runs) == 2
        assert any(name != threading.main_thread().name for name in tool.runs)
        assert output.degraded is True

    def test_the_ledger_reaches_crewai_s_timeout_pool(self):
        """``_execute_with_timeout`` runs the turn in a ``copy_context`` pool.
        The harness no longer arms it, but a caller that sets the field on a
        built agent still does — and the ledger must survive it."""
        transport, tool = LLM(model="gpt-4o"), self._failing()
        harness, agent, tasks, crew = _crew(transport, tool)
        object.__setattr__(agent, "max_execution_time", 60)
        agent_class = crewai_symbols()["Agent"]

        with patch.object(
            agent_class,
            "_execute_with_timeout",
            autospec=True,
            side_effect=agent_class._execute_with_timeout,
        ) as pooled:
            _kickoff(harness, transport, _Client(tool_rounds=1), crew)

        assert pooled.called, "the turn did not run in CrewAI's timeout pool"
        assert len(tool.runs) == 1
        assert tasks[0].output.degraded is True

    def test_a_partly_working_tool_is_not_flagged(self):
        transport, tool = LLM(model="gpt-4o"), _Search()
        harness, _, tasks, crew = _crew(transport, tool)
        _kickoff(harness, transport, _Client(tool_rounds=2), crew)
        assert getattr(tasks[0].output, "degraded", None) is not True
        assert DEGRADED_MARKER not in tasks[0].output.raw

    def test_a_started_task_can_still_be_deep_copied(self):
        """What is kept on the task is the ledger, not a whole Context."""
        import copy

        from src.services.execution.harnesses.crewai.outcome import begin_task

        task = binding_for("crewai").build_task(description="d", expected_output="o")
        begin_task(task)
        copy.deepcopy(task)

    def test_each_task_starts_a_fresh_ledger(self):
        """Task one's dead source must not be held against task two."""
        transport = LLM(model="gpt-4o")
        tool = self._failing()
        harness, _, tasks, crew = _crew(transport, tool, tasks=2)
        client = _Client(tool_rounds=1)
        original = client._create

        def _second_task_needs_no_tools(**params):
            if len(client.requests) >= 2:
                client.requests.append(True)
                return _text("answered from context")
            return original(**params)

        client.chat.completions.create = _second_task_needs_no_tools
        _kickoff(harness, transport, client, crew)

        assert tasks[0].output.degraded is True
        assert getattr(tasks[1].output, "degraded", None) is not True
