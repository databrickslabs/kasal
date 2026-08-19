"""What must be TRUE OF BOTH ENGINES.

Every other test file here checks one harness's binding. This one checks the
claim the whole layer exists to support: that switching the harness changes which
runtime executes and *nothing else a user can observe*.

Each test runs the same input through both bindings and compares. When one of
these fails, the two harnesses have diverged on something a run depends on — and
the failure names which thing, which is the entire point of writing them this
way rather than as two separate suites that happen to pass.

A behaviour a binding legitimately cannot support is not silently skipped here:
it is declared absent in ``capabilities()``, and ``TestCapabilitiesAreHonest``
checks the declaration against what the binding actually does.
"""

import pytest
from pydantic import BaseModel, Field

from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses import binding_for, reset_for_tests
from src.services.execution.harnesses.binding import Capability, HarnessName
from src.services.execution.runtime.identity import task_identity
from src.services.tools.base import BaseTool as KasalBaseTool

ENGINES = [HarnessName.KASAL.value, HarnessName.CREWAI.value]


class _Args(BaseModel):
    q: str = Field(description="query")


class _Search(KasalBaseTool):
    name: str = "search"
    description: str = "Search for things"
    args_schema: type = _Args

    def _run(self, q: str) -> str:
        return f"results for {q}"


@pytest.fixture(autouse=True)
def _clean():
    reset_for_tests()
    yield
    reset_for_tests()


@pytest.fixture(params=ENGINES)
def harness(request):
    """Each test body runs once per harness."""
    return binding_for(request.param)


def _agent_kwargs(**overrides):
    kwargs = dict(
        role="Researcher",
        goal="Find things",
        backstory="You research.",
        tools=[_Search()],
        llm=LLM(model="gpt-4o", temperature=0.2),
        verbose=True,
        allow_delegation=False,
        max_iter=25,
        max_rpm=10,
        inject_date=True,
        date_format="%Y-%m-%d",
    )
    kwargs.update(overrides)
    return kwargs


def _invoke_tool(harness, tool, **kwargs):
    """Call a tool the way ``harness`` calls it during a run.

    The two differ, and the difference is the reason this helper exists rather
    than ``harness.adapt_tools(...)[0].run(...)``:

    * the Kasal runtime wraps tools at call time in ``executor.wrap_tool``, so
      a bare ``tool.run()`` bypasses the whole hook pipeline;
    * the CrewAI adapter has no such outer layer, so it calls ``wrap_tool``
      from inside its own ``_run``.

    Either way the pipeline is the same pipeline. Testing through the harness's
    own invocation path is what makes "approval applies on both" a claim about
    real runs rather than about a helper.
    """
    adapted = harness.adapt_tools([tool])[0]
    if harness.name is HarnessName.KASAL:
        from src.services.execution.runtime.executor import wrap_tool

        return wrap_tool(adapted)(**kwargs)
    return adapted.run(**kwargs)


def _crew(harness, descriptions=("step one", "step two", "step three"), **crew_kwargs):
    agent = harness.build_agent(**_agent_kwargs())
    tasks = [
        harness.build_task(description=d, expected_output="o", agent=agent)
        for d in descriptions
    ]
    kwargs = dict(agents=[agent], tasks=tasks, process=harness.process("sequential"))
    kwargs.update(crew_kwargs)
    return harness.build_crew(**kwargs), tasks


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestBothEnginesBuildTheSameThing:
    def test_an_agent_keeps_the_settings_the_kernel_chose(self, harness):
        agent = harness.build_agent(**_agent_kwargs())
        assert agent.role == "Researcher"
        assert agent.goal == "Find things"
        assert agent.max_iter == 25
        assert agent.max_rpm == 10

    def test_an_agent_gets_its_tools(self, harness):
        agent = harness.build_agent(**_agent_kwargs())
        assert [t.name for t in agent.tools] == ["search"]

    def test_the_same_tool_implementation_runs_on_both(self, harness):
        """One tool, one Databricks query, one redaction rule — per harness
        would be two of each, free to drift."""
        agent = harness.build_agent(**_agent_kwargs())
        assert agent.tools[0].run(q="x") == "results for x"

    def test_a_crew_carries_its_tasks_and_agents(self, harness):
        crew, tasks = _crew(harness)
        assert len(crew.tasks) == len(tasks)
        assert crew.agents[0].role == "Researcher"

    def test_hierarchical_is_understood_by_both(self, harness):
        assert str(getattr(harness.process("hierarchical"), "value", "")) == (
            "hierarchical"
        )


# ---------------------------------------------------------------------------
# The LLM path — the reason a comparison between harnesses means anything
# ---------------------------------------------------------------------------


class TestTheModelIsCalledTheSameWay:
    def test_the_agent_s_llm_resolves_to_the_same_model(self, harness):
        inner = LLM(model="gpt-4o")
        agent = harness.build_agent(**_agent_kwargs(llm=inner))
        assert getattr(agent.llm, "model", None) == "gpt-4o"

    def test_both_engines_run_on_kasal_s_transport(self, harness):
        """Not "an equivalent LLM" — the same object, on both.

        If the two harnesses called models through different stacks, every
        difference in an answer could be the runtime or could be the provider
        layer, and "did switching change anything?" would be unanswerable.
        """
        inner = LLM(model="gpt-4o")
        agent = harness.build_agent(**_agent_kwargs(llm=inner))
        assert agent.llm is inner or getattr(agent.llm, "inner", None) is inner

    def test_the_context_window_is_the_transport_s_answer(self, harness):
        inner = LLM(model="gpt-4o")
        agent = harness.build_agent(**_agent_kwargs(llm=inner))
        llm = agent.llm
        if hasattr(llm, "get_context_window_size"):
            assert llm.get_context_window_size() == inner.get_context_window_size()

    def test_no_engine_adds_a_second_place_the_credential_can_leak(self, harness):
        """Execution logs are downloadable from the UI.

        Note what this does NOT claim: that ``repr(llm)`` is safe. The Kasal
        transport object prints its api_key, which is exactly why the kernel
        redacts it at every log site (``agent_builder.redact_llm_repr``). The
        invariant here is narrower and is the one a harness can break — that
        wrapping for a second runtime does not COPY the credential onto another
        object, giving it a second repr, a second serialization and a second
        way out.
        """
        inner = LLM(model="gpt-4o", api_key="dapi-SECRET")
        agent = harness.build_agent(**_agent_kwargs(llm=inner))
        if agent.llm is inner:
            return  # no wrapper: nothing new to leak from
        assert agent.llm.api_key != "dapi-SECRET"
        assert "SECRET" not in repr(agent.llm)
        assert "SECRET" not in str(agent.llm.model_dump())


# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------


class TestMemoryIsWiredTheSameWay:
    def test_the_backend_survives_construction(self, harness):
        """CrewAI's own `memory` flag is forced False so its chromadb/lancedb
        store never initialises; losing the Kasal backend with it would leave
        the crew silently memory-less."""
        backend = object()
        crew, _ = _crew(harness, memory=backend)
        assert harness.crew_memory(crew) is backend

    def test_recall_and_persistence_attach(self, harness):
        crew, _ = _crew(harness, memory=object())
        harness.wire_memory(crew, provider=lambda **k: "M", sink=lambda **k: None)
        assert len(crew.context_providers) == 1
        assert len(crew.output_sinks) == 1

    def test_wiring_nothing_is_not_an_error(self, harness):
        """A crew without memory configured wires neither, and must not raise."""
        crew, _ = _crew(harness)
        harness.wire_memory(crew, provider=None, sink=None)
        assert crew.context_providers == []


# ---------------------------------------------------------------------------
# Checkpoint resume — the same prefix, or a resume means different things
# ---------------------------------------------------------------------------


def _checkpoint_for(tasks, count=2):
    return {
        "version": 1,
        "task_count": len(tasks),
        "process": "sequential",
        "completed": [
            {
                "index": i,
                "task_key": task_identity(tasks[i]),
                "output_raw": f"{i} done",
                "agent": "Researcher",
            }
            for i in range(count)
        ],
    }


def _restored_count(harness, descriptions, checkpoint):
    """How many tasks this harness would restore, via its own loader."""
    crew, tasks = _crew(harness, descriptions)
    if harness.name is HarnessName.KASAL:
        return len(crew._load_checkpoint(checkpoint) or {})
    from src.services.execution.harnesses.crewai.checkpoint import restorable_outputs

    return len(restorable_outputs(tasks, checkpoint, sequential=True))


class TestResumeRestoresTheSamePrefix:
    """Restore the longest contiguous prefix whose identities still match.

    Both harnesses must agree, and for the same reason: a restored task's inputs
    have to be exactly the restored tasks before it, or the resume silently
    changes the context of work it did not redo.
    """

    BASE = ("step one", "step two", "step three")

    @pytest.mark.parametrize(
        "descriptions,expected",
        [
            (BASE, 2),
            (("step one", "step two CHANGED", "step three"), 1),
            (("step one CHANGED", "step two", "step three"), 0),
        ],
        ids=["unchanged", "second-task-edited", "first-task-edited"],
    )
    def test_the_prefix_matches(self, harness, descriptions, expected):
        """The checkpoint is built from THIS harness's own tasks.

        Deliberately not from one harness's tasks and replayed against the
        other: task identity is harness-dependent, because CrewAI's ``Task``
        inherits its agent's tools and Kasal's does not, so the hashes differ
        for what is otherwise the same task. That is not a defect to paper over
        here — it is why a run's harness is recorded and a resume reuses it (see
        ``execution/harness_choice.py``). What this asserts is that the RULE for
        choosing the prefix is the same rule on both.
        """
        reference, ref_tasks = _crew(harness, self.BASE)
        checkpoint = _checkpoint_for(ref_tasks, count=2)
        assert _restored_count(harness, descriptions, checkpoint) == expected

    def test_a_missing_checkpoint_runs_from_scratch_on_both(self, harness):
        assert _restored_count(harness, self.BASE, None) == 0

    def test_a_malformed_checkpoint_runs_from_scratch_rather_than_raising(
        self, harness
    ):
        """ "Start over" is always safe. A resume that half-works is not."""
        assert _restored_count(harness, self.BASE, {"completed": "nonsense"}) == 0


# ---------------------------------------------------------------------------
# The honesty check
# ---------------------------------------------------------------------------


class TestCapabilitiesAreHonest:
    """A declared capability has to be one the binding actually delivers.

    This is the check that keeps the enum from becoming decoration. It caught a
    real over-claim: TOOL_APPROVAL and TOOL_REPLAY were declared on the CrewAI
    binding while its tool adapter still called the tool directly, bypassing
    the hook pipeline they both depend on.
    """

    def test_kasal_is_the_reference_and_supports_everything(self):
        assert binding_for(HarnessName.KASAL.value).capabilities() == frozenset(
            Capability
        )

    def test_tool_policy_is_real_wherever_it_is_claimed(self, harness):
        if not harness.supports(Capability.TOOL_REPLAY):
            pytest.skip(f"{harness.name} does not claim tool replay")

        from src.services.execution.runtime import (
            ToolCallAnswered,
            register_tool_hooks,
            unregister_tool_hooks,
        )

        def _answer(t, kwargs, agent, task):
            return ToolCallAnswered(output="REPLAYED", source="test")

        register_tool_hooks(pre=_answer)
        try:
            assert _invoke_tool(harness, _Search(), q="x") == "REPLAYED"
        finally:
            unregister_tool_hooks(pre=_answer)

    def test_approval_can_block_a_call_wherever_it_is_claimed(self, harness):
        if not harness.supports(Capability.TOOL_APPROVAL):
            pytest.skip(f"{harness.name} does not claim tool approval")

        from src.services.execution.runtime import (
            ToolExecutionBlockedError,
            register_tool_hooks,
            unregister_tool_hooks,
        )

        def _deny(t, kwargs, agent, task):
            raise ToolExecutionBlockedError("denied")

        register_tool_hooks(pre=_deny)
        try:
            with pytest.raises(ToolExecutionBlockedError):
                _invoke_tool(harness, _Search(), q="x")
        finally:
            unregister_tool_hooks(pre=_deny)

    def test_memory_seams_exist_wherever_they_are_claimed(self, harness):
        if not harness.supports(Capability.CONTEXT_PROVIDERS):
            pytest.skip(f"{harness.name} does not claim context providers")
        crew, _ = _crew(harness, memory=object())
        harness.wire_memory(crew, provider=lambda **k: "M")
        assert crew.context_providers

    def test_both_harnesses_can_be_exported(self):
        """A bundle can be produced for either runtime.

        The Kasal bundle vendors the runtime and ships no third-party framework.
        The CrewAI bundle puts CrewAI's Agent/Task/Crew on that SAME vendored
        transport — not CrewAI's own LLM stack, which would be an app nobody
        tested. Export was Kasal-only until the CrewAI bundle existed.
        """
        assert binding_for(HarnessName.KASAL.value).supports(Capability.EXPORT)
        assert binding_for(HarnessName.CREWAI.value).supports(Capability.EXPORT)

    def test_every_engine_describes_itself_for_the_api(self, harness):
        described = harness.describe()
        assert described["name"] == harness.name.value
        assert described["available"] is True
        assert described["version"]
        assert set(described["capabilities"]) == {
            c.value for c in harness.capabilities()
        }
