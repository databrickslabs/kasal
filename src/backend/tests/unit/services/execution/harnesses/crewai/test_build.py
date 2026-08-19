"""Translating the kernel's kwargs into CrewAI's constructors.

The kernel builds ONE dict per agent/task/crew, harness-neutral by design. What
this file protects is that translating it is honest: everything CrewAI accepts
is passed, everything it cannot is reported, and nothing is lost quietly.
"""

import logging

import pytest
from pydantic import BaseModel, Field

from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses import bind, binding_for
from src.services.execution.harnesses.crewai import build as crew_build
from src.services.execution.harnesses.crewai.availability import crewai_symbols
from src.services.tools.base import BaseTool as KasalBaseTool


class _Args(BaseModel):
    q: str = Field(description="query")


class _Search(KasalBaseTool):
    name: str = "search"
    description: str = "Search"
    args_schema: type = _Args

    def _run(self, q: str) -> str:
        return f"results for {q}"


def _agent_kwargs(**overrides):
    """What ``kernel/agent_builder.build_agent_kwargs`` actually produces."""
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
        max_execution_time=900,
        inject_date=True,
        date_format="%Y-%m-%d",
        max_tokens=4096,
        respect_context_window=True,
    )
    kwargs.update(overrides)
    return kwargs


class TestAgent:
    def test_the_kernel_s_kwargs_produce_a_crewai_agent(self):
        agent = crew_build.build_agent(**_agent_kwargs())
        assert isinstance(agent, crewai_symbols()["Agent"])
        assert agent.role == "Researcher"

    def test_settings_crewai_shares_are_carried_not_dropped(self):
        agent = crew_build.build_agent(**_agent_kwargs())
        assert agent.max_iter == 25
        assert agent.max_rpm == 10
        assert agent.max_execution_time == 900
        assert agent.inject_date is True
        assert agent.date_format == "%Y-%m-%d"

    def test_the_llm_is_wrapped_onto_kasal_s_transport(self):
        inner = LLM(model="gpt-4o")
        agent = crew_build.build_agent(**_agent_kwargs(llm=inner))
        assert type(agent.llm).__name__ == "KasalBackedLLM"
        assert agent.llm.inner is inner

    def test_an_llm_already_in_crewai_s_shape_is_left_alone(self):
        from src.services.execution.harnesses.crewai.llm import build_kasal_backed_llm

        wrapped = build_kasal_backed_llm(LLM(model="gpt-4o"))
        agent = crew_build.build_agent(**_agent_kwargs(llm=wrapped))
        assert agent.llm is wrapped

    def test_a_model_name_string_is_left_alone(self):
        """Kernel fallback: configuration failed, so the name is passed through."""
        agent = crew_build.build_agent(**_agent_kwargs(llm="gpt-4o"))
        assert agent.llm is not None

    def test_tools_are_adapted(self):
        agent = crew_build.build_agent(**_agent_kwargs())
        from crewai.tools import BaseTool as CrewBaseTool

        assert all(isinstance(t, CrewBaseTool) for t in agent.tools)


class TestDropsAreReportedNotSilent:
    """A run that quietly ignores half its settings looks like one that worked."""

    def test_a_kasal_only_setting_is_dropped_with_its_reason(self):
        _, dropped = crew_build.translate(
            {"role": "R", "rpm_controller": object()},
            crewai_symbols()["Agent"],
            "agent 'R'",
        )
        assert dropped
        assert "rpm_controller" in dropped.summary()
        assert "builds its own from max_rpm" in dropped.summary()

    def test_the_per_agent_context_window_is_carried_not_dropped(self):
        """CrewAI has no such field, but it is not really CrewAI's business.

        ``transport._effective_context_window`` reads it off ``from_agent``, and
        CrewAI passes the agent through to every call — so carrying it keeps a
        per-agent window override working on both harnesses instead of silently
        falling back to the model's default.
        """
        agent = crew_build.build_agent(**_agent_kwargs(max_context_window_size=131072))
        assert getattr(agent, "max_context_window_size", None) == 131072

    def test_every_classified_drop_states_why(self):
        assert all(reason for reason in crew_build._KNOWN_DROPS.values())

    def test_an_unclassified_kwarg_is_dropped_loudly(self):
        """That means the kernel grew a setting this file has not caught up
        with — it must be noisy, not absorbed.

        Asserted against a handler attached to the crew logger rather than
        ``caplog``: LoggerManager sets ``propagate = False`` on these loggers,
        so the root handler pytest installs never sees the record.
        """
        records = []
        handler = logging.Handler()
        handler.emit = records.append
        crew_build.logger.addHandler(handler)
        try:
            translated, dropped = crew_build.translate(
                {"role": "R", "some_new_kernel_setting": 1},
                crewai_symbols()["Agent"],
                "agent 'R'",
            )
        finally:
            crew_build.logger.removeHandler(handler)

        assert "some_new_kernel_setting" not in translated
        assert "not classified" in dropped.summary()
        assert any(
            "_KNOWN_DROPS" in str(r.msg) and r.levelno >= logging.WARNING
            for r in records
        )

    def test_nothing_crewai_accepts_is_dropped(self):
        translated, dropped = crew_build.translate(
            {"role": "R", "goal": "G", "backstory": "B", "max_iter": 7},
            crewai_symbols()["Agent"],
            "agent",
        )
        assert translated["max_iter"] == 7
        assert not dropped


class TestCrew:
    def test_crew_memory_is_forced_off(self):
        """Kasal's memory is Databricks Vector Search + SQLite, group-scoped
        and keyed on a deterministic crew id. Letting CrewAI's unified memory
        initialise would fork tenant data across two stores — and import
        lancedb, which this harness is careful never to load."""
        agent = crew_build.build_agent(**_agent_kwargs())
        task = crew_build.build_task(description="d", expected_output="o", agent=agent)
        crew = crew_build.build_crew(agents=[agent], tasks=[task], memory=True)
        assert crew.memory is False

    def test_an_empty_crew_is_refused_by_crewai_but_not_by_kasal(self):
        """A real behavioural difference, recorded rather than papered over.

        CrewAI validates that a crew has agents and tasks at CONSTRUCTION; the
        Kasal runtime accepts an empty crew and produces an empty result. Any
        caller that builds a crew before it knows its tasks works on one harness
        and raises on the other, so it is worth knowing this is where it fails.
        """
        with pytest.raises(Exception, match="agents"):
            crew_build.build_crew(agents=[], tasks=[])

        binding_for("kasal").build_crew(agents=[], tasks=[])  # no raise

    def test_a_crew_builds_from_the_kernel_s_kwargs(self):
        agent = crew_build.build_agent(**_agent_kwargs())
        task = crew_build.build_task(
            description="Find the top customers",
            expected_output="A list",
            agent=agent,
        )
        crew = crew_build.build_crew(
            agents=[agent],
            tasks=[task],
            process=crewai_symbols()["Process"]("sequential"),
            verbose=True,
        )
        assert len(crew.tasks) == 1
        assert crew.agents[0].role == "Researcher"


class TestTheBindingSwapsWhatIsBuilt:
    """The point of the whole layer: same kwargs in, different runtime out."""

    def test_the_same_kwargs_build_each_engine_s_agent(self):
        kwargs = _agent_kwargs()
        with bind("crewai"):
            crew_agent = binding_for("crewai").build_agent(**kwargs)
        with bind("kasal"):
            kasal_agent = binding_for("kasal").build_agent(**kwargs)

        assert type(crew_agent).__module__.startswith("crewai")
        assert type(kasal_agent).__module__.startswith("src.services.execution.runtime")
        # Same intent, both ways round.
        assert crew_agent.role == kasal_agent.role == "Researcher"

    def test_crewai_claims_everything_except_export_and_the_plan_tool(self):
        """Capabilities keep the product honest, so they track what is real.

        Two absences, for different reasons:

        * EXPORT — the exported Databricks App vendors the KASAL runtime so it
          runs with no third-party agent framework at all; shipping CrewAI into
          exported apps is a separate project, not a gap in this one.
        * AGENT_PLAN — the plan tool is written for Kasal's executor, and
          CrewAI's agent executor plans its own way. Given both, one run called
          `todo` 28 times without writing a single item.

        Everything else — memory seams, checkpoint resume, flows, tool approval
        and replay — is implemented on both and therefore declared on both.
        """
        from src.services.execution.harnesses.binding import Capability

        crewai = binding_for("crewai")
        kasal = binding_for("kasal")

        assert kasal.capabilities() - crewai.capabilities() == {
            Capability.EXPORT,
            Capability.AGENT_PLAN,
        }
        assert not crewai.supports(Capability.EXPORT)
        assert not crewai.supports(Capability.AGENT_PLAN)
        assert kasal.supports(Capability.EXPORT)
        assert kasal.supports(Capability.AGENT_PLAN)
