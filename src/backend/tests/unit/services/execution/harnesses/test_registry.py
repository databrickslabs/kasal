"""The harness registry: what `active_harness()` hands back, and what it reports.

Two things matter here and neither is about the Kasal runtime:

* **A missing harness is REPORTED, not raised, by ``describe_harnesses``.** That
  endpoint exists to answer "why is CrewAI greyed out?", so it has to survive
  the very failure it is describing.
* **The Kasal binding is a pass-through.** It replaced direct
  ``Agent(**kwargs)`` calls at twenty call sites; if it does anything else, the
  refactor that introduced it changed behaviour while claiming not to.
"""

import pytest

from src.services.execution import harnesses
from src.services.execution.harnesses.binding import (
    Capability,
    HarnessName,
    HarnessUnavailableError,
)
from src.services.execution.harnesses.kasal import KasalBinding


@pytest.fixture(autouse=True)
def _clean_registry():
    harnesses.reset_for_tests()
    yield
    harnesses.reset_for_tests()


class TestRegistry:
    def test_active_engine_is_kasal_by_default(self):
        assert harnesses.active_harness().name is HarnessName.KASAL

    def test_bindings_are_cached_per_process(self):
        """Not merely an optimisation: ``import crewai`` costs seconds."""
        assert harnesses.binding_for("kasal") is harnesses.binding_for("kasal")

    def test_an_unknown_name_falls_back_to_the_default_engine(self):
        assert harnesses.binding_for("nonsense").name is HarnessName.KASAL

    def test_describe_reports_every_engine_including_unavailable_ones(
        self, monkeypatch
    ):
        def _explode(name):
            if name is HarnessName.CREWAI:
                raise HarnessUnavailableError("crewai is not installed")
            return KasalBinding()

        monkeypatch.setattr(harnesses, "_construct", _explode)
        described = {row["name"]: row for row in harnesses.describe_harnesses()}

        assert described["kasal"]["available"] is True
        assert described["crewai"]["available"] is False
        # The REASON is the whole point — a disabled option with no explanation
        # is indistinguishable from a bug.
        assert "not installed" in described["crewai"]["unavailable_reason"]

    def test_describe_survives_an_engine_that_fails_in_an_unexpected_way(
        self, monkeypatch
    ):
        def _explode(name):
            if name is HarnessName.CREWAI:
                raise ImportError("No module named 'crewai'")
            return KasalBinding()

        monkeypatch.setattr(harnesses, "_construct", _explode)
        described = {row["name"]: row for row in harnesses.describe_harnesses()}
        assert described["crewai"]["available"] is False


class TestKasalBindingIsAPassThrough:
    @pytest.fixture
    def binding(self):
        return KasalBinding()

    def test_builds_the_runtime_agent(self, binding):
        from src.services.execution.runtime import Agent

        agent = binding.build_agent(role="R", goal="G", backstory="B")
        assert isinstance(agent, Agent)
        assert agent.role == "R"

    def test_builds_the_runtime_task(self, binding):
        from src.services.execution.runtime import Task

        task = binding.build_task(description="d", expected_output="o")
        assert isinstance(task, Task)

    def test_builds_the_runtime_crew(self, binding):
        from src.services.execution.runtime import Crew

        assert isinstance(binding.build_crew(), Crew)

    @pytest.mark.parametrize("name", ["sequential", "hierarchical", "HIERARCHICAL "])
    def test_process_accepts_the_names_the_config_builders_use(self, binding, name):
        from src.services.execution.runtime import Process

        assert binding.process(name) in (Process.sequential, Process.hierarchical)

    def test_adapt_tools_is_the_identity(self, binding):
        """The tool factory already produces this runtime's tools."""
        tools = [object(), object()]
        assert binding.adapt_tools(tools) == tools
        assert binding.adapt_tools(None) == []

    def test_supports_everything(self, binding):
        """Kasal is the reference the other harness is measured against."""
        assert binding.capabilities() == frozenset(Capability)
        assert all(binding.supports(c) for c in Capability)

    def test_event_bridge_is_a_no_op_context(self, binding):
        """The runtime already publishes on the bus; nothing to translate."""
        with binding.event_bridge():
            pass

    def test_describe_names_itself_and_claims_availability(self, binding):
        described = binding.describe()
        assert described["name"] == "kasal"
        assert described["available"] is True
        assert "checkpoint_resume" in described["capabilities"]
