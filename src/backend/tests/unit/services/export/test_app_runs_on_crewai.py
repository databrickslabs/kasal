"""The CrewAI bundle: CrewAI's Agent/Task/Crew over Kasal's vendored transport.

A spin-off team may prefer a mainstream framework at the top of the stack — one
they can hire for, upgrade and patch — rather than a copy of Kasal's runtime that
has no upstream. That is what this bundle is for.

What it is NOT is a fresh CrewAI project. The transport stays Kasal's, which is
the same arrangement the platform runs: if the exported app called models through
CrewAI's own LLM stack it would be an app nobody tested, missing the endpoint
fixes Kasal's handlers carry (stripping ``cache_breakpoint``, an empty ``name``,
``strict``) — and the first Databricks 400 would land in a customer's workspace.
"""

import importlib

import pytest

crewai = pytest.importorskip("crewai", reason="a CrewAI bundle needs crewai installed")


def _import(module: str):
    return importlib.import_module(f"agent_server.{module}")


class TestTheBundleIsCrewAIs:
    def test_the_runtime_is_stamped_at_export_time(self, crewai_app_bundle):
        assert _import("runtime_binding").RUNTIME == "crewai"

    def test_the_classes_are_crewais(self, crewai_app_bundle):
        binding = _import("runtime_binding")
        assert issubclass(binding.Agent, crewai.Agent)
        assert issubclass(binding.Task, crewai.Task)
        assert issubclass(binding.Crew, crewai.Crew)
        assert binding.Process is crewai.Process

    def test_agent_py_constructs_through_the_seam(self, crewai_app_bundle):
        """One import line is the whole seam — the file reads identically for
        either runtime."""
        agent_py = (crewai_app_bundle / "agent_server" / "agent.py").read_text()
        assert "from agent_server.runtime_binding import Agent, Crew, Process, Task" in agent_py


class TestTheAdaptersAreShipped:
    def test_the_harness_adapters_are_vendored(self, crewai_app_bundle):
        vendored = crewai_app_bundle / "agent_server" / "kasal_runtime" / "services" / "execution" / "harnesses"
        assert (vendored / "crewai" / "llm.py").is_file()
        assert (vendored / "crewai" / "tools.py").is_file()
        assert (vendored / "crewai" / "build.py").is_file()

    def test_the_logger_shim_lets_them_vendor_verbatim(self, crewai_app_bundle):
        """The adapters log through LoggerManager upstream. Rather than editing
        them — an edited copy is one that drifts — the bundle ships a shim with
        the same call shape over stdlib logging."""
        shim = crewai_app_bundle / "agent_server" / "kasal_runtime" / "core" / "logger.py"
        assert shim.is_file()
        assert "class LoggerManager" in shim.read_text()

    def test_crewai_is_pinned_exactly(self, crewai_app_bundle):
        """This bundle ships no uv.lock, and a floating crewai+litellm pair is
        what broke exports before — resolving to a mismatch that failed at the
        first LLM call, in the customer's workspace."""
        pyproject = (crewai_app_bundle / "pyproject.toml").read_text()
        assert 'crewai==' in pyproject


class TestItActuallyBuilds:
    def test_an_agent_is_built_from_the_apps_own_kwargs(self, crewai_app_bundle, fake_llm):
        """The kwargs agent.py assembles are Kasal's vocabulary. CrewAI rejects
        some of them, so the binding translates rather than passing them
        through — this is the check that the translation is wired."""
        binding = _import("runtime_binding")
        agent = binding.Agent(
            role="Researcher",
            goal="Find things",
            backstory="Thorough.",
            llm=fake_llm,
            tools=[],
            verbose=True,
            allow_delegation=False,
            max_iter=25,
            # Kasal concepts CrewAI's Agent does not declare — dropped, not raised on.
            inject_date=True,
            date_format="%Y-%m-%d",
        )
        assert isinstance(agent, crewai.Agent)
        assert agent.role == "Researcher"

    def test_the_transport_is_presented_to_crewai_as_an_llm(self, crewai_app_bundle, fake_llm):
        """Kasal's transport object is not a CrewAI LLM; the binding wraps it.
        Without this the agent would carry an object CrewAI cannot call."""
        from crewai.llms.base_llm import BaseLLM

        binding = _import("runtime_binding")
        agent = binding.Agent(role="R", goal="g", backstory="b", llm=fake_llm, tools=[])
        assert isinstance(agent.llm, BaseLLM)

    def test_a_crew_is_built_with_memory_off(self, crewai_app_bundle, fake_llm):
        """CrewAI's own memory would start chromadb inside the app, and Kasal's
        memory subsystem is not part of a bundle — so it is off whatever the
        crew was configured with, exactly as the platform's binding does."""
        binding = _import("runtime_binding")
        agent = binding.Agent(role="R", goal="g", backstory="b", llm=fake_llm, tools=[])
        task = binding.Task(description="d", expected_output="e", agent=agent, tools=[])
        crew = binding.Crew(
            name="Demo",
            agents=[agent],
            tasks=[task],
            process=binding.Process.sequential,
            memory=True,
            verbose=True,
        )
        assert isinstance(crew, crewai.Crew)
        assert crew.memory is False
