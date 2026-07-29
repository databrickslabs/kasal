"""The exported app runs a crew on Kasal's runtime — proven by running one.

Every other assertion about this migration is a string match on rendered text.
These tests render the bundle, import it, build the crew and kick it off, so
"the exported app uses Kasal's engine" is a fact the suite establishes rather
than a claim it repeats.
"""

import importlib

import pytest

VENDOR_PKG = "agent_server.kasal_runtime"


def _import(name):
    return importlib.import_module(f"agent_server.{name}")


class TestTheAppImports:
    @pytest.mark.parametrize(
        "module",
        [
            "tool_adapter",
            "crew_progress",
            "conversation",
            "databricks_llm",
            "databricks_responses_llm",
            "agent",
        ],
    )
    def test_module_imports_from_a_rendered_bundle(self, app_bundle, module):
        """Import is the only check that catches a name the template renders but
        cannot resolve — every string assertion passes on a broken import."""
        assert _import(module) is not None

    def test_no_app_module_imports_crewai(self, app_bundle):
        """crewai is not installed in this venv, so a leftover top-level import
        would already have raised above. This pins the intent explicitly."""
        for path in sorted((app_bundle / "agent_server").rglob("*.py")):
            if "kasal_runtime" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith(("import crewai", "from crewai")):
                    # crewai_tools is Phase 5; crewai itself must be gone.
                    assert stripped.startswith(
                        "from crewai_tools"
                    ), f"{path.name}: {stripped}"


class TestTheCrewIsKasals:
    def test_build_crew_returns_runtime_objects(self, app_bundle, fake_llm):
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm

        crew = agent_mod.build_crew()
        assert type(crew).__module__.startswith(VENDOR_PKG)
        assert type(crew.agents[0]).__module__.startswith(VENDOR_PKG)
        assert type(crew.tasks[0]).__module__.startswith(VENDOR_PKG)
        assert len(crew.agents) == 1 and len(crew.tasks) == 1

    def test_the_crew_actually_runs(self, app_bundle, fake_llm):
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm

        output = agent_mod.build_crew().kickoff()
        assert "Hello from the Kasal runtime." in str(output)
        assert fake_llm.calls, "the runtime never reached the LLM"

    def test_config_yaml_still_drives_the_crew(self, app_bundle, fake_llm):
        """The agents.yaml/tasks.yaml contract must survive the engine swap —
        that is what lets an existing export be re-rendered without re-planning."""
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm

        crew = agent_mod.build_crew()
        assert crew.agents[0].role == "Researcher"
        assert crew.tasks[0].expected_output == "a greeting"

    def test_hierarchical_process_maps_across(self, app_bundle, fake_llm):
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm
        runtime = importlib.import_module(f"{VENDOR_PKG}.services.execution.runtime")

        assert agent_mod.build_crew().process is runtime.Process.sequential


class TestGuardrail:
    def test_a_planned_guardrail_becomes_a_runtime_guardrail(
        self, app_bundle, fake_llm
    ):
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm
        runtime = importlib.import_module(f"{VENDOR_PKG}.services.execution.runtime")

        guardrail = agent_mod._make_task_guardrail({"guardrail": "Must cite sources."})
        assert isinstance(guardrail, runtime.LLMGuardrail)
        assert guardrail.description == "Must cite sources."

    def test_the_dict_shape_is_tolerated(self, app_bundle, fake_llm):
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm
        guardrail = agent_mod._make_task_guardrail(
            {"guardrail": {"description": "Must cite sources."}}
        )
        assert guardrail is not None

    def test_no_guardrail_in_the_plan_means_no_guardrail(self, app_bundle, fake_llm):
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm
        assert agent_mod._make_task_guardrail({}) is None
        assert agent_mod._make_task_guardrail({"guardrail": "  "}) is None


class TestToolAdapter:
    """``runtime.Agent.tools`` is typed ``list[BaseTool]``; a crewai_tools
    built-in or an MCP tool reaches it as a ValidationError unless adapted."""

    def test_a_foreign_tool_becomes_usable_by_a_runtime_agent(self, app_bundle):
        adapter = _import("tool_adapter")
        runtime = importlib.import_module(f"{VENDOR_PKG}.services.execution.runtime")

        class ForeignTool:  # shaped like a crewai_tools built-in
            name = "search"
            description = "Search the web."

            def _run(self, query: str) -> str:
                return f"results for {query}"

        adapted = adapter.as_kasal_tool(ForeignTool())
        agent = runtime.Agent(
            role="R", goal="g", backstory="b", llm=None, tools=[adapted]
        )
        assert agent.tools[0].name == "search"
        assert (
            adapted.run(query="kasal") == "results for kasal"
            or adapted._run(query="kasal") == "results for kasal"
        )

    def test_an_unadapted_foreign_tool_is_rejected_by_the_runtime(self, app_bundle):
        """Proves the adapter is load-bearing, not decorative."""
        runtime = importlib.import_module(f"{VENDOR_PKG}.services.execution.runtime")

        class ForeignTool:
            name = "search"
            description = "Search the web."

        with pytest.raises(Exception):
            runtime.Agent(
                role="R", goal="g", backstory="b", llm=None, tools=[ForeignTool()]
            )

    def test_a_native_kasal_tool_passes_through_unwrapped(self, app_bundle):
        adapter = _import("tool_adapter")
        base = importlib.import_module(f"{VENDOR_PKG}.services.tools.base")

        class NativeTool(base.BaseTool):
            name: str = "native"
            description: str = "A real Kasal tool."

            def _run(self, **kwargs):
                return "ok"

        tool = NativeTool()
        assert adapter.as_kasal_tool(tool) is tool

    def test_the_foreign_tools_argument_schema_is_preserved(self, app_bundle):
        """Re-deriving a schema from the ``*args, **kwargs`` wrapper would
        advertise a tool that accepts anything and validates nothing."""
        from pydantic import BaseModel

        adapter = _import("tool_adapter")

        class Args(BaseModel):
            query: str

        class ForeignTool:
            name = "search"
            description = "Search."
            args_schema = Args

            def _run(self, query):
                return query

        assert adapter.as_kasal_tool(ForeignTool()).args_schema is Args

    def test_an_unusable_tool_is_skipped_not_fatal(self, app_bundle):
        """One broken tool must not stop the crew starting."""
        adapter = _import("tool_adapter")

        class NotATool:
            name = "broken"
            description = "no callable body"

        assert adapter.as_kasal_tools([NotATool()]) == []

    def test_wrapped_tools_are_reported(self, app_bundle):
        """An adapter nobody can see the edges of becomes permanent."""
        adapter = _import("tool_adapter")

        class ForeignTool:
            name = "serper"
            description = "Search."

            def _run(self):
                return "x"

        adapter.as_kasal_tool(ForeignTool())
        assert "serper" in adapter.wrapped_tool_names()


class TestProgressListener:
    def test_runtime_events_drive_the_progress_feed(self, app_bundle):
        """crew_progress moved from CrewAI's bus to the vendored one; the whole
        point is that it still reports, so emit real events and read it back."""
        crew_progress = _import("crew_progress")
        progress = _import("progress")
        events = importlib.import_module(f"{VENDOR_PKG}.core.events")

        cid = "conv-1"
        progress.set_current(cid)
        crew_progress.install()
        try:
            events.event_bus.emit(
                None, events.CrewKickoffStartedEvent(crew_name="c", inputs=None)
            )
            assert "Starting" in progress.get(cid)["status"]

            events.event_bus.emit(
                None, events.TaskStartedEvent(task_name="Research the topic")
            )
            assert "Research the topic" in progress.get(cid)["status"]

            events.event_bus.emit(
                None, events.ToolUsageStartedEvent(tool_name="serper", tool_args={})
            )
            assert "serper" in progress.get(cid)["status"]

            events.event_bus.emit(
                None,
                events.AgentExecutionStartedEvent(
                    agent=None, task=None, task_prompt="p", agent_role="Researcher"
                ),
            )
            assert "Researcher is thinking" in progress.get(cid)["status"]
        finally:
            progress.clear_current()
            progress.clear(cid)

    def test_install_is_idempotent(self, app_bundle):
        crew_progress = _import("crew_progress")
        assert crew_progress.install() is crew_progress.install()


class TestBundledTools:
    def test_bundled_tools_do_not_ship_a_dangling_src_import(self, app_bundle):
        """``perplexity_tool.py`` used to ship ``from src.services.tools.base
        import BaseTool`` verbatim — a module that does not exist in a
        standalone app, so the tool raised ImportError on first use."""
        for path in sorted((app_bundle / "tools").glob("*.py")):
            for line in path.read_text(encoding="utf-8").splitlines():
                assert not line.strip().startswith(
                    ("from src.", "import src.")
                ), f"{path.name}: {line.strip()}"
