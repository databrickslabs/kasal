"""Which tools an agent actually gets for a task.

A task's tool selection replaces the agent's — that is what picking tools per
task means. The exception is machinery the agent's PROMPT already refers to:
dropping that leaves the model instructed to call something it cannot see, which
is worse than never advertising it.

The runtime honours a generic flag rather than knowing what skills are, so
``runtime/`` keeps its rule of never depending on ``services/``.
"""

from types import SimpleNamespace
from unittest.mock import patch

from src.services.execution.runtime.agent import Agent


def _tool(name, always=False):
    tool = SimpleNamespace(name=name)
    if always:
        tool._kasal_always_available = True
    return tool


def _agent(tools):
    agent = Agent(role="r", goal="g", backstory="b")
    agent.tools = tools
    return agent


def _task(tools=None):
    return SimpleNamespace(
        tools=tools, prompt=lambda: "do the thing", description="d", name=None
    )


def _run_and_capture(agent, task):
    """Run execute_task far enough to see the tool list it chose."""
    seen = {}

    def _fake_run_agent(a, prompt, tools, task=None):
        seen["tools"] = [t.name for t in (tools or [])]
        return "ok"

    with (
        patch("src.services.execution.runtime.agent.run_agent", new=_fake_run_agent),
        patch("src.services.execution.runtime.agent.event_bus"),
    ):
        agent.execute_task(task)
    return seen["tools"]


class TestEffectiveTools:
    def test_a_task_selection_replaces_the_agents_ordinary_tools(self):
        agent = _agent([_tool("agent_only")])
        assert _run_and_capture(agent, _task([_tool("PerplexityTool")])) == [
            "PerplexityTool"
        ]

    def test_always_available_tools_survive_a_task_selection(self):
        """This is the bug that made an attached skill useless: the agent was
        told to call load_skill, and the task's own tool list hid it."""
        agent = _agent([_tool("load_skill", always=True), _tool("agent_only")])

        chosen = _run_and_capture(agent, _task([_tool("PerplexityTool")]))

        assert chosen == ["PerplexityTool", "load_skill"]
        assert "agent_only" not in chosen

    def test_with_no_task_tools_the_agent_keeps_everything(self):
        agent = _agent([_tool("load_skill", always=True), _tool("agent_only")])
        assert _run_and_capture(agent, _task(None)) == ["load_skill", "agent_only"]

    def test_an_explicit_tool_list_also_keeps_the_always_available_ones(self):
        """This is how the CREW calls it — ``task.execute_sync(agent, context,
        task.tools)`` — so the explicit list is the task's selection arriving by
        another route, not a deliberate exclusion. Treating it as one is what
        made an attached skill invisible to the model."""
        agent = _agent([_tool("load_skill", always=True)])
        seen = {}

        def _fake_run_agent(a, prompt, tools, task=None):
            seen["tools"] = [t.name for t in (tools or [])]
            return "ok"

        with (
            patch(
                "src.services.execution.runtime.agent.run_agent", new=_fake_run_agent
            ),
            patch("src.services.execution.runtime.agent.event_bus"),
        ):
            agent.execute_task(_task([_tool("PerplexityTool")]), tools=[_tool("only")])

        assert seen["tools"] == ["only", "load_skill"]

    def test_a_duplicate_is_not_added_twice(self):
        shared = _tool("load_skill", always=True)
        agent = _agent([shared])
        assert _run_and_capture(agent, _task([shared])) == ["load_skill"]


class TestTheCrewsActualCallShape:
    """The crew invokes ``task.execute_sync(agent, context, task.tools)``.

    That path was the live bug: the agent had load_skill attached and the LLM
    request carried only the task's own tool, because an explicit list looked
    like a deliberate exclusion.
    """

    def test_skill_tools_reach_the_model_on_the_crew_path(self):
        agent = _agent([_tool("load_skill", always=True)])
        task = _task([_tool("PerplexityTool")])
        seen = {}

        def _fake_run_agent(a, prompt, tools, task=None):
            seen["tools"] = [t.name for t in (tools or [])]
            return "ok"

        with (
            patch(
                "src.services.execution.runtime.agent.run_agent", new=_fake_run_agent
            ),
            patch("src.services.execution.runtime.agent.event_bus"),
        ):
            # Exactly how crew.py calls it.
            agent.execute_task(task, None, list(task.tools))

        assert seen["tools"] == ["PerplexityTool", "load_skill"]
