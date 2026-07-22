"""Tests for _dedupe_flow_agent_task_tools — the flow-path agent∩task fix."""
from types import SimpleNamespace

from src.engines.crewai.paths.flow.modules.flow_methods import (
    _dedupe_flow_agent_task_tools,
)


def _tool(name):
    return SimpleNamespace(name=name)


def _agent(role, tools):
    return SimpleNamespace(role=role, tools=list(tools))


def _task(agent, tools):
    return SimpleNamespace(agent=agent, tools=list(tools))


def test_removes_duplicate_tool_from_agent():
    t90 = _tool("Pipeline Config Generator")
    agent = _agent("Specialist", [t90])
    task = _task(agent, [t90])
    _dedupe_flow_agent_task_tools([agent], [task])
    assert agent.tools == []          # agent copy removed
    assert task.tools == [t90]        # task copy kept


def test_keeps_agent_only_tool():
    search = _tool("SearchTool")
    agent = _agent("R", [search])
    task = _task(agent, [])           # task has no tools
    _dedupe_flow_agent_task_tools([agent], [task])
    assert agent.tools == [search]    # untouched


def test_partial_overlap():
    dup = _tool("Dup")
    extra = _tool("Extra")
    agent = _agent("R", [dup, extra])
    task = _task(agent, [dup])
    _dedupe_flow_agent_task_tools([agent], [task])
    assert [t.name for t in agent.tools] == ["Extra"]


def test_two_agents_isolated():
    """A duplicate on agent2's task must not strip agent1's same-named tool."""
    a1_tool = _tool("X")
    a2_tool = _tool("X")            # same NAME, different instance
    agent1 = _agent("R1", [a1_tool])
    agent2 = _agent("R2", [a2_tool])
    task2 = _task(agent2, [a2_tool])
    _dedupe_flow_agent_task_tools([agent1, agent2], [task2])
    assert agent1.tools == [a1_tool]  # agent1 keeps its tool (its task isn't here)
    assert agent2.tools == []         # agent2's duplicate removed


def test_no_agent_on_task_is_safe():
    task = _task(None, [_tool("X")])
    _dedupe_flow_agent_task_tools([], [task])  # must not raise


def test_never_raises_on_bad_objects():
    # task.agent lacks .tools etc. — helper is best-effort, must swallow.
    weird = SimpleNamespace(agent=SimpleNamespace())
    _dedupe_flow_agent_task_tools([], [weird])


def test_inheriting_task_preserves_agent_tool_flow():
    """Flow: agent with a tool-less task is skipped (that task inherits tools)."""
    dup = _tool("PCG")
    agent = _agent("R", [dup])
    task_with = _task(agent, [dup])
    task_inherits = _task(agent, [])   # relies on inheriting agent.tools
    _dedupe_flow_agent_task_tools([agent], [task_with, task_inherits])
    assert agent.tools == [dup]        # untouched — inheriting task needs it
