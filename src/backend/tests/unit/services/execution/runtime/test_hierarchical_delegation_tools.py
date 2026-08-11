"""A task's tools must survive the hierarchical delegation hop.

In the hierarchical process the manager agent holds ONLY the delegate/ask tools
and executes each task by delegating it to a coworker. A task's own tools
(regular tools and MCP tools alike — both are ``BaseTool`` instances that live in
``task.tools`` and are attached to no agent) used to be dropped at that hop, so a
crew whose search tool sat on the task could never actually search. These tests
pin the fix: the originating task's tools travel to whichever coworker the
manager delegates to, unioned with that coworker's own tools.
"""

from types import SimpleNamespace
from unittest.mock import patch

from src.services.execution.runtime.executor import (
    AskQuestionTool,
    DelegateWorkTool,
    _tools_for_delegate,
    delegation_tools,
)


def _tool(name):
    return SimpleNamespace(name=name)


def _agent(role, tools=None):
    return SimpleNamespace(role=role, tools=tools or [])


class TestToolsForDelegate:
    def test_task_tools_are_handed_to_a_toolless_coworker(self):
        task_tools = [_tool("PerplexityTool")]
        agent = _agent("Researcher")
        assert [t.name for t in _tools_for_delegate(task_tools, agent)] == [
            "PerplexityTool"
        ]

    def test_coworkers_own_tools_are_merged_in(self):
        task_tools = [_tool("PerplexityTool")]
        agent = _agent("Researcher", [_tool("agent_own")])
        assert [t.name for t in _tools_for_delegate(task_tools, agent)] == [
            "PerplexityTool",
            "agent_own",
        ]

    def test_duplicates_by_name_are_not_added_twice(self):
        task_tools = [_tool("PerplexityTool")]
        agent = _agent("Researcher", [_tool("PerplexityTool")])
        assert [t.name for t in _tools_for_delegate(task_tools, agent)] == [
            "PerplexityTool"
        ]

    def test_no_task_tools_falls_back_to_the_coworkers_own(self):
        agent = _agent("Researcher", [_tool("agent_own")])
        assert [t.name for t in _tools_for_delegate([], agent)] == ["agent_own"]


class TestDelegationToolsCarryTaskTools:
    def test_delegate_and_ask_receive_the_task_tools(self):
        task_tools = [_tool("PerplexityTool")]
        tools = delegation_tools([_agent("Researcher")], task_tools)
        delegate, ask = tools
        assert isinstance(delegate, DelegateWorkTool)
        assert isinstance(ask, AskQuestionTool)
        assert [t.name for t in delegate.task_tools] == ["PerplexityTool"]
        assert [t.name for t in ask.task_tools] == ["PerplexityTool"]

    def test_delegation_tools_still_work_with_no_task_tools(self):
        tools = delegation_tools([_agent("Researcher")])
        assert tools[0].task_tools == []
        assert tools[1].task_tools == []


class _FakeTask:
    """Stands in for ``runtime.task.Task``, which is a pydantic model that would
    reject the ``SimpleNamespace`` fakes at construction. Records what it was
    built and executed with."""

    last = None

    def __init__(self, description, expected_output, agent, tools=None):
        self.description = description
        self.agent = agent
        self.tools = tools or []
        _FakeTask.last = self

    def execute_sync(self, agent=None, context=None, tools=None):
        self.exec_tools = tools
        return SimpleNamespace(raw="done")


class TestDelegateWorkToolRunsWithTools:
    def test_delegated_task_is_executed_with_the_task_tools(self):
        agent = _agent("Researcher")
        tool = DelegateWorkTool(agents=[agent], task_tools=[_tool("PerplexityTool")])

        with patch("src.services.execution.runtime.task.Task", new=_FakeTask):
            out = tool._run(task="find X", context="ctx", coworker="Researcher")

        assert out == "done"
        built = _FakeTask.last
        # Passed to execute_sync...
        assert [t.name for t in built.exec_tools] == ["PerplexityTool"]
        # ...and set on the delegated Task itself, so the agent's ``task.tools``
        # fallback in Agent.execute_task sees them too.
        assert [t.name for t in built.tools] == ["PerplexityTool"]

    def test_ask_question_tool_also_carries_the_tools(self):
        agent = _agent("Researcher")
        tool = AskQuestionTool(agents=[agent], task_tools=[_tool("PerplexityTool")])

        with patch("src.services.execution.runtime.task.Task", new=_FakeTask):
            out = tool._run(question="what is X?", context="ctx", coworker="Researcher")

        assert out == "done"
        assert [t.name for t in _FakeTask.last.exec_tools] == ["PerplexityTool"]
