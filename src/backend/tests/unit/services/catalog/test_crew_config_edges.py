"""A rebuilt crew runs each task on the agent the canvas connects it to.

The canvas assigns agents with agent->task edges, and that is what a UI run
uses. The task row's ``agent_id`` is a second copy that several writers used to
blank (the canvas task editor, the Tools chip, the catalog clone), and when it
is blank CrewPreparation silently puts the task on the crew's FIRST agent. So
every server-side projection — a crew published over MCP/A2A, a triggered run,
a resume — reads the edges first and falls back to ``agent_id`` only when a
task has no edge.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.catalog.crew_config import (
    build_crew_execution_config,
    build_crew_execution_config_by_id,
)
from src.services.external.invocation import _crew_to_yaml
from src.utils.user_context import GroupContext

CREW_ID = str(uuid.UUID(int=7))
# The projection reads only the caller's workspace; these rows are its.
CALLER = GroupContext(group_ids=["ws-a"], group_email="a@example.com")


def agent(agent_id, name):
    return SimpleNamespace(
        id=agent_id, name=name, role=name.lower(), goal="g", backstory="b", tools=[]
    )


def task(task_id, name, agent_id=None):
    return SimpleNamespace(
        id=task_id,
        name=name,
        description=f"{name} it",
        expected_output="done",
        agent_id=agent_id,
        tools=[],
        tool_configs=None,
        context=[],
    )


def node(kind, row_id, node_id=None):
    data_key = "agentId" if kind == "agent" else "taskId"
    return {
        "id": node_id or f"{kind}-{row_id}",
        "type": f"{kind}Node",
        "data": {"label": row_id, data_key: row_id},
    }


def edge(source, target):
    return {"id": f"{source}->{target}", "source": source, "target": target}


AGENTS = {"a1": agent("a1", "Researcher"), "a2": agent("a2", "Writer")}


def two_agent_crew(tasks_agent_ids=(None, None), edges=None):
    """Researcher -> research, Writer -> write, both task rows unassigned."""
    tasks = {
        "t1": task("t1", "research", tasks_agent_ids[0]),
        "t2": task("t2", "write", tasks_agent_ids[1]),
    }
    crew_row = SimpleNamespace(
        id=CREW_ID,
        agent_ids=["a1", "a2"],
        task_ids=["t1", "t2"],
        nodes=[node("agent", "a1"), node("agent", "a2")]
        + [node("task", "t1"), node("task", "t2")],
        edges=(
            edges
            if edges is not None
            else [edge("agent-a1", "task-t1"), edge("agent-a2", "task-t2")]
        ),
    )
    return crew_row, tasks


def patch_catalog(agents, tasks):
    def service(rows):
        svc = AsyncMock()
        svc.get = AsyncMock(side_effect=lambda i: rows.get(str(i)))
        svc.get_with_group_check = AsyncMock(side_effect=lambda i, _c: rows.get(str(i)))
        return svc

    return (
        patch("src.services.catalog.agents.AgentService", return_value=service(agents)),
        patch("src.services.catalog.tasks.TaskService", return_value=service(tasks)),
    )


async def project(crew_row, tasks, agents=AGENTS):
    agent_p, task_p = patch_catalog(agents, tasks)
    with agent_p, task_p:
        return await build_crew_execution_config(None, crew_row, CALLER)


def assignments(tasks_yaml):
    return {key: entry.get("agent") for key, entry in tasks_yaml.items()}


class TestEdgesAssignAgents:
    @pytest.mark.asyncio
    async def test_unassigned_rows_take_their_agent_from_the_edges(self):
        crew_row, tasks = two_agent_crew()
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_an_edge_wins_over_a_stale_agent_id(self):
        crew_row, tasks = two_agent_crew(tasks_agent_ids=("a1", "a1"))
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_agent_id_is_used_when_a_task_has_no_edge(self):
        crew_row, tasks = two_agent_crew(
            tasks_agent_ids=(None, "a2"), edges=[edge("agent-a1", "task-t1")]
        )
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_an_edge_to_an_agent_outside_the_crew_falls_back_to_agent_id(self):
        crew_row, tasks = two_agent_crew(
            tasks_agent_ids=("a1", "a2"),
            edges=[edge("agent-gone", "task-t2")],
        )
        crew_row.nodes.append(node("agent", "gone"))
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml)["write"] == "Writer"

    @pytest.mark.asyncio
    async def test_node_data_ids_win_over_the_node_id(self):
        """A node's id is a canvas handle; ``data.agentId``/``data.taskId`` name
        the rows — the same order CrewService uses to build agent_ids/task_ids."""
        crew_row, tasks = two_agent_crew(edges=[edge("n-1", "n-3"), edge("n-2", "n-4")])
        crew_row.nodes = [
            node("agent", "a1", node_id="n-1"),
            node("agent", "a2", node_id="n-2"),
            node("task", "t1", node_id="n-3"),
            node("task", "t2", node_id="n-4"),
        ]
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_a_node_without_data_ids_is_read_from_its_prefixed_id(self):
        crew_row, tasks = two_agent_crew()
        for n in crew_row.nodes:
            n["data"] = {"label": n["id"]}
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_task_to_task_edges_do_not_assign_agents(self):
        crew_row, tasks = two_agent_crew(
            tasks_agent_ids=("a1", "a2"), edges=[edge("task-t1", "task-t2")]
        )
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_with_two_agent_edges_the_last_one_wins_as_on_the_canvas(self):
        """A canvas run (JobExecutionService) assigns agents edge by edge, so
        the last agent edge into a task is the one that runs it."""
        crew_row, tasks = two_agent_crew(
            edges=[
                edge("agent-a1", "task-t1"),
                edge("agent-a1", "task-t2"),
                edge("agent-a2", "task-t2"),
            ]
        )
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_a_crew_without_nodes_or_edges_still_projects(self):
        crew_row, tasks = two_agent_crew(tasks_agent_ids=("a1", "a2"))
        crew_row.nodes = None
        crew_row.edges = None
        _, tasks_yaml = await project(crew_row, tasks)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}


class TestEveryServerSideProjection:
    @pytest.mark.asyncio
    async def test_a_published_crew_runs_each_task_on_its_own_agent(self):
        """MCP tools and A2A messages reach the crew through ``_crew_to_yaml``."""
        crew_row, tasks = two_agent_crew()
        agent_p, task_p = patch_catalog(AGENTS, tasks)
        with agent_p, task_p:
            _, tasks_yaml = await _crew_to_yaml(crew_row, None, CALLER)
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}

    @pytest.mark.asyncio
    async def test_a_triggered_or_resumed_crew_runs_each_task_on_its_own_agent(self):
        """The trigger queue and resume load the crew by id."""
        crew_row, tasks = two_agent_crew()
        crews = AsyncMock()
        crews.get_by_group = AsyncMock(return_value=crew_row)
        agent_p, task_p = patch_catalog(AGENTS, tasks)
        with (
            agent_p,
            task_p,
            patch("src.services.catalog.crews.CrewService", return_value=crews),
        ):
            projected = await build_crew_execution_config_by_id(None, CREW_ID, CALLER)
        _, tasks_yaml = projected
        assert assignments(tasks_yaml) == {"research": "Researcher", "write": "Writer"}
