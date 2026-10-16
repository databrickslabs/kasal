"""Two workspaces' saved crews in a real SQLite database.

Shared by the tests of the server-side crew projection (``catalog/crew_config``)
and of the trigger queue that starts a crew through it. Workspace A owns a
two-agent crew whose task rows carry a STALE or empty ``agent_id`` (so only the
canvas edges assign the right agent) and whose agents carry LLM overrides on the
row. Workspace B owns a crew whose name, tool configs and agent must never reach
a run started from A.
"""

import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.agent import Agent
from src.models.crew import Crew
from src.models.task import Task
from src.utils.user_context import GroupContext

A, B = "ws-a", "ws-b"
A_CREW = uuid.UUID("aaaaaaaa-cccc-4000-8000-00000000000a")
B_CREW = uuid.UUID("bbbbbbbb-cccc-4000-8000-00000000000b")
A_RESEARCHER = "aaaaaaaa-0000-4000-8000-000000000001"
A_WRITER = "aaaaaaaa-0000-4000-8000-000000000002"
A_RESEARCH = "aaaaaaaa-1111-4000-8000-000000000001"
A_WRITE = "aaaaaaaa-1111-4000-8000-000000000002"
B_AGENT = "bbbbbbbb-0000-4000-8000-000000000001"
B_TASK = "bbbbbbbb-1111-4000-8000-000000000001"
A_CONFIGS = {"SerperDevTool": {"serper_api_key": "a-own-key"}}
B_CONFIGS = {"SerperDevTool": {"serper_api_key": "b-secret-key"}}
B_CREW_NAME = "Bravo secret crew"

CALLER_A = GroupContext(group_ids=[A], group_email="a@example.com")


def _canvas(agent_to_task):
    """Nodes and agent->task edges for ``[(agent id, task id), ...]``."""
    nodes, edges = [], []
    for agent_id, task_id in agent_to_task:
        nodes.append({"id": f"agent-{agent_id}", "type": "agentNode", "data": {}})
        nodes.append({"id": f"task-{task_id}", "type": "taskNode", "data": {}})
        edges.append(
            {"id": task_id, "source": f"agent-{agent_id}", "target": f"task-{task_id}"}
        )
    return nodes, edges


def _rows():
    a_nodes, a_edges = _canvas([(A_RESEARCHER, A_RESEARCH), (A_WRITER, A_WRITE)])
    b_nodes, b_edges = _canvas([(B_AGENT, B_TASK)])
    common = {"goal": "g", "backstory": "b", "tools": [], "llm": "test-model"}
    return [
        Agent(
            id=A_RESEARCHER,
            name="Researcher",
            role="researcher",
            group_id=A,
            tool_configs=A_CONFIGS,
            temperature=30,
            reasoning_effort="high",
            max_tokens=7000,
            max_iter=7,
            **common,
        ),
        Agent(
            id=A_WRITER,
            name="Writer",
            role="writer",
            group_id=A,
            temperature=80,
            thinking_budget_tokens=2048,
            **common,
        ),
        Agent(
            id=B_AGENT,
            name="Bravo agent",
            role="bravo agent",
            group_id=B,
            tool_configs=B_CONFIGS,
            temperature=99,
            **common,
        ),
        # The research row names the WRITER (stale) and the write row nobody:
        # only the edges put each task on its own agent.
        Task(
            id=A_RESEARCH,
            name="research",
            description="research it",
            expected_output="notes",
            agent_id=A_WRITER,
            group_id=A,
            tools=[],
        ),
        Task(
            id=A_WRITE,
            name="write",
            description="write it",
            expected_output="draft",
            agent_id=None,
            group_id=A,
            tools=[],
        ),
        Task(
            id=B_TASK,
            name="bravo task",
            description="b description",
            expected_output="o",
            agent_id=B_AGENT,
            group_id=B,
            tools=[],
            tool_configs=B_CONFIGS,
        ),
        Crew(
            id=A_CREW,
            name="Alpha crew",
            agent_ids=[A_RESEARCHER, A_WRITER],
            task_ids=[A_RESEARCH, A_WRITE],
            nodes=a_nodes,
            edges=a_edges,
            group_id=A,
        ),
        Crew(
            id=B_CREW,
            name=B_CREW_NAME,
            agent_ids=[B_AGENT],
            task_ids=[B_TASK],
            nodes=b_nodes,
            edges=b_edges,
            group_id=B,
        ),
    ]


@asynccontextmanager
async def seeded_database() -> AsyncIterator[async_sessionmaker]:
    """A session maker over an in-memory database holding both workspaces."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in (Agent, Task, Crew):
            await conn.run_sync(model.__table__.create)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        session.add_all(_rows())
        await session.commit()
    try:
        yield maker
    finally:
        await engine.dispose()


def routed_to(maker):
    """A stand-in for ``routed_scoped_session`` that opens a session on ``maker``."""

    @asynccontextmanager
    async def routed():
        async with maker() as session:
            yield session

    return routed
