"""A crew run fills its agent and task specs only from rows in its own workspace.

``prepare_and_run_crew`` completes each agents_yaml / tasks_yaml entry from the
stored row: tool_configs when the payload has no key for them, and the agent's
LLM overrides. The id comes from the payload, so it is not proof of ownership. A
run in workspace A that names an agent or task of workspace B (by id, or by
name) must not receive B's tool_configs or LLM overrides; the run continues
with the payload alone. An id in A still fills in exactly as before, and a run
with no workspace at all fails before anything runs.

Real AgentService / TaskService over a real SQLite database, so the group check
being exercised is the one the services apply, not a mock of it.
"""

import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.agent import Agent
from src.models.task import Task
from src.services.execution.kasal_service import KasalExecutionService
from src.utils.user_context import GroupContext

A, B = "ws-a", "ws-b"
A_AGENT = "aaaaaaaa-0000-4000-8000-00000000000a"
B_AGENT = "bbbbbbbb-0000-4000-8000-00000000000b"
A_TASK = "aaaaaaaa-1111-4000-8000-00000000000a"
B_TASK = "bbbbbbbb-1111-4000-8000-00000000000b"
A_CONFIGS = {"SerperDevTool": {"serper_api_key": "a-own-key"}}
B_CONFIGS = {"SerperDevTool": {"serper_api_key": "b-secret-key"}}
SHARED_NAME = "Research Analyst"


def _agent(agent_id, group_id, configs, temperature):
    return Agent(
        id=agent_id,
        name=SHARED_NAME,
        role=SHARED_NAME,
        goal="g",
        backstory="b",
        group_id=group_id,
        tools=[],
        tool_configs=configs,
        temperature=temperature,
        reasoning_effort="high",
        max_tokens=7000,
    )


def _task(task_id, group_id, configs):
    return Task(
        id=task_id,
        name=SHARED_NAME,
        description=SHARED_NAME,
        expected_output="o",
        group_id=group_id,
        tools=[],
        tool_configs=configs,
    )


@pytest_asyncio.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: Agent.__table__.create(sync))
        await conn.run_sync(lambda sync: Task.__table__.create(sync))
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        session.add_all(
            [
                _agent(A_AGENT, A, A_CONFIGS, 30),
                _agent(B_AGENT, B, B_CONFIGS, 90),
                _task(A_TASK, A, A_CONFIGS),
                _task(B_TASK, B, B_CONFIGS),
            ]
        )
        await session.commit()
    yield maker
    await engine.dispose()


async def _run(maker, agents_yaml, tasks_yaml, group_context):
    """prepare_and_run_crew up to the engine hand-off; returns what it was given."""

    @asynccontextmanager
    async def routed():
        async with maker() as session:
            yield session

    engine = MagicMock()
    engine._init_task = MagicMock()
    engine._init_task.done.return_value = True
    engine.run_execution = AsyncMock()
    config = SimpleNamespace(
        agents_yaml=agents_yaml,
        tasks_yaml=tasks_yaml,
        agents=None,
        tasks=None,
        inputs=None,
        model="m",
    )
    service = KasalExecutionService()
    with (
        patch("src.services.execution.kasal_service.routed_scoped_session", routed),
        patch.object(
            KasalExecutionService, "_prepare_engine", AsyncMock(return_value=engine)
        ),
    ):
        await service.prepare_and_run_crew("e1", config, group_context=group_context)
    execution_config = engine.run_execution.await_args.args[1]
    return execution_config["agents"], execution_config["tasks"]


def _spec(**extra):
    return {"role": SHARED_NAME, "goal": "g", "backstory": "b", **extra}


def _task_spec(**extra):
    return {"description": "d", "expected_output": "o", **extra}


CALLER_A = GroupContext(group_ids=[A], group_email="a@example.com")


@pytest.mark.asyncio
async def test_another_workspaces_agent_and_task_ids_are_not_merged(db):
    agents, tasks = await _run(
        db,
        {f"agent_{B_AGENT}": _spec()},
        {f"task_{B_TASK}": _task_spec()},
        CALLER_A,
    )

    assert "b-secret-key" not in str(agents) + str(tasks)
    assert not agents[0].get("tool_configs")
    assert agents[0].get("temperature") is None
    assert agents[0].get("reasoning_effort") is None
    assert agents[0].get("max_tokens") is None
    assert not tasks[0].get("tool_configs")


@pytest.mark.asyncio
async def test_a_name_never_resolves_a_row_from_another_workspace(db):
    # A non-UUID key misses the id lookup; the old name fallback then returned
    # the first row with that name from ANY workspace. Only B has one here.
    async with db() as session:
        await session.delete(await session.get(Agent, A_AGENT))
        await session.delete(await session.get(Task, A_TASK))
        await session.commit()

    agents, tasks = await _run(
        db,
        {"analyst": _spec(name=SHARED_NAME)},
        {"analysis": _task_spec(name=SHARED_NAME)},
        CALLER_A,
    )

    assert "b-secret-key" not in str(agents) + str(tasks)
    assert agents[0].get("temperature") is None
    assert not tasks[0].get("tool_configs")


@pytest.mark.asyncio
@pytest.mark.parametrize("context", [None, GroupContext(group_ids=[])])
async def test_a_run_with_no_workspace_fails_before_anything_runs(db, context):
    # Running it would read no catalog row, so it would run without the
    # tool configs and approval gates its agents and tasks add.
    update = AsyncMock()
    with (
        patch(
            "src.services.execution.kasal_service.ExecutionStatusService.update_status",
            update,
        ),
        pytest.raises(ValueError, match="no workspace"),
    ):
        await _run(
            db,
            {f"agent_{A_AGENT}": _spec()},
            {f"task_{A_TASK}": _task_spec()},
            context,
        )

    assert update.await_args.kwargs["status"] == "FAILED"
    assert "no workspace" in update.await_args.kwargs["message"]


@pytest.mark.asyncio
async def test_ids_in_the_runs_own_workspace_fill_in_as_before(db):
    agents, tasks = await _run(
        db,
        {f"agent_{A_AGENT}": _spec()},
        {f"task_{A_TASK}": _task_spec()},
        CALLER_A,
    )

    assert agents[0]["tool_configs"] == A_CONFIGS
    assert agents[0]["temperature"] == 30
    assert agents[0]["reasoning_effort"] == "high"
    assert agents[0]["max_tokens"] == 7000
    assert tasks[0]["tool_configs"] == A_CONFIGS


@pytest.mark.asyncio
async def test_a_refused_lookup_is_logged_by_id_only(db):
    with patch("src.services.execution.kasal_service.crew_logger") as log:
        await _run(
            db,
            {f"agent_{B_AGENT}": _spec()},
            {f"task_{B_TASK}": _task_spec()},
            CALLER_A,
        )

    warnings = " ".join(
        str(c) for c in log.log.call_args_list if c.args[0] == logging.WARNING
    )
    assert B_AGENT in warnings and B_TASK in warnings
    assert "b-secret-key" not in warnings


@pytest.mark.asyncio
async def test_a_spec_keyed_by_name_only_misses_quietly(db):
    # No id to miss: a name-keyed entry is routine, so it is not a warning on
    # every run (an id that misses still is, above).
    with patch("src.services.execution.kasal_service.crew_logger") as log:
        await _run(
            db,
            {"analyst": _spec(name=SHARED_NAME)},
            {"analysis": _task_spec(name=SHARED_NAME)},
            CALLER_A,
        )

    levels = {c.args[0] for c in log.log.call_args_list}
    assert levels == {logging.DEBUG}
