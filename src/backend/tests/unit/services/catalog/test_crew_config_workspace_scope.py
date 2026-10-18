"""The server-side crew projection reads only the caller's workspace.

``build_crew_execution_config(_by_id)`` rebuilds a saved crew for a run with no
canvas: a crew trigger, a resume, the checkpoint preview, an MCP/A2A start. The
crew id, and the agent and task ids it lists, can come from the caller, so each
read is group-checked with the caller's context (``catalog/run_lookup``):

- another workspace's crew is "not found" (None), the same as a deleted one;
- with no group context, or no group ids, nothing is read at all;
- a crew in the caller's workspace projects with each task on its edge agent.
  Each agent entry carries its row id, so the run's group-checked fill-in in
  kasal_service applies, and the row's temperature.

Real services over a real SQLite database (``crew_workspaces``), so the group
check under test is the one the services apply.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.models.crew import Crew
from src.schemas.execution import CrewConfig
from src.services.agent_builder.crew_preparation import CrewPreparation
from src.services.catalog.agents import AgentService
from src.services.catalog.crew_config import (
    build_crew_execution_config,
    build_crew_execution_config_by_id,
)
from src.services.catalog.crews import CrewService
from src.services.catalog.tasks import TaskService
from src.services.execution.checkpointing.resume_config import (
    build_crew_resume_config,
)
from src.services.execution.config.agent_settings_snapshot import (
    snapshot_agent_settings,
)
from src.services.execution.kasal_service import KasalExecutionService
from src.utils.user_context import GroupContext
from tests.unit.services.catalog.crew_workspaces import (
    A_CONFIGS,
    A_CREW,
    A_RESEARCHER,
    A_WRITER,
    B_CREW,
    CALLER_A,
    A,
    B,
    routed_to,
    seeded_database,
)

EDGE_AGENTS = {"research": "Researcher", "write": "Writer"}


@pytest_asyncio.fixture
async def db():
    async with seeded_database() as maker:
        yield maker


async def _project(maker, crew_id, group_context=CALLER_A):
    async with maker() as session:
        return await build_crew_execution_config_by_id(session, crew_id, group_context)


async def _prepare_and_run(maker, agents_yaml, tasks_yaml, group_context=CALLER_A):
    """``prepare_and_run_crew`` up to the engine hand-off; returns what it got."""
    engine = MagicMock()
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
    with (
        patch(
            "src.services.execution.kasal_service.routed_scoped_session",
            routed_to(maker),
        ),
        patch.object(
            KasalExecutionService, "_prepare_engine", AsyncMock(return_value=engine)
        ),
    ):
        await KasalExecutionService().prepare_and_run_crew(
            "run-1", config, group_context=group_context
        )
    execution_config = engine.run_execution.await_args.args[1]
    return execution_config["agents"], execution_config["tasks"]


async def _engine_assignments(agents, tasks):
    """Task key -> role of the agent CrewPreparation actually puts it on."""
    prep = CrewPreparation({"agents": agents, "tasks": tasks})
    with (
        patch(
            "src.services.agent_builder.crew_preparation.create_agent",
            AsyncMock(
                side_effect=lambda **kw: SimpleNamespace(
                    role=kw["agent_config"]["role"]
                )
            ),
        ),
        patch(
            "src.services.tools.mcp_integration.MCPIntegration."
            "collect_agent_mcp_requirements",
            AsyncMock(return_value={}),
        ),
        patch.object(
            CrewPreparation,
            "_lookup_kasal_agent_uuid_via_service",
            AsyncMock(return_value=None),
        ),
    ):
        assert await prep._create_agents()
    # _find_agent_by_reference returns None when CrewPreparation would fall
    # back to the crew's FIRST agent.
    found = {t["id"]: prep._find_agent_by_reference(t.get("agent")) for t in tasks}
    return {task_id: getattr(agent, "role", None) for task_id, agent in found.items()}


@pytest.mark.asyncio
async def test_another_workspaces_crew_is_not_found(db):
    # Before: the crew was loaded unscoped and projected to ({}, {}), which is
    # truthy, so a trigger went on to start a run for it.
    assert await _project(db, B_CREW) is None
    assert await _project(db, str(B_CREW)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("context", [None, GroupContext(group_ids=[])])
async def test_with_no_workspace_nothing_is_read(db, context):
    reads = {
        (service, name): AsyncMock(return_value=None)
        for service, names in (
            (AgentService, ("get", "get_with_group_check")),
            (TaskService, ("get", "get_with_group_check")),
            (CrewService, ("get", "get_by_group")),
        )
        for name in names
    }
    async with db() as session:
        crew = await session.get(Crew, A_CREW)
        patches = [patch.object(s, n, mock) for (s, n), mock in reads.items()]
        for p in patches:
            p.start()
        try:
            by_id = await build_crew_execution_config_by_id(session, A_CREW, context)
            direct = await build_crew_execution_config(session, crew, context)
        finally:
            for p in patches:
                p.stop()

    assert by_id is None
    assert direct == ({}, {})
    for (service, name), mock in reads.items():
        assert not mock.await_count, f"{service.__name__}.{name} was read"


@pytest.mark.asyncio
async def test_a_crew_of_the_callers_workspace_carries_ids_and_row_settings(db):
    agents_yaml, tasks_yaml = await _project(db, str(A_CREW))

    assert list(agents_yaml) == ["Researcher", "Writer"]
    assert {k: t["agent"] for k, t in tasks_yaml.items()} == EDGE_AGENTS
    researcher, writer = agents_yaml["Researcher"], agents_yaml["Writer"]
    assert (researcher["id"], writer["id"]) == (A_RESEARCHER, A_WRITER)
    # The raw 0-100 row value: the kernel scales spec["temperature"] itself.
    assert (researcher["temperature"], writer["temperature"]) == (30, 80)
    assert researcher["reasoning_effort"] == "high"
    assert researcher["max_tokens"] == 7000
    assert researcher["max_iter"] == 7
    assert researcher["tool_configs"] == A_CONFIGS
    assert writer["thinking_budget_tokens"] == 2048


@pytest.mark.asyncio
async def test_a_rebuilt_crew_runs_with_its_row_settings_on_its_edge_agents(db):
    """Trigger and resume hand the projection straight to prepare_and_run_crew."""
    agents_yaml, tasks_yaml = await _project(db, A_CREW)

    agents, tasks = await _prepare_and_run(db, agents_yaml, tasks_yaml)

    by_role = {a["role"]: a for a in agents}
    # Filled once: the projection and the group-checked fill-in read the same
    # row, and neither scales or merges what the other set.
    assert by_role["researcher"]["temperature"] == 30
    assert by_role["writer"]["temperature"] == 80
    assert by_role["researcher"]["tool_configs"] == A_CONFIGS
    assert by_role["researcher"]["id"] == A_RESEARCHER
    assert await _engine_assignments(agents, tasks) == {
        t["id"]: role for t, role in zip(tasks, ("researcher", "writer"))
    }


@pytest.mark.asyncio
async def test_an_mcp_started_crew_freezes_and_keeps_its_row_settings(db):
    """An MCP/A2A start goes through create_execution, which snapshots the
    saved agent settings by id before the run record is written."""
    agents_yaml, tasks_yaml = await _project(db, A_CREW)
    config = CrewConfig(agents_yaml=agents_yaml, tasks_yaml=tasks_yaml, inputs={})
    async with db() as session:
        await snapshot_agent_settings(config, session, CALLER_A)

    researcher = config.agents_yaml["Researcher"]
    assert researcher["agent_settings_snapshot"] is True
    assert researcher["temperature"] == 30

    agents, tasks = await _prepare_and_run(db, config.agents_yaml, config.tasks_yaml)
    by_role = {a["role"]: a for a in agents}
    assert (by_role["researcher"]["temperature"], by_role["writer"]["temperature"]) == (
        30,
        80,
    )
    assert by_role["writer"]["thinking_budget_tokens"] == 2048
    assert set((await _engine_assignments(agents, tasks)).values()) == {
        "researcher",
        "writer",
    }


@pytest.mark.asyncio
async def test_a_resumed_crew_is_rebuilt_with_its_agents_temperature(db):
    source = SimpleNamespace(crew_id=A_CREW, job_id="run-0")
    stored = {
        "agents_yaml": {"old": {"role": "old"}},
        "tasks_yaml": {"old": {"description": "old"}},
        "inputs": {},
    }
    async with db() as session:
        config, _, resume_inputs = await build_crew_resume_config(
            session, source, stored, None, None, CALLER_A
        )

    assert config.agents_yaml["Researcher"]["temperature"] == 30
    assert config.agents_yaml["Researcher"]["id"] == A_RESEARCHER
    assert {k: t["agent"] for k, t in config.tasks_yaml.items()} == EDGE_AGENTS
    assert resume_inputs["agents_yaml"]["Writer"]["temperature"] == 80


SNAPSHOT = {
    "agents_yaml": {"old": {"role": "old"}},
    "tasks_yaml": {"old": {"description": "old"}},
    "inputs": {},
}


async def _resumed_roles(maker, source, group_ids):
    """A caller with no workspace selected: its context spans both."""
    caller = GroupContext(group_ids=group_ids, group_email="ab@example.com")
    async with maker() as session:
        config, _, _ = await build_crew_resume_config(
            session, source, SNAPSHOT, None, None, caller
        )
    return [a["role"] for a in config.agents_yaml.values()]


@pytest.mark.asyncio
async def test_a_multi_group_resume_is_rebuilt_from_its_runs_crew(db):
    """The run is in B, the second of the caller's groups; so is its crew."""
    source = SimpleNamespace(crew_id=B_CREW, job_id="run-b", group_id=B)
    assert await _resumed_roles(db, source, [A, B]) == ["bravo agent"]


@pytest.mark.asyncio
async def test_a_resume_reads_its_crew_only_in_the_runs_workspace(db):
    """A run in A naming B's crew: B is in the caller's scope (first, even)
    but not the run's, so the resume falls back to the stored snapshot."""
    source = SimpleNamespace(crew_id=B_CREW, job_id="run-a", group_id=A)
    assert await _resumed_roles(db, source, [B, A]) == ["old"]
