"""A published crew run is projected only from agents and tasks of its workspace.

An external (MCP/A2A) start rebuilds the crew's agents_yaml/tasks_yaml from the
ids the saved crew lists. A crew imported from another workspace still lists
that workspace's ids, so the projection must read through the group-checked
accessors with the caller's (publication's) context: rows of another workspace
are left out, as deleted rows are, and their tool_configs never reach the run.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.agent import Agent
from src.models.task import Task
from src.services.external.identity import ExternalCaller
from src.services.external.invocation import start_run
from src.utils.user_context import GroupContext

A, B = "ws-a", "ws-b"
A_AGENT = "aaaaaaaa-0000-4000-8000-00000000000a"
B_AGENT = "bbbbbbbb-0000-4000-8000-00000000000b"
A_TASK = "aaaaaaaa-1111-4000-8000-00000000000a"
B_TASK = "bbbbbbbb-1111-4000-8000-00000000000b"


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: Agent.__table__.create(sync))
        await conn.run_sync(lambda sync: Task.__table__.create(sync))
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        for agent_id, task_id, group_id in ((A_AGENT, A_TASK, A), (B_AGENT, B_TASK, B)):
            configs = {"SerperDevTool": {"serper_api_key": f"{group_id}-key"}}
            s.add(
                Agent(
                    id=agent_id,
                    name=f"{group_id} agent",
                    role=f"{group_id} role",
                    goal="g",
                    group_id=group_id,
                    tools=[],
                    tool_configs=configs,
                )
            )
            s.add(
                Task(
                    id=task_id,
                    name=f"{group_id} task",
                    description=f"{group_id} description",
                    expected_output="o",
                    agent_id=agent_id,
                    group_id=group_id,
                    tools=[],
                    tool_configs=configs,
                )
            )
        await s.commit()
        yield s
    await engine.dispose()


@pytest.mark.asyncio
async def test_an_imported_crews_foreign_rows_are_left_out(session):
    crew = SimpleNamespace(
        id="00000000-0000-0000-0000-000000000001",
        agent_ids=[A_AGENT, B_AGENT],
        task_ids=[A_TASK, B_TASK],
        reasoning_config=None,
    )
    crews = MagicMock()
    crews.get_by_group = AsyncMock(return_value=crew)
    service = MagicMock()
    service.create_execution = AsyncMock(
        return_value={"execution_id": "run-1", "status": "pending"}
    )
    caller = ExternalCaller(
        group_context=GroupContext(group_ids=[A], group_email="a@example.com"),
        protocol="mcp",
        identifier="a@example.com",
    )
    publication = SimpleNamespace(
        entity_type="crew",
        entity_id=crew.id,
        external_name="research",
        group_id=A,
    )
    with (
        patch("src.services.catalog.crews.CrewService", return_value=crews),
        patch("src.services.execution.service.ExecutionService", return_value=service),
    ):
        await start_run(caller, publication, session=session)

    config = service.create_execution.await_args.kwargs["config"]
    assert [a["role"] for a in config.agents_yaml.values()] == [f"{A} role"]
    assert [t["id"] for t in config.tasks_yaml.values()] == [A_TASK]
    assert f"{B}-key" not in str(config.agents_yaml) + str(config.tasks_yaml)


@pytest.mark.asyncio
async def test_the_published_crew_is_loaded_only_from_the_publications_workspace():
    """The crew row itself is workspace-checked, not only the rows it lists."""
    from tests.unit.services.catalog.crew_workspaces import (
        A_CREW,
        B_CREW,
        CALLER_A,
        seeded_database,
    )

    service = MagicMock()
    service.create_execution = AsyncMock(
        return_value={"execution_id": "run-1", "status": "pending"}
    )
    caller = ExternalCaller(
        group_context=CALLER_A, protocol="mcp", identifier="a@example.com"
    )

    def _publication(crew_id):
        return SimpleNamespace(
            entity_type="crew",
            entity_id=str(crew_id),
            external_name="research",
            group_id=CALLER_A.primary_group_id,
        )

    async with seeded_database() as maker:
        with patch(
            "src.services.execution.service.ExecutionService", return_value=service
        ):
            async with maker() as session:
                with pytest.raises(ValueError, match="no longer exists"):
                    await start_run(caller, _publication(B_CREW), session=session)
                service.create_execution.assert_not_awaited()

                await start_run(caller, _publication(A_CREW), session=session)
    config = service.create_execution.await_args.kwargs["config"]
    assert list(config.agents_yaml) == ["Researcher", "Writer"]
