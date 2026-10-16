"""A chat turn fills missing tool_configs only from an agent in its own workspace.

The light agent takes the first agents_yaml entry, and when that spec carries no
tool_configs it reads them from the stored agent named by the spec's id. The id
comes from the request, so a turn in workspace A naming an agent of workspace B
must build its agent WITHOUT B's tool_configs: the turn still runs, with the
spec's own tools and no stored overrides. An agent of A still fills in as before.
A turn with no workspace at all fails before its agent is built.

The agent read goes through the real AgentService over a real SQLite database;
only the parts of a turn that are not under test (tool factory, model build,
status write) are stubbed, as in test_execution_runner_light_agent.py.
"""

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.db.all_models  # noqa: F401  (registers every table on Base)
from src.db.base import Base
from src.models.agent import Agent
from src.models.execution_status import ExecutionStatus
from src.schemas.execution import CrewConfig
from src.services.chat.service import LightAgentService, run_light_agent
from src.utils.user_context import GroupContext

A, B = "ws-a", "ws-b"
A_AGENT = "aaaaaaaa-0000-4000-8000-00000000000a"
B_AGENT = "bbbbbbbb-0000-4000-8000-00000000000b"
A_CONFIGS = {"SerperDevTool": {"serper_api_key": "a-own-key"}}
B_CONFIGS = {"SerperDevTool": {"serper_api_key": "b-secret-key"}}


@pytest_asyncio.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        for agent_id, group_id, configs in (
            (A_AGENT, A, A_CONFIGS),
            (B_AGENT, B, B_CONFIGS),
        ):
            session.add(
                Agent(
                    id=agent_id,
                    name="Analyst",
                    role="Analyst",
                    goal="g",
                    group_id=group_id,
                    tools=[],
                    tool_configs=configs,
                )
            )
        await session.commit()
    yield maker
    await engine.dispose()


async def _turn(maker, agent_id, group_context):
    """One chat turn naming ``agent_<agent_id>``; returns the tool_configs built."""

    @asynccontextmanager
    async def routed():
        async with maker() as session:
            yield session

    agent = MagicMock()
    agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw="answer"))
    build = AsyncMock(return_value=agent)
    config = CrewConfig(
        model="m",
        inputs={},
        agents_yaml={
            f"agent_{agent_id}": {
                "role": "Analyst",
                "goal": "g",
                "backstory": "b",
                "tools": ["SerperDevTool"],
            }
        },
        tasks_yaml={"task_t": {"description": "Answer: hi", "expected_output": "x"}},
    )
    update = AsyncMock(return_value=True)
    with (
        patch("src.db.session.routed_scoped_session", routed),
        patch("src.utils.user_context.UserContext"),
        patch("src.services.settings.api_keys.ApiKeysService"),
        patch.object(LightAgentService, "_attach_memory", AsyncMock(return_value=None)),
        patch(
            "src.services.execution.logs.writer_task.LogWriterTask.ensure_writer_started",
            AsyncMock(),
        ),
        patch(
            "src.services.tools.tool_factory.ToolFactory.create",
            new_callable=AsyncMock,
            return_value=MagicMock(),
        ),
        patch(
            "src.services.execution.kernel.agent_tools.build_agent_with_tools", build
        ),
        patch(
            "src.services.execution.status.ExecutionStatusService.update_status",
            update,
        ),
    ):
        result = await run_light_agent(
            f"chat-{uuid.uuid4()}", config, group_context=group_context
        )
    return result, build


async def _built_tool_configs(maker, agent_id, group_context):
    result, build = await _turn(maker, agent_id, group_context)
    assert result["status"] == ExecutionStatus.COMPLETED.value
    return build.await_args.kwargs["tool_configs"]


CALLER_A = GroupContext(group_ids=[A], group_email="a@example.com")


@pytest.mark.asyncio
async def test_another_workspaces_agent_lends_no_tool_configs(db):
    tool_configs = await _built_tool_configs(db, B_AGENT, CALLER_A)

    assert "b-secret-key" not in str(tool_configs)
    assert not tool_configs


@pytest.mark.asyncio
@pytest.mark.parametrize("context", [None, GroupContext(group_ids=[])])
async def test_a_turn_with_no_workspace_fails_before_its_agent_is_built(db, context):
    result, build = await _turn(db, A_AGENT, context)

    assert result["status"] == ExecutionStatus.FAILED.value
    assert "no workspace" in result["error"]
    build.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_agent_in_the_turns_own_workspace_still_fills_in(db):
    assert await _built_tool_configs(db, A_AGENT, CALLER_A) == A_CONFIGS
