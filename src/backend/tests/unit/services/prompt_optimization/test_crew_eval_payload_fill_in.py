"""A GEPA evaluation run gets its crew's stored settings, as the real crew does.

``start_crew_optimization`` keys its eval payload by agent and task NAME and
carries no tool_configs or LLM overrides: the run fills those in from the rows.
Runs fill in by id only (a name is not unique across workspaces), so each entry
must carry its row's id. Without it every baseline and candidate evaluation ran
with no tool configs or MCP servers, none of the approval gates the agents and
tasks add, and the model defaults instead of the agent's temperature, effort
and token limits. GEPA then applied the prompts it tuned there to the real crew.

The payload is captured from the real ``start_crew_optimization`` over a real
SQLite database, frozen the way ``ExecutionService.create_execution`` freezes
it for GEPA (no session), and prepared by the real ``prepare_and_run_crew``.
"""

import copy
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.agent import Agent
from src.models.crew import Crew
from src.models.task import Task
from src.schemas.execution import CrewConfig
from src.schemas.prompt_optimization import CrewOptimizationRequest
from src.services.execution.config import run_freeze
from src.services.execution.kasal_service import KasalExecutionService
from src.services.prompt_optimization import service as svc_module
from src.services.prompt_optimization.service import PromptOptimizationService
from src.utils.user_context import GroupContext

WS = "ws-a"
CREW_ID = uuid.UUID("aaaaaaaa-2222-4000-8000-00000000000a")
AGENT_ID = "aaaaaaaa-0000-4000-8000-00000000000a"
TASK_ID = "aaaaaaaa-1111-4000-8000-00000000000a"
GATED = {"SerperDevTool": {"requires_approval": True}}
CONTEXT = GroupContext(group_ids=[WS], group_email="a@example.com")


@pytest_asyncio.fixture
async def maker():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in (Agent, Task, Crew):
            await conn.run_sync(lambda sync, m=model: m.__table__.create(sync))
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        session.add(
            Agent(
                id=AGENT_ID,
                name="Researcher",
                role="r",
                goal="g",
                backstory="b",
                group_id=WS,
                llm="m",
                tools=["SerperDevTool"],
                tool_configs=GATED,
                temperature=20,
                reasoning_effort="high",
                max_tokens=6000,
            )
        )
        session.add(
            Task(
                id=TASK_ID,
                name="Research",
                description="d",
                expected_output="e",
                agent_id=AGENT_ID,
                group_id=WS,
                tools=["SerperDevTool"],
                tool_configs=GATED,
            )
        )
        session.add(
            Crew(
                id=CREW_ID,
                name="C",
                group_id=WS,
                agent_ids=[AGENT_ID],
                task_ids=[TASK_ID],
            )
        )
        await session.commit()
    yield maker
    await engine.dispose()


async def _gepa_payload(maker):
    """The agents_yaml/tasks_yaml start_crew_optimization hands its worker."""
    service = PromptOptimizationService.__new__(PromptOptimizationService)
    service._rubric_with_feedback = AsyncMock(side_effect=lambda r, c, g: r)
    service._prepare_run_labels = AsyncMock(
        return_value=SimpleNamespace(
            feedback_count=0,
            judge_ids=[],
            expectations={},
            human_requirements=None,
            skipped=[],
        )
    )
    service._resolve_registry = AsyncMock(return_value=("databricks-uc", "c.s.p"))
    service._resolve_crew_traces_experiment = AsyncMock(return_value="exp")
    service._record_run = AsyncMock()
    service._prune_runs = MagicMock()
    service._run_optimization = AsyncMock()
    async with maker() as session:
        service.session = session
        with patch.object(
            svc_module, "resolve_run_judge", AsyncMock(return_value=("j", 1))
        ):
            result = await service.start_crew_optimization(
                CrewOptimizationRequest(crew_id=str(CREW_ID)), CONTEXT
            )
    svc_module._RUNS.pop(result["run_id"], None)
    kwargs = service._run_optimization.call_args.kwargs
    return kwargs["agents_yaml"], kwargs["tasks_yaml"]


async def _prepared_eval_run(maker, agents_yaml, tasks_yaml):
    """One evaluation run, frozen and prepared as the eval path does it."""
    agents_over, tasks_over = copy.deepcopy(agents_yaml), copy.deepcopy(tasks_yaml)
    for entity in [*agents_over.values(), *tasks_over.values()]:
        entity.pop("_field_prefix", None)  # crew_runner._apply_fields does this
    config = CrewConfig(
        agents_yaml=agents_over,
        tasks_yaml=tasks_over,
        inputs={},
        model="m",
        execution_type="crew",
    )
    # GEPA creates its runs with ExecutionService() — no session.
    await run_freeze.freeze(config, None, CONTEXT)

    @asynccontextmanager
    async def routed():
        async with maker() as session:
            yield session

    engine = MagicMock()
    engine._init_task = MagicMock()
    engine._init_task.done.return_value = True
    engine.run_execution = AsyncMock()
    with (
        patch("src.services.execution.kasal_service.routed_scoped_session", routed),
        patch.object(
            KasalExecutionService, "_prepare_engine", AsyncMock(return_value=engine)
        ),
    ):
        await KasalExecutionService().prepare_and_run_crew(
            "gepa-eval", config, group_context=CONTEXT
        )
    run = engine.run_execution.await_args.args[1]
    return run["agents"][0], run["tasks"][0]


@pytest.mark.asyncio
async def test_each_entry_names_its_row(maker):
    agents_yaml, tasks_yaml = await _gepa_payload(maker)
    assert agents_yaml["Researcher"]["id"] == AGENT_ID
    assert tasks_yaml["Research"]["id"] == TASK_ID


@pytest.mark.asyncio
async def test_an_eval_run_gets_the_rows_tool_configs_and_approval_gates(maker):
    agent, task = await _prepared_eval_run(maker, *await _gepa_payload(maker))
    assert agent["tool_configs"] == GATED
    assert task["tool_configs"] == GATED


@pytest.mark.asyncio
async def test_an_eval_run_gets_the_agents_llm_overrides(maker):
    agent, _ = await _prepared_eval_run(maker, *await _gepa_payload(maker))
    assert agent["temperature"] == 20
    assert agent["reasoning_effort"] == "high"
    assert agent["max_tokens"] == 6000
