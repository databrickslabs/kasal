"""Regression test for PERF-034: entity enrichment in prepare_and_run_crew
must look up agents/tasks by ID FIRST (the YAML key embeds the DB UUID and
virtually always hits). The old name-first order was a guaranteed miss —
find_by_name is exact-equality and YAML configs carry no 'name' — doubling
sequential DB round trips for every agent and task on every execution.

Also pins the payload-vs-row ``tool_configs`` precedence, which runs in the
same enrichment pass: an explicit ``{}`` in the payload is kept, only a
missing key falls back to the row."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.kasal_service import KasalExecutionService


@pytest.mark.asyncio
async def test_agent_and_task_lookup_is_id_first():
    agent_uuid = "11111111-2222-3333-4444-555555555555"
    task_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    config = SimpleNamespace(
        agents_yaml={
            f"agent_{agent_uuid}": {
                "role": "Data Analyst with a long role sentence",
                "goal": "g",
                "backstory": "b",
            }
        },
        tasks_yaml={
            f"task_{task_uuid}": {
                "description": "A long multi-sentence description that never matches a name.",
                "expected_output": "out",
            }
        },
        agents=None,
        tasks=None,
        inputs=None,
    )

    db_agent = SimpleNamespace(tool_configs={"some_tool": {"k": "v"}})
    db_task = SimpleNamespace(tool_configs={})

    agent_service = MagicMock()
    agent_service.get = AsyncMock(return_value=db_agent)
    agent_service.find_by_name = AsyncMock()
    task_service = MagicMock()
    task_service.get = AsyncMock(return_value=db_task)
    task_service.find_by_name = AsyncMock()

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
    session_cm.__aexit__ = AsyncMock(return_value=None)

    service = KasalExecutionService.__new__(KasalExecutionService)

    mock_engine = MagicMock()
    mock_engine._init_task = MagicMock()
    mock_engine._init_task.done.return_value = True
    # Stop right after enrichment: the engine's run_execution is the next step.
    mock_engine.run_execution = AsyncMock(
        side_effect=RuntimeError("stop-after-enrichment")
    )

    with (
        patch(
            "src.services.execution.kasal_service.routed_scoped_session",
            return_value=session_cm,
        ),
        patch(
            "src.services.execution.kasal_service.AgentService",
            return_value=agent_service,
        ),
        patch(
            "src.services.execution.kasal_service.TaskService",
            return_value=task_service,
        ),
        patch.object(
            KasalExecutionService,
            "_prepare_engine",
            new_callable=AsyncMock,
            return_value=mock_engine,
            create=True,
        ),
    ):
        try:
            await service.prepare_and_run_crew(
                execution_id="e1", config=config, group_context=None
            )
        except Exception:
            pass  # later stages may fail; enrichment already ran

    # ID lookups hit -> name lookups must never fire.
    agent_service.get.assert_awaited_once_with(agent_uuid)
    agent_service.find_by_name.assert_not_awaited()
    task_service.get.assert_awaited_once_with(task_uuid)
    task_service.find_by_name.assert_not_awaited()


async def _enrich(config, db_agent, db_task) -> None:
    """Run prepare_and_run_crew up to the end of enrichment (the same harness
    as the lookup-order test, factored so the precedence tests stay short)."""
    agent_service = MagicMock()
    agent_service.get = AsyncMock(return_value=db_agent)
    agent_service.find_by_name = AsyncMock()
    task_service = MagicMock()
    task_service.get = AsyncMock(return_value=db_task)
    task_service.find_by_name = AsyncMock()

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
    session_cm.__aexit__ = AsyncMock(return_value=None)

    service = KasalExecutionService.__new__(KasalExecutionService)
    mock_engine = MagicMock()
    mock_engine._init_task = MagicMock()
    mock_engine._init_task.done.return_value = True
    mock_engine.run_execution = AsyncMock(
        side_effect=RuntimeError("stop-after-enrichment")
    )
    with (
        patch(
            "src.services.execution.kasal_service.routed_scoped_session",
            return_value=session_cm,
        ),
        patch(
            "src.services.execution.kasal_service.AgentService",
            return_value=agent_service,
        ),
        patch(
            "src.services.execution.kasal_service.TaskService",
            return_value=task_service,
        ),
        patch.object(
            KasalExecutionService,
            "_prepare_engine",
            new_callable=AsyncMock,
            return_value=mock_engine,
            create=True,
        ),
    ):
        try:
            await service.prepare_and_run_crew(
                execution_id="e1", config=config, group_context=None
            )
        except Exception:
            pass  # later stages may fail; enrichment already ran


AGENT_UUID = "11111111-2222-3333-4444-555555555555"
TASK_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
BROWSER = {"MCP_SERVERS": {"servers": ["browser"]}}


def _config(agent_extra: dict, task_extra: dict) -> SimpleNamespace:
    return SimpleNamespace(
        agents_yaml={
            f"agent_{AGENT_UUID}": {
                "role": "r",
                "goal": "g",
                "backstory": "b",
                **agent_extra,
            }
        },
        tasks_yaml={
            f"task_{TASK_UUID}": {
                "description": "d",
                "expected_output": "o",
                **task_extra,
            }
        },
        agents=None,
        tasks=None,
        inputs=None,
    )


# Observed: a task's browser MCP was removed on the canvas, the row still held
# it, and the payload's explicit `tool_configs: {}` was treated as "absent" —
# so the row's MCP_SERVERS came back and the run called browser_web_search
# while the node read "MCP: 0".
@pytest.mark.asyncio
async def test_explicit_empty_tool_configs_in_the_payload_beats_the_row():
    config = _config({"tool_configs": {}}, {"tool_configs": {}})

    await _enrich(
        config,
        db_agent=SimpleNamespace(tool_configs=BROWSER),
        db_task=SimpleNamespace(tool_configs=BROWSER),
    )

    assert config.agents_yaml[f"agent_{AGENT_UUID}"]["tool_configs"] == {}
    assert config.tasks_yaml[f"task_{TASK_UUID}"]["tool_configs"] == {}


@pytest.mark.asyncio
async def test_missing_tool_configs_in_the_payload_falls_back_to_the_row():
    # Older clients and API callers send no key at all: the row is the only
    # source, exactly as before.
    config = _config({}, {})

    await _enrich(
        config,
        db_agent=SimpleNamespace(tool_configs=BROWSER),
        db_task=SimpleNamespace(tool_configs=BROWSER),
    )

    assert config.agents_yaml[f"agent_{AGENT_UUID}"]["tool_configs"] == BROWSER
    assert config.tasks_yaml[f"task_{TASK_UUID}"]["tool_configs"] == BROWSER
