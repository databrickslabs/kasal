"""Regression test for PERF-034: entity enrichment in prepare_and_run_crew
must look up agents/tasks by ID FIRST (the YAML key embeds the DB UUID and
virtually always hits). The old name-first order was a guaranteed miss —
find_by_name is exact-equality and YAML configs carry no 'name' — doubling
sequential DB round trips for every agent and task on every execution."""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.crewai_execution_service import CrewAIExecutionService


@pytest.mark.asyncio
async def test_agent_and_task_lookup_is_id_first():
    agent_uuid = "11111111-2222-3333-4444-555555555555"
    task_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    config = SimpleNamespace(
        agents_yaml={
            f"agent_{agent_uuid}": {
                "role": "Data Analyst with a long role sentence",
                "goal": "g", "backstory": "b",
            }
        },
        tasks_yaml={
            f"task_{task_uuid}": {
                "description": "A long multi-sentence description that never matches a name.",
                "expected_output": "out",
            }
        },
        agents=None, tasks=None, inputs=None,
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

    service = CrewAIExecutionService.__new__(CrewAIExecutionService)

    mock_engine = MagicMock()
    mock_engine._init_task = MagicMock()
    mock_engine._init_task.done.return_value = True
    # Stop right after enrichment: the engine's run_execution is the next step.
    mock_engine.run_execution = AsyncMock(side_effect=RuntimeError("stop-after-enrichment"))

    with patch("src.services.crewai_execution_service.request_scoped_session", return_value=session_cm), \
         patch("src.services.crewai_execution_service.AgentService", return_value=agent_service), \
         patch("src.services.crewai_execution_service.TaskService", return_value=task_service), \
         patch.object(CrewAIExecutionService, "_prepare_engine", new_callable=AsyncMock,
                      return_value=mock_engine, create=True):
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
