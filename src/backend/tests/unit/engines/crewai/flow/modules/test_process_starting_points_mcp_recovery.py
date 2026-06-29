"""Drives process_starting_points through the MCP-recovery wiring.

When the flow's starting-point taskId is stale (its task row has no MCP) but the
crew's CURRENT task carries MCP_SERVERS, recovery must inject the MCP into the
agent build's crew_tool_configs. The recover_mcp_from_current_tasks helper is
unit-tested separately; this exercises the wiring inside process_starting_points.
"""

import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.paths.flow.modules.flow_processors import FlowProcessorManager


@pytest.mark.asyncio
async def test_recovers_mcp_from_current_task_into_agent_build():
    stale_task_id = str(uuid.uuid4())
    current_task_id = str(uuid.uuid4())
    crew_id = str(uuid.uuid4())
    agent_id = str(uuid.uuid4())

    flow_config = {'startingPoints': [{'taskId': stale_task_id, 'crewId': crew_id}]}

    crew = MagicMock()
    crew.name = "Search Crew"
    crew.nodes = []
    crew.edges = []
    crew.tool_configs = {}
    # Include the stale id too so the skip-self branch is exercised alongside the
    # current task that actually carries MCP.
    crew.task_ids = [stale_task_id, current_task_id]

    stale_task = MagicMock()
    stale_task.agent_id = agent_id
    stale_task.async_execution = False
    stale_task.name = "Gather and Retrieve Online Mentions"
    stale_task.tool_configs = {}  # stale row → no MCP

    current_task = MagicMock()
    current_task.name = "Gather and Retrieve Online Mentions"
    current_task.tool_configs = {"MCP_SERVERS": {"servers": ["nemotemoyou"]}}

    def _task_get(tid):
        return current_task if str(tid) == current_task_id else stale_task

    repos = {
        'crew': AsyncMock(get=AsyncMock(return_value=crew)),
        'task': AsyncMock(get=AsyncMock(side_effect=_task_get)),
        'agent': AsyncMock(get=AsyncMock(return_value=MagicMock(role="Researcher"))),
    }

    with patch('src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig') as mock_agent_config, \
         patch('src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig') as mock_task_config:
        agent_obj = MagicMock(role="Researcher")
        mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=agent_obj)
        task_obj = MagicMock(agent=agent_obj)
        mock_task_config.configure_task = AsyncMock(return_value=task_obj)

        await FlowProcessorManager.process_starting_points(flow_config, {}, repos)

        # The recovered MCP_SERVERS must reach the agent build via crew_tool_configs.
        assert mock_agent_config.configure_agent_and_tools.await_count == 1
        _, kwargs = mock_agent_config.configure_agent_and_tools.call_args
        assert kwargs.get('crew_tool_configs', {}).get('MCP_SERVERS') == {"servers": ["nemotemoyou"]}


@pytest.mark.asyncio
async def test_mcp_recovery_swallows_errors_and_continues():
    """A failure while looking up the crew's current tasks must be swallowed
    (best-effort recovery) — the agent build still proceeds without MCP."""
    stale_task_id = str(uuid.uuid4())
    bad_task_id = str(uuid.uuid4())
    crew_id = str(uuid.uuid4())
    agent_id = str(uuid.uuid4())

    flow_config = {'startingPoints': [{'taskId': stale_task_id, 'crewId': crew_id}]}

    crew = MagicMock()
    crew.name = "Search Crew"
    crew.nodes = []
    crew.edges = []
    crew.tool_configs = {}
    crew.task_ids = [bad_task_id]  # recovery will fetch this and blow up

    stale_task = MagicMock()
    stale_task.agent_id = agent_id
    stale_task.async_execution = False
    stale_task.name = "Gather"
    stale_task.tool_configs = {}

    def _task_get(tid):
        if str(tid) == bad_task_id:
            raise RuntimeError("db blew up during recovery")
        return stale_task

    repos = {
        'crew': AsyncMock(get=AsyncMock(return_value=crew)),
        'task': AsyncMock(get=AsyncMock(side_effect=_task_get)),
        'agent': AsyncMock(get=AsyncMock(return_value=MagicMock(role="Researcher"))),
    }

    with patch('src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig') as mock_agent_config, \
         patch('src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig') as mock_task_config:
        agent_obj = MagicMock(role="Researcher")
        mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=agent_obj)
        mock_task_config.configure_task = AsyncMock(return_value=MagicMock(agent=agent_obj))

        # Must not raise — the recovery error is swallowed and the build continues.
        await FlowProcessorManager.process_starting_points(flow_config, {}, repos)

        assert mock_agent_config.configure_agent_and_tools.await_count == 1
        _, kwargs = mock_agent_config.configure_agent_and_tools.call_args
        assert 'MCP_SERVERS' not in (kwargs.get('crew_tool_configs') or {})
