"""Flow tasks must inherit MCP_SERVERS from the crew's canvas node.

Chat-mode crew saves write tool_configs (e.g. MCP_SERVERS) into the crew's
`nodes` JSON, NOT onto the task DB record. A crew run reads the nodes so MCP
works; a flow run loads the DB task (empty tool_configs) and would otherwise
drop the MCP servers. process_starting_points must merge the crew canvas node's
tool_configs so the flow keeps the crew's MCP servers.
"""

import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.paths.flow.modules.flow_processors import FlowProcessorManager


@pytest.mark.asyncio
async def test_merges_mcp_from_crew_node_when_db_task_has_none():
    task_id = str(uuid.uuid4())
    crew_id = str(uuid.uuid4())
    agent_id = str(uuid.uuid4())

    flow_config = {'startingPoints': [{'taskId': task_id, 'crewId': crew_id}]}

    # The crew canvas node for this task carries the chat-saved MCP servers.
    crew = MagicMock()
    crew.name = "Snowflake News Crew"
    crew.edges = []
    crew.tool_configs = {}
    crew.task_ids = [task_id]
    crew.nodes = [
        {
            'id': f'task-{task_id}',
            'type': 'taskNode',
            'data': {'tool_configs': {'MCP_SERVERS': {'servers': ['nemotemo']}}},
        }
    ]

    # The DB task record has NO tool_configs (MCP was only saved onto the node).
    db_task = MagicMock()
    db_task.agent_id = agent_id
    db_task.async_execution = False
    db_task.name = "Search for Latest Snowflake News"
    db_task.tool_configs = {}

    repos = {
        'crew': AsyncMock(get=AsyncMock(return_value=crew)),
        'task': AsyncMock(get=AsyncMock(return_value=db_task)),
        'agent': AsyncMock(get=AsyncMock(return_value=MagicMock(role="Researcher"))),
    }

    with patch('src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig') as mock_agent_config, \
         patch('src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig') as mock_task_config:
        agent_obj = MagicMock(role="Researcher")
        mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=agent_obj)
        mock_task_config.configure_task = AsyncMock(return_value=MagicMock(agent=agent_obj))

        await FlowProcessorManager.process_starting_points(flow_config, {}, repos)

        assert mock_agent_config.configure_agent_and_tools.await_count == 1
        _, kwargs = mock_agent_config.configure_agent_and_tools.call_args
        assert kwargs.get('crew_tool_configs', {}).get('MCP_SERVERS') == {"servers": ["nemotemo"]}


@pytest.mark.asyncio
async def test_db_task_tool_configs_take_priority_over_node():
    """A non-empty DB task tool_configs must win on key conflicts — the node only
    fills gaps, it does not clobber values the DB task actually carries."""
    task_id = str(uuid.uuid4())
    crew_id = str(uuid.uuid4())
    agent_id = str(uuid.uuid4())

    flow_config = {'startingPoints': [{'taskId': task_id, 'crewId': crew_id}]}

    crew = MagicMock()
    crew.name = "Crew"
    crew.edges = []
    crew.tool_configs = {}
    crew.task_ids = [task_id]
    crew.nodes = [
        {
            'id': f'task-{task_id}',
            'type': 'taskNode',
            'data': {'tool_configs': {'MCP_SERVERS': {'servers': ['stale-node']}}},
        }
    ]

    db_task = MagicMock()
    db_task.agent_id = agent_id
    db_task.async_execution = False
    db_task.name = "Task"
    db_task.tool_configs = {'MCP_SERVERS': {'servers': ['current-db']}}

    repos = {
        'crew': AsyncMock(get=AsyncMock(return_value=crew)),
        'task': AsyncMock(get=AsyncMock(return_value=db_task)),
        'agent': AsyncMock(get=AsyncMock(return_value=MagicMock(role="Researcher"))),
    }

    with patch('src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig') as mock_agent_config, \
         patch('src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig') as mock_task_config:
        agent_obj = MagicMock(role="Researcher")
        mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=agent_obj)
        mock_task_config.configure_task = AsyncMock(return_value=MagicMock(agent=agent_obj))

        await FlowProcessorManager.process_starting_points(flow_config, {}, repos)

        _, kwargs = mock_agent_config.configure_agent_and_tools.call_args
        assert kwargs.get('crew_tool_configs', {}).get('MCP_SERVERS') == {"servers": ["current-db"]}
