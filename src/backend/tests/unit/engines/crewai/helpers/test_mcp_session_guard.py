"""Regression tests for PERF-027: agent/task helpers must not open a DB
session + MCPService when the config declares no MCP servers (the integration
returns [] without touching the service — the session was pure waste,
N_agents + N_tasks session opens per execution)."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _no_session_patch():
    return patch(
        "src.db.session.request_scoped_session",
        side_effect=AssertionError("DB session opened despite no MCP servers"),
    )


@pytest.mark.asyncio
async def test_agent_helper_skips_session_without_mcp_servers():
    from src.engines.crewai.helpers.agent_adapter import create_agent

    mock_llm = MagicMock()
    with patch(
        "src.engines.crewai.common.agent_builder.Agent", return_value=MagicMock()
    ), patch(
        "src.core.llm_manager.LLMManager.configure_crewai_llm",
        new_callable=AsyncMock, return_value=mock_llm,
    ), _no_session_patch():
        agent = await create_agent(
            agent_key="a1",
            agent_config={"role": "R", "goal": "G", "backstory": "B",
                          "tool_configs": {}},  # no MCP servers
            config={"group_id": "g1"},
        )
    assert agent is not None


@pytest.mark.asyncio
async def test_task_helper_skips_session_without_mcp_servers():
    from src.engines.crewai.helpers import task_adapter

    mock_agent = MagicMock()
    mock_agent.role = "R"

    with patch.object(task_adapter, "Task", return_value=MagicMock()), \
         _no_session_patch():
        task = await task_adapter.create_task(
            task_key="t1",
            task_config={"description": "d", "expected_output": "o",
                         "tool_configs": {}},
            agent=mock_agent,
            config={"group_id": "g1"},
        )
    assert task is not None
