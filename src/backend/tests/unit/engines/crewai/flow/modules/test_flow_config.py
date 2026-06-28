"""
Unit tests for src/engines/crewai/flow/modules/flow_config.py.

Covers:
  - FlowConfigManager.collect_agent_mcp_requirements()

All repositories are mocked with AsyncMock. No real DB, no real logger calls.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task(
    task_id: str = "t1",
    name: str = "MyTask",
    agent_id: str | None = "agent-1",
    tool_configs: dict | None = None,
):
    """Build a minimal task data object."""
    t = MagicMock()
    t.id = task_id
    t.name = name
    t.agent_id = agent_id
    t.tool_configs = tool_configs
    return t


def _make_crew(nodes: list | None = None, edges: list | None = None):
    """Build a minimal crew data object."""
    c = MagicMock()
    c.nodes = nodes or []
    c.edges = edges or []
    return c


def _make_repos(task_data=None, crew_data=None):
    """Build repositories dict with mocked async get() methods."""
    task_repo = MagicMock()
    task_repo.get = AsyncMock(return_value=task_data)

    crew_repo = MagicMock()
    crew_repo.get = AsyncMock(return_value=crew_data)

    return {"task": task_repo, "crew": crew_repo}


# ---------------------------------------------------------------------------
# No repositories provided
# ---------------------------------------------------------------------------


class TestCollectAgentMcpRequirementsNoRepos:

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_repositories_none(self):
        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager

        result = await FlowConfigManager.collect_agent_mcp_requirements(
            {"startingPoints": [], "listeners": []}, None
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_task_repo_absent(self):
        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager

        result = await FlowConfigManager.collect_agent_mcp_requirements(
            {"startingPoints": [], "listeners": []}, {"crew": MagicMock()}
        )
        assert result == {}


# ---------------------------------------------------------------------------
# Empty flow config
# ---------------------------------------------------------------------------


class TestCollectAgentMcpRequirementsEmptyFlow:

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_tasks_in_flow(self):
        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager

        repos = _make_repos()
        result = await FlowConfigManager.collect_agent_mcp_requirements(
            {"startingPoints": [], "listeners": []}, repos
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_flow_config_is_empty(self):
        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager

        repos = _make_repos()
        result = await FlowConfigManager.collect_agent_mcp_requirements({}, repos)
        assert result == {}


# ---------------------------------------------------------------------------
# Task with agent_id — resolved directly
# ---------------------------------------------------------------------------


class TestCollectAgentMcpRequirementsDirectAgentId:

    @pytest.mark.asyncio
    async def test_single_task_with_mcp_servers_and_agent_id(self):
        task = _make_task(
            task_id="t1",
            agent_id="agent-abc",
            tool_configs={"MCP_SERVERS": {"servers": ["server_A", "server_B"]}},
        )
        repos = _make_repos(task_data=task)

        flow_config = {
            "startingPoints": [{"taskId": "t1", "crewId": "c1", "taskName": "Task One"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager

        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert "agent-abc" in result
        assert "server_A" in result["agent-abc"]
        assert "server_B" in result["agent-abc"]

    @pytest.mark.asyncio
    async def test_mcp_servers_as_list_in_tool_configs(self):
        task = _make_task(
            task_id="t2",
            agent_id="agent-list",
            tool_configs={"MCP_SERVERS": ["server_X", "server_Y"]},
        )
        repos = _make_repos(task_data=task)

        flow_config = {
            "startingPoints": [{"taskId": "t2", "crewId": "c2"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert "server_X" in result["agent-list"]
        assert "server_Y" in result["agent-list"]

    @pytest.mark.asyncio
    async def test_duplicate_servers_are_not_added_twice(self):
        """Same MCP server from two tasks under the same agent should appear once."""
        task1 = _make_task(
            task_id="t1",
            agent_id="agent-dup",
            tool_configs={"MCP_SERVERS": {"servers": ["dup_server"]}},
        )
        task2 = _make_task(
            task_id="t2",
            agent_id="agent-dup",
            tool_configs={"MCP_SERVERS": {"servers": ["dup_server"]}},
        )

        task_repo = MagicMock()
        task_repo.get = AsyncMock(side_effect=lambda tid: task1 if tid == "t1" else task2)
        repos = {"task": task_repo, "crew": MagicMock()}

        flow_config = {
            "startingPoints": [
                {"taskId": "t1", "crewId": "c1"},
                {"taskId": "t2", "crewId": "c1"},
            ],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert result["agent-dup"].count("dup_server") == 1

    @pytest.mark.asyncio
    async def test_task_without_tool_configs_skipped(self):
        task = _make_task(task_id="t3", agent_id="agent-no-cfg", tool_configs=None)
        repos = _make_repos(task_data=task)

        flow_config = {
            "startingPoints": [{"taskId": "t3", "crewId": "c3"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert result == {}

    @pytest.mark.asyncio
    async def test_task_with_empty_mcp_servers_list_skipped(self):
        task = _make_task(
            task_id="t4",
            agent_id="agent-empty-mcp",
            tool_configs={"MCP_SERVERS": {"servers": []}},
        )
        repos = _make_repos(task_data=task)

        flow_config = {
            "startingPoints": [{"taskId": "t4", "crewId": "c4"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert result == {}


# ---------------------------------------------------------------------------
# Task not found in DB
# ---------------------------------------------------------------------------


class TestCollectAgentMcpRequirementsTaskNotFound:

    @pytest.mark.asyncio
    async def test_task_not_in_db_returns_empty(self):
        repos = _make_repos(task_data=None)

        flow_config = {
            "startingPoints": [{"taskId": "missing-task", "crewId": "c5"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert result == {}


# ---------------------------------------------------------------------------
# Task from listener
# ---------------------------------------------------------------------------


class TestCollectAgentMcpRequirementsFromListeners:

    @pytest.mark.asyncio
    async def test_listener_tasks_are_processed(self):
        task = _make_task(
            task_id="lt1",
            agent_id="agent-listener",
            tool_configs={"MCP_SERVERS": {"servers": ["listener_srv"]}},
        )
        repos = _make_repos(task_data=task)

        flow_config = {
            "startingPoints": [],
            "listeners": [
                {
                    "crewId": "c6",
                    "tasks": [{"id": "lt1", "name": "Listener Task"}],
                }
            ],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert "agent-listener" in result
        assert "listener_srv" in result["agent-listener"]


# ---------------------------------------------------------------------------
# Crew-structure resolution (no agent_id on task)
# ---------------------------------------------------------------------------


class TestCollectAgentMcpRequirementsCrewResolution:

    @pytest.mark.asyncio
    async def test_resolves_agent_id_from_crew_structure(self):
        """When task.agent_id is None, agent should be resolved from crew nodes/edges."""
        task_id = "task-no-agent"
        task = _make_task(
            task_id=task_id,
            agent_id=None,
            tool_configs={"MCP_SERVERS": {"servers": ["crew_srv"]}},
        )

        # Build crew with matching nodes and edges
        task_node = {"id": f"taskNode-{task_id}", "type": "taskNode"}
        agent_node = {"id": "agentNode-agent-resolved", "type": "agentNode"}
        edge = {"source": "agentNode-agent-resolved", "target": f"taskNode-{task_id}"}
        crew = _make_crew(nodes=[task_node, agent_node], edges=[edge])

        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task)
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=crew)
        repos = {"task": task_repo, "crew": crew_repo}

        flow_config = {
            "startingPoints": [{"taskId": task_id, "crewId": "c7"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        # Agent resolved from crew structure
        assert "agent-resolved" in result
        assert "crew_srv" in result["agent-resolved"]

    @pytest.mark.asyncio
    async def test_crew_not_found_logs_warning_and_skips(self):
        """If crew is not found, the task's MCP servers are silently skipped."""
        task = _make_task(task_id="t-no-crew", agent_id=None,
                          tool_configs={"MCP_SERVERS": {"servers": ["s1"]}})

        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task)
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=None)
        repos = {"task": task_repo, "crew": crew_repo}

        flow_config = {
            "startingPoints": [{"taskId": "t-no-crew", "crewId": "missing-crew"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        assert result == {}

    @pytest.mark.asyncio
    async def test_exception_in_task_processing_is_caught(self):
        """An exception from task_repo.get() should not crash the whole function."""
        task_repo = MagicMock()
        task_repo.get = AsyncMock(side_effect=RuntimeError("DB boom"))
        repos = {"task": task_repo}

        flow_config = {
            "startingPoints": [{"taskId": "t-exc", "crewId": "c-exc"}],
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        # Should return empty rather than propagate
        assert result == {}

    @pytest.mark.asyncio
    async def test_group_context_accepted_without_error(self):
        """group_context param should be accepted even if not used."""
        repos = _make_repos()
        flow_config = {"startingPoints": [], "listeners": []}

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(
            flow_config, repos, group_context=MagicMock()
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_task_without_crew_id_skips_crew_resolution(self):
        """Task with MCP servers but no crew_id should be skipped gracefully."""
        task = _make_task(task_id="t-no-crew-id", agent_id=None,
                          tool_configs={"MCP_SERVERS": {"servers": ["s2"]}})

        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task)
        repos = {"task": task_repo, "crew": MagicMock()}

        flow_config = {
            "startingPoints": [{"taskId": "t-no-crew-id"}],  # No crewId key
            "listeners": [],
        }

        from src.engines.crewai.paths.flow.modules.flow_config import FlowConfigManager
        result = await FlowConfigManager.collect_agent_mcp_requirements(flow_config, repos)

        # No agent resolved, so result is empty
        assert result == {}
