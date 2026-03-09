"""Unit tests for MCP_SERVERS exclusion in create_task auto-resolution.

When the task's `tool_configs` map contains the key 'MCP_SERVERS', the
auto-resolution logic in `create_task` must skip it because MCP servers are
handled by the dedicated MCPIntegration code path, not the tool factory.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from contextlib import asynccontextmanager

from crewai import Agent, Task

from src.engines.crewai.helpers.task_helpers import create_task


@asynccontextmanager
async def _fake_scoped_session():
    """Async context manager that yields a mock session."""
    yield AsyncMock()


class TestMCPServersExcludedFromAutoResolution:
    """Verify that 'MCP_SERVERS' in tool_configs is never sent to the tool factory."""

    @pytest.fixture(autouse=True)
    def mock_openai_api_key(self, monkeypatch):
        """Set a dummy OPENAI_API_KEY for tests that create CrewAI Agent instances."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-dummy-key-for-unit-tests")

    def _make_agent(self) -> Agent:
        """Create a minimal CrewAI Agent for test use."""
        return Agent(
            role="TestRole",
            goal="Test Goal",
            backstory="Test Backstory",
            verbose=False,
        )

    def _make_task_config(self, *, tools=None, tool_configs=None):
        """Build a minimal valid task_config dict."""
        cfg = {
            "description": "Test description",
            "expected_output": "Test expected output",
        }
        if tools is not None:
            cfg["tools"] = tools
        if tool_configs is not None:
            cfg["tool_configs"] = tool_configs
        return cfg

    def _make_tool_factory(self):
        """Return a mock tool factory that records which tool names were requested.

        create_tool returns None so the tool is not appended to task_tools.
        This avoids CrewAI's BaseTool validation error while still letting us
        verify which tool names the factory was called with.
        """
        factory = MagicMock()
        factory.create_tool.return_value = None
        return factory

    # ----- Helper to run create_task with MCP/session patches -----

    async def _run_create_task(self, task_config, agent, tool_factory=None, tool_service=None):
        """Run create_task with all external I/O patched out."""
        mock_mcp_integration = MagicMock()
        mock_mcp_integration.create_mcp_tools_for_task = AsyncMock(return_value=[])

        mock_mcp_service_cls = MagicMock()

        with patch(
            "src.engines.crewai.tools.mcp_integration.MCPIntegration",
            mock_mcp_integration,
        ), patch(
            "src.services.mcp_service.MCPService",
            mock_mcp_service_cls,
        ), patch(
            "src.db.session.request_scoped_session",
            _fake_scoped_session,
        ):
            task = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=agent,
                tool_factory=tool_factory,
                tool_service=tool_service,
            )
        return task

    # ---- Tests ----

    @pytest.mark.asyncio
    async def test_mcp_servers_key_excluded_from_auto_resolution(self):
        """When tool_configs has MCP_SERVERS + GenieTool, only GenieTool is auto-resolved."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()

        task_config = self._make_task_config(
            tools=[],  # empty -> triggers auto-resolve from tool_configs keys
            tool_configs={
                "MCP_SERVERS": {"servers": [{"name": "s1"}]},
                "GenieTool": {"spaceId": "abc-123"},
            },
        )

        await self._run_create_task(task_config, agent, tool_factory=tool_factory)

        # tool_factory.create_tool should have been called only for GenieTool
        called_tool_names = [
            call.args[0] for call in tool_factory.create_tool.call_args_list
        ]
        assert "GenieTool" in called_tool_names
        assert "MCP_SERVERS" not in called_tool_names

    @pytest.mark.asyncio
    async def test_only_mcp_servers_in_tool_configs_no_auto_resolution(self):
        """When tool_configs contains only MCP_SERVERS, no tools are auto-resolved."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()

        task_config = self._make_task_config(
            tools=[],
            tool_configs={
                "MCP_SERVERS": {"servers": [{"name": "s1"}]},
            },
        )

        await self._run_create_task(task_config, agent, tool_factory=tool_factory)

        # tool_factory.create_tool should never have been called
        tool_factory.create_tool.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_real_tools_with_mcp_servers(self):
        """When tool_configs has MCP_SERVERS + multiple real tools, all real tools are resolved."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()

        task_config = self._make_task_config(
            tools=[],
            tool_configs={
                "MCP_SERVERS": {"servers": []},
                "GenieTool": {"spaceId": "space-1"},
                "SerperDevTool": {"api_key": "test"},
            },
        )

        await self._run_create_task(task_config, agent, tool_factory=tool_factory)

        called_tool_names = [
            call.args[0] for call in tool_factory.create_tool.call_args_list
        ]
        assert "GenieTool" in called_tool_names
        assert "SerperDevTool" in called_tool_names
        assert "MCP_SERVERS" not in called_tool_names
        assert len(called_tool_names) == 2

    @pytest.mark.asyncio
    async def test_empty_tool_configs_no_auto_resolution(self):
        """When tool_configs is empty, no auto-resolution happens."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()

        task_config = self._make_task_config(
            tools=[],
            tool_configs={},
        )

        await self._run_create_task(task_config, agent, tool_factory=tool_factory)

        tool_factory.create_tool.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_tool_configs_key_no_auto_resolution(self):
        """When tool_configs key is absent from task_config, no auto-resolution happens."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()

        task_config = self._make_task_config(tools=[])
        # tool_configs not set at all

        await self._run_create_task(task_config, agent, tool_factory=tool_factory)

        tool_factory.create_tool.assert_not_called()

    @pytest.mark.asyncio
    async def test_tool_configs_with_empty_string_key_is_skipped(self):
        """Keys that are empty strings in tool_configs should also be excluded."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()

        task_config = self._make_task_config(
            tools=[],
            tool_configs={
                "": {"invalid": True},
                "MCP_SERVERS": {"servers": []},
                "ValidTool": {},
            },
        )

        await self._run_create_task(task_config, agent, tool_factory=tool_factory)

        called_tool_names = [
            call.args[0] for call in tool_factory.create_tool.call_args_list
        ]
        assert called_tool_names == ["ValidTool"]

    @pytest.mark.asyncio
    async def test_tools_array_not_empty_skips_auto_resolution(self):
        """When the tools array is non-empty, auto-resolution from tool_configs is skipped
        even if tool_configs contains non-MCP entries."""
        agent = self._make_agent()
        tool_factory = self._make_tool_factory()
        mock_tool_service = AsyncMock()
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})

        task_config = self._make_task_config(
            tools=["existing_tool_id"],
            tool_configs={
                "MCP_SERVERS": {"servers": []},
                "GenieTool": {"spaceId": "abc"},
            },
        )

        # Patch resolve_tool_ids_to_names so the tool_service path works
        with patch(
            "src.engines.crewai.helpers.task_helpers.resolve_tool_ids_to_names",
            new_callable=AsyncMock,
            return_value=["ResolvedTool"],
        ):
            await self._run_create_task(
                task_config, agent, tool_factory=tool_factory, tool_service=mock_tool_service
            )

        # The tool_factory should have been called via the normal resolution path
        # for "ResolvedTool", not via auto-resolution for "GenieTool"
        called_tool_names = [
            call.args[0] for call in tool_factory.create_tool.call_args_list
        ]
        assert "ResolvedTool" in called_tool_names
        # GenieTool should NOT be auto-resolved because tools array was not empty
        assert "GenieTool" not in called_tool_names
        assert "MCP_SERVERS" not in called_tool_names
