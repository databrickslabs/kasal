"""Shared agent tool-sourcing + assembly (common/agent_tools) used by BOTH the
crew path (tool_service id→name) and the flow path (ToolFactory by id + flow
graph). Pins the unified behavior so the two paths can't diverge."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.common.agent_tools import (
    resolve_tool_override,
    resolve_agent_tools,
    add_mcp_tools,
    build_agent_with_tools,
)


class TestResolveToolOverride:
    def test_direct_id_match(self):
        assert resolve_tool_override(MagicMock(), "35", {"35": {"a": 1}}) == {"a": 1}

    def test_title_based_match(self):
        factory = MagicMock()
        factory.get_tool_info.return_value = MagicMock(title="GenieTool")
        assert resolve_tool_override(factory, "35", {"GenieTool": {"spaceId": "s"}}) == {"spaceId": "s"}

    def test_no_match_returns_none(self):
        factory = MagicMock()
        factory.get_tool_info.return_value = None
        assert resolve_tool_override(factory, "35", {"Other": {}}) is None

    def test_empty_configs_returns_none(self):
        assert resolve_tool_override(MagicMock(), "35", {}) is None

    def test_get_tool_info_exception_returns_none(self):
        factory = MagicMock()
        factory.get_tool_info.side_effect = Exception("boom")
        assert resolve_tool_override(factory, "35", {"X": {}}) is None


class TestResolveAgentToolsFlowMode:
    """Flow: no tool_service → create_tool by id, override via resolve_tool_override."""

    @pytest.mark.asyncio
    async def test_creates_by_id_with_override(self):
        fake = MagicMock(name="genie")
        factory = MagicMock()
        factory.create_tool.return_value = fake
        factory.get_tool_info.return_value = MagicMock(title="GenieTool")

        tools = await resolve_agent_tools(
            ["35"], factory, tool_configs={"GenieTool": {"spaceId": "agent-space"}}, tool_service=None
        )
        assert tools == [fake]
        factory.create_tool.assert_called_once_with(
            "35", result_as_answer=False, tool_config_override={"spaceId": "agent-space"}
        )

    @pytest.mark.asyncio
    async def test_no_override_passes_empty_dict(self):
        fake = MagicMock()
        factory = MagicMock()
        factory.create_tool.return_value = fake
        factory.get_tool_info.return_value = None
        tools = await resolve_agent_tools(["10"], factory, tool_configs=None, tool_service=None)
        assert tools == [fake]
        factory.create_tool.assert_called_once_with("10", result_as_answer=False, tool_config_override={})

    @pytest.mark.asyncio
    async def test_no_tool_ids_returns_empty(self):
        assert await resolve_agent_tools([], MagicMock(), tool_service=None) == []

    @pytest.mark.asyncio
    async def test_no_factory_returns_empty(self):
        # Flow mode with no factory → nothing can be created.
        assert await resolve_agent_tools(["x"], None, tool_service=None) == []

    @pytest.mark.asyncio
    async def test_create_tool_exception_skips(self):
        factory = MagicMock()
        factory.get_tool_info.return_value = None
        factory.create_tool.side_effect = Exception("boom")
        tools = await resolve_agent_tools(["10"], factory, tool_service=None)
        assert tools == []


class TestResolveAgentToolsCrewMode:
    """Crew: tool_service maps ids→names + supplies result_as_answer."""

    @pytest.mark.asyncio
    async def test_maps_names_and_result_as_answer(self):
        svc = MagicMock()
        svc.get_tool_config_by_name = AsyncMock(return_value={"result_as_answer": True})
        fake = MagicMock()
        factory = MagicMock()
        factory.create_tool.return_value = fake
        with patch("src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock, return_value=["GenieTool"]):
            tools = await resolve_agent_tools(
                ["35"], factory, tool_configs={"GenieTool": {"spaceId": "s"}}, tool_service=svc
            )
        assert tools == [fake]
        factory.create_tool.assert_called_once_with(
            "GenieTool", result_as_answer=True, tool_config_override={"spaceId": "s"}
        )

    @pytest.mark.asyncio
    async def test_mcp_tuple_expanded(self):
        sub1, sub2 = MagicMock(), MagicMock()
        svc = MagicMock()
        svc.get_tool_config_by_name = AsyncMock(return_value={})
        factory = MagicMock()
        factory.create_tool.return_value = (True, [sub1, sub2])
        with patch("src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock, return_value=["MCPTool"]):
            tools = await resolve_agent_tools(["x"], factory, tool_service=svc)
        assert sub1 in tools and sub2 in tools

    @pytest.mark.asyncio
    async def test_no_factory_falls_back_to_names(self):
        svc = MagicMock()
        with patch("src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock, return_value=["SomeTool"]):
            tools = await resolve_agent_tools(["id"], None, tool_service=svc)
        assert tools == ["SomeTool"]

    @pytest.mark.asyncio
    async def test_resolution_exception_does_not_raise(self):
        svc = MagicMock()
        factory = MagicMock()
        with patch("src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock, side_effect=Exception("boom")):
            tools = await resolve_agent_tools(["id"], factory, tool_service=svc)
        assert tools == []


class TestAddMcpTools:
    @pytest.mark.asyncio
    async def test_no_servers_skips_session(self):
        # No MCP servers → returns [] without opening a DB session.
        with patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mcp:
            mcp._extract_mcp_servers_from_config.return_value = []
            tools = await add_mcp_tools({"tool_configs": {}}, "agent", {})
        assert tools == []

    @pytest.mark.asyncio
    async def test_never_raises(self):
        # A broken config must not raise.
        assert await add_mcp_tools(None, "agent", None) == []


class TestBuildAgentWithTools:
    @pytest.mark.asyncio
    async def test_combines_base_mcp_resolved_then_builds(self):
        base = MagicMock(name="base_tool")
        resolved = MagicMock(name="resolved_tool")
        factory = MagicMock()
        factory.create_tool.return_value = resolved
        factory.get_tool_info.return_value = None
        agent_obj = MagicMock()

        with patch("src.engines.crewai.common.agent_tools.add_mcp_tools",
                   new_callable=AsyncMock, return_value=[]), \
             patch("src.engines.crewai.common.agent_tools.build_agent",
                   new_callable=AsyncMock, return_value=agent_obj) as mock_build:
            out = await build_agent_with_tools(
                {"role": "R", "goal": "G", "backstory": "B"},
                group_id="g1",
                default_model="m",
                label="A",
                base_tools=[base],
                tool_ids=["10"],
                tool_factory=factory,
                tool_service=None,
            )
        assert out is agent_obj
        # build_agent received base + resolved tools.
        passed_tools = mock_build.call_args.args[1]
        assert base in passed_tools and resolved in passed_tools
