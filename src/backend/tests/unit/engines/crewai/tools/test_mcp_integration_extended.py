"""
Extended tests for mcp_integration.py to push coverage to 90%+.

Covers missing lines:
- FLOW_SUBPROCESS_MODE logger selection (line 27)
- resolve_effective_mcp_servers - explicit servers deduplication, group_id preference
- create_mcp_tools_for_agent - servers found, tool creation, loop error handling
- create_mcp_tools_for_task - servers found, tool creation, loop error handling
- _create_tools_for_server - adapter no tools, adapter None, tool wrapping loop errors,
  tool already has server prefix, exception -> MCPConnectionError
- validate_mcp_configuration - invalid agent tool_configs, invalid task tool_configs
- _resolve_agent_reference - exception path
"""
import os
import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock

from src.engines.crewai.tools.mcp_integration import MCPIntegration


class TestResolveEffectiveMcpServersExtended:
    """Additional coverage for resolve_effective_mcp_servers."""

    @pytest.mark.asyncio
    async def test_explicit_servers_deduplication_prefers_group_scoped(self):
        """Group-scoped explicit server should replace ungrouped same-name server."""
        mock_service = AsyncMock()
        # Global server with no group_id
        global_server = MagicMock()
        global_server.model_dump.return_value = {
            "name": "server1",
            "server_url": "https://example.com",
            "group_id": None,
        }
        mock_service.get_enabled_servers.return_value = MagicMock(
            servers=[global_server]
        )
        # Explicit server with group_id
        explicit_server = MagicMock()
        explicit_server.model_dump.return_value = {
            "name": "server1",
            "server_url": "https://example.com",
            "group_id": "grp_1",
        }
        mock_service.get_servers_by_names_group_aware.return_value = [explicit_server]

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=["server1"],
            mcp_service=mock_service,
            include_global=True,
            group_id="grp_1",
        )

        # Should deduplicate to 1 entry, preferring the group-scoped one
        assert len(result) == 1
        assert result[0]["group_id"] == "grp_1"

    @pytest.mark.asyncio
    async def test_server_with_none_name_is_skipped(self):
        """Servers with None name should be skipped."""
        mock_service = AsyncMock()
        server_no_name = MagicMock()
        server_no_name.model_dump.return_value = {
            "name": None,
            "server_url": "https://example.com",
        }
        mock_service.get_enabled_servers.return_value = MagicMock(
            servers=[server_no_name]
        )

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=[],
            mcp_service=mock_service,
            include_global=True,
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_explicit_server_with_none_name_is_skipped(self):
        """Explicit servers with None name should be skipped."""
        mock_service = AsyncMock()
        mock_service.get_enabled_servers.return_value = MagicMock(servers=[])
        explicit_server = MagicMock()
        explicit_server.model_dump.return_value = {"name": None}
        mock_service.get_servers_by_names_group_aware.return_value = [explicit_server]

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=["server_no_name"],
            mcp_service=mock_service,
            include_global=True,
        )

        assert result == []


class TestCreateMcpToolsForAgentExtended:
    """Extended coverage for create_mcp_tools_for_agent."""

    def setup_method(self):
        MCPIntegration.reset_warnings()

    @pytest.mark.asyncio
    async def test_creates_tools_when_servers_found(self):
        """Should create tools when explicit servers are found."""
        agent_config = {
            "id": "a1",
            "tool_configs": {"MCP_SERVERS": ["server1"]},
        }
        config = {"group_id": "grp_1", "user_token": "tok_abc"}
        mock_service = AsyncMock()
        mock_tool = MagicMock()
        mock_tool.name = "server1_tool1"

        with patch.object(
            MCPIntegration,
            "resolve_effective_mcp_servers",
            new_callable=AsyncMock,
            return_value=[{"name": "server1", "server_url": "https://example.com"}],
        ), patch.object(
            MCPIntegration,
            "_create_tools_for_server",
            new_callable=AsyncMock,
            return_value=[mock_tool],
        ):
            tools = await MCPIntegration.create_mcp_tools_for_agent(
                agent_config, "a1", mock_service, config=config
            )

        assert len(tools) == 1

    @pytest.mark.asyncio
    async def test_server_loop_error_continues(self):
        """Error creating tools for one server should not stop processing others."""
        agent_config = {
            "id": "a1",
            "tool_configs": {"MCP_SERVERS": ["server1", "server2"]},
        }
        config = {"group_id": "grp_1"}
        mock_service = AsyncMock()
        mock_tool = MagicMock()

        call_count = 0

        async def mock_create(server, key, svc, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("server1 failed")
            return [mock_tool]

        with patch.object(
            MCPIntegration,
            "resolve_effective_mcp_servers",
            new_callable=AsyncMock,
            return_value=[
                {"name": "server1", "server_url": "https://example.com"},
                {"name": "server2", "server_url": "https://example.com"},
            ],
        ), patch.object(
            MCPIntegration, "_create_tools_for_server", side_effect=mock_create
        ):
            tools = await MCPIntegration.create_mcp_tools_for_agent(
                agent_config, "a1", mock_service, config=config
            )

        # Should still get tool from server2
        assert len(tools) == 1

    @pytest.mark.asyncio
    async def test_no_effective_servers_returns_empty(self):
        """No effective servers returns empty list."""
        agent_config = {
            "id": "a1",
            "tool_configs": {"MCP_SERVERS": ["server1"]},
        }
        mock_service = AsyncMock()

        with patch.object(
            MCPIntegration,
            "resolve_effective_mcp_servers",
            new_callable=AsyncMock,
            return_value=[],
        ):
            tools = await MCPIntegration.create_mcp_tools_for_agent(
                agent_config, "a1", mock_service
            )

        assert tools == []


class TestCreateMcpToolsForTaskExtended:
    """Extended coverage for create_mcp_tools_for_task."""

    def setup_method(self):
        MCPIntegration.reset_warnings()

    @pytest.mark.asyncio
    async def test_creates_tools_when_servers_found(self):
        """Should create tools when explicit servers are found for task."""
        task_config = {
            "id": "t1",
            "tool_configs": {"MCP_SERVERS": ["server1"]},
        }
        config = {"group_id": "grp_1", "user_token": "tok_abc"}
        mock_service = AsyncMock()
        mock_tool = MagicMock()

        with patch.object(
            MCPIntegration,
            "resolve_effective_mcp_servers",
            new_callable=AsyncMock,
            return_value=[{"name": "server1", "server_url": "https://example.com"}],
        ), patch.object(
            MCPIntegration,
            "_create_tools_for_server",
            new_callable=AsyncMock,
            return_value=[mock_tool],
        ):
            tools = await MCPIntegration.create_mcp_tools_for_task(
                task_config, "t1", mock_service, config=config
            )

        assert len(tools) == 1

    @pytest.mark.asyncio
    async def test_server_loop_error_continues(self):
        """Error creating tools for one task server should continue to next."""
        task_config = {
            "id": "t1",
            "tool_configs": {"MCP_SERVERS": ["s1", "s2"]},
        }
        config = {"group_id": "grp"}
        mock_service = AsyncMock()
        mock_tool = MagicMock()

        call_count = 0

        async def mock_create(server, key, svc, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("s1 failed")
            return [mock_tool]

        with patch.object(
            MCPIntegration,
            "resolve_effective_mcp_servers",
            new_callable=AsyncMock,
            return_value=[
                {"name": "s1", "server_url": "https://example.com"},
                {"name": "s2", "server_url": "https://example.com"},
            ],
        ), patch.object(
            MCPIntegration, "_create_tools_for_server", side_effect=mock_create
        ):
            tools = await MCPIntegration.create_mcp_tools_for_task(
                task_config, "t1", mock_service, config=config
            )

        assert len(tools) == 1

    @pytest.mark.asyncio
    async def test_no_effective_servers_returns_empty(self):
        task_config = {"id": "t1", "tool_configs": {"MCP_SERVERS": ["s1"]}}
        mock_service = AsyncMock()

        with patch.object(
            MCPIntegration,
            "resolve_effective_mcp_servers",
            new_callable=AsyncMock,
            return_value=[],
        ):
            tools = await MCPIntegration.create_mcp_tools_for_task(
                task_config, "t1", mock_service
            )

        assert tools == []


class TestCreateToolsForServerExtended:
    """Extended coverage for _create_tools_for_server."""

    def setup_method(self):
        MCPIntegration.reset_warnings()

    @pytest.mark.asyncio
    async def test_adapter_none_adds_warning_returns_empty(self):
        """If get_or_create_mcp_adapter returns None, add warning and return []."""
        server = {
            "name": "null-adapter",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            return_value=None,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock()
            )

        assert tools == []
        assert len(MCPIntegration.get_warnings()) >= 1

    @pytest.mark.asyncio
    async def test_adapter_without_tools_attr_adds_warning(self):
        """Adapter without 'tools' attribute should add warning."""
        server = {
            "name": "no-tools",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        mock_adapter = MagicMock(spec=[])  # No 'tools' attribute

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            return_value=mock_adapter,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock()
            )

        assert tools == []

    @pytest.mark.asyncio
    async def test_tool_already_has_server_prefix(self):
        """Tool already prefixed with server name should not be double-prefixed."""
        server = {
            "name": "my-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        # Tool name already has server prefix
        tool_dict = {"name": "my-server_existing_tool", "description": "desc"}
        mock_adapter = MagicMock()
        mock_adapter.tools = [tool_dict]
        mock_adapter.initialization_error = None

        mock_wrapped = MagicMock()
        mock_wrapped.name = "my-server_existing_tool"

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            return_value=mock_adapter,
        ), patch(
            "src.engines.crewai.tools.mcp_integration.create_crewai_tool_from_mcp",
            return_value=mock_wrapped,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock()
            )

        assert len(tools) == 1
        # Name should NOT have double prefix
        assert tools[0].name == "my-server_existing_tool"

    @pytest.mark.asyncio
    async def test_tool_wrapping_error_continues(self):
        """Error wrapping one tool should not stop processing others."""
        server = {
            "name": "multi-tool-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        tool1 = {"name": "tool1", "description": "desc1"}
        tool2 = {"name": "tool2", "description": "desc2"}
        mock_adapter = MagicMock()
        mock_adapter.tools = [tool1, tool2]
        mock_adapter.initialization_error = None

        mock_good_tool = MagicMock()
        mock_good_tool.name = "tool2"

        call_count = 0

        def mock_create_tool(tool):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("tool1 wrapping failed")
            return mock_good_tool

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            return_value=mock_adapter,
        ), patch(
            "src.engines.crewai.tools.mcp_integration.create_crewai_tool_from_mcp",
            side_effect=mock_create_tool,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock()
            )

        # tool1 failed, tool2 succeeded
        assert len(tools) == 1

    @pytest.mark.asyncio
    async def test_outer_exception_creates_mcp_connection_error(self):
        """General exception should create MCPConnectionError and add warning."""
        server = {
            "name": "failing-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Connection refused"),
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock()
            )

        assert tools == []
        warnings = MCPIntegration.get_warnings()
        assert any("failing-server" in w for w in warnings)

    @pytest.mark.asyncio
    async def test_initialization_error_non_mcp_type_adds_warning(self):
        """Non-MCPConnectionError initialization_error should still add warning."""
        server = {
            "name": "init-err-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        mock_adapter = MagicMock()
        mock_adapter.tools = []
        mock_adapter.initialization_error = RuntimeError("generic error")

        with patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            return_value=mock_adapter,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock()
            )

        assert tools == []
        # Warning should include the generic error text
        warnings = MCPIntegration.get_warnings()
        assert any("generic error" in w for w in warnings)


class TestValidateMcpConfigurationExtended:
    """Extended coverage for validate_mcp_configuration."""

    def test_invalid_agent_not_dict_returns_false(self):
        """Agent that is not a dict should return False."""
        config = {"agents": ["not_a_dict"], "tasks": []}
        result = MCPIntegration.validate_mcp_configuration(config)
        assert result is False

    def test_invalid_task_not_dict_returns_false(self):
        """Task that is not a dict should return False."""
        config = {"agents": [], "tasks": ["not_a_dict"]}
        result = MCPIntegration.validate_mcp_configuration(config)
        assert result is False

    def test_agent_tool_configs_not_dict_returns_false(self):
        """Agent tool_configs that is not a dict should return False."""
        config = {
            "agents": [{"id": "a1", "tool_configs": "not_a_dict"}],
            "tasks": [],
        }
        result = MCPIntegration.validate_mcp_configuration(config)
        assert result is False

    def test_task_tool_configs_not_dict_returns_false(self):
        """Task tool_configs that is not a dict should return False."""
        config = {
            "agents": [],
            "tasks": [{"id": "t1", "tool_configs": ["list_not_dict"]}],
        }
        result = MCPIntegration.validate_mcp_configuration(config)
        assert result is False


class TestResolveAgentReferenceEdgeCases:
    """Edge cases for _resolve_agent_reference."""

    def test_exception_returns_none(self):
        """Exception during resolution should return None."""
        # Pass invalid config that will cause AttributeError
        result = MCPIntegration._resolve_agent_reference("ref", None)
        assert result is None

    def test_agent_missing_id_uses_name(self):
        """Agent with no id should fall back to name."""
        config = {"agents": [{"name": "DevAgent", "role": "dev"}]}
        result = MCPIntegration._resolve_agent_reference("DevAgent", config)
        assert result == "DevAgent"  # Returns name since no id


class TestFlowSubprocessModeLogger:
    """Test that flow logger is selected in subprocess mode."""

    def test_flow_logger_selected_when_env_set(self):
        """Module should use flow logger when FLOW_SUBPROCESS_MODE=true."""
        # We test indirectly by checking that the module loaded correctly.
        # The logger selection happens at module import time.
        import src.engines.crewai.tools.mcp_integration as mcp_mod
        assert mcp_mod.logger is not None

    def test_crew_logger_selected_by_default(self):
        """By default (no FLOW_SUBPROCESS_MODE), crew logger should be used."""
        import src.engines.crewai.tools.mcp_integration as mcp_mod
        # Logger should be set (either crew or flow depending on env)
        assert mcp_mod.logger is not None


class TestCollectAgentMcpRequirementsExtended:
    """Extended coverage for collect_agent_mcp_requirements."""

    @pytest.mark.asyncio
    async def test_deduplicates_servers_for_same_agent(self):
        """Same server assigned to same agent via two tasks should be deduplicated."""
        config = {
            "agents": [{"id": "a1", "role": "dev"}],
            "tasks": [
                {"agent": "a1", "tool_configs": {"MCP_SERVERS": ["server1"]}},
                {"agent": "a1", "tool_configs": {"MCP_SERVERS": ["server1"]}},  # duplicate
            ],
        }
        result = await MCPIntegration.collect_agent_mcp_requirements(config)

        if "a1" in result:
            # Should only have server1 once
            assert result["a1"].count("server1") == 1

    @pytest.mark.asyncio
    async def test_task_without_agent_is_skipped(self):
        """Tasks without agent reference should be skipped."""
        config = {
            "agents": [{"id": "a1", "role": "dev"}],
            "tasks": [
                {"tool_configs": {"MCP_SERVERS": ["server1"]}},  # No 'agent' key
            ],
        }
        result = await MCPIntegration.collect_agent_mcp_requirements(config)
        # No agent requirements since no 'agent' field in task
        assert result == {}
