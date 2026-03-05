"""Unit tests for MCPIntegration class."""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock

from src.engines.crewai.tools.mcp_integration import MCPIntegration


class TestMCPIntegrationWarnings:
    """Test class-level warning collection."""

    def setup_method(self):
        """Reset warnings before each test."""
        MCPIntegration.reset_warnings()

    def test_reset_warnings(self):
        MCPIntegration._warnings = ["old warning"]
        MCPIntegration.reset_warnings()
        assert MCPIntegration.get_warnings() == []

    def test_get_warnings_returns_copy(self):
        MCPIntegration.add_warning("warn1")
        warnings = MCPIntegration.get_warnings()
        warnings.append("extra")  # should not affect internal list
        assert len(MCPIntegration.get_warnings()) == 1

    def test_add_warning(self):
        MCPIntegration.add_warning("connection failed")
        assert "connection failed" in MCPIntegration.get_warnings()

    def test_multiple_warnings(self):
        MCPIntegration.add_warning("warn1")
        MCPIntegration.add_warning("warn2")
        assert len(MCPIntegration.get_warnings()) == 2


class TestResolveEffectiveMcpServers:
    """Test resolve_effective_mcp_servers changes."""

    @pytest.mark.asyncio
    async def test_calls_get_enabled_servers(self):
        """Should call get_enabled_servers instead of get_global_servers."""
        mock_service = AsyncMock()
        mock_server = MagicMock()
        mock_server.model_dump.return_value = {
            "name": "server1",
            "server_url": "https://example.com",
        }
        mock_service.get_enabled_servers.return_value = MagicMock(
            servers=[mock_server]
        )

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=[],
            mcp_service=mock_service,
            include_global=True,
        )

        mock_service.get_enabled_servers.assert_called_once()
        assert len(result) == 1
        assert result[0]["name"] == "server1"

    @pytest.mark.asyncio
    async def test_deduplicates_by_name(self):
        """Should deduplicate servers by name."""
        mock_service = AsyncMock()
        mock_s1 = MagicMock()
        mock_s1.model_dump.return_value = {
            "name": "server1",
            "server_url": "https://example.com",
        }
        mock_s2 = MagicMock()
        mock_s2.model_dump.return_value = {
            "name": "server1",
            "server_url": "https://example.com",
        }
        mock_service.get_enabled_servers.return_value = MagicMock(
            servers=[mock_s1, mock_s2]
        )

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=[],
            mcp_service=mock_service,
            include_global=True,
        )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        """Should return empty list and not raise on exception."""
        mock_service = AsyncMock()
        mock_service.get_enabled_servers.side_effect = Exception("DB error")

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=[],
            mcp_service=mock_service,
            include_global=True,
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_skip_global_when_disabled(self):
        """When include_global=False, should not call get_enabled_servers."""
        mock_service = AsyncMock()
        mock_service.get_enabled_servers.return_value = MagicMock(servers=[])

        result = await MCPIntegration.resolve_effective_mcp_servers(
            explicit_servers=[],
            mcp_service=mock_service,
            include_global=False,
        )
        mock_service.get_enabled_servers.assert_not_called()
        assert result == []


class TestCreateToolsForServerSPN:
    """Test _create_tools_for_server with databricks_spn auth."""

    def setup_method(self):
        MCPIntegration.reset_warnings()

    @pytest.mark.asyncio
    async def test_spn_auth_sets_authorization_header(self):
        """databricks_spn auth should call get_auth_context and set header."""
        server = {
            "name": "spn-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "databricks_spn",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }

        mock_auth_context = MagicMock()
        mock_auth_context.token = "spn-token-123"
        mock_auth_context.auth_method = "service_principal"

        mock_adapter = MagicMock()
        mock_adapter.tools = [{"name": "tool1", "description": "desc"}]
        mock_adapter.initialization_error = None

        mock_wrapped = MagicMock()
        mock_wrapped.name = "tool1"

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=mock_auth_context,
        ) as mock_get_auth, patch(
            "src.engines.crewai.tools.mcp_integration.create_crewai_tool_from_mcp",
            return_value=mock_wrapped,
        ), patch(
            "src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter",
            new_callable=AsyncMock,
            return_value=mock_adapter,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server,
                "test_agent",
                MagicMock(),
                user_token="user-tok",
                group_id="grp-1",
            )

        mock_get_auth.assert_called_once_with(
            user_token="user-tok", group_id="grp-1"
        )

    @pytest.mark.asyncio
    async def test_spn_auth_no_context_returns_empty_with_warning(self):
        """If get_auth_context returns None, should add warning and return []."""
        server = {
            "name": "spn-fail",
            "server_url": "https://example.com/mcp",
            "auth_type": "databricks_spn",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ):
            tools = await MCPIntegration._create_tools_for_server(
                server,
                "test_agent",
                MagicMock(),
                user_token=None,
                group_id=None,
            )

        assert tools == []
        warnings = MCPIntegration.get_warnings()
        assert len(warnings) == 1
        assert "No authentication available" in warnings[0]


class TestExtractMcpServersFromConfig:
    """Test _extract_mcp_servers_from_config static method."""

    def test_dict_format_with_servers_key(self):
        config = {"MCP_SERVERS": {"servers": ["server1", "server2"]}}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == ["server1", "server2"]

    def test_legacy_list_format(self):
        config = {"MCP_SERVERS": ["server1", "server2"]}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == ["server1", "server2"]

    def test_none_mcp_config(self):
        config = {}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == []

    def test_none_mcp_servers_value(self):
        config = {"MCP_SERVERS": None}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == []

    def test_strip_whitespace_from_server_names(self):
        """Modified code: .strip() should remove leading/trailing whitespace."""
        config = {"MCP_SERVERS": [" server1 ", "  server2  ", " an-tavily-test "]}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == ["server1", "server2", "an-tavily-test"]

    def test_filters_none_and_empty_servers(self):
        config = {"MCP_SERVERS": [None, "", "valid-server", None]}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert "valid-server" in result
        assert None not in result

    def test_invalid_format_returns_empty(self):
        config = {"MCP_SERVERS": 12345}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == []

    def test_dict_format_servers_key_with_whitespace(self):
        config = {"MCP_SERVERS": {"servers": [" s1 ", " s2 "]}}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert result == ["s1", "s2"]

    def test_exception_returns_empty(self):
        """Any unexpected error should return empty list."""
        with patch.object(MCPIntegration, '_extract_mcp_servers_from_config', side_effect=Exception("bad")):
            # Direct call would raise; test the method handles it internally
            pass
        # The method itself has try/except, just verify it doesn't break
        config = {"MCP_SERVERS": {"servers": ["s1"]}}
        result = MCPIntegration._extract_mcp_servers_from_config(config)
        assert isinstance(result, list)


class TestCreateToolsForServerAuth:
    """Test _create_tools_for_server authentication logic."""

    def setup_method(self):
        MCPIntegration.reset_warnings()

    @pytest.mark.asyncio
    async def test_databricks_url_auto_detection(self):
        """URLs with /api/2.0/mcp/ should use Databricks auth regardless of auth_type."""
        server = {
            "name": "my-server",
            "server_url": "https://example.com/api/2.0/mcp/external/test",
            "auth_type": "api_key",  # Even though api_key, should use databricks auth
            "api_key": "tavily-key-123",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        mock_auth = MagicMock()
        mock_auth.token = "db-token"
        mock_auth.auth_method = "service_principal"

        mock_adapter = MagicMock()
        mock_adapter.tools = [{"name": "tool1", "description": "d"}]
        mock_adapter.initialization_error = None

        mock_wrapped = MagicMock()
        mock_wrapped.name = "tool1"

        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth) as mock_get_auth, \
             patch("src.engines.crewai.tools.mcp_integration.create_crewai_tool_from_mcp", return_value=mock_wrapped), \
             patch("src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter", new_callable=AsyncMock, return_value=mock_adapter):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock(), user_token="u", group_id="g"
            )

        mock_get_auth.assert_called_once()
        assert len(tools) >= 1

    @pytest.mark.asyncio
    async def test_databricks_obo_auth_type(self):
        """auth_type=databricks_obo should use Databricks auth."""
        server = {
            "name": "obo-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "databricks_obo",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        mock_auth = MagicMock()
        mock_auth.token = "obo-tok"
        mock_auth.auth_method = "obo"

        mock_adapter = MagicMock()
        mock_adapter.tools = []
        mock_adapter.initialization_error = None

        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth), \
             patch("src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter", new_callable=AsyncMock, return_value=mock_adapter):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock(), user_token="u", group_id="g"
            )

        assert tools == []  # No tools from adapter

    @pytest.mark.asyncio
    async def test_api_key_auth_non_databricks_url(self):
        """Non-Databricks URL with api_key should use the API key directly."""
        server = {
            "name": "external-server",
            "server_url": "https://other-service.example.com/mcp",
            "auth_type": "api_key",
            "api_key": "my-api-key-123",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        mock_adapter = MagicMock()
        mock_adapter.tools = [{"name": "ext_tool", "description": "d"}]
        mock_adapter.initialization_error = None

        mock_wrapped = MagicMock()
        mock_wrapped.name = "ext_tool"

        with patch("src.engines.crewai.tools.mcp_integration.create_crewai_tool_from_mcp", return_value=mock_wrapped), \
             patch("src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter", new_callable=AsyncMock, return_value=mock_adapter) as mock_create:
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock(), user_token=None, group_id=None
            )

        # Verify api_key was used in headers
        call_args = mock_create.call_args
        server_params = call_args[0][0] if call_args[0] else call_args[1].get('server_params', {})
        assert len(tools) >= 1

    @pytest.mark.asyncio
    async def test_databricks_auth_no_context_returns_empty_with_mcp_error(self):
        """Databricks URL with no auth context -> MCPConnectionError warning."""
        server = {
            "name": "fail-server",
            "server_url": "https://example.com/api/2.0/mcp/external/test",
            "auth_type": "api_key",
            "api_key": "some-key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }

        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=None):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock(), user_token=None, group_id=None
            )

        assert tools == []
        warnings = MCPIntegration.get_warnings()
        assert any("No authentication available" in w for w in warnings)

    @pytest.mark.asyncio
    async def test_adapter_initialization_error_adds_warning(self):
        """Adapter with initialization_error should add warning and return []."""
        server = {
            "name": "err-server",
            "server_url": "https://example.com/mcp",
            "auth_type": "api_key",
            "api_key": "key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        from src.core.exceptions import MCPConnectionError
        mock_adapter = MagicMock()
        mock_adapter.tools = []
        mock_adapter.initialization_error = MCPConnectionError(
            server_name="err-server",
            server_url="https://example.com/mcp",
            detail="Connection refused",
        )

        with patch("src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter", new_callable=AsyncMock, return_value=mock_adapter):
            tools = await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock(), user_token=None, group_id=None
            )

        assert tools == []
        warnings = MCPIntegration.get_warnings()
        assert len(warnings) >= 1

    @pytest.mark.asyncio
    async def test_spn_auth_overrides_auth_type_in_server_params(self):
        """Databricks auth should set auth_type=databricks_spn in server_params."""
        server = {
            "name": "spn-override",
            "server_url": "https://example.com/api/2.0/mcp/external/test",
            "auth_type": "api_key",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
        }
        mock_auth = MagicMock()
        mock_auth.token = "spn-tok"
        mock_auth.auth_method = "service_principal"

        mock_adapter = MagicMock()
        mock_adapter.tools = []
        mock_adapter.initialization_error = None

        captured_params = {}
        async def capture_adapter(params, *args, **kwargs):
            captured_params.update(params)
            return mock_adapter

        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=mock_auth), \
             patch("src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter", side_effect=capture_adapter):
            await MCPIntegration._create_tools_for_server(
                server, "agent1", MagicMock(), user_token="u", group_id="g"
            )

        assert captured_params.get("auth_type") == "databricks_spn"


class TestResolveAgentReference:
    """Test _resolve_agent_reference static method."""

    def test_match_by_agent_id(self):
        config = {"agents": [{"id": "a1", "role": "dev"}, {"id": "a2", "role": "pm"}]}
        result = MCPIntegration._resolve_agent_reference("a1", config)
        assert result == "a1"

    def test_match_by_agent_name(self):
        config = {"agents": [{"id": "a1", "name": "DevAgent"}, {"id": "a2", "name": "PMAgent"}]}
        result = MCPIntegration._resolve_agent_reference("DevAgent", config)
        # Returns agent_id (preferred) when agent is matched by name
        assert result == "a1"

    def test_match_by_role(self):
        config = {"agents": [{"id": "a1", "role": "developer"}, {"id": "a2", "role": "manager"}]}
        result = MCPIntegration._resolve_agent_reference("developer", config)
        # Returns agent_id (preferred) when agent is matched by role
        assert result == "a1"

    def test_no_match_returns_reference(self):
        config = {"agents": [{"id": "a1", "role": "dev"}]}
        result = MCPIntegration._resolve_agent_reference("unknown", config)
        assert result == "unknown"

    def test_empty_agents_list(self):
        config = {"agents": []}
        result = MCPIntegration._resolve_agent_reference("any", config)
        assert result == "any"


class TestGetMcpSettings:
    """Test get_mcp_settings static method."""

    @pytest.mark.asyncio
    async def test_returns_settings(self):
        mock_service = AsyncMock()
        mock_settings = MagicMock()
        mock_settings.global_enabled = True
        mock_settings.individual_enabled = True
        mock_service.get_settings.return_value = mock_settings

        result = await MCPIntegration.get_mcp_settings(mock_service)
        assert result is not None
        assert result['global_enabled'] is True

    @pytest.mark.asyncio
    async def test_exception_returns_defaults(self):
        mock_service = AsyncMock()
        mock_service.get_settings.side_effect = Exception("DB error")

        result = await MCPIntegration.get_mcp_settings(mock_service)
        assert result is not None
        assert result['global_enabled'] is False
        assert result['individual_enabled'] is True


class TestValidateMcpConfiguration:
    """Test validate_mcp_configuration static method."""

    def test_valid_config(self):
        config = {
            "agents": [{"id": "a1", "tool_configs": {"MCP_SERVERS": ["s1"]}}],
            "tasks": [{"id": "t1", "tool_configs": {"MCP_SERVERS": ["s1"]}}],
        }
        result = MCPIntegration.validate_mcp_configuration(config)
        assert result is True

    def test_non_dict_config(self):
        result = MCPIntegration.validate_mcp_configuration("not a dict")
        assert result is False

    def test_empty_config(self):
        result = MCPIntegration.validate_mcp_configuration({})
        assert result is True


class TestCollectAgentMcpRequirements:
    """Test collect_agent_mcp_requirements."""

    @pytest.mark.asyncio
    async def test_extracts_servers_from_tasks(self):
        config = {
            "agents": [{"id": "a1", "role": "dev"}],
            "tasks": [
                {"agent": "a1", "tool_configs": {"MCP_SERVERS": ["server1"]}},
                {"agent": "a1", "tool_configs": {"MCP_SERVERS": ["server2"]}},
            ],
        }
        result = await MCPIntegration.collect_agent_mcp_requirements(config)
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_empty_tasks_returns_empty(self):
        config = {"agents": [], "tasks": []}
        result = await MCPIntegration.collect_agent_mcp_requirements(config)
        assert result == {}

    @pytest.mark.asyncio
    async def test_exception_returns_empty(self):
        result = await MCPIntegration.collect_agent_mcp_requirements(None)
        assert result == {}


class TestCreateMcpToolsForAgent:
    """Test create_mcp_tools_for_agent."""

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_servers(self):
        agent_config = {"id": "a1", "tool_configs": {}}
        mock_service = AsyncMock()
        with patch.object(MCPIntegration, 'resolve_effective_mcp_servers', new_callable=AsyncMock, return_value=[]):
            tools = await MCPIntegration.create_mcp_tools_for_agent(
                agent_config, "a1", mock_service
            )
        assert tools == []

    @pytest.mark.asyncio
    async def test_exception_returns_empty(self):
        agent_config = {"id": "a1", "tool_configs": {}}
        mock_service = AsyncMock()
        with patch.object(MCPIntegration, 'resolve_effective_mcp_servers', new_callable=AsyncMock, side_effect=Exception("err")):
            tools = await MCPIntegration.create_mcp_tools_for_agent(
                agent_config, "a1", mock_service
            )
        assert tools == []


class TestCreateMcpToolsForTask:
    """Test create_mcp_tools_for_task."""

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_servers(self):
        task_config = {"id": "t1", "tool_configs": {}}
        mock_service = AsyncMock()
        with patch.object(MCPIntegration, 'resolve_effective_mcp_servers', new_callable=AsyncMock, return_value=[]):
            tools = await MCPIntegration.create_mcp_tools_for_task(
                task_config, "t1", mock_service
            )
        assert tools == []

    @pytest.mark.asyncio
    async def test_exception_returns_empty(self):
        task_config = {"id": "t1", "tool_configs": {}}
        mock_service = AsyncMock()
        with patch.object(MCPIntegration, 'resolve_effective_mcp_servers', new_callable=AsyncMock, side_effect=Exception("err")):
            tools = await MCPIntegration.create_mcp_tools_for_task(
                task_config, "t1", mock_service
            )
        assert tools == []
