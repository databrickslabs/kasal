"""
Comprehensive unit tests for src/engines/crewai/helpers/agent_helpers.py

Goal: push coverage from 32.1% to 50%+

Tests cover:
- _build_security_preamble
- create_agent: validation, LLM config, security preamble injection, tool resolution,
  MCP tools, fallback paths, additional params, prompt template handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call


# ============================================================================
# _build_security_preamble
# ============================================================================

class TestBuildSecurityPreamble:

    def test_returns_non_empty_string(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble
        result = _build_security_preamble()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_security_instruction(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble
        result = _build_security_preamble()
        assert "SECURITY" in result.upper() or "security" in result.lower()

    def test_mentions_injection(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble
        result = _build_security_preamble()
        assert "injection" in result.lower() or "inject" in result.lower()

    def test_mentions_untrusted_data(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble
        result = _build_security_preamble()
        assert "untrusted" in result.lower() or "external" in result.lower()

    def test_idempotent_returns_same_value_each_call(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble
        assert _build_security_preamble() == _build_security_preamble()


# ============================================================================
# create_agent – shared fixtures
# ============================================================================

BASE_CONFIG = {
    "role": "Data Analyst",
    "goal": "Produce accurate reports.",
    "backstory": "Expert analyst with 10 years of experience.",
    "verbose": True,
    "allow_delegation": False,
}

GLOBAL_CONFIG = {"group_id": "test-group-xyz", "api_keys": {}}


def _patch_all_deps():
    """Patch every external dependency of create_agent."""
    return (
        patch("src.engines.crewai.helpers.agent_helpers.Agent"),
        patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names", new_callable=AsyncMock, return_value=[]),
        patch("src.db.session.request_scoped_session"),
        patch("src.services.mcp_service.MCPService"),
        patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
        patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
    )


async def _run(agent_config, config=None, tools=None, tool_service=None, tool_factory=None, agent_id=None):
    from src.engines.crewai.helpers.agent_helpers import create_agent
    return await create_agent(
        agent_key="test_agent",
        agent_config=agent_config,
        tools=tools,
        config=config or GLOBAL_CONFIG,
        tool_service=tool_service,
        tool_factory=tool_factory,
        agent_id=agent_id,
    )


# ============================================================================
# Validation tests
# ============================================================================

class TestCreateAgentValidation:

    @pytest.mark.asyncio
    async def test_missing_role_raises_value_error(self):
        from src.engines.crewai.helpers.agent_helpers import create_agent
        bad_config = {"goal": "g", "backstory": "b"}
        with pytest.raises(ValueError, match="role"):
            await create_agent("k", bad_config, config=GLOBAL_CONFIG)

    @pytest.mark.asyncio
    async def test_missing_goal_raises_value_error(self):
        from src.engines.crewai.helpers.agent_helpers import create_agent
        bad_config = {"role": "r", "backstory": "b"}
        with pytest.raises(ValueError, match="goal"):
            await create_agent("k", bad_config, config=GLOBAL_CONFIG)

    @pytest.mark.asyncio
    async def test_missing_backstory_raises_value_error(self):
        from src.engines.crewai.helpers.agent_helpers import create_agent
        bad_config = {"role": "r", "goal": "g"}
        with pytest.raises(ValueError, match="backstory"):
            await create_agent("k", bad_config, config=GLOBAL_CONFIG)

    @pytest.mark.asyncio
    async def test_empty_role_raises_value_error(self):
        from src.engines.crewai.helpers.agent_helpers import create_agent
        bad_config = {"role": "", "goal": "g", "backstory": "b"}
        with pytest.raises(ValueError, match="role"):
            await create_agent("k", bad_config, config=GLOBAL_CONFIG)


# ============================================================================
# Security preamble injection
# ============================================================================

class TestSecurityPreambleInjection:

    @pytest.mark.asyncio
    async def test_system_prompt_starts_with_preamble_no_template(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble, create_agent

        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)

            await create_agent("test_key", dict(BASE_CONFIG), config=GLOBAL_CONFIG)

        preamble = _build_security_preamble()
        # The preamble lives in backstory (embedded by CrewAI's default system
        # prompt); 'system_prompt' is not a CrewAI Agent field and was being
        # silently dropped by Pydantic.
        assert "system_prompt" not in captured
        assert captured["backstory"].startswith(preamble)

    @pytest.mark.asyncio
    async def test_system_prompt_prepends_preamble_to_custom_template(self):
        from src.engines.crewai.helpers.agent_helpers import _build_security_preamble, create_agent

        custom_template = "You are a helpful assistant."
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["system_template"] = custom_template

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            await create_agent("test_key", agent_config, config=GLOBAL_CONFIG)

        preamble = _build_security_preamble()
        assert captured["system_template"].startswith(preamble)
        assert custom_template in captured["system_template"]

    @pytest.mark.asyncio
    async def test_allow_code_execution_always_false(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            # Even if agent config says True, it must be forced to False
            agent_config = dict(BASE_CONFIG)
            agent_config["allow_code_execution"] = True
            await create_agent("test_key", agent_config, config=GLOBAL_CONFIG)

        assert captured.get("allow_code_execution") is False


# ============================================================================
# LLM configuration
# ============================================================================

class TestCreateAgentLlmConfig:

    @pytest.mark.asyncio
    async def test_string_llm_uses_llm_manager(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        mock_llm = MagicMock()
        agent_config = dict(BASE_CONFIG)
        agent_config["llm"] = "gpt-4"

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=mock_llm) as mock_configure,
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        mock_configure.assert_called_once_with("gpt-4", GLOBAL_CONFIG["group_id"], None)
        assert captured["llm"] is mock_llm

    @pytest.mark.asyncio
    async def test_temperature_converted_from_100_scale(self):
        captured_args = {}

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", return_value=MagicMock()),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()) as mock_configure,
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            agent_config = dict(BASE_CONFIG)
            agent_config["llm"] = "gpt-4"
            agent_config["temperature"] = 70  # 70/100 = 0.7
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        # Temperature override should be 0.7
        call_args = mock_configure.call_args
        assert call_args[0][2] == pytest.approx(0.7)

    @pytest.mark.asyncio
    async def test_no_llm_in_config_uses_default_gpt4o(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        mock_llm = MagicMock()

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=mock_llm) as mock_configure,
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            # BASE_CONFIG has no 'llm' key
            await create_agent("k", dict(BASE_CONFIG), config=GLOBAL_CONFIG)

        # Should call configure_crewai_llm("gpt-4o", ...)
        mock_configure.assert_called_once()
        assert mock_configure.call_args[0][0] == "gpt-4o"


# ============================================================================
# Knowledge sources removal
# ============================================================================

class TestKnowledgeSourcesRemoval:

    @pytest.mark.asyncio
    async def test_knowledge_sources_removed_from_config(self):
        """Deprecated knowledge_sources key should be stripped before Agent creation."""
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["knowledge_sources"] = ["some_source"]

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert "knowledge_sources" not in captured


# ============================================================================
# Tool resolution
# ============================================================================

class TestToolResolution:

    @pytest.mark.asyncio
    async def test_tools_passed_directly_are_preserved(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        mock_tool = MagicMock()
        mock_tool.name = "MockTool"

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", dict(BASE_CONFIG), tools=[mock_tool], config=GLOBAL_CONFIG)

        assert mock_tool in captured["tools"]

    @pytest.mark.asyncio
    async def test_mcp_tools_added_to_agent_tools(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        mcp_tool = MagicMock()
        mcp_tool.name = "MCPTool"

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch(
                "src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent",
                new_callable=AsyncMock,
                return_value=[mcp_tool],
            ),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            # MCP servers must be declared in tool_configs — without them the
            # helper skips the DB session + MCP integration entirely (PERF-027).
            agent_config = {**BASE_CONFIG, "tool_configs": {"MCP_SERVERS": ["server1"]}}
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert mcp_tool in captured["tools"]

    @pytest.mark.asyncio
    async def test_tool_factory_creates_tool_and_adds_it(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        mock_tool_instance = MagicMock()
        mock_tool_instance.name = "ResolvedTool"

        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool = MagicMock(return_value=mock_tool_instance)

        mock_tool_svc = AsyncMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch(
                "src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("src.services.mcp_service.MCPService"),
            patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names", new_callable=AsyncMock, return_value=["ResolvedTool"]),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            agent_config = dict(BASE_CONFIG)
            agent_config["tools"] = [99]  # Some tool ID
            await create_agent(
                "k",
                agent_config,
                config=GLOBAL_CONFIG,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        assert mock_tool_instance in captured["tools"]

    @pytest.mark.asyncio
    async def test_tool_factory_none_result_logged_not_added(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool = MagicMock(return_value=None)

        mock_tool_svc = AsyncMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch(
                "src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch("src.services.mcp_service.MCPService"),
            patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names", new_callable=AsyncMock, return_value=["SomeTool"]),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            agent_config = dict(BASE_CONFIG)
            agent_config["tools"] = [1]
            await create_agent(
                "k",
                agent_config,
                config=GLOBAL_CONFIG,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        # No tools from factory since it returned None
        factory_tools = [t for t in captured["tools"] if not isinstance(t, MagicMock) or t.name == "SomeTool"]
        assert len(captured["tools"]) == 0


# ============================================================================
# Additional agent parameters
# ============================================================================

class TestAdditionalAgentParams:

    @pytest.mark.asyncio
    async def test_max_iter_passed_to_agent(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["max_iter"] = 5

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert captured.get("max_iter") == 5

    @pytest.mark.asyncio
    async def test_memory_passed_to_agent(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["memory"] = True

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert "memory" not in captured  # memory deliberately not propagated to the Agent

    @pytest.mark.asyncio
    async def test_none_additional_params_not_passed(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["max_iter"] = None  # None should NOT be passed

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert "max_iter" not in captured


# ============================================================================
# Prompt template handling
# ============================================================================

class TestPromptTemplates:

    # NOTE: templates map to CrewAI's real field names (system_template /
    # prompt_template / response_template). The old system_prompt/task_prompt/
    # format_prompt kwargs were silently dropped by Pydantic.
    @pytest.mark.asyncio
    async def test_system_template_passed_through(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["system_template"] = "Custom system template."

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert "Custom system template." in captured["system_template"]
        # Lone system_template requires a passthrough user template, or CrewAI
        # ignores both and falls back to the default format.
        assert captured.get("prompt_template") == "{{ .Prompt }}"

    @pytest.mark.asyncio
    async def test_prompt_template_passed_through(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["prompt_template"] = "Task prompt here."

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert captured.get("prompt_template") == "Task prompt here."

    @pytest.mark.asyncio
    async def test_response_template_passed_through(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["response_template"] = "Response format."

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        assert captured.get("response_template") == "Response format."


# ============================================================================
# Default agent settings
# ============================================================================

class TestDefaultAgentSettings:

    @pytest.mark.asyncio
    async def test_use_system_prompt_true_by_default(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", dict(BASE_CONFIG), config=GLOBAL_CONFIG)

        assert captured.get("use_system_prompt") is True
        assert captured.get("respect_context_window") is True

    @pytest.mark.asyncio
    async def test_max_retry_limit_defaults_to_3(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", dict(BASE_CONFIG), config=GLOBAL_CONFIG)

        assert captured.get("max_retry_limit") == 3

    @pytest.mark.asyncio
    async def test_agent_key_stored_as_attribute(self):
        mock_agent = MagicMock()

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", return_value=mock_agent),
            patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            result = await create_agent("my_special_key", dict(BASE_CONFIG), config=GLOBAL_CONFIG)

        assert result is mock_agent


# ============================================================================
# LLM fallback on exception
# ============================================================================

class TestLlmFallback:

    @pytest.mark.asyncio
    async def test_llm_exception_falls_back_to_string_model(self):
        captured = {}

        def capture_agent(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        agent_config = dict(BASE_CONFIG)
        agent_config["llm"] = "my-fallback-model"

        with (
            patch("src.engines.crewai.helpers.agent_helpers.Agent", side_effect=capture_agent),
            patch(
                "src.core.llm_manager.LLMManager.configure_crewai_llm",
                new_callable=AsyncMock,
                side_effect=Exception("LLM config failed"),
            ),
            patch("src.db.session.request_scoped_session") as mock_sess,
            patch("src.engines.crewai.tools.mcp_integration.MCPIntegration.create_mcp_tools_for_agent", new_callable=AsyncMock, return_value=[]),
            patch("src.services.mcp_service.MCPService"),
        ):
            mock_sess.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_sess.return_value.__aexit__ = AsyncMock(return_value=False)
            from src.engines.crewai.helpers.agent_helpers import create_agent
            await create_agent("k", agent_config, config=GLOBAL_CONFIG)

        # Fallback: llm should be the string "my-fallback-model"
        assert captured.get("llm") == "my-fallback-model"
