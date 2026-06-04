"""
Unit tests for src/engines/crewai/helpers/agent_helpers.py

Targets uncovered lines (52% → 85%+).
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.engines.crewai.helpers.agent_helpers import (
    create_agent,
    _build_security_preamble,
    _SECURITY_PREAMBLE,
)


# ---------------------------------------------------------------------------
# _build_security_preamble
# ---------------------------------------------------------------------------

class TestBuildSecurityPreamble:
    def test_returns_preamble_string(self):
        result = _build_security_preamble()
        assert isinstance(result, str)
        assert "SECURITY" in result
        assert result == _SECURITY_PREAMBLE


# ---------------------------------------------------------------------------
# create_agent – helpers / fixtures
# ---------------------------------------------------------------------------

def _base_config(**overrides):
    cfg = {
        "role": "Analyst",
        "goal": "Analyse data",
        "backstory": "An experienced analyst",
        "llm": "databricks-claude-3-5-sonnet",
        "group_id": "grp-1",
    }
    cfg.update(overrides)
    return cfg


async def _make_agent(agent_config=None, config=None, **kwargs):
    """Helper that patches the heavy dependencies and calls create_agent."""
    if agent_config is None:
        agent_config = _base_config()
    if config is None:
        config = {"group_id": "grp-1"}

    with patch("src.core.llm_manager.LLMManager") as mock_lm, \
         patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
         patch("src.db.session.request_scoped_session") as mock_sess, \
         patch("src.services.mcp_service.MCPService") as mock_mcp_svc, \
         patch("src.core.unit_of_work.UnitOfWork") as mock_uow, \
         patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls:

        mock_llm_instance = MagicMock()
        mock_llm_instance.model = agent_config.get("llm", "gpt-4o")
        mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_llm_instance)

        mock_mcp_instance = MagicMock()
        mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_sess.return_value = mock_session

        mock_agent_instance = MagicMock()
        mock_agent_instance.llm = mock_llm_instance
        mock_agent_cls.return_value = mock_agent_instance

        agent = await create_agent(
            agent_key="test_agent",
            agent_config=agent_config,
            config=config,
            **kwargs
        )
        return agent, mock_agent_cls, mock_lm


# ---------------------------------------------------------------------------
# Basic creation
# ---------------------------------------------------------------------------

class TestCreateAgentBasic:
    """Basic agent creation tests."""

    @pytest.mark.asyncio
    async def test_creates_agent_successfully(self):
        agent, mock_cls, _ = await _make_agent()
        assert agent is not None
        mock_cls.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_role_raises(self):
        cfg = {"goal": "G", "backstory": "B"}
        with pytest.raises(ValueError, match="role"):
            await _make_agent(agent_config=cfg, config={"group_id": "g1"})

    @pytest.mark.asyncio
    async def test_empty_role_raises(self):
        cfg = _base_config(role="")
        with pytest.raises(ValueError, match="role"):
            await _make_agent(agent_config=cfg)

    @pytest.mark.asyncio
    async def test_missing_goal_raises(self):
        cfg = {"role": "R", "backstory": "B"}
        with pytest.raises(ValueError, match="goal"):
            await _make_agent(agent_config=cfg, config={"group_id": "g1"})

    @pytest.mark.asyncio
    async def test_missing_backstory_raises(self):
        cfg = {"role": "R", "goal": "G"}
        with pytest.raises(ValueError, match="backstory"):
            await _make_agent(agent_config=cfg, config={"group_id": "g1"})

    @pytest.mark.asyncio
    async def test_knowledge_sources_removed_with_warning(self):
        cfg = _base_config(knowledge_sources=[{"type": "pdf", "path": "/doc.pdf"}])
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        # knowledge_sources should have been stripped from config
        call_kwargs = mock_cls.call_args[1]
        assert "knowledge_sources" not in call_kwargs

    @pytest.mark.asyncio
    async def test_security_preamble_in_system_prompt(self):
        agent, mock_cls, _ = await _make_agent()
        call_kwargs = mock_cls.call_args[1]
        assert "SECURITY INSTRUCTION" in call_kwargs.get("system_prompt", "")

    @pytest.mark.asyncio
    async def test_custom_system_template_prepended_with_preamble(self):
        cfg = _base_config(system_template="You are a custom agent.")
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        prompt = call_kwargs.get("system_prompt", "")
        assert "SECURITY INSTRUCTION" in prompt
        assert "You are a custom agent." in prompt

    @pytest.mark.asyncio
    async def test_allow_code_execution_hardcoded_false(self):
        cfg = _base_config(allow_code_execution=True)
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["allow_code_execution"] is False

    @pytest.mark.asyncio
    async def test_agent_key_stored_as_attribute(self):
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())

            agent = await create_agent(
                agent_key="unique_key_123",
                agent_config=_base_config(),
                config={"group_id": "grp-1"},
            )
            assert hasattr(agent, "_agent_key")
            assert agent._agent_key == "unique_key_123"


# ---------------------------------------------------------------------------
# LLM configuration paths
# ---------------------------------------------------------------------------

class TestCreateAgentLLMConfig:
    """Test LLM configuration branches."""

    @pytest.mark.asyncio
    async def test_llm_string_uses_llm_manager(self):
        cfg = _base_config(llm="databricks-claude-sonnet-3-5", temperature=75)
        _, mock_cls, mock_lm = await _make_agent(agent_config=cfg)
        # LLMManager.configure_crewai_llm should be called with model and group_id and temperature
        mock_lm.configure_crewai_llm.assert_called()
        call_args = mock_lm.configure_crewai_llm.call_args
        assert call_args[0][0] == "databricks-claude-sonnet-3-5"
        # temperature converted from 75 → 0.75
        assert call_args[0][2] == pytest.approx(0.75)

    @pytest.mark.asyncio
    async def test_llm_dict_with_model_key(self):
        cfg = _base_config(llm={"model": "gpt-4o", "temperature": 0.5})
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("crewai.LLM") as mock_llm_cls:

            mock_configured = MagicMock()
            mock_configured.model = "gpt-4o"
            mock_configured.api_key = "sk-test"
            mock_configured.api_base = None
            mock_configured.temperature = 0.5
            mock_configured.max_completion_tokens = None
            mock_configured.max_tokens = None
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_configured)
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_llm_cls.return_value = MagicMock()

            agent = await create_agent(
                agent_key="dict-llm-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        assert agent is not None

    @pytest.mark.asyncio
    async def test_llm_dict_databricks_model_gets_retry_llm(self):
        cfg = _base_config(llm={"model": "databricks-meta-llama-4"})
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksRetryLLM") as mock_retry:

            mock_configured = MagicMock()
            mock_configured.model = "databricks/databricks-meta-llama-4"
            mock_configured.api_key = None
            mock_configured.api_base = None
            mock_configured.temperature = None
            mock_configured.max_completion_tokens = None
            mock_configured.max_tokens = None
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_configured)
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_retry.return_value = MagicMock()
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())

            agent = await create_agent(
                agent_key="databricks-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        assert agent is not None

    @pytest.mark.asyncio
    async def test_no_llm_in_config_uses_default(self):
        cfg = {"role": "R", "goal": "G", "backstory": "B"}
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())

            agent = await create_agent(
                agent_key="default-llm",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        # Should call configure_crewai_llm with "gpt-4o"
        mock_lm.configure_crewai_llm.assert_called()
        call_args = mock_lm.configure_crewai_llm.call_args
        assert call_args[0][0] == "gpt-4o"

    @pytest.mark.asyncio
    async def test_no_group_id_falls_back_to_string_llm(self):
        """When group_id is missing, LLM config fails but agent falls back to string model."""
        cfg = _base_config(llm="gpt-4o")
        # The ValueError from configure_crewai_llm is caught and falls back
        # to string LLM. Agent should still be created.
        agent, mock_cls, _ = await _make_agent(agent_config=cfg, config={})
        # Agent should have been created with string LLM as fallback
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("llm") == "gpt-4o"

    @pytest.mark.asyncio
    async def test_llm_config_exception_falls_back_to_string(self):
        cfg = _base_config(llm="some-model")
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls:

            mock_lm.configure_crewai_llm = AsyncMock(side_effect=Exception("LLM config error"))
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())

            agent = await create_agent(
                agent_key="fallback-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        # Should have fallen back to string llm
        call_kwargs = mock_agent_cls.call_args[1]
        assert call_kwargs.get("llm") == "some-model"


# ---------------------------------------------------------------------------
# Tool resolution
# ---------------------------------------------------------------------------

class TestCreateAgentToolResolution:
    """Tool resolution paths in create_agent."""

    @pytest.mark.asyncio
    async def test_with_tool_service_resolves_tools(self):
        cfg = _base_config(tools=["tool-id-1", "tool-id-2"])

        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_tool_instance = MagicMock()
        mock_tool_instance.name = "SearchTool"
        mock_tool_factory.create_tool = MagicMock(return_value=mock_tool_instance)

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["SearchTool"]

            agent = await create_agent(
                agent_key="tool-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        mock_resolve.assert_called_once()
        mock_tool_factory.create_tool.assert_called_with(
            "SearchTool",
            result_as_answer=False,
            tool_config_override={},
        )

    @pytest.mark.asyncio
    async def test_mcp_tuple_tool_expanded(self):
        cfg = _base_config(tools=["mcp-tool-id"])

        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mcp_sub1 = MagicMock(name="mcp_sub1")
        mcp_sub2 = MagicMock(name="mcp_sub2")
        mock_tool_factory.create_tool = MagicMock(return_value=(True, [mcp_sub1, mcp_sub2]))

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["MCPTool"]

            agent = await create_agent(
                agent_key="mcp-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        call_kwargs = mock_agent_cls.call_args[1]
        tools_list = call_kwargs.get("tools", [])
        assert mcp_sub1 in tools_list
        assert mcp_sub2 in tools_list

    @pytest.mark.asyncio
    async def test_mcp_service_adapter_skipped(self):
        cfg = _base_config(tools=["mcp-adapter-id"])

        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool = MagicMock(return_value=(True, "mcp_service_adapter"))

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["MCPAdapterTool"]

            agent = await create_agent(
                agent_key="mcp-adapter-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )
        # Adapter skipped — tools should be empty from this resolution path
        call_kwargs = mock_agent_cls.call_args[1]
        tools_list = call_kwargs.get("tools", [])
        assert "mcp_service_adapter" not in tools_list

    @pytest.mark.asyncio
    async def test_tool_none_logs_error(self):
        cfg = _base_config(tools=["missing-tool-id"])

        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool = MagicMock(return_value=None)

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["MissingTool"]

            agent = await create_agent(
                agent_key="missing-tool-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )
        # None tools should not appear in tools list
        call_kwargs = mock_agent_cls.call_args[1]
        for t in call_kwargs.get("tools", []):
            assert t is not None

    @pytest.mark.asyncio
    async def test_no_tool_factory_uses_tool_names(self):
        cfg = _base_config(tools=["tool-id-abc"])

        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["SomeToolName"]

            agent = await create_agent(
                agent_key="no-factory-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=None,  # No factory
            )
        # Should include tool names as fallback
        call_kwargs = mock_agent_cls.call_args[1]
        tools_list = call_kwargs.get("tools", [])
        assert "SomeToolName" in tools_list


# ---------------------------------------------------------------------------
# Additional parameter paths
# ---------------------------------------------------------------------------

class TestCreateAgentAdditionalParams:
    """Test optional/additional parameter handling."""

    @pytest.mark.asyncio
    async def test_max_iter_param(self):
        cfg = _base_config(max_iter=20)
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("max_iter") == 20

    @pytest.mark.asyncio
    async def test_max_rpm_param(self):
        cfg = _base_config(max_rpm=10)
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("max_rpm") == 10

    @pytest.mark.asyncio
    async def test_memory_param(self):
        cfg = _base_config(memory=True)
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("memory") is True

    @pytest.mark.asyncio
    async def test_reasoning_param(self):
        cfg = _base_config(reasoning=True, max_reasoning_attempts=3)
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("reasoning") is True
        assert call_kwargs.get("max_reasoning_attempts") == 3

    @pytest.mark.asyncio
    async def test_inject_date_param(self):
        cfg = _base_config(inject_date=True, date_format="%Y-%m-%d")
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("inject_date") is True

    @pytest.mark.asyncio
    async def test_prompt_template_param(self):
        cfg = _base_config(prompt_template="Custom prompt {task}")
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("task_prompt") == "Custom prompt {task}"

    @pytest.mark.asyncio
    async def test_response_template_param(self):
        cfg = _base_config(response_template="Response: {response}")
        agent, mock_cls, _ = await _make_agent(agent_config=cfg)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("format_prompt") == "Response: {response}"

    @pytest.mark.asyncio
    async def test_genie_tool_debug_logging(self):
        cfg = _base_config(
            tools=["genie-id"],
            tool_configs={"GenieTool": {"spaceId": "space-123"}},
        )
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_genie = MagicMock(name="GenieTool")
        mock_tool_factory.create_tool = MagicMock(return_value=mock_genie)

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["GenieTool"]

            agent = await create_agent(
                agent_key="genie-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )
        # GenieTool override applied
        mock_tool_factory.create_tool.assert_called_with(
            "GenieTool",
            result_as_answer=False,
            tool_config_override={"spaceId": "space-123"},
        )

    @pytest.mark.asyncio
    async def test_mcp_tools_error_continues(self):
        cfg = _base_config()

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(side_effect=Exception("MCP error"))
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())

            # Should not raise
            agent = await create_agent(
                agent_key="mcp-error-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        assert agent is not None

    @pytest.mark.asyncio
    async def test_tool_resolution_exception_does_not_fail(self):
        cfg = _base_config(tools=["tool-err-id"])

        mock_tool_svc = MagicMock()

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.side_effect = Exception("tool svc crash")

            # Should not raise
            agent = await create_agent(
                agent_key="resolve-err",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
            )
        assert agent is not None


class TestCreateAgentLLMConfigExtended:
    """Additional LLM config path tests."""

    @pytest.mark.asyncio
    async def test_llm_dict_with_model_and_temperature_override(self):
        """Dict LLM config with temperature set at agent level."""
        cfg = _base_config(llm={"model": "gpt-4o"}, temperature=60)
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("crewai.LLM") as mock_llm_cls:

            mock_configured = MagicMock()
            mock_configured.model = "gpt-4o"
            mock_configured.api_key = None
            mock_configured.api_base = None
            mock_configured.temperature = None
            mock_configured.max_completion_tokens = None
            mock_configured.max_tokens = None
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_configured)
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_llm_cls.return_value = MagicMock()

            agent = await create_agent(
                agent_key="temp-override-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        # Temperature was overridden - should call configure_crewai_llm with temperature
        call_args = mock_lm.configure_crewai_llm.call_args
        assert call_args[0][2] == pytest.approx(0.6)

    @pytest.mark.asyncio
    async def test_llm_dict_no_model_key_uses_default(self):
        """Dict LLM config without 'model' key uses gpt-4o default."""
        cfg = _base_config(llm={"temperature": 0.5, "max_tokens": 1000})
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("crewai.LLM") as mock_llm_cls:

            mock_configured = MagicMock()
            mock_configured.model = "gpt-4o"
            mock_configured.api_key = None
            mock_configured.api_base = None
            mock_configured.temperature = None
            mock_configured.max_completion_tokens = None
            mock_configured.max_tokens = None
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_configured)
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_llm_cls.return_value = MagicMock()

            agent = await create_agent(
                agent_key="no-model-dict-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        # Should use gpt-4o as default
        call_args = mock_lm.configure_crewai_llm.call_args
        assert call_args[0][0] == "gpt-4o"

    @pytest.mark.asyncio
    async def test_unexpected_mcp_tools_format_logged(self):
        """When MCP tools format is unexpected (not a list or 'mcp_service_adapter'), logs warning."""
        cfg = _base_config(tools=["mcp-weird"])
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        # Return unexpected format (True, "unexpected_string")
        mock_tool_factory.create_tool = MagicMock(return_value=(True, "unexpected_not_list"))

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.engines.crewai.helpers.agent_helpers.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_resolve.return_value = ["MCPWeirdTool"]

            agent = await create_agent(
                agent_key="mcp-weird-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )
        # Should not raise - unexpected format is just logged
        assert agent is not None

    @pytest.mark.asyncio
    async def test_llm_dict_databricks_prefix_ensured(self):
        """Model name without 'databricks/' prefix gets it added."""
        cfg = _base_config(llm={"model": "databricks-meta-llama"})
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksRetryLLM") as mock_retry:

            mock_configured = MagicMock()
            mock_configured.model = "databricks/databricks-meta-llama"
            mock_configured.api_key = None
            mock_configured.api_base = None
            mock_configured.temperature = None
            mock_configured.max_completion_tokens = None
            mock_configured.max_tokens = None
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_configured)
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_retry.return_value = MagicMock()
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())

            agent = await create_agent(
                agent_key="databricks-prefix-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        # DatabricksRetryLLM should have been called for databricks model
        mock_retry.assert_called()

    @pytest.mark.asyncio
    async def test_llm_dict_configured_llm_no_model_attr_fallback(self):
        """When configured_llm doesn't have 'model' attr, uses fallback path."""
        cfg = _base_config(llm={"model": "some-model"})
        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.mcp_service.MCPService"), \
             patch("src.core.unit_of_work.UnitOfWork"), \
             patch("src.engines.crewai.helpers.agent_helpers.Agent") as mock_agent_cls, \
             patch("crewai.LLM") as mock_llm_cls:

            # Return a configured_llm without 'model' attribute
            mock_configured = MagicMock(spec=[])  # no attributes
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_configured)
            mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_agent_cls.return_value = MagicMock(llm=MagicMock())
            mock_llm_cls.return_value = MagicMock()

            agent = await create_agent(
                agent_key="no-model-attr-agent",
                agent_config=cfg,
                config={"group_id": "grp-1"},
            )
        assert agent is not None
