"""
Unit tests for the prompt hardening / spotlighting security feature in
src/engines/crewai/helpers/agent_helpers.py.

The security preamble is injected into every agent's system_prompt to mitigate
indirect prompt injection attacks (Databricks AI Security team, Feb 2026).

Test coverage:
1. Preamble is present in system_prompt when no custom system_template is provided
2. Preamble is prepended when user provides a custom system_template
3. Preamble contains key security phrases
4. Role / goal / backstory content is still present alongside the preamble
5. allow_code_execution is always hardcoded to False (orthogonal security control)
6. Preamble is present even when additional optional parameters are configured
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from src.engines.crewai.helpers.agent_helpers import create_agent, _build_security_preamble


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def base_agent_config():
    return {
        "role": "Data Analyst",
        "goal": "Analyse business data and produce accurate summaries.",
        "backstory": "You are an expert data analyst with 10 years of experience.",
        "verbose": True,
        "allow_delegation": False,
    }


@pytest.fixture
def mock_config():
    return {"api_keys": {"openai": "test-key"}, "group_id": "test-group-abc"}


@pytest.fixture
def mock_tools():
    t = MagicMock()
    t.name = "MockTool"
    return [t]


def _patch_create_agent_deps():
    """Return a context manager stack that mocks all external deps of create_agent."""
    return (
        patch('src.engines.crewai.helpers.agent_helpers.Agent'),
        patch('src.core.llm_manager.LLMManager'),
        patch('src.db.session.request_scoped_session'),
        patch('src.services.mcp_service.MCPService'),
        patch('src.engines.crewai.tools.mcp_integration.MCPIntegration'),
    )


async def _run_create_agent(agent_config, mock_config, mock_tools, agent_class_mock):
    """Helper: call create_agent and return the kwargs passed to Agent(...)."""
    mock_llm = MagicMock()
    mock_llm.model = "gpt-4o"

    with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
         patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
         patch('src.db.session.request_scoped_session') as mock_session_factory, \
         patch('src.services.mcp_service.MCPService'), \
         patch('src.engines.crewai.tools.mcp_integration.MCPIntegration') as mock_mcp:

        mock_agent_instance = MagicMock()
        mock_agent_class.return_value = mock_agent_instance
        mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_session

        mock_mcp.create_mcp_tools_for_agent = AsyncMock(return_value=[])

        await create_agent(
            agent_key="test_agent",
            agent_config=agent_config,
            tools=mock_tools,
            config=mock_config,
        )

        # Return the kwargs Agent() was called with
        return mock_agent_class.call_args[1]


# ---------------------------------------------------------------------------
# Tests for _build_security_preamble()
# ---------------------------------------------------------------------------

class TestBuildSecurityPreamble:
    """Unit tests for the _build_security_preamble helper function."""

    def test_returns_non_empty_string(self):
        preamble = _build_security_preamble()
        assert isinstance(preamble, str)
        assert len(preamble) > 0

    def test_contains_security_instruction_marker(self):
        preamble = _build_security_preamble()
        assert "SECURITY INSTRUCTION" in preamble

    def test_contains_untrusted_keyword(self):
        preamble = _build_security_preamble()
        assert "untrusted" in preamble.lower()

    def test_contains_prompt_injection_keyword(self):
        preamble = _build_security_preamble()
        assert "prompt-injection" in preamble.lower() or "prompt injection" in preamble.lower()

    def test_contains_instruction_hierarchy_concept(self):
        """Preamble must declare that system instructions are highest priority."""
        preamble = _build_security_preamble()
        assert "highest" in preamble.lower() or "authoritative" in preamble.lower()

    def test_is_deterministic(self):
        """Same preamble returned on every call."""
        assert _build_security_preamble() == _build_security_preamble()


# ---------------------------------------------------------------------------
# Tests for preamble injection in create_agent()
# ---------------------------------------------------------------------------

class TestCreateAgentSecurityPreamble:
    """Test suite verifying security preamble is injected into agent system prompts."""

    @pytest.mark.asyncio
    async def test_system_prompt_contains_preamble_without_custom_template(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Preamble is present in system_prompt when user has not set system_template."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)

        assert "system_prompt" in kwargs
        assert "SECURITY INSTRUCTION" in kwargs["system_prompt"]

    @pytest.mark.asyncio
    async def test_system_prompt_contains_preamble_with_custom_template(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Preamble is prepended when user provides a custom system_template."""
        config = {**base_agent_config, "system_template": "You are a custom assistant."}
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)

        assert "system_prompt" in kwargs
        system_prompt = kwargs["system_prompt"]
        # Preamble must come BEFORE the custom template content
        preamble_pos = system_prompt.index("SECURITY INSTRUCTION")
        custom_pos = system_prompt.index("You are a custom assistant.")
        assert preamble_pos < custom_pos

    @pytest.mark.asyncio
    async def test_role_is_present_in_system_prompt(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Agent role is preserved in system_prompt alongside the preamble."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        assert "Data Analyst" in kwargs["system_prompt"]

    @pytest.mark.asyncio
    async def test_goal_is_present_in_system_prompt(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Agent goal is preserved in system_prompt alongside the preamble."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        assert "Analyse business data" in kwargs["system_prompt"]

    @pytest.mark.asyncio
    async def test_backstory_is_present_in_system_prompt(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Agent backstory is preserved in system_prompt alongside the preamble."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        assert "expert data analyst" in kwargs["system_prompt"]

    @pytest.mark.asyncio
    async def test_preamble_comes_before_role_content(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Security preamble appears at the start of system_prompt — highest priority."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        system_prompt = kwargs["system_prompt"]
        # Preamble index must be before role/goal content
        preamble_pos = system_prompt.index("SECURITY INSTRUCTION")
        role_pos = system_prompt.index("Data Analyst")
        assert preamble_pos < role_pos

    @pytest.mark.asyncio
    async def test_allow_code_execution_always_false(
        self, base_agent_config, mock_config, mock_tools
    ):
        """allow_code_execution is hardcoded to False regardless of agent config."""
        config = {**base_agent_config, "allow_code_execution": True}
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)
        assert kwargs["allow_code_execution"] is False

    @pytest.mark.asyncio
    async def test_preamble_present_with_date_awareness_params(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Preamble is injected even when date awareness params are also set."""
        config = {**base_agent_config, "inject_date": True, "date_format": "%Y-%m-%d"}
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)
        assert "SECURITY INSTRUCTION" in kwargs["system_prompt"]

    @pytest.mark.asyncio
    async def test_custom_template_content_fully_preserved(
        self, base_agent_config, mock_config, mock_tools
    ):
        """The full custom system_template text is preserved after prepending preamble."""
        custom = "You are SpecialBot. Never deviate from your mission."
        config = {**base_agent_config, "system_template": custom}
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)
        # Entire custom template must be in the resulting prompt
        assert custom in kwargs["system_prompt"]

    @pytest.mark.asyncio
    async def test_system_prompt_key_always_set(
        self, base_agent_config, mock_config, mock_tools
    ):
        """system_prompt key is always present in agent kwargs after hardening."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        assert "system_prompt" in kwargs
        assert kwargs["system_prompt"] is not None
        assert len(kwargs["system_prompt"]) > 0
