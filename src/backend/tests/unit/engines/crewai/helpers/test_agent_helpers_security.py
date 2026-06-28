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

from src.engines.crewai.paths.crew.agent_adapter import (
    create_agent,
    _build_security_preamble,
    inject_security_preamble,
)


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
        patch('src.engines.crewai.kernel.agent_builder.Agent'),
        patch('src.core.llm_manager.LLMManager'),
        patch('src.db.session.request_scoped_session'),
        patch('src.services.mcp_service.MCPService'),
        patch('src.engines.crewai.tools.mcp_integration.MCPIntegration'),
    )


async def _run_create_agent(agent_config, mock_config, mock_tools, agent_class_mock):
    """Helper: call create_agent and return the kwargs passed to Agent(...)."""
    mock_llm = MagicMock()
    mock_llm.model = "gpt-4o"

    with patch('src.engines.crewai.kernel.agent_builder.Agent') as mock_agent_class, \
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
#
# REGRESSION CONTEXT: create_agent used to pass system_prompt / task_prompt /
# format_prompt kwargs, which are NOT CrewAI Agent fields — Pydantic silently
# dropped them, so the security preamble and custom templates never reached
# the LLM while a "[SECURITY]" log line claimed they were active. The fixed
# contract: preamble goes into `backstory` (embedded by CrewAI's default
# system prompt) or into `system_template` when one is configured.
# ---------------------------------------------------------------------------

class TestCreateAgentSecurityPreamble:
    """Test suite verifying security preamble is injected into agent prompts."""

    @pytest.mark.asyncio
    async def test_all_kwargs_are_real_crewai_agent_fields(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Every kwarg passed to Agent() must be an actual field on the
        installed CrewAI Agent — otherwise Pydantic silently drops it."""
        from crewai import Agent as RealAgent

        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        unknown = set(kwargs) - set(RealAgent.model_fields)
        assert not unknown, f"kwargs silently dropped by CrewAI Agent: {unknown}"

    @pytest.mark.asyncio
    async def test_backstory_contains_preamble_without_custom_template(
        self, base_agent_config, mock_config, mock_tools
    ):
        """Without a custom template, the preamble is prepended to backstory —
        which CrewAI's default system prompt embeds via {backstory}."""
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)

        assert "system_prompt" not in kwargs  # the silently-dropped kwarg is gone
        assert "SECURITY INSTRUCTION" in kwargs["backstory"]

    @pytest.mark.asyncio
    async def test_preamble_comes_before_original_backstory(
        self, base_agent_config, mock_config, mock_tools
    ):
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        backstory = kwargs["backstory"]
        assert backstory.index("SECURITY INSTRUCTION") < backstory.index("expert data analyst")

    @pytest.mark.asyncio
    async def test_system_template_gets_preamble_with_custom_template(
        self, base_agent_config, mock_config, mock_tools
    ):
        """With a custom template, the preamble is prepended to system_template
        (the real CrewAI field, not the old dropped 'system_prompt' kwarg)."""
        config = {**base_agent_config, "system_template": "You are a custom assistant."}
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)

        assert "system_template" in kwargs
        template = kwargs["system_template"]
        assert template.index("SECURITY INSTRUCTION") < template.index("You are a custom assistant.")

    @pytest.mark.asyncio
    async def test_lone_system_template_gets_passthrough_prompt_template(
        self, base_agent_config, mock_config, mock_tools
    ):
        """CrewAI only honors custom templates when BOTH system_template and
        prompt_template are set — a passthrough user template must be added."""
        config = {**base_agent_config, "system_template": "You are a custom assistant."}
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)

        assert kwargs.get("prompt_template") == "{{ .Prompt }}"

    @pytest.mark.asyncio
    async def test_user_prompt_template_is_passed_through(
        self, base_agent_config, mock_config, mock_tools
    ):
        config = {
            **base_agent_config,
            "system_template": "Custom system.",
            "prompt_template": "Custom user: {{ .Prompt }}",
            "response_template": "Answer: {{ .Response }}",
        }
        kwargs = await _run_create_agent(config, mock_config, mock_tools, None)

        assert kwargs["prompt_template"] == "Custom user: {{ .Prompt }}"
        assert kwargs["response_template"] == "Answer: {{ .Response }}"

    @pytest.mark.asyncio
    async def test_role_and_goal_kwargs_preserved(
        self, base_agent_config, mock_config, mock_tools
    ):
        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        assert kwargs["role"] == "Data Analyst"
        assert "Analyse business data" in kwargs["goal"]

    @pytest.mark.asyncio
    async def test_rendered_default_prompt_contains_preamble(
        self, base_agent_config, mock_config, mock_tools
    ):
        """End-to-end: a real CrewAI Agent built from the kwargs renders a
        system prompt that actually contains the preamble."""
        from crewai import Agent as RealAgent
        from crewai.utilities.prompts import Prompts

        kwargs = await _run_create_agent(base_agent_config, mock_config, mock_tools, None)
        kwargs = {**kwargs, "tools": [], "llm": "gpt-4o"}  # real Agent needs serializable llm
        agent = RealAgent(**kwargs)
        result = Prompts(agent=agent, has_tools=False, use_system_prompt=True).task_execution()
        rendered = result["system"] if isinstance(result, dict) else str(result)
        assert "SECURITY INSTRUCTION" in rendered

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
        assert "SECURITY INSTRUCTION" in kwargs["backstory"]


class TestInjectSecurityPreambleShared:
    """The shared injection helper used by BOTH the crew path (create_agent) and
    the flow path (flow.modules.agent_adapter) — single source of truth."""

    def test_injects_into_system_template_when_present(self):
        kwargs = {"system_template": "MY TEMPLATE", "backstory": "bs"}
        injected_into = inject_security_preamble(kwargs)
        assert injected_into == "system_template"
        assert kwargs["system_template"] == _build_security_preamble() + "\n\n" + "MY TEMPLATE"
        assert kwargs["backstory"] == "bs"  # untouched

    def test_injects_into_backstory_when_no_system_template(self):
        kwargs = {"backstory": "original backstory"}
        injected_into = inject_security_preamble(kwargs)
        assert injected_into == "backstory"
        assert kwargs["backstory"] == _build_security_preamble() + "\n\n" + "original backstory"

    def test_backstory_none_is_handled(self):
        kwargs = {}
        injected_into = inject_security_preamble(kwargs)
        assert injected_into == "backstory"
        assert kwargs["backstory"] == _build_security_preamble() + "\n\n" + ""

    def test_crew_and_flow_call_pattern_produce_identical_result(self):
        # Same inputs through the same shared helper must yield byte-identical
        # output regardless of which path calls it.
        for src in ({"system_template": "T"}, {"backstory": "B"}, {}):
            crew_kwargs = dict(src)
            flow_kwargs = dict(src)
            inject_security_preamble(crew_kwargs)
            inject_security_preamble(flow_kwargs)
            assert crew_kwargs == flow_kwargs
