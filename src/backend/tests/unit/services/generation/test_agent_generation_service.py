"""
Coverage tests for services/agent_generation_service.py
Covers: _log_llm_interaction (exception),
generate_agent, _prepare_prompt_template, _generate_agent_config, _process_agent_config
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.generation.agents import AgentGenerationService


def make_service():
    session = AsyncMock()
    with (
        patch("src.services.generation.agents.LLMLogRepository"),
        patch("src.services.generation.agents.LLMLogService") as MockLLS,
    ):
        MockLLS.return_value = AsyncMock()
        svc = AgentGenerationService(session)
    return svc


# ---- _log_llm_interaction ----


@pytest.mark.asyncio
async def test_log_llm_interaction_exception():
    """Test _log_llm_interaction handles exception gracefully."""
    svc = make_service()
    svc.log_service.create_log = AsyncMock(side_effect=Exception("db error"))
    # Should not raise
    await svc._log_llm_interaction(endpoint="test", prompt="p", response="r", model="m")


# ---- _prepare_prompt_template ----


@pytest.mark.asyncio
async def test_prepare_prompt_template_not_found():
    """Test _prepare_prompt_template raises ValueError when template not found."""
    svc = make_service()
    with patch("src.services.generation.agents.TemplateService") as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="not found"):
            await svc._prepare_prompt_template([], None)


@pytest.mark.asyncio
async def test_prepare_prompt_template_success():
    """Test _prepare_prompt_template returns system message."""
    svc = make_service()
    with patch("src.services.generation.agents.TemplateService") as MockTS:
        MockTS.get_effective_template_content = AsyncMock(
            return_value="You are an agent generator."
        )
        result = await svc._prepare_prompt_template([], None)
    assert result == "You are an agent generator."


# ---- _generate_agent_config ----


@pytest.mark.asyncio
async def test_generate_agent_config_success():
    """Test _generate_agent_config with valid LLM response."""
    svc = make_service()
    with patch("src.services.generation.agents.LLMManager") as MockLLM:
        MockLLM.completion = AsyncMock(
            return_value='{"name": "Researcher", "role": "Research Analyst", "goal": "Find info", "backstory": "Expert"}'
        )
        with patch("src.services.generation.agents.robust_json_parser") as MockParser:
            MockParser.return_value = {
                "name": "Researcher",
                "role": "Research Analyst",
                "goal": "Find information",
                "backstory": "Expert researcher",
            }
            result = await svc._generate_agent_config(
                "Research agent", "System message", "model-x", fast_planning=True
            )
    assert result["name"] == "Researcher"


@pytest.mark.asyncio
async def test_generate_agent_config_llm_exception():
    """Test _generate_agent_config raises ValueError on LLM exception."""
    svc = make_service()
    with patch("src.services.generation.agents.LLMManager") as MockLLM:
        MockLLM.completion = AsyncMock(side_effect=Exception("LLM error"))
        with pytest.raises(ValueError, match="Failed to generate"):
            await svc._generate_agent_config("Research agent", "System msg", "model-x")


# ---- _process_agent_config ----


def test_process_agent_config_missing_field():
    """Test _process_agent_config raises ValueError on missing field."""
    svc = make_service()
    with pytest.raises(ValueError, match="Missing required field"):
        svc._process_agent_config({"name": "A"}, "model-x")


def test_process_agent_config_new_advanced_config():
    """Test _process_agent_config with no existing advanced_config."""
    svc = make_service()
    setup = {
        "name": "Agent",
        "role": "Researcher",
        "goal": "Find info",
        "backstory": "Expert",
    }
    result = svc._process_agent_config(setup, "model-x")
    assert "advanced_config" in result
    assert result["advanced_config"]["llm"] == "model-x"
    assert result["tools"] == []


def test_process_agent_config_existing_advanced_config():
    """Test _process_agent_config updates existing advanced_config."""
    svc = make_service()
    setup = {
        "name": "Agent",
        "role": "Researcher",
        "goal": "Find info",
        "backstory": "Expert",
        "advanced_config": {"llm": "old-model", "max_iter": 30},
    }
    result = svc._process_agent_config(setup, "new-model")
    assert result["advanced_config"]["llm"] == "new-model"
    assert result["advanced_config"]["max_iter"] == 30
    assert "function_calling_llm" in result["advanced_config"]


def test_process_agent_config_slow_planning():
    """Test _process_agent_config with fast_planning=False config."""
    svc = make_service()
    setup = {
        "name": "Agent",
        "role": "Researcher",
        "goal": "Find info",
        "backstory": "Expert",
    }
    result = svc._process_agent_config(setup, "model-y", tools=["tool1", "tool2"])
    # tools should be cleared
    assert result["tools"] == []


# ---- generate_agent (integration) ----


@pytest.mark.asyncio
async def test_generate_agent_success():
    """Test full generate_agent pipeline."""
    svc = make_service()
    with patch("src.services.generation.agents.TemplateService") as MockTS:
        MockTS.get_effective_template_content = AsyncMock(
            return_value="You are an agent."
        )
        with patch("src.services.generation.agents.LLMManager") as MockLLM:
            MockLLM.completion = AsyncMock(return_value="{}")
            with patch(
                "src.services.generation.agents.robust_json_parser"
            ) as MockParse:
                MockParse.return_value = {
                    "name": "Test Agent",
                    "role": "Tester",
                    "goal": "Test things",
                    "backstory": "Experienced tester",
                }
                svc.log_service.create_log = AsyncMock()
                result = await svc.generate_agent("Create a test agent")

    assert result["name"] == "Test Agent"


@pytest.mark.asyncio
async def test_generate_agent_log_failure_non_fatal():
    """Test generate_agent doesn't fail if logging fails."""
    svc = make_service()
    with patch("src.services.generation.agents.TemplateService") as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value="System message")
        with patch("src.services.generation.agents.LLMManager") as MockLLM:
            MockLLM.completion = AsyncMock(return_value="{}")
            with patch(
                "src.services.generation.agents.robust_json_parser"
            ) as MockParse:
                MockParse.return_value = {
                    "name": "Agent",
                    "role": "R",
                    "goal": "G",
                    "backstory": "B",
                }
                svc.log_service.create_log = AsyncMock(
                    side_effect=Exception("log error")
                )
                result = await svc.generate_agent("Create an agent")

    assert result is not None


@pytest.mark.asyncio
async def test_generate_agent_propagates_exception():
    """Test generate_agent propagates template not found exception."""
    svc = make_service()
    with patch("src.services.generation.agents.TemplateService") as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value=None)
        with pytest.raises(Exception):
            await svc.generate_agent("Create an agent")


# ==========================================================================
# Additional isolated unit tests using fakes: process_agent_config advanced-
# config branches and generate_agent available_tools parameter handling
# ==========================================================================
import sys
from types import SimpleNamespace

from src.services.generation.agents import AgentGenerationService as Svc


class FakeLogService:
    def __init__(self, repo):
        self.repo = repo
        self.logged = []

    async def create_log(self, **kwargs):
        self.logged.append(kwargs)


class FakeLLMManager:
    @staticmethod
    async def completion(messages, model, temperature=0.7, max_tokens=4000):
        return '{"name": "TestAgent", "role": "Analyst", "goal": "Analyze data", "backstory": "Expert analyst"}'


class FakeTemplateService:
    @staticmethod
    async def get_effective_template_content(name: str, group_context):
        if name == "generate_agent":
            return "You are an agent generator."
        return ""


@pytest.fixture
def monkeypatch_imports(monkeypatch):
    # Mock LLMManager
    fake_llm_mod = SimpleNamespace()
    fake_llm_mod.LLMManager = FakeLLMManager
    monkeypatch.setitem(sys.modules, "src.services.llm.manager", fake_llm_mod)

    # Mock TemplateService
    fake_template_mod = SimpleNamespace()
    fake_template_mod.TemplateService = FakeTemplateService
    monkeypatch.setitem(
        sys.modules, "src.services.catalog.templates", fake_template_mod
    )

    return monkeypatch


# Skip complex integration tests that require full mocking
# Focus on unit tests for individual methods


# Test individual methods without complex external dependencies


@pytest.mark.asyncio
async def test_process_agent_config_missing_required_field():
    svc = Svc(SimpleNamespace())

    # Missing 'goal' field
    setup = {"name": "TestAgent", "role": "Analyst", "backstory": "Expert"}

    with pytest.raises(ValueError) as exc:
        svc._process_agent_config(setup, "test-model")
    assert "Missing required field" in str(exc.value)
    assert "goal" in str(exc.value)


@pytest.mark.asyncio
async def test_process_agent_config_adds_advanced_config():
    svc = Svc(SimpleNamespace())

    setup = {
        "name": "TestAgent",
        "role": "Analyst",
        "goal": "Analyze",
        "backstory": "Expert",
    }

    out = svc._process_agent_config(setup, "test-model")

    assert "advanced_config" in out
    assert out["advanced_config"]["llm"] == "test-model"
    assert out["advanced_config"]["max_iter"] == 25
    assert out["advanced_config"]["verbose"] is False
    assert out["tools"] == []


@pytest.mark.asyncio
async def test_process_agent_config_updates_existing_advanced_config():
    svc = Svc(SimpleNamespace())

    setup = {
        "name": "TestAgent",
        "role": "Analyst",
        "goal": "Analyze",
        "backstory": "Expert",
        "advanced_config": {"llm": "old-model", "verbose": True},
    }

    out = svc._process_agent_config(setup, "new-model")

    assert out["advanced_config"]["llm"] == "new-model"  # Updated
    assert out["advanced_config"]["verbose"] is True  # Preserved
    assert out["advanced_config"]["max_iter"] == 25  # Added default


# ---------------------------------------------------------------------------
# Tests for available_tools parameter on generate_agent
# ---------------------------------------------------------------------------


@pytest.fixture
def _mock_llm_and_template():
    """Patch LLMManager.completion and TemplateService.get_effective_template_content
    so that generate_agent can run end-to-end without hitting real LLM or DB."""
    fake_llm_response = (
        '{"name": "TestAgent", "role": "Analyst", '
        '"goal": "Analyze data", "backstory": "Expert analyst"}'
    )
    with (
        patch(
            "src.services.generation.agents.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=fake_llm_response,
        ),
        patch(
            "src.services.generation.agents.TemplateService.get_effective_template_content",
            new_callable=AsyncMock,
            return_value="You are an agent generator.",
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_generate_agent_available_tools_parameter(_mock_llm_and_template):
    """The new available_tools kwarg is accepted without error."""
    svc = Svc(SimpleNamespace())
    svc.log_service = SimpleNamespace(create_log=AsyncMock())

    available = [{"name": "web_search", "description": "Search the web"}]

    result = await svc.generate_agent(
        prompt_text="Create a research analyst",
        available_tools=available,
    )

    # Should return a valid agent config dict (from mocked LLM JSON)
    assert isinstance(result, dict)
    assert result["name"] == "TestAgent"


@pytest.mark.asyncio
async def test_generate_agent_tools_always_empty_array(_mock_llm_and_template):
    """Generated agent has tools=[] regardless of available_tools input."""
    svc = Svc(SimpleNamespace())
    svc.log_service = SimpleNamespace(create_log=AsyncMock())

    available = [
        {"name": "web_search", "description": "Search the web"},
        {"name": "calculator", "description": "Do math"},
    ]

    result = await svc.generate_agent(
        prompt_text="Create an agent",
        available_tools=available,
    )

    # Tools are assigned at the task level, so agents always get empty tools
    assert result["tools"] == []


@pytest.mark.asyncio
async def test_generate_agent_signature_with_available_tools(_mock_llm_and_template):
    """Method signature works when available_tools is passed as a keyword argument."""
    svc = Svc(SimpleNamespace())
    svc.log_service = SimpleNamespace(create_log=AsyncMock())

    # Call with every parameter explicitly to verify the full signature
    result = await svc.generate_agent(
        prompt_text="Create an agent",
        model=None,
        tools=None,
        group_context=None,
        fast_planning=True,
        available_tools=[{"name": "tool_a", "description": "A tool"}],
    )

    assert isinstance(result, dict)
    assert "name" in result
    assert "role" in result
    assert "goal" in result
    assert "backstory" in result
