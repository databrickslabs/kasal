import pytest
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.agent_generation_service import AgentGenerationService as Svc


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
    monkeypatch.setitem(sys.modules, 'src.core.llm_manager', fake_llm_mod)

    # Mock TemplateService
    fake_template_mod = SimpleNamespace()
    fake_template_mod.TemplateService = FakeTemplateService
    monkeypatch.setitem(sys.modules, 'src.services.template_service', fake_template_mod)

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

    setup = {"name": "TestAgent", "role": "Analyst", "goal": "Analyze", "backstory": "Expert"}

    out = svc._process_agent_config(setup, "test-model")

    assert 'advanced_config' in out
    assert out['advanced_config']['llm'] == "test-model"
    assert out['advanced_config']['max_iter'] == 25
    assert out['advanced_config']['verbose'] is False
    assert out['tools'] == []


@pytest.mark.asyncio
async def test_process_agent_config_updates_existing_advanced_config():
    svc = Svc(SimpleNamespace())

    setup = {
        "name": "TestAgent",
        "role": "Analyst",
        "goal": "Analyze",
        "backstory": "Expert",
        "advanced_config": {"llm": "old-model", "verbose": True}
    }

    out = svc._process_agent_config(setup, "new-model")

    assert out['advanced_config']['llm'] == "new-model"  # Updated
    assert out['advanced_config']['verbose'] is True  # Preserved
    assert out['advanced_config']['max_iter'] == 25  # Added default


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
    with patch(
        "src.services.agent_generation_service.LLMManager.completion",
        new_callable=AsyncMock,
        return_value=fake_llm_response,
    ), patch(
        "src.services.agent_generation_service.TemplateService.get_effective_template_content",
        new_callable=AsyncMock,
        return_value="You are an agent generator.",
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
