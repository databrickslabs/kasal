import pytest
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.services.agent_generation_service import AgentGenerationService as Svc


class FakeLogService:
    def __init__(self, repo):
        self.repo = repo
        self.logged = []
    async def create_log(self, **kwargs):
        self.logged.append(kwargs)


class FakeLLMManager:
    @staticmethod
    async def configure_litellm(model: str):
        return {"model": model}


class FakeTemplateService:
    @staticmethod
    async def get_effective_template_content(name: str, group_context):
        if name == "generate_agent":
            return "You are an agent generator."
        return ""


async def fake_acompletion(**kwargs):
    return {"choices": [{"message": {"content": '{"name": "TestAgent", "role": "Analyst", "goal": "Analyze data", "backstory": "Expert analyst"}'}}]}


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

    # Mock litellm
    fake_litellm = SimpleNamespace()
    fake_litellm.acompletion = fake_acompletion
    monkeypatch.setitem(sys.modules, 'litellm', fake_litellm)

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
