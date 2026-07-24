"""
Coverage tests for services/agent_generation_service.py
Covers: _log_llm_interaction (exception),
generate_agent, _prepare_prompt_template, _generate_agent_config, _process_agent_config
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.agent_generation_service import AgentGenerationService


def make_service():
    session = AsyncMock()
    with patch('src.services.agent_generation_service.LLMLogRepository'), \
         patch('src.services.agent_generation_service.LLMLogService') as MockLLS:
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
    await svc._log_llm_interaction(
        endpoint="test", prompt="p", response="r", model="m"
    )


# ---- _prepare_prompt_template ----

@pytest.mark.asyncio
async def test_prepare_prompt_template_not_found():
    """Test _prepare_prompt_template raises ValueError when template not found."""
    svc = make_service()
    with patch('src.services.agent_generation_service.TemplateService') as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="not found"):
            await svc._prepare_prompt_template([], None)


@pytest.mark.asyncio
async def test_prepare_prompt_template_success():
    """Test _prepare_prompt_template returns system message."""
    svc = make_service()
    with patch('src.services.agent_generation_service.TemplateService') as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value="You are an agent generator.")
        result = await svc._prepare_prompt_template([], None)
    assert result == "You are an agent generator."


# ---- _generate_agent_config ----

@pytest.mark.asyncio
async def test_generate_agent_config_success():
    """Test _generate_agent_config with valid LLM response."""
    svc = make_service()
    with patch('src.services.agent_generation_service.LLMManager') as MockLLM:
        MockLLM.completion = AsyncMock(return_value='{"name": "Researcher", "role": "Research Analyst", "goal": "Find info", "backstory": "Expert"}')
        with patch('src.services.agent_generation_service.robust_json_parser') as MockParser:
            MockParser.return_value = {
                "name": "Researcher",
                "role": "Research Analyst",
                "goal": "Find information",
                "backstory": "Expert researcher"
            }
            result = await svc._generate_agent_config(
                "Research agent", "System message", "model-x", fast_planning=True
            )
    assert result["name"] == "Researcher"


@pytest.mark.asyncio
async def test_generate_agent_config_llm_exception():
    """Test _generate_agent_config raises ValueError on LLM exception."""
    svc = make_service()
    with patch('src.services.agent_generation_service.LLMManager') as MockLLM:
        MockLLM.completion = AsyncMock(side_effect=Exception("LLM error"))
        with pytest.raises(ValueError, match="Failed to generate"):
            await svc._generate_agent_config(
                "Research agent", "System msg", "model-x"
            )


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
        "backstory": "Expert"
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
        "advanced_config": {
            "llm": "old-model",
            "max_iter": 30
        }
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
        "backstory": "Expert"
    }
    result = svc._process_agent_config(setup, "model-y", tools=["tool1", "tool2"])
    # tools should be cleared
    assert result["tools"] == []


# ---- generate_agent (integration) ----

@pytest.mark.asyncio
async def test_generate_agent_success():
    """Test full generate_agent pipeline."""
    svc = make_service()
    with patch('src.services.agent_generation_service.TemplateService') as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value="You are an agent.")
        with patch('src.services.agent_generation_service.LLMManager') as MockLLM:
            MockLLM.completion = AsyncMock(return_value='{}')
            with patch('src.services.agent_generation_service.robust_json_parser') as MockParse:
                MockParse.return_value = {
                    "name": "Test Agent",
                    "role": "Tester",
                    "goal": "Test things",
                    "backstory": "Experienced tester"
                }
                svc.log_service.create_log = AsyncMock()
                result = await svc.generate_agent("Create a test agent")

    assert result["name"] == "Test Agent"


@pytest.mark.asyncio
async def test_generate_agent_log_failure_non_fatal():
    """Test generate_agent doesn't fail if logging fails."""
    svc = make_service()
    with patch('src.services.agent_generation_service.TemplateService') as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value="System message")
        with patch('src.services.agent_generation_service.LLMManager') as MockLLM:
            MockLLM.completion = AsyncMock(return_value='{}')
            with patch('src.services.agent_generation_service.robust_json_parser') as MockParse:
                MockParse.return_value = {
                    "name": "Agent",
                    "role": "R",
                    "goal": "G",
                    "backstory": "B"
                }
                svc.log_service.create_log = AsyncMock(side_effect=Exception("log error"))
                result = await svc.generate_agent("Create an agent")

    assert result is not None


@pytest.mark.asyncio
async def test_generate_agent_propagates_exception():
    """Test generate_agent propagates template not found exception."""
    svc = make_service()
    with patch('src.services.agent_generation_service.TemplateService') as MockTS:
        MockTS.get_effective_template_content = AsyncMock(return_value=None)
        with pytest.raises(Exception):
            await svc.generate_agent("Create an agent")
