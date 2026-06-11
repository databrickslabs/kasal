import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution_name_service import ExecutionNameService as Svc
from src.schemas.execution import ExecutionNameGenerationRequest


@pytest.mark.asyncio
async def test_generate_execution_name_success(monkeypatch):
    from src.services import execution_name_service as module

    # Stub template content
    class FakeTemplateService:
        async def get_template_content(self, name: str):
            return "Name template"

    # Stub log service
    class FakeLogService:
        @classmethod
        def create(cls, session):
            return cls()
        async def create_log(self, **kwargs):
            return True

    # Stub LLMManager (now uses LLMManager.completion which returns a string directly)
    class FakeLLMManager:
        @staticmethod
        async def completion(messages, model, temperature=0.7, max_tokens=4000, extra_headers=None):
            return "My Nice Name"

    monkeypatch.setattr(module, "LLMManager", FakeLLMManager, raising=True)

    svc = Svc(log_service=FakeLogService(), template_service=FakeTemplateService())
    req = ExecutionNameGenerationRequest(model="gpt-test", agents_yaml={}, tasks_yaml={})
    out = await svc.generate_execution_name(req)
    assert out.name == "My Nice Name"


@pytest.mark.asyncio
async def test_generate_execution_name_fallback_on_exception(monkeypatch):
    from src.services import execution_name_service as module

    class FakeTemplateService:
        async def get_template_content(self, name: str):
            return "Name template"

    class FakeLogService:
        @classmethod
        def create(cls, session):
            return cls()
        async def create_log(self, **kwargs):
            return True

    class FakeLLMManager:
        @staticmethod
        async def completion(messages, model, temperature=0.7, max_tokens=4000, extra_headers=None):
            raise RuntimeError("boom")

    monkeypatch.setattr(module, "LLMManager", FakeLLMManager, raising=True)

    svc = Svc(log_service=FakeLogService(), template_service=FakeTemplateService())
    req = ExecutionNameGenerationRequest(model="gpt-test", agents_yaml={}, tasks_yaml={})
    out = await svc.generate_execution_name(req)
    assert out.name.startswith("Execution-")


@pytest.mark.asyncio
async def test_log_llm_interaction_rollback_on_failure():
    """Verify session.rollback() is called when create_log fails in _log_llm_interaction."""
    # Create a mock session that tracks rollback calls
    mock_session = AsyncMock()

    # Create a mock log_service whose create_log raises an exception
    mock_log_service = AsyncMock()
    mock_log_service.create_log = AsyncMock(side_effect=RuntimeError("DB write failed"))
    # Wire the session into the repository path the service accesses for rollback
    mock_log_service.repository = MagicMock()
    mock_log_service.repository.session = mock_session

    mock_template_service = AsyncMock()

    svc = Svc(log_service=mock_log_service, template_service=mock_template_service)

    # Call the private method directly
    await svc._log_llm_interaction(
        endpoint="test-endpoint",
        prompt="test prompt",
        response="test response",
        model="test-model",
    )

    # Verify create_log was called and failed
    mock_log_service.create_log.assert_awaited_once()

    # The critical assertion: session.rollback() must have been called
    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_prompt_contains_only_roles_and_task_names(monkeypatch):
    """Regression (LLM-011): the name prompt must carry agent roles + task
    names only — NOT the full agents/tasks config (backstories, goals,
    tool_configs), which cost ~1.5-2.5k prompt tokens for a 2-4 word name
    on every execution start."""
    from src.services.execution_name_service import ExecutionNameService
    from src.schemas.execution import ExecutionNameGenerationRequest

    captured = {}

    class StubLLM:
        @staticmethod
        async def completion(messages, model, temperature=0.7, max_tokens=4000, extra_headers=None):
            captured["messages"] = messages
            return "Zip Code Analysis"

    monkeypatch.setattr("src.services.execution_name_service.LLMManager", StubLLM)

    service = ExecutionNameService.__new__(ExecutionNameService)
    service.template_service = type(
        "TS", (), {"get_template_content": staticmethod(
            _async_return("Generate a concise name. Only return the name.")
        )}
    )()
    service._log_llm_interaction = _async_noop

    secret_backstory = "Twenty years of proprietary methodology details " * 30
    req = ExecutionNameGenerationRequest(
        model="gpt-test",
        agents_yaml={
            "agent_1": {"role": "Postal Code Analyst", "backstory": secret_backstory,
                        "goal": "g" * 500, "tool_configs": {"k": "v" * 200}},
        },
        tasks_yaml={
            "task_1": {"name": "Analyze Zip Codes", "description": "d" * 800},
        },
    )

    result = await service.generate_execution_name(req)

    user_prompt = captured["messages"][1]["content"]
    assert "Postal Code Analyst" in user_prompt
    assert "Analyze Zip Codes" in user_prompt
    assert secret_backstory[:60] not in user_prompt  # no backstory shipped
    assert "tool_configs" not in user_prompt
    assert len(user_prompt) < 500  # was multi-KB with full YAML
    assert result.name == "Zip Code Analysis"


def _async_return(value):
    async def _inner(*args, **kwargs):
        return value
    return _inner


async def _async_noop(*args, **kwargs):
    return None
