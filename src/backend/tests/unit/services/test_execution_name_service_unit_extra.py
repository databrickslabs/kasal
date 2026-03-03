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
        async def completion(messages, model, temperature=0.7, max_tokens=4000):
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
        async def completion(messages, model, temperature=0.7, max_tokens=4000):
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
