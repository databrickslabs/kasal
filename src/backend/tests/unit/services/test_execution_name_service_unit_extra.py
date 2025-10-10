import pytest
from types import SimpleNamespace

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

    # Stub LLMManager and litellm
    class FakeLLMManager:
        @staticmethod
        async def configure_litellm(model: str):
            return {"model": model}

    async def fake_acompletion(**kwargs):
        return {"choices": [{"message": {"content": "My Nice Name"}}]}

    monkeypatch.setattr(module, "LLMManager", FakeLLMManager, raising=True)
    import litellm
    monkeypatch.setattr(litellm, "acompletion", fake_acompletion, raising=True)

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
        async def configure_litellm(model: str):
            raise RuntimeError("boom")

    monkeypatch.setattr(module, "LLMManager", FakeLLMManager, raising=True)

    svc = Svc(log_service=FakeLogService(), template_service=FakeTemplateService())
    req = ExecutionNameGenerationRequest(model="gpt-test", agents_yaml={}, tasks_yaml={})
    out = await svc.generate_execution_name(req)
    assert out.name.startswith("Execution-")

