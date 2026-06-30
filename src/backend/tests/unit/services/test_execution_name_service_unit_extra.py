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


# ---------------------------------------------------------------------------
# None-session path: open a standalone session for the template read + log
# ---------------------------------------------------------------------------
#
# The chat auto-execute builds the whole execution stack with session=None
# (ExecutionService(session=None)). Previously the name service crashed on a
# None session ("'NoneType' has no attribute 'execute'/'add'"); now it opens its
# OWN request_scoped_session() per DB call, like the rest of that stack.

def _fake_session_cm():
    """Async-context-manager DB session with awaitable commit/rollback."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return session


def test_create_with_none_session_defers_dependencies():
    svc = Svc.create(None)
    assert svc.log_service is None
    assert svc.template_service is None
    assert svc._session is None


@pytest.mark.asyncio
async def test_get_name_template_opens_standalone_session_when_none():
    """With no injected session, the template read runs on a fresh session
    instead of crashing on None."""
    svc = Svc.create(None)
    fake_template = MagicMock()
    fake_template.get_template_content = AsyncMock(return_value="TEMPLATE BODY")

    with patch("src.db.session.request_scoped_session", return_value=_fake_session_cm()), \
         patch("src.services.template_service.TemplateService", return_value=fake_template):
        out = await svc._get_name_template()

    assert out == "TEMPLATE BODY"
    fake_template.get_template_content.assert_awaited_once_with("generate_job_name")


@pytest.mark.asyncio
async def test_log_llm_interaction_standalone_commits_when_no_session():
    """With no injected session, the LLM-interaction log opens a standalone
    session, writes, and COMMITs (the repository only flushes)."""
    svc = Svc.create(None)
    session = _fake_session_cm()
    fake_log = MagicMock()
    fake_log.create_log = AsyncMock()

    with patch("src.db.session.request_scoped_session", return_value=session), \
         patch.object(Svc, "_log_llm_interaction", Svc._log_llm_interaction), \
         patch("src.services.log_service.LLMLogService.create", return_value=fake_log):
        await svc._log_llm_interaction(
            endpoint="generate-execution-name", prompt="p", response="Name", model="m",
        )

    fake_log.create_log.assert_awaited_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_log_llm_interaction_standalone_swallows_errors():
    """A failure on the standalone log path is swallowed (never breaks the run)."""
    svc = Svc.create(None)
    session = _fake_session_cm()
    fake_log = MagicMock()
    fake_log.create_log = AsyncMock(side_effect=RuntimeError("db down"))

    with patch("src.db.session.request_scoped_session", return_value=session), \
         patch("src.services.log_service.LLMLogService.create", return_value=fake_log):
        # Must not raise.
        await svc._log_llm_interaction(
            endpoint="generate-execution-name", prompt="p", response="Name", model="m",
        )


@pytest.mark.asyncio
async def test_generate_execution_name_none_session_end_to_end(monkeypatch):
    """REGRESSION: create(None).generate_execution_name() must not crash on a
    None session — generate_execution_name must route the template read through
    _get_name_template (standalone session), not self.template_service directly."""
    from src.services import execution_name_service as module

    svc = Svc.create(None)  # session=None → template_service/log_service are None

    fake_template = MagicMock()
    fake_template.get_template_content = AsyncMock(return_value="SYS TEMPLATE")
    fake_log = MagicMock()
    fake_log.create_log = AsyncMock()

    class FakeLLMManager:
        @staticmethod
        async def completion(messages, model, temperature=0.7, max_tokens=4000, extra_headers=None):
            return "Cool Run"

    monkeypatch.setattr(module, "LLMManager", FakeLLMManager, raising=True)

    req = ExecutionNameGenerationRequest(model="m", agents_yaml={"a": {"role": "R"}}, tasks_yaml={"t": {"name": "T"}})

    with patch("src.db.session.request_scoped_session", return_value=_fake_session_cm()), \
         patch("src.services.template_service.TemplateService", return_value=fake_template), \
         patch("src.services.log_service.LLMLogService.create", return_value=fake_log):
        out = await svc.generate_execution_name(req)

    assert out.name == "Cool Run"
    fake_template.get_template_content.assert_awaited_once_with("generate_job_name")
