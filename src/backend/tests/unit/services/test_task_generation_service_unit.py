"""
Comprehensive unit tests for TaskGenerationService.

Covers all methods, branches, and error paths including:
- __init__ construction
- _log_llm_interaction (success + error paths)
- _get_relevant_documentation (success, no embedding, no docs, exception, agent context)
- generate_task (happy path, no template, agent context, code-block extraction,
  trailing-comma cleanup, empty content, LLM error, JSON parse error,
  missing required fields, tools default, advanced_config defaults and fixes,
  markdown flag, fast_planning vs normal, llm_guardrail passthrough)
- generate_and_save_task (delegates correctly)
- convert_to_task_create (dict tools, string tools, output_json serialization)
"""

import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.task_generation_service import TaskGenerationService
from src.schemas.task_generation import (
    TaskGenerationRequest,
    TaskGenerationResponse,
    AdvancedConfig,
    Agent,
    LLMGuardrailConfig,
)
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_group_context(**overrides):
    defaults = dict(
        group_ids=["grp1"],
        group_email="user@example.com",
        email_domain="example.com",
    )
    defaults.update(overrides)
    return GroupContext(**defaults)


def _make_llm_response(content: str):
    """Build a fake litellm-style response dict."""
    return {"choices": [{"message": {"content": content}}]}


def _valid_task_json(**overrides):
    base = {
        "name": "Research Task",
        "description": "Research the topic thoroughly",
        "expected_output": "A detailed report",
    }
    base.update(overrides)
    return json.dumps(base)


def _make_generation_response_ns(**overrides):
    """Build a SimpleNamespace that mimics TaskGenerationResponse.

    This allows us to pass arbitrary tool types (strings, dicts, mixed)
    to convert_to_task_create without Pydantic validation rejecting them.
    """
    ac_defaults = dict(
        async_execution=False,
        context=[],
        output_json=None,
        output_pydantic=None,
        output_file=None,
        human_input=False,
        retry_on_fail=True,
        max_retries=3,
        timeout=None,
        priority=1,
        dependencies=[],
        callback=None,
        error_handling="default",
        output_parser=None,
        cache_response=True,
        cache_ttl=3600,
        markdown=False,
    )
    ac_overrides = overrides.pop("advanced_config", {})
    if isinstance(ac_overrides, dict):
        ac_defaults.update(ac_overrides)
        ac = SimpleNamespace(**ac_defaults)
    else:
        ac = ac_overrides  # Allow passing a ready-made object

    defaults = dict(
        name="Task1",
        description="Desc",
        expected_output="Output",
        tools=[],
        advanced_config=ac,
        llm_guardrail=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Tests for __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_session_stored(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        assert svc.session is session

    def test_log_service_created(self):
        session = MagicMock()
        with patch("src.services.task_generation_service.LLMLogRepository") as MockRepo, \
             patch("src.services.task_generation_service.LLMLogService") as MockLogSvc:
            svc = TaskGenerationService(session)
            MockRepo.assert_called_once_with(session)
            MockLogSvc.assert_called_once_with(MockRepo.return_value)
            assert svc.log_service is MockLogSvc.return_value


# ---------------------------------------------------------------------------
# Tests for _log_llm_interaction
# ---------------------------------------------------------------------------


class TestLogLLMInteraction:
    @pytest.mark.asyncio
    async def test_success_logging(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()

        gc = _make_group_context()
        await svc._log_llm_interaction(
            endpoint="generate-task",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="success",
            group_context=gc,
        )

        svc.log_service.create_log.assert_awaited_once_with(
            endpoint="generate-task",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="success",
            error_message=None,
            group_context=gc,
        )

    @pytest.mark.asyncio
    async def test_error_status_logging(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()

        await svc._log_llm_interaction(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
            status="error",
            error_message="boom",
        )

        svc.log_service.create_log.assert_awaited_once()
        call_kwargs = svc.log_service.create_log.call_args.kwargs
        assert call_kwargs["status"] == "error"
        assert call_kwargs["error_message"] == "boom"

    @pytest.mark.asyncio
    async def test_create_log_exception_does_not_propagate(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock(side_effect=RuntimeError("db down"))

        # Should not raise
        await svc._log_llm_interaction(
            endpoint="ep", prompt="p", response="r", model="m"
        )


# ---------------------------------------------------------------------------
# Tests for _get_relevant_documentation
# ---------------------------------------------------------------------------


class TestGetRelevantDocumentation:
    @pytest.mark.asyncio
    async def test_returns_formatted_docs(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        doc1 = SimpleNamespace(
            source="best_practices/task.md",
            title="Task Best Practices",
            content="Always define expected output.",
        )
        doc2 = SimpleNamespace(
            source="general/overview.md",
            title="Overview",
            content="General overview text.",
        )

        with patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.DocumentationEmbeddingService") as MockDocSvc:
            MockLLM.get_embedding = AsyncMock(return_value=[0.1, 0.2, 0.3])
            mock_doc_instance = MagicMock()
            mock_doc_instance.search_similar_embeddings = AsyncMock(return_value=[doc1, doc2])
            MockDocSvc.return_value = mock_doc_instance

            result = await svc._get_relevant_documentation("build a task")

        assert "Task Best Practices" in result
        assert "Overview" in result
        assert "Always define expected output." in result

    @pytest.mark.asyncio
    async def test_with_agent_context(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        with patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.DocumentationEmbeddingService") as MockDocSvc:
            MockLLM.get_embedding = AsyncMock(return_value=[0.1])
            mock_doc_instance = MagicMock()
            mock_doc_instance.search_similar_embeddings = AsyncMock(return_value=[])
            MockDocSvc.return_value = mock_doc_instance

            result = await svc._get_relevant_documentation("prompt", agent_context="role: analyst")

            # Verify agent context was included in the embedding query
            call_args = MockLLM.get_embedding.call_args
            query_text = call_args.args[0] if call_args.args else call_args.kwargs.get("text", "")
            assert "analyst" in query_text.lower() or "agent context" in query_text.lower()

    @pytest.mark.asyncio
    async def test_no_embedding_returns_empty(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        with patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockLLM.get_embedding = AsyncMock(return_value=None)
            result = await svc._get_relevant_documentation("prompt")

        assert result == ""

    @pytest.mark.asyncio
    async def test_empty_docs_returns_empty(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        with patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.DocumentationEmbeddingService") as MockDocSvc:
            MockLLM.get_embedding = AsyncMock(return_value=[0.1])
            mock_doc_instance = MagicMock()
            mock_doc_instance.search_similar_embeddings = AsyncMock(return_value=[])
            MockDocSvc.return_value = mock_doc_instance

            result = await svc._get_relevant_documentation("prompt")

        assert result == ""

    @pytest.mark.asyncio
    async def test_exception_returns_empty(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        with patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockLLM.get_embedding = AsyncMock(side_effect=RuntimeError("embed fail"))
            result = await svc._get_relevant_documentation("prompt")

        assert result == ""

    @pytest.mark.asyncio
    async def test_best_practices_doc_prioritized(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        bp_doc = SimpleNamespace(
            source="best_practices/task.md",
            title="Task Patterns",
            content="BP content.",
        )
        other_doc = SimpleNamespace(
            source="other/generic.md",
            title="Generic Info",
            content="Generic content.",
        )

        with patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.DocumentationEmbeddingService") as MockDocSvc:
            MockLLM.get_embedding = AsyncMock(return_value=[0.1])
            mock_doc_instance = MagicMock()
            mock_doc_instance.search_similar_embeddings = AsyncMock(return_value=[bp_doc, other_doc])
            MockDocSvc.return_value = mock_doc_instance

            result = await svc._get_relevant_documentation("prompt")

        # best_practices doc should appear before the header
        bp_pos = result.find("Task Patterns")
        header_pos = result.find("## Task Generation Best Practices")
        assert bp_pos < header_pos

    @pytest.mark.asyncio
    async def test_task_title_doc_prioritized(self):
        session = MagicMock()
        svc = TaskGenerationService(session)

        task_doc = SimpleNamespace(
            source="templates/some.md",
            title="task configuration guide",
            content="task guide content.",
        )
        generic_doc = SimpleNamespace(
            source="other/info.md",
            title="Other Info",
            content="other content.",
        )

        with patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.DocumentationEmbeddingService") as MockDocSvc:
            MockLLM.get_embedding = AsyncMock(return_value=[0.1])
            mock_doc_instance = MagicMock()
            mock_doc_instance.search_similar_embeddings = AsyncMock(return_value=[task_doc, generic_doc])
            MockDocSvc.return_value = mock_doc_instance

            result = await svc._get_relevant_documentation("prompt")

        # The task-titled doc should be prepended before the header
        task_pos = result.find("task configuration guide")
        header_pos = result.find("## Task Generation Best Practices")
        assert task_pos < header_pos


# ---------------------------------------------------------------------------
# Tests for generate_task
# ---------------------------------------------------------------------------


class TestGenerateTask:
    """Core generation method tests."""

    def _build_service(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        return svc

    # -- happy path --

    @pytest.mark.asyncio
    async def test_happy_path_minimal(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="build a research task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="System prompt here")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            result = await svc.generate_task(request)

        assert isinstance(result, TaskGenerationResponse)
        assert result.name == "Research Task"
        assert result.description == "Research the topic thoroughly"
        assert result.expected_output == "A detailed report"
        assert result.tools == []
        assert result.advanced_config is not None

    @pytest.mark.asyncio
    async def test_uses_model_from_request(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task", model="my-custom-model")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "my-custom-model"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            await svc.generate_task(request)

            MockLLM.configure_litellm.assert_awaited_once_with("my-custom-model")

    @pytest.mark.asyncio
    async def test_falls_back_to_env_model(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch.dict("os.environ", {"TASK_MODEL": "env-model"}):
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "env-model"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            await svc.generate_task(request)

            MockLLM.configure_litellm.assert_awaited_once_with("env-model")

    # -- template missing --

    @pytest.mark.asyncio
    async def test_no_template_raises_value_error(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS:
            MockTS.get_effective_template_content = AsyncMock(return_value="")

            with pytest.raises(ValueError, match="not found in database"):
                await svc.generate_task(request)

    @pytest.mark.asyncio
    async def test_none_template_raises_value_error(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS:
            MockTS.get_effective_template_content = AsyncMock(return_value=None)

            with pytest.raises(ValueError, match="not found in database"):
                await svc.generate_task(request)

    # -- agent context --

    @pytest.mark.asyncio
    async def test_agent_context_appended_to_prompt(self):
        svc = self._build_service()
        agent = Agent(name="Analyst", role="Data Analyst", goal="Analyze data", backstory="Expert")
        request = TaskGenerationRequest(text="analyze sales", agent=agent)
        captured_messages = []

        async def capture_acompletion(**kwargs):
            captured_messages.extend(kwargs.get("messages", []))
            return _make_llm_response(_valid_task_json())

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="Base prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = capture_acompletion

            await svc.generate_task(request)

        system_msg = captured_messages[0]["content"]
        assert "Analyst" in system_msg
        assert "Data Analyst" in system_msg
        assert "Analyze data" in system_msg
        assert "Expert" in system_msg

    # -- code-block extraction --

    @pytest.mark.asyncio
    async def test_json_in_code_block_extracted(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        wrapped = '```json\n' + _valid_task_json() + '\n```'

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(wrapped))

            result = await svc.generate_task(request)

        assert result.name == "Research Task"

    @pytest.mark.asyncio
    async def test_json_in_plain_code_block_extracted(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        wrapped = '```\n' + _valid_task_json() + '\n```'

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(wrapped))

            result = await svc.generate_task(request)

        assert result.name == "Research Task"

    # -- trailing comma cleanup --

    @pytest.mark.asyncio
    async def test_trailing_comma_cleaned(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        # JSON with trailing comma
        bad_json = '{"name": "T", "description": "D", "expected_output": "E",}'

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(bad_json))

            result = await svc.generate_task(request)

        assert result.name == "T"

    # -- empty content --

    @pytest.mark.asyncio
    async def test_empty_content_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(""))

            with pytest.raises(ValueError, match="Error generating completion"):
                await svc.generate_task(request)

    @pytest.mark.asyncio
    async def test_none_content_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value={"choices": [{"message": {"content": None}}]}
            )

            with pytest.raises(ValueError, match="Error generating completion"):
                await svc.generate_task(request)

    # -- LLM error --

    @pytest.mark.asyncio
    async def test_llm_error_logs_and_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(side_effect=RuntimeError("LLM unavailable"))

            with pytest.raises(ValueError, match="Error generating completion"):
                await svc.generate_task(request)

        # Verify error was logged
        assert svc.log_service.create_log.await_count == 1
        call_kwargs = svc.log_service.create_log.call_args.kwargs
        assert call_kwargs["status"] == "error"

    # -- JSON parse error --

    @pytest.mark.asyncio
    async def test_json_parse_error_logs_and_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.robust_json_parser") as mock_parser:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response("not valid json"))
            mock_parser.side_effect = ValueError("cannot parse")

            with pytest.raises(ValueError, match="Could not parse response as JSON"):
                await svc.generate_task(request)

        # Two log calls: success for LLM response + error for parse failure
        assert svc.log_service.create_log.await_count == 2

    # -- missing required fields --

    @pytest.mark.asyncio
    async def test_missing_name_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        incomplete = json.dumps({"description": "D", "expected_output": "E"})

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(incomplete))

            with pytest.raises(ValueError, match="Missing required field: name"):
                await svc.generate_task(request)

    @pytest.mark.asyncio
    async def test_missing_description_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        incomplete = json.dumps({"name": "N", "expected_output": "E"})

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(incomplete))

            with pytest.raises(ValueError, match="Missing required field: description"):
                await svc.generate_task(request)

    @pytest.mark.asyncio
    async def test_missing_expected_output_raises(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        incomplete = json.dumps({"name": "N", "description": "D"})

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(incomplete))

            with pytest.raises(ValueError, match="Missing required field: expected_output"):
                await svc.generate_task(request)

    # -- tools default --

    @pytest.mark.asyncio
    async def test_tools_default_to_empty_list(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            result = await svc.generate_task(request)

        assert result.tools == []

    @pytest.mark.asyncio
    async def test_tools_preserved_when_present(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_json = _valid_task_json(tools=[{"name": "web_search"}])

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(task_json))

            result = await svc.generate_task(request)

        assert result.tools == [{"name": "web_search"}]

    # -- advanced_config defaults (no advanced_config in JSON) --

    @pytest.mark.asyncio
    async def test_advanced_config_defaults_created(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            result = await svc.generate_task(request)

        ac = result.advanced_config
        assert ac.async_execution is False
        assert ac.context == []
        assert ac.output_json is None
        assert ac.output_pydantic is None
        assert ac.output_file is None
        assert ac.human_input is False
        assert ac.markdown is False
        assert ac.retry_on_fail is True
        assert ac.max_retries == 3
        assert ac.timeout is None
        assert ac.priority == 1
        assert ac.dependencies == []

    # -- advanced_config fixes --

    @pytest.mark.asyncio
    async def test_output_json_bool_fixed_to_none(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"output_json": True},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.output_json is None

    @pytest.mark.asyncio
    async def test_output_json_valid_string_parsed_to_dict(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"output_json": '{"key": "value"}'},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.output_json == {"key": "value"}

    @pytest.mark.asyncio
    async def test_output_json_invalid_string_fixed_to_none(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"output_json": "not-json-at-all"},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.output_json is None

    @pytest.mark.asyncio
    async def test_output_pydantic_bool_fixed_to_none(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"output_pydantic": True},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.output_pydantic is None

    @pytest.mark.asyncio
    async def test_context_non_list_fixed(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"context": "not-a-list"},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.context == []

    @pytest.mark.asyncio
    async def test_dependencies_non_list_fixed(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"dependencies": "not-a-list"},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.dependencies == []

    @pytest.mark.asyncio
    async def test_llm_field_set_in_existing_advanced_config(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"async_execution": True},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "used-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        # The LLM field is set on the raw dict, not on AdvancedConfig schema
        # but async_execution should be preserved
        assert result.advanced_config.async_execution is True

    # -- markdown flag --

    @pytest.mark.asyncio
    async def test_markdown_flag_appends_instructions(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"markdown": True},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert "Markdown" in result.description
        assert "Markdown" in result.expected_output

    @pytest.mark.asyncio
    async def test_markdown_false_does_not_append(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "Desc only", "expected_output": "Out only",
            "advanced_config": {"markdown": False},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert "Markdown" not in result.description
        assert "Markdown" not in result.expected_output

    # -- fast_planning parameter --

    @pytest.mark.asyncio
    async def test_fast_planning_uses_low_temperature(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        captured_kwargs = {}

        async def capture_acompletion(**kwargs):
            captured_kwargs.update(kwargs)
            return _make_llm_response(_valid_task_json())

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = capture_acompletion

            await svc.generate_task(request, fast_planning=True)

        assert captured_kwargs["temperature"] == 0.2
        assert captured_kwargs["max_tokens"] == 1200

    @pytest.mark.asyncio
    async def test_normal_planning_uses_higher_temperature(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        captured_kwargs = {}

        async def capture_acompletion(**kwargs):
            captured_kwargs.update(kwargs)
            return _make_llm_response(_valid_task_json())

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = capture_acompletion

            await svc.generate_task(request, fast_planning=False)

        assert captured_kwargs["temperature"] == 0.7
        assert captured_kwargs["max_tokens"] == 4000

    # -- llm_guardrail passthrough --

    @pytest.mark.asyncio
    async def test_llm_guardrail_passed_through(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "llm_guardrail": {"description": "Check for correctness", "llm_model": "some-model"},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.llm_guardrail is not None
        assert result.llm_guardrail.description == "Check for correctness"

    @pytest.mark.asyncio
    async def test_llm_guardrail_none_by_default(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            result = await svc.generate_task(request)

        assert result.llm_guardrail is None

    # -- group_context passed to log --

    @pytest.mark.asyncio
    async def test_group_context_forwarded_to_log(self):
        svc = self._build_service()
        gc = _make_group_context()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            await svc.generate_task(request, group_context=gc)

        call_kwargs = svc.log_service.create_log.call_args.kwargs
        assert call_kwargs["group_context"] is gc

    # -- advanced_config setdefault fills missing fields --

    @pytest.mark.asyncio
    async def test_advanced_config_missing_fields_get_defaults(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        # Provide advanced_config with only one field
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"priority": 5},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        ac = result.advanced_config
        assert ac.priority == 5  # preserved
        assert ac.async_execution is False  # default
        assert ac.retry_on_fail is True  # default
        assert ac.max_retries == 3  # default

    # -- output_json as dict passes through --

    @pytest.mark.asyncio
    async def test_output_json_dict_preserved(self):
        svc = self._build_service()
        request = TaskGenerationRequest(text="task")
        task_data = {
            "name": "T", "description": "D", "expected_output": "E",
            "advanced_config": {"output_json": {"schema": "v1"}},
        }

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(json.dumps(task_data))
            )

            result = await svc.generate_task(request)

        assert result.advanced_config.output_json == {"schema": "v1"}


# ---------------------------------------------------------------------------
# Tests for generate_and_save_task
# ---------------------------------------------------------------------------


class TestGenerateAndSaveTask:
    @pytest.mark.asyncio
    async def test_delegates_to_generate_task(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()

        gc = _make_group_context()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            result = await svc.generate_and_save_task(request, gc)

        assert isinstance(result, dict)
        assert result["name"] == "Research Task"
        assert result["description"] == "Research the topic thoroughly"

    @pytest.mark.asyncio
    async def test_passes_fast_planning_flag(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        gc = _make_group_context()
        request = TaskGenerationRequest(text="task")

        captured_kwargs = {}

        async def capture_acompletion(**kwargs):
            captured_kwargs.update(kwargs)
            return _make_llm_response(_valid_task_json())

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = capture_acompletion

            # fast_planning=True
            await svc.generate_and_save_task(request, gc, fast_planning=True)

        assert captured_kwargs["temperature"] == 0.2

    @pytest.mark.asyncio
    async def test_returns_model_dump_dict(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        gc = _make_group_context()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response(_valid_task_json()))

            result = await svc.generate_and_save_task(request, gc)

        # Verify it is a dict with all expected keys
        assert "name" in result
        assert "description" in result
        assert "expected_output" in result
        assert "tools" in result
        assert "advanced_config" in result


# ---------------------------------------------------------------------------
# Tests for convert_to_task_create
# ---------------------------------------------------------------------------


class TestConvertToTaskCreate:
    def _make_response(self, **overrides):
        defaults = dict(
            name="Task1",
            description="Desc",
            expected_output="Output",
            tools=[],
            advanced_config=AdvancedConfig(),
            llm_guardrail=None,
        )
        defaults.update(overrides)
        return TaskGenerationResponse(**defaults)

    def test_basic_conversion(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response()

        result = svc.convert_to_task_create(resp)

        assert result.name == "Task1"
        assert result.description == "Desc"
        assert result.expected_output == "Output"
        assert result.tools == []

    def test_dict_tools_converted_to_names(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response(tools=[{"name": "web_search"}, {"name": "calculator"}])

        result = svc.convert_to_task_create(resp)

        assert result.tools == ["web_search", "calculator"]

    def test_string_tools_preserved(self):
        """Test that string tools in the tools list are correctly extracted.

        TaskGenerationResponse.tools is List[Dict], so we use a SimpleNamespace
        to simulate a response object where tools contains raw string entries
        (as would come from poorly-structured LLM JSON output).
        """
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = _make_generation_response_ns(tools=["tool_a", "tool_b"])

        result = svc.convert_to_task_create(resp)

        assert result.tools == ["tool_a", "tool_b"]

    def test_mixed_tools(self):
        """Test mixed dict/string tools, verifying dicts without 'name' are skipped."""
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = _make_generation_response_ns(tools=[{"name": "t1"}, "t2", {"no_name": True}])

        result = svc.convert_to_task_create(resp)

        # dict without 'name' key is skipped, only named entries and strings kept
        assert result.tools == ["t1", "t2"]

    def test_output_json_serialized(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response(
            advanced_config=AdvancedConfig(output_json={"schema": "v2"})
        )

        result = svc.convert_to_task_create(resp)

        assert result.output_json == '{"schema": "v2"}'
        assert result.config.output_json == '{"schema": "v2"}'

    def test_output_json_none_when_not_set(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response()

        result = svc.convert_to_task_create(resp)

        assert result.output_json is None

    def test_advanced_config_fields_mapped(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response(
            advanced_config=AdvancedConfig(
                async_execution=True,
                context=["ctx1"],
                human_input=True,
                markdown=True,
                output_file="/tmp/out.txt",
                output_pydantic="MyModel",
                callback="my_callback",
            )
        )

        result = svc.convert_to_task_create(resp)

        assert result.async_execution is True
        assert result.context == ["ctx1"]
        assert result.human_input is True
        assert result.markdown is True
        assert result.output_file == "/tmp/out.txt"
        assert result.output_pydantic == "MyModel"
        assert result.callback == "my_callback"

    def test_config_fields_mapped(self):
        """Verify TaskConfig fields are populated from AdvancedConfig.

        Note: AdvancedConfig.max_retries is passed to TaskConfig constructor
        but TaskConfig does not have a 'max_retries' field (it has
        'guardrail_max_retries'), so that value is silently ignored by Pydantic.
        """
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response(
            advanced_config=AdvancedConfig(
                retry_on_fail=False,
                max_retries=5,
                timeout=120,
                priority=3,
                error_handling="retry",
                cache_response=False,
                cache_ttl=7200,
            )
        )

        result = svc.convert_to_task_create(resp)

        assert result.config.retry_on_fail is False
        assert result.config.timeout == 120
        assert result.config.priority == 3
        assert result.config.error_handling == "retry"
        assert result.config.cache_response is False
        assert result.config.cache_ttl == 7200

    def test_dict_tool_without_name_key_is_skipped(self):
        """A dict tool entry without a 'name' key should be ignored."""
        session = MagicMock()
        svc = TaskGenerationService(session)
        resp = self._make_response(tools=[{"description": "some tool"}])

        result = svc.convert_to_task_create(resp)

        assert result.tools == []


# ---------------------------------------------------------------------------
# Edge case: LLM error logging with group_context
# ---------------------------------------------------------------------------


class TestGenerateTaskErrorLogging:
    @pytest.mark.asyncio
    async def test_llm_error_includes_group_context_in_log(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        gc = _make_group_context()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(side_effect=RuntimeError("fail"))

            with pytest.raises(ValueError):
                await svc.generate_task(request, group_context=gc)

        call_kwargs = svc.log_service.create_log.call_args.kwargs
        assert call_kwargs["group_context"] is gc
        assert call_kwargs["status"] == "error"

    @pytest.mark.asyncio
    async def test_json_parse_error_includes_group_context_in_log(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        gc = _make_group_context()
        request = TaskGenerationRequest(text="task")

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM, \
             patch("src.services.task_generation_service.robust_json_parser") as mock_parser:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = AsyncMock(return_value=_make_llm_response("bad json"))
            mock_parser.side_effect = ValueError("parse fail")

            with pytest.raises(ValueError):
                await svc.generate_task(request, group_context=gc)

        # Second call should be the error log for parse failure
        error_call = svc.log_service.create_log.call_args_list[1]
        assert error_call.kwargs["group_context"] is gc
        assert error_call.kwargs["status"] == "error"


# ---------------------------------------------------------------------------
# Message construction tests
# ---------------------------------------------------------------------------


class TestMessageConstruction:
    @pytest.mark.asyncio
    async def test_system_and_user_messages_sent(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        request = TaskGenerationRequest(text="My specific task request")
        captured_messages = []

        async def capture_acompletion(**kwargs):
            captured_messages.extend(kwargs.get("messages", []))
            return _make_llm_response(_valid_task_json())

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="System template")
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "m"})
            MockLLM.acompletion = capture_acompletion

            await svc.generate_task(request)

        assert len(captured_messages) == 2
        assert captured_messages[0]["role"] == "system"
        assert captured_messages[0]["content"] == "System template"
        assert captured_messages[1]["role"] == "user"
        assert captured_messages[1]["content"] == "My specific task request"

    @pytest.mark.asyncio
    async def test_model_params_forwarded_to_acompletion(self):
        session = MagicMock()
        svc = TaskGenerationService(session)
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock()
        request = TaskGenerationRequest(text="task")
        captured_kwargs = {}

        async def capture_acompletion(**kwargs):
            captured_kwargs.update(kwargs)
            return _make_llm_response(_valid_task_json())

        with patch("src.services.task_generation_service.TemplateService") as MockTS, \
             patch("src.services.task_generation_service.LLMManager") as MockLLM:
            MockTS.get_effective_template_content = AsyncMock(return_value="prompt")
            MockLLM.configure_litellm = AsyncMock(
                return_value={"model": "custom-model", "api_base": "https://example.com"}
            )
            MockLLM.acompletion = capture_acompletion

            await svc.generate_task(request)

        assert captured_kwargs["model"] == "custom-model"
        assert captured_kwargs["api_base"] == "https://example.com"
