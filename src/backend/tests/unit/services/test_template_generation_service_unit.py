"""
Comprehensive unit tests for TemplateGenerationService.

Covers:
- __init__ constructor (group_id validation, attribute assignment)
- _log_llm_interaction (success path, exception handling)
- generate_templates (happy path with various field-name styles, missing model config,
  missing prompt template, LLM completion error, JSON parse failure,
  missing/empty required fields in parsed response, json.JSONDecodeError branch,
  generic exception propagation)
"""

import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.schemas.template_generation import (
    TemplateGenerationRequest,
    TemplateGenerationResponse,
)
from src.services.template_generation_service import TemplateGenerationService


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

VALID_GROUP_ID = "group-abc-123"

VALID_MODEL_CONFIG = {
    "key": "test-model",
    "name": "test-model",
    "provider": "databricks",
    "temperature": 0.7,
    "context_window": 128000,
    "max_output_tokens": 4000,
    "extended_thinking": False,
    "enabled": True,
}

SYSTEM_MESSAGE = "You are a template generator. Produce JSON with system_template, prompt_template, response_template."

GOOD_LLM_JSON = json.dumps(
    {
        "system_template": "You are {role}.",
        "prompt_template": "Given {goal}, execute the task.",
        "response_template": "Here is the result: {output}",
    }
)


def _make_llm_response(content: str) -> dict:
    """Build a minimal litellm-style response dict."""
    return {"choices": [{"message": {"content": content}}]}


def _build_request(**overrides) -> TemplateGenerationRequest:
    defaults = {
        "role": "Data Analyst",
        "goal": "Analyze data trends",
        "backstory": "Expert in data science with 10 years experience",
        "model": "test-model",
    }
    defaults.update(overrides)
    return TemplateGenerationRequest(**defaults)


@pytest.fixture
def mock_session():
    """Provide a lightweight mock database session."""
    return MagicMock(name="mock_session")


@pytest.fixture
def service(mock_session):
    """
    Build a TemplateGenerationService with patched LLMLogService
    so we never touch the real DB for logging.
    """
    with patch(
        "src.services.template_generation_service.LLMLogService"
    ) as MockLogService:
        mock_log_instance = MagicMock()
        mock_log_instance.create_log = AsyncMock()
        MockLogService.return_value = mock_log_instance
        svc = TemplateGenerationService(mock_session, group_id=VALID_GROUP_ID)
    return svc


# ---------------------------------------------------------------------------
# Tests: __init__
# ---------------------------------------------------------------------------


class TestInit:
    """Tests for TemplateGenerationService constructor."""

    def test_init_stores_session_and_group_id(self):
        session = MagicMock()
        with patch("src.services.template_generation_service.LLMLogService"):
            svc = TemplateGenerationService(session, group_id=VALID_GROUP_ID)
        assert svc.session is session
        assert svc.group_id == VALID_GROUP_ID

    def test_init_creates_log_service(self):
        session = MagicMock()
        with patch(
            "src.services.template_generation_service.LLMLogService"
        ) as MockLog:
            svc = TemplateGenerationService(session, group_id=VALID_GROUP_ID)
        MockLog.assert_called_once_with(session)
        assert svc.log_service is MockLog.return_value

    @pytest.mark.parametrize("bad_group_id", [None, "", 0, False])
    def test_init_raises_on_missing_group_id(self, bad_group_id):
        with pytest.raises(ValueError, match="SECURITY"):
            TemplateGenerationService(MagicMock(), group_id=bad_group_id)


# ---------------------------------------------------------------------------
# Tests: _log_llm_interaction
# ---------------------------------------------------------------------------


class TestLogLLMInteraction:
    """Tests for the internal logging helper."""

    @pytest.mark.asyncio
    async def test_log_success(self, service):
        service.log_service.create_log = AsyncMock()

        await service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="hello",
            response="world",
            model="test-model",
            status="success",
            error_message=None,
        )

        service.log_service.create_log.assert_awaited_once_with(
            endpoint="test-endpoint",
            prompt="hello",
            response="world",
            model="test-model",
            status="success",
            error_message=None,
        )

    @pytest.mark.asyncio
    async def test_log_with_error_message(self, service):
        service.log_service.create_log = AsyncMock()

        await service._log_llm_interaction(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
            status="error",
            error_message="something went wrong",
        )

        service.log_service.create_log.assert_awaited_once()
        call_kwargs = service.log_service.create_log.call_args.kwargs
        assert call_kwargs["status"] == "error"
        assert call_kwargs["error_message"] == "something went wrong"

    @pytest.mark.asyncio
    async def test_log_swallows_exception(self, service):
        """Logging failures must not propagate to callers."""
        service.log_service.create_log = AsyncMock(
            side_effect=RuntimeError("DB down")
        )

        # Should NOT raise
        await service._log_llm_interaction(
            endpoint="ep",
            prompt="p",
            response="r",
            model="m",
        )


# ---------------------------------------------------------------------------
# Tests: generate_templates -- happy path
# ---------------------------------------------------------------------------


class TestGenerateTemplatesHappyPath:
    """Verify the main success flow with various field-name conventions."""

    @pytest.mark.asyncio
    async def test_generate_templates_lowercase_keys(self, service):
        """Standard lowercase field names in LLM response."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            # ModelConfigService
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            # TemplateService
            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            # LLMManager
            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            # robust_json_parser
            mock_parser.return_value = {
                "system_template": "sys tpl",
                "prompt_template": "prompt tpl",
                "response_template": "resp tpl",
            }

            result = await service.generate_templates(request)

        assert isinstance(result, TemplateGenerationResponse)
        assert result.system_template == "sys tpl"
        assert result.prompt_template == "prompt tpl"
        assert result.response_template == "resp tpl"

    @pytest.mark.asyncio
    async def test_generate_templates_title_case_keys(self, service):
        """LLM returns Title Case keys like 'System Template'."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            mock_parser.return_value = {
                "System Template": "sys",
                "Prompt Template": "pmt",
                "Response Template": "rsp",
            }

            result = await service.generate_templates(request)

        assert result.system_template == "sys"
        assert result.prompt_template == "pmt"
        assert result.response_template == "rsp"

    @pytest.mark.asyncio
    async def test_generate_templates_mixed_case_keys(self, service):
        """LLM returns mixed-style keys like System_Template."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            mock_parser.return_value = {
                "System_Template": "sys_t",
                "Prompt_Template": "pmt_t",
                "Response_Template": "rsp_t",
            }

            result = await service.generate_templates(request)

        assert result.system_template == "sys_t"
        assert result.prompt_template == "pmt_t"
        assert result.response_template == "rsp_t"

    @pytest.mark.asyncio
    async def test_logs_successful_interaction(self, service):
        """Verify that a successful generation logs the interaction."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        # The log_service.create_log should have been called at least once for success
        service.log_service.create_log.assert_awaited()
        call_kwargs = service.log_service.create_log.call_args.kwargs
        assert call_kwargs["endpoint"] == "generate-templates"
        assert call_kwargs["model"] == "test-model"
        assert call_kwargs.get("status", "success") == "success"


# ---------------------------------------------------------------------------
# Tests: generate_templates -- error paths
# ---------------------------------------------------------------------------


class TestGenerateTemplatesModelConfigErrors:
    """Errors related to model config retrieval."""

    @pytest.mark.asyncio
    async def test_model_config_not_found(self, service):
        """When get_model_config returns None, raise ValueError."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=None)
            MockMCS.return_value = mock_mcs_inst

            with pytest.raises(ValueError, match="not found in the database"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_model_config_raises_exception(self, service):
        """When get_model_config itself raises, the exception propagates."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(
                side_effect=RuntimeError("DB connection lost")
            )
            MockMCS.return_value = mock_mcs_inst

            with pytest.raises(RuntimeError, match="DB connection lost"):
                await service.generate_templates(request)


class TestGenerateTemplatesPromptTemplateErrors:
    """Errors related to prompt template retrieval."""

    @pytest.mark.asyncio
    async def test_prompt_template_not_found_empty_string(self, service):
        """Empty string from get_template_content triggers ValueError."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value="")
            MockTS.return_value = mock_ts_inst

            with pytest.raises(ValueError, match="Required prompt template"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_prompt_template_not_found_none(self, service):
        """None from get_template_content triggers ValueError."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=None)
            MockTS.return_value = mock_ts_inst

            with pytest.raises(ValueError, match="Required prompt template"):
                await service.generate_templates(request)


class TestGenerateTemplatesLLMCompletionErrors:
    """Errors during LLM completion calls."""

    @pytest.mark.asyncio
    async def test_acompletion_raises_value_error(self, service):
        """When LLMManager.acompletion fails, logs error and raises ValueError."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                side_effect=RuntimeError("Rate limit exceeded")
            )

            with pytest.raises(ValueError, match="Failed to generate templates"):
                await service.generate_templates(request)

        # Verify that the error was logged
        service.log_service.create_log.assert_awaited()
        call_kwargs = service.log_service.create_log.call_args.kwargs
        assert call_kwargs["status"] == "error"
        assert "Error generating completion" in call_kwargs["error_message"]

    @pytest.mark.asyncio
    async def test_acompletion_error_log_failure_does_not_mask_original(self, service):
        """If logging the error also fails, the original ValueError still propagates."""
        request = _build_request()
        service.log_service.create_log = AsyncMock(
            side_effect=RuntimeError("log DB down")
        )

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                side_effect=RuntimeError("LLM timeout")
            )

            with pytest.raises(ValueError, match="Failed to generate templates"):
                await service.generate_templates(request)


class TestGenerateTemplatesJSONParseErrors:
    """Errors during JSON parsing of LLM output."""

    @pytest.mark.asyncio
    async def test_robust_parser_raises_value_error(self, service):
        """When robust_json_parser raises ValueError, it should propagate."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("not valid json at all")
            )

            mock_parser.side_effect = ValueError(
                "Could not parse response as JSON after multiple recovery attempts"
            )

            with pytest.raises(ValueError, match="Could not parse"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_json_decode_error_branch(self, service):
        """
        Exercise the json.JSONDecodeError except clause in generate_templates.
        Even though robust_json_parser handles most cases, the code has an explicit
        json.JSONDecodeError handler.
        """
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("{invalid")
            )

            mock_parser.side_effect = json.JSONDecodeError(
                "Expecting value", "{invalid", 0
            )

            with pytest.raises(ValueError, match="Failed to parse AI response as JSON"):
                await service.generate_templates(request)


class TestGenerateTemplatesMissingFields:
    """Tests for missing or empty required fields after parsing."""

    @pytest.mark.asyncio
    async def test_missing_system_template(self, service):
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            # system_template is missing
            mock_parser.return_value = {
                "prompt_template": "b",
                "response_template": "c",
            }

            with pytest.raises(ValueError, match="system_template"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_missing_prompt_template_field(self, service):
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            mock_parser.return_value = {
                "system_template": "a",
                "response_template": "c",
            }

            with pytest.raises(ValueError, match="prompt_template"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_missing_response_template_field(self, service):
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
            }

            with pytest.raises(ValueError, match="response_template"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_empty_string_field_treated_as_missing(self, service):
        """An empty-string value for a required field triggers ValueError."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            mock_parser.return_value = {
                "system_template": "",
                "prompt_template": "b",
                "response_template": "c",
            }

            with pytest.raises(ValueError, match="system_template"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_all_fields_none_first_fails(self, service):
        """When all template fields resolve to None, the first one triggers the error."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            # Empty dict from parser -- all fields will be None after normalization
            mock_parser.return_value = {}

            with pytest.raises(ValueError, match="system_template"):
                await service.generate_templates(request)


# ---------------------------------------------------------------------------
# Tests: generate_templates -- integration-like scenarios
# ---------------------------------------------------------------------------


class TestGenerateTemplatesEdgeCases:
    """Additional edge-case scenarios."""

    @pytest.mark.asyncio
    async def test_configure_litellm_called_with_model_name(self, service):
        """Verify configure_litellm receives the name from model_config dict."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            custom_config = {**VALID_MODEL_CONFIG, "name": "special-model-v2"}
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=custom_config)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(
                return_value={"model": "special-model-v2"}
            )
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        MockLLM.configure_litellm.assert_awaited_once_with("special-model-v2")

    @pytest.mark.asyncio
    async def test_acompletion_receives_correct_params(self, service):
        """Verify acompletion is called with model_params, messages, temperature, max_tokens."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(
                return_value={"model": "test-model", "api_key": "key123"}
            )
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        call_kwargs = MockLLM.acompletion.call_args.kwargs
        assert call_kwargs["model"] == "test-model"
        assert call_kwargs["api_key"] == "key123"
        assert call_kwargs["temperature"] == 0.7
        assert call_kwargs["max_tokens"] == 4000
        assert len(call_kwargs["messages"]) == 2
        assert call_kwargs["messages"][0]["role"] == "system"
        assert call_kwargs["messages"][1]["role"] == "user"

    @pytest.mark.asyncio
    async def test_user_prompt_contains_request_fields(self, service):
        """The user prompt sent to the LLM should include role, goal, backstory."""
        request = _build_request(
            role="Architect",
            goal="Design systems",
            backstory="20 years in software",
        )
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        user_message = MockLLM.acompletion.call_args.kwargs["messages"][1]["content"]
        assert "Architect" in user_message
        assert "Design systems" in user_message
        assert "20 years in software" in user_message

    @pytest.mark.asyncio
    async def test_model_config_service_gets_group_id(self, service):
        """Verify ModelConfigService is instantiated with the correct group_id."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        MockMCS.assert_called_once_with(service.session, group_id=VALID_GROUP_ID)

    @pytest.mark.asyncio
    async def test_template_service_gets_correct_template_name(self, service):
        """Verify TemplateService.get_template_content is called with 'generate_templates'."""
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response(GOOD_LLM_JSON)
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        mock_ts_inst.get_template_content.assert_awaited_once_with(
            "generate_templates"
        )

    @pytest.mark.asyncio
    async def test_generic_exception_reraises(self, service):
        """
        The outer except block re-raises non-JSONDecodeError / non-ValueError exceptions.
        """
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("content")
            )

            # Parser succeeds but returns valid data; we trigger an error after
            # by making TemplateGenerationResponse constructor fail via a TypeError
            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            # Patch TemplateGenerationResponse to raise TypeError
            with patch(
                "src.services.template_generation_service.TemplateGenerationResponse",
                side_effect=TypeError("unexpected kwarg"),
            ):
                with pytest.raises(TypeError, match="unexpected kwarg"):
                    await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_fallback_normalization_prefers_lowercase(self, service):
        """
        When both lowercase and Title Case keys exist, lowercase wins
        because of the `or` chain evaluation order.
        """
        request = _build_request()
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("irrelevant")
            )

            mock_parser.return_value = {
                "system_template": "lowercase_wins",
                "System Template": "should_not_win",
                "prompt_template": "lowercase_prompt",
                "Prompt Template": "should_not_win_prompt",
                "response_template": "lowercase_response",
                "Response Template": "should_not_win_response",
            }

            result = await service.generate_templates(request)

        assert result.system_template == "lowercase_wins"
        assert result.prompt_template == "lowercase_prompt"
        assert result.response_template == "lowercase_response"

    @pytest.mark.asyncio
    async def test_configure_litellm_error_propagates(self, service):
        """When configure_litellm fails, the exception propagates through the outer handler."""
        request = _build_request()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(return_value=SYSTEM_MESSAGE)
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(
                side_effect=ValueError("provider not supported")
            )

            with pytest.raises(ValueError, match="provider not supported"):
                await service.generate_templates(request)

    @pytest.mark.asyncio
    async def test_successful_log_after_completion(self, service):
        """
        Verify the prompt logged includes both the system message and user prompt.
        """
        request = _build_request(role="Logger")
        service.log_service.create_log = AsyncMock()

        with (
            patch(
                "src.services.template_generation_service.ModelConfigService"
            ) as MockMCS,
            patch(
                "src.services.template_generation_service.TemplateService"
            ) as MockTS,
            patch(
                "src.services.template_generation_service.LLMManager"
            ) as MockLLM,
            patch(
                "src.services.template_generation_service.robust_json_parser"
            ) as mock_parser,
        ):
            mock_mcs_inst = AsyncMock()
            mock_mcs_inst.get_model_config = AsyncMock(return_value=VALID_MODEL_CONFIG)
            MockMCS.return_value = mock_mcs_inst

            mock_ts_inst = AsyncMock()
            mock_ts_inst.get_template_content = AsyncMock(
                return_value="System prompt text"
            )
            MockTS.return_value = mock_ts_inst

            MockLLM.configure_litellm = AsyncMock(return_value={"model": "test-model"})
            MockLLM.acompletion = AsyncMock(
                return_value=_make_llm_response("llm output")
            )

            mock_parser.return_value = {
                "system_template": "a",
                "prompt_template": "b",
                "response_template": "c",
            }

            await service.generate_templates(request)

        call_kwargs = service.log_service.create_log.call_args.kwargs
        assert "System: System prompt text" in call_kwargs["prompt"]
        assert "User:" in call_kwargs["prompt"]
        assert "Logger" in call_kwargs["prompt"]
        assert call_kwargs["response"] == "llm output"
