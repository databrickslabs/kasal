"""
Unit tests for PromptImprovementService.

Covers construction, the improve_prompt happy path, template usage,
fallbacks for missing/empty improved fields, non-dict LLM responses,
and the error/logging paths.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.prompt_improvement_service import PromptImprovementService
from src.utils.user_context import GroupContext


def _make_group_context(**overrides):
    defaults = dict(
        group_ids=["grp1"],
        group_email="user@example.com",
        email_domain="example.com",
    )
    defaults.update(overrides)
    return GroupContext(**defaults)


AGENT_FIELDS = {"role": "helper", "goal": "help with data", "backstory": "knows data"}


class TestInit:
    def test_session_stored(self):
        session = MagicMock()
        svc = PromptImprovementService(session)
        assert svc.session is session

    def test_log_service_created(self):
        session = MagicMock()
        with patch("src.services.prompt_improvement_service.LLMLogRepository") as MockRepo, \
             patch("src.services.prompt_improvement_service.LLMLogService") as MockLogSvc:
            svc = PromptImprovementService(session)
            MockRepo.assert_called_once_with(session)
            MockLogSvc.assert_called_once_with(MockRepo.return_value)
            assert svc.log_service is MockLogSvc.return_value


class TestImprovePrompt:
    @pytest.mark.asyncio
    async def test_returns_improved_fields_using_passed_model(self):
        svc = PromptImprovementService(MagicMock())
        svc._log_llm_interaction = AsyncMock()
        improved = {
            "role": "Data Analysis Specialist",
            "goal": "Analyze datasets to surface trends",
            "backstory": "Seasoned analyst with reporting expertise.",
        }
        with patch("src.services.prompt_improvement_service.TemplateService") as MockTpl, \
             patch("src.services.prompt_improvement_service.LLMManager") as MockLLM:
            MockTpl.get_effective_template_content = AsyncMock(return_value="SYSTEM TPL")
            MockLLM.completion = AsyncMock(return_value=json.dumps(improved))
            result = await svc.improve_prompt(
                target="agent",
                fields=AGENT_FIELDS,
                model="databricks-claude-sonnet-4-5",
                group_context=_make_group_context(),
            )
        assert result == improved
        # Uses the model passed (the form's selection), not a hardcoded one.
        assert MockLLM.completion.call_args.kwargs["model"] == "databricks-claude-sonnet-4-5"
        # The system prompt is the group-overridable template.
        messages = MockLLM.completion.call_args.kwargs["messages"]
        assert messages[0] == {"role": "system", "content": "SYSTEM TPL"}
        # The user message carries target + fields as JSON.
        payload = json.loads(messages[1]["content"])
        assert payload["target"] == "agent"
        assert payload["fields"] == AGENT_FIELDS
        svc._log_llm_interaction.assert_awaited()

    @pytest.mark.asyncio
    async def test_falls_back_to_a_default_model_when_none_passed(self):
        svc = PromptImprovementService(MagicMock())
        svc._log_llm_interaction = AsyncMock()
        with patch("src.services.prompt_improvement_service.TemplateService") as MockTpl, \
             patch("src.services.prompt_improvement_service.LLMManager") as MockLLM:
            MockTpl.get_effective_template_content = AsyncMock(return_value="tpl")
            MockLLM.completion = AsyncMock(return_value=json.dumps(AGENT_FIELDS))
            await svc.improve_prompt(target="agent", fields=AGENT_FIELDS, model=None)
        assert MockLLM.completion.call_args.kwargs["model"]

    @pytest.mark.asyncio
    async def test_missing_or_empty_improved_fields_fall_back_to_original(self):
        svc = PromptImprovementService(MagicMock())
        svc._log_llm_interaction = AsyncMock()
        # LLM drops "backstory", blanks "goal", and adds an unknown key.
        partial = {"role": "Improved Role", "goal": "   ", "extra": "ignored"}
        with patch("src.services.prompt_improvement_service.TemplateService") as MockTpl, \
             patch("src.services.prompt_improvement_service.LLMManager") as MockLLM:
            MockTpl.get_effective_template_content = AsyncMock(return_value="tpl")
            MockLLM.completion = AsyncMock(return_value=json.dumps(partial))
            result = await svc.improve_prompt(target="agent", fields=AGENT_FIELDS)
        assert result == {
            "role": "Improved Role",
            "goal": AGENT_FIELDS["goal"],
            "backstory": AGENT_FIELDS["backstory"],
        }

    @pytest.mark.asyncio
    async def test_non_dict_response_raises_and_logs_error(self):
        svc = PromptImprovementService(MagicMock())
        svc._log_llm_interaction = AsyncMock()
        with patch("src.services.prompt_improvement_service.TemplateService") as MockTpl, \
             patch("src.services.prompt_improvement_service.LLMManager") as MockLLM, \
             patch("src.services.prompt_improvement_service.robust_json_parser", return_value=["not", "a", "dict"]):
            MockTpl.get_effective_template_content = AsyncMock(return_value="tpl")
            MockLLM.completion = AsyncMock(return_value="[]")
            with pytest.raises(ValueError):
                await svc.improve_prompt(target="task", fields={"description": "d"})
        assert svc._log_llm_interaction.await_args.kwargs.get("status") == "error"

    @pytest.mark.asyncio
    async def test_llm_error_propagates_and_logs_error(self):
        svc = PromptImprovementService(MagicMock())
        svc._log_llm_interaction = AsyncMock()
        with patch("src.services.prompt_improvement_service.TemplateService") as MockTpl, \
             patch("src.services.prompt_improvement_service.LLMManager") as MockLLM:
            MockTpl.get_effective_template_content = AsyncMock(return_value="tpl")
            MockLLM.completion = AsyncMock(side_effect=RuntimeError("boom"))
            with pytest.raises(RuntimeError):
                await svc.improve_prompt(target="agent", fields=AGENT_FIELDS)
        assert svc._log_llm_interaction.await_args.kwargs.get("status") == "error"


class TestLogLLMInteraction:
    @pytest.mark.asyncio
    async def test_logging_failure_is_swallowed(self):
        svc = PromptImprovementService(MagicMock())
        svc.log_service = MagicMock()
        svc.log_service.create_log = AsyncMock(side_effect=RuntimeError("db down"))
        # Must not raise.
        await svc._log_llm_interaction(prompt="p", response="r", model="m")
