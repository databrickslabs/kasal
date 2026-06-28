"""
Unit tests for LLMInjectionGuardrail (prompt_injection_check type).

Tests patch ``_run_completion`` — the module-level helper that calls
LLMManager.completion() — so no real LLM or DB calls are made.
"""
import pytest
from unittest.mock import MagicMock, patch

# Path to the module-level helper used by validate()
_RUN_COMPLETION = "src.engines.crewai.guardrails.core.llm_injection_guardrail._run_completion"


def _make_guardrail(config=None):
    """Create a guardrail with _run_completion mocked."""
    cfg = config or {"llm_model": "databricks-test-model"}
    from src.engines.crewai.guardrails.core.llm_injection_guardrail import LLMInjectionGuardrail
    return LLMInjectionGuardrail(cfg)


class TestLLMInjectionGuardrailInit:
    def test_default_model_stored(self):
        g = _make_guardrail(None)
        # Default from helper is "databricks-test-model"
        assert g._model_name == "databricks-test-model"

    def test_model_name_stored_without_prefix(self):
        g = _make_guardrail({"llm_model": "databricks-claude-sonnet-4-5"})
        assert g._model_name == "databricks-claude-sonnet-4-5"

    def test_model_already_prefixed_prefix_stripped(self):
        g = _make_guardrail({"llm_model": "databricks/some-model"})
        assert g._model_name == "some-model"

    def test_cache_size_default(self):
        g = _make_guardrail({})
        assert g._cache_max == 128


class TestLLMInjectionGuardrailValidate:
    @pytest.fixture
    def guardrail(self):
        return _make_guardrail({"llm_model": "databricks-test-model"})

    @patch(_RUN_COMPLETION, return_value="INJECTION")
    def test_injection_verdict_returns_invalid(self, mock_rc, guardrail):
        result = guardrail.validate("Some suspicious output.")
        assert result["valid"] is False
        assert "injection" in result["feedback"].lower()

    @patch(_RUN_COMPLETION, return_value="SAFE")
    def test_safe_verdict_returns_valid(self, mock_rc, guardrail):
        result = guardrail.validate("Normal task output.")
        assert result["valid"] is True
        assert result["feedback"] == ""

    @patch(_RUN_COMPLETION, return_value="injection\n")
    def test_verdict_case_insensitive_injection(self, mock_rc, guardrail):
        result = guardrail.validate("Some output.")
        assert result["valid"] is False

    @patch(_RUN_COMPLETION, return_value="  safe  ")
    def test_verdict_case_insensitive_safe(self, mock_rc, guardrail):
        result = guardrail.validate("Normal output.")
        assert result["valid"] is True

    @patch(_RUN_COMPLETION, return_value="UNKNOWN")
    def test_unknown_verdict_treated_as_safe(self, mock_rc, guardrail):
        result = guardrail.validate("Some output.")
        assert result["valid"] is True

    @patch(_RUN_COMPLETION, side_effect=RuntimeError("API unavailable"))
    def test_llm_exception_fail_open(self, mock_rc, guardrail):
        result = guardrail.validate("Some output.")
        assert result["valid"] is True
        assert result["feedback"] == ""

    def test_empty_string_output_passes_without_llm_call(self, guardrail):
        with patch(_RUN_COMPLETION) as mock_rc:
            result = guardrail.validate("")
            mock_rc.assert_not_called()
        assert result["valid"] is True

    def test_none_output_passes_without_llm_call(self, guardrail):
        with patch(_RUN_COMPLETION) as mock_rc:
            result = guardrail.validate(None)
            mock_rc.assert_not_called()
        assert result["valid"] is True

    @patch(_RUN_COMPLETION, return_value="SAFE")
    def test_task_output_object_extracted(self, mock_rc, guardrail):
        mock_task_output = MagicMock()
        mock_task_output.raw = "This is the task result."
        result = guardrail.validate(mock_task_output)
        assert result["valid"] is True
        # Check messages passed to _run_completion
        messages = mock_rc.call_args[0][1]
        user_msg = next(m for m in messages if m["role"] == "user")
        assert "This is the task result." in user_msg["content"]

    @patch(_RUN_COMPLETION, return_value="SAFE")
    def test_long_text_truncated_to_3000_chars(self, mock_rc, guardrail):
        long_text = "A" * 5000
        guardrail.validate(long_text)
        messages = mock_rc.call_args[0][1]
        user_msg = next(m for m in messages if m["role"] == "user")
        assert len(user_msg["content"]) <= 3000

    @patch(_RUN_COMPLETION, return_value="SAFE")
    def test_dict_output_extracted(self, mock_rc, guardrail):
        guardrail.validate({"output": "task result"})
        messages = mock_rc.call_args[0][1]
        user_msg = next(m for m in messages if m["role"] == "user")
        assert "task result" in user_msg["content"]


class TestLLMInjectionGuardrailFactoryRegistration:
    def test_factory_creates_correct_type(self):
        from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
        from src.engines.crewai.guardrails.core.llm_injection_guardrail import LLMInjectionGuardrail
        guardrail = GuardrailFactory.create_guardrail(
            {"type": "prompt_injection_check", "llm_model": "databricks-test-model"}
        )
        assert isinstance(guardrail, LLMInjectionGuardrail)

    def test_factory_returns_none_for_unknown_type(self):
        from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
        guardrail = GuardrailFactory.create_guardrail({"type": "nonexistent_type"})
        assert guardrail is None
