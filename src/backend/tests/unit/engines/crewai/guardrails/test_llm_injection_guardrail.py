"""
Unit tests for LLMInjectionGuardrail (prompt_injection_check type).

Note: The guardrail imports `from crewai import LLM` inside __init__, so tests
patch `crewai.LLM` directly before the guardrail is instantiated.
"""
import pytest
from unittest.mock import MagicMock, patch


def _make_guardrail(config=None, llm_mock=None):
    """Helper: create a guardrail with a mocked crewai.LLM."""
    cfg = config or {"llm_model": "databricks-test-model"}
    mock_llm_instance = MagicMock() if llm_mock is None else llm_mock
    mock_llm_class = MagicMock(return_value=mock_llm_instance)
    with patch("crewai.LLM", mock_llm_class):
        from src.engines.crewai.guardrails.llm_injection_guardrail import LLMInjectionGuardrail
        guardrail = LLMInjectionGuardrail(cfg)
    # Overwrite _llm so tests can control return values
    guardrail._llm = mock_llm_instance
    return guardrail, mock_llm_class, mock_llm_instance


class TestLLMInjectionGuardrailInit:
    def test_default_model_normalised(self):
        _, mock_llm_class, _ = _make_guardrail({})
        called_model = mock_llm_class.call_args[1]["model"]
        assert called_model.startswith("databricks/")

    def test_model_name_normalised_from_databricks_prefix(self):
        _, mock_llm_class, _ = _make_guardrail({"llm_model": "databricks-claude-sonnet-4-5"})
        called_model = mock_llm_class.call_args[1]["model"]
        assert called_model == "databricks/databricks-claude-sonnet-4-5"

    def test_model_already_prefixed_not_double_prefixed(self):
        _, mock_llm_class, _ = _make_guardrail({"llm_model": "databricks/some-model"})
        called_model = mock_llm_class.call_args[1]["model"]
        assert called_model == "databricks/some-model"
        assert called_model.count("databricks/") == 1

    def test_temperature_zero(self):
        _, mock_llm_class, _ = _make_guardrail({"llm_model": "databricks-test-model"})
        assert mock_llm_class.call_args[1]["temperature"] == 0.0


class TestLLMInjectionGuardrailValidate:
    @pytest.fixture
    def guardrail(self):
        g, _, llm_instance = _make_guardrail({"llm_model": "databricks-test-model"})
        return g

    def test_injection_verdict_returns_invalid(self, guardrail):
        guardrail._llm.call.return_value = "INJECTION"
        result = guardrail.validate("Some suspicious output.")
        assert result["valid"] is False
        assert "injection" in result["feedback"].lower()

    def test_safe_verdict_returns_valid(self, guardrail):
        guardrail._llm.call.return_value = "SAFE"
        result = guardrail.validate("Normal task output.")
        assert result["valid"] is True
        assert result["feedback"] == ""

    def test_verdict_case_insensitive_injection(self, guardrail):
        guardrail._llm.call.return_value = "injection\n"
        result = guardrail.validate("Some output.")
        assert result["valid"] is False

    def test_verdict_case_insensitive_safe(self, guardrail):
        guardrail._llm.call.return_value = "  safe  "
        result = guardrail.validate("Normal output.")
        assert result["valid"] is True

    def test_unknown_verdict_treated_as_safe(self, guardrail):
        guardrail._llm.call.return_value = "UNKNOWN"
        result = guardrail.validate("Some output.")
        assert result["valid"] is True

    def test_llm_exception_fail_open(self, guardrail):
        guardrail._llm.call.side_effect = RuntimeError("API unavailable")
        result = guardrail.validate("Some output.")
        assert result["valid"] is True
        assert result["feedback"] == ""

    def test_empty_string_output_passes_without_llm_call(self, guardrail):
        result = guardrail.validate("")
        guardrail._llm.call.assert_not_called()
        assert result["valid"] is True

    def test_none_output_passes_without_llm_call(self, guardrail):
        result = guardrail.validate(None)
        guardrail._llm.call.assert_not_called()
        assert result["valid"] is True

    def test_task_output_object_extracted(self, guardrail):
        guardrail._llm.call.return_value = "SAFE"
        mock_task_output = MagicMock()
        mock_task_output.raw = "This is the task result."
        result = guardrail.validate(mock_task_output)
        assert result["valid"] is True
        call_args = guardrail._llm.call.call_args[0][0]
        user_msg = next(m for m in call_args if m["role"] == "user")
        assert "This is the task result." in user_msg["content"]

    def test_long_text_truncated_to_3000_chars(self, guardrail):
        guardrail._llm.call.return_value = "SAFE"
        long_text = "A" * 5000
        guardrail.validate(long_text)
        call_args = guardrail._llm.call.call_args[0][0]
        user_msg = next(m for m in call_args if m["role"] == "user")
        assert len(user_msg["content"]) <= 3000

    def test_dict_output_extracted(self, guardrail):
        guardrail._llm.call.return_value = "SAFE"
        guardrail.validate({"output": "task result"})
        call_args = guardrail._llm.call.call_args[0][0]
        user_msg = next(m for m in call_args if m["role"] == "user")
        assert "task result" in user_msg["content"]


class TestLLMInjectionGuardrailFactoryRegistration:
    def test_factory_creates_correct_type(self):
        with patch("crewai.LLM"):
            from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
            from src.engines.crewai.guardrails.llm_injection_guardrail import LLMInjectionGuardrail
            guardrail = GuardrailFactory.create_guardrail(
                {"type": "prompt_injection_check", "llm_model": "databricks-test-model"}
            )
        assert isinstance(guardrail, LLMInjectionGuardrail)

    def test_factory_returns_none_for_unknown_type(self):
        from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
        guardrail = GuardrailFactory.create_guardrail({"type": "nonexistent_type"})
        assert guardrail is None
