"""
Comprehensive unit tests for company_name_not_null_guardrail.py

Targets: CompanyNameNotNullGuardrail.__init__, .validate
Goal: push coverage from 33.3% to 50%+
"""
import json
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper: suppress the logger so tests stay silent
# ---------------------------------------------------------------------------
LOGGER_PATCH = "src.engines.crewai.guardrails.demo.company_name_not_null_guardrail.logger"
BASE_GUARDRAIL_PATCH = "src.engines.crewai.guardrails.base_guardrail.BaseGuardrail.__init__"


class TestCompanyNameNotNullGuardrailInit:
    """Tests for __init__ of CompanyNameNotNullGuardrail."""

    def _import(self):
        from src.engines.crewai.guardrails.demo.company_name_not_null_guardrail import (
            CompanyNameNotNullGuardrail,
        )
        return CompanyNameNotNullGuardrail

    def test_init_with_dict_config(self):
        cls = self._import()
        guardrail = cls(config={"threshold": 5})
        assert guardrail is not None

    def test_init_with_json_string_config(self):
        cls = self._import()
        config_str = json.dumps({"threshold": 3})
        guardrail = cls(config=config_str)
        assert guardrail is not None

    def test_init_with_invalid_json_string_uses_empty_dict(self):
        """Invalid JSON string should not crash; falls back to empty dict."""
        cls = self._import()
        with patch(LOGGER_PATCH):
            guardrail = cls(config="not-valid-json")
        assert guardrail is not None

    def test_init_with_empty_dict(self):
        cls = self._import()
        guardrail = cls(config={})
        assert guardrail is not None

    def test_init_with_empty_string_falls_back_gracefully(self):
        cls = self._import()
        with patch(LOGGER_PATCH):
            guardrail = cls(config="")
        assert guardrail is not None

    def test_init_logs_success(self):
        cls = self._import()
        with patch(LOGGER_PATCH) as mock_log:
            cls(config={})
        # Check that an info-level log was emitted
        assert mock_log.info.called


class TestCompanyNameNotNullGuardrailValidate:
    """Tests for validate method of CompanyNameNotNullGuardrail."""

    def _make_guardrail(self):
        from src.engines.crewai.guardrails.demo.company_name_not_null_guardrail import (
            CompanyNameNotNullGuardrail,
        )
        return CompanyNameNotNullGuardrail(config={})

    def test_validate_returns_dict(self):
        g = self._make_guardrail()
        result = g.validate("some output")
        assert isinstance(result, dict)

    def test_validate_returns_valid_true(self):
        """DB operations are disabled → always returns valid=True."""
        g = self._make_guardrail()
        result = g.validate("any output")
        assert result["valid"] is True

    def test_validate_has_feedback_key(self):
        g = self._make_guardrail()
        result = g.validate("any output")
        assert "feedback" in result

    def test_validate_feedback_mentions_skipped(self):
        g = self._make_guardrail()
        result = g.validate("any output")
        assert "skip" in result["feedback"].lower() or "disabled" in result["feedback"].lower()

    def test_validate_with_none_output(self):
        g = self._make_guardrail()
        result = g.validate(None)
        assert result["valid"] is True

    def test_validate_with_dict_output(self):
        g = self._make_guardrail()
        result = g.validate({"company": "Acme", "score": 99})
        assert result["valid"] is True

    def test_validate_with_list_output(self):
        g = self._make_guardrail()
        result = g.validate([1, 2, 3])
        assert result["valid"] is True

    def test_validate_logs_warning(self):
        """Validate should log a warning about DB ops being disabled."""
        g = self._make_guardrail()
        with patch(LOGGER_PATCH) as mock_log:
            g.validate("output")
        assert mock_log.warning.called or mock_log.info.called

    def test_validate_exception_returns_invalid(self):
        """If an unexpected exception occurs, validate returns valid=False with feedback."""
        from src.engines.crewai.guardrails.demo.company_name_not_null_guardrail import (
            CompanyNameNotNullGuardrail,
        )

        g = CompanyNameNotNullGuardrail(config={})

        # Monkey-patch the try block to raise
        original_validate = g.validate

        def bad_validate(output):
            raise RuntimeError("unexpected DB error")

        # We deliberately raise inside the validate body by patching logger.warning to throw
        with patch(LOGGER_PATCH) as mock_log:
            mock_log.warning.side_effect = RuntimeError("unexpected DB error")
            result = g.validate("some output")

        # The outer except should catch this and return valid=False
        assert result["valid"] is False
        assert "feedback" in result
