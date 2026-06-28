"""
Unit tests for LLM guardrail result caching.

Verifies that identical outputs hit the cache and skip LLM calls,
and that the LRU eviction policy works correctly.
"""
import pytest
from unittest.mock import MagicMock, patch

_INJECTION_RC = "src.engines.crewai.guardrails.core.llm_injection_guardrail._run_completion"
_REFLECTION_RC = "src.engines.crewai.guardrails.core.self_reflection_guardrail._run_completion"


def _make_injection_guardrail(config=None, cache_size=128):
    cfg = config or {"llm_model": "databricks-test-model", "cache_size": cache_size}
    from src.engines.crewai.guardrails.core.llm_injection_guardrail import LLMInjectionGuardrail
    return LLMInjectionGuardrail(cfg)


def _make_self_reflection_guardrail(config=None, cache_size=128):
    cfg = config or {
        "llm_model": "databricks-test-model",
        "task_description": "Summarise revenue.",
        "cache_size": cache_size,
    }
    from src.engines.crewai.guardrails.core.self_reflection_guardrail import SelfReflectionGuardrail
    return SelfReflectionGuardrail(cfg)


class TestLLMInjectionGuardrailCaching:
    def test_second_call_with_same_text_uses_cache(self):
        guardrail = _make_injection_guardrail()
        with patch(_INJECTION_RC, return_value="SAFE") as mock_rc:
            result1 = guardrail.validate("Same output text.")
            result2 = guardrail.validate("Same output text.")
            assert result1["valid"] is True
            assert result2["valid"] is True
            assert mock_rc.call_count == 1

    def test_different_text_calls_llm_twice(self):
        guardrail = _make_injection_guardrail()
        with patch(_INJECTION_RC, return_value="SAFE") as mock_rc:
            guardrail.validate("Output A.")
            guardrail.validate("Output B.")
            assert mock_rc.call_count == 2

    def test_injection_verdict_cached(self):
        guardrail = _make_injection_guardrail()
        with patch(_INJECTION_RC, return_value="INJECTION") as mock_rc:
            result1 = guardrail.validate("Suspicious output.")
            result2 = guardrail.validate("Suspicious output.")
            assert result1["valid"] is False
            assert result2["valid"] is False
            assert mock_rc.call_count == 1

    def test_cache_lru_eviction(self):
        guardrail = _make_injection_guardrail(cache_size=2)
        with patch(_INJECTION_RC, return_value="SAFE") as mock_rc:
            guardrail.validate("Text 1")
            guardrail.validate("Text 2")
            guardrail.validate("Text 3")  # evicts "Text 1"
            guardrail.validate("Text 1")  # should call LLM again (evicted)
            assert mock_rc.call_count == 4

            guardrail.validate("Text 3")  # still cached
            assert mock_rc.call_count == 4

    def test_llm_failure_not_cached(self):
        guardrail = _make_injection_guardrail()
        mock_rc = MagicMock(side_effect=RuntimeError("API error"))
        with patch(_INJECTION_RC, mock_rc):
            result1 = guardrail.validate("Failing output.")
            assert result1["valid"] is True  # fail-open

        with patch(_INJECTION_RC, return_value="SAFE") as mock_rc2:
            result2 = guardrail.validate("Failing output.")
            assert result2["valid"] is True
            assert mock_rc2.call_count == 1

    def test_empty_string_not_cached(self):
        guardrail = _make_injection_guardrail()
        with patch(_INJECTION_RC) as mock_rc:
            guardrail.validate("")
            guardrail.validate("")
            mock_rc.assert_not_called()
        assert len(guardrail._cache) == 0


class TestSelfReflectionGuardrailCaching:
    def test_second_call_with_same_text_uses_cache(self):
        guardrail = _make_self_reflection_guardrail()
        with patch(_REFLECTION_RC, return_value="PASS") as mock_rc:
            result1 = guardrail.validate("Revenue was $10M.")
            result2 = guardrail.validate("Revenue was $10M.")
            assert result1["valid"] is True
            assert result2["valid"] is True
            assert mock_rc.call_count == 1

    def test_different_text_calls_llm_twice(self):
        guardrail = _make_self_reflection_guardrail()
        with patch(_REFLECTION_RC, return_value="PASS") as mock_rc:
            guardrail.validate("Output A.")
            guardrail.validate("Output B.")
            assert mock_rc.call_count == 2

    def test_fail_verdict_cached(self):
        guardrail = _make_self_reflection_guardrail()
        with patch(_REFLECTION_RC, return_value="FAIL") as mock_rc:
            result1 = guardrail.validate("Bad output.")
            result2 = guardrail.validate("Bad output.")
            assert result1["valid"] is False
            assert result2["valid"] is False
            assert mock_rc.call_count == 1

    def test_cache_includes_task_description(self):
        """Different task descriptions → different cache keys."""
        g1 = _make_self_reflection_guardrail({
            "llm_model": "databricks-test-model",
            "task_description": "Task A",
        })
        g2 = _make_self_reflection_guardrail({
            "llm_model": "databricks-test-model",
            "task_description": "Task B",
        })
        with patch(_REFLECTION_RC, return_value="PASS") as mock_rc:
            g1.validate("Same output.")
            g2.validate("Same output.")
            # Both should call LLM (different task descriptions = different cache keys)
            assert mock_rc.call_count == 2

    def test_llm_failure_not_cached(self):
        guardrail = _make_self_reflection_guardrail()
        mock_rc_err = MagicMock(side_effect=RuntimeError("timeout"))
        with patch(_REFLECTION_RC, mock_rc_err):
            result1 = guardrail.validate("Some output.")
            assert result1["valid"] is True

        with patch(_REFLECTION_RC, return_value="PASS") as mock_rc2:
            result2 = guardrail.validate("Some output.")
            assert result2["valid"] is True
            assert mock_rc2.call_count == 1
