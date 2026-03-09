"""
Unit tests for LLM guardrail result caching.

Verifies that identical outputs hit the cache and skip LLM calls,
and that the LRU eviction policy works correctly.
"""
import pytest
from unittest.mock import MagicMock, patch


def _make_injection_guardrail(config=None, cache_size=128):
    """Create an LLMInjectionGuardrail with mocked LLM."""
    cfg = config or {"llm_model": "databricks-test-model", "cache_size": cache_size}
    mock_llm_instance = MagicMock()
    mock_llm_class = MagicMock(return_value=mock_llm_instance)
    with patch("crewai.LLM", mock_llm_class):
        from src.engines.crewai.guardrails.llm_injection_guardrail import LLMInjectionGuardrail
        guardrail = LLMInjectionGuardrail(cfg)
    guardrail._llm = mock_llm_instance
    return guardrail


def _make_self_reflection_guardrail(config=None, cache_size=128):
    """Create a SelfReflectionGuardrail with mocked LLM."""
    cfg = config or {
        "llm_model": "databricks-test-model",
        "task_description": "Summarise revenue.",
        "cache_size": cache_size,
    }
    mock_llm_instance = MagicMock()
    mock_llm_class = MagicMock(return_value=mock_llm_instance)
    with patch("crewai.LLM", mock_llm_class):
        from src.engines.crewai.guardrails.self_reflection_guardrail import SelfReflectionGuardrail
        guardrail = SelfReflectionGuardrail(cfg)
    guardrail._llm = mock_llm_instance
    return guardrail


class TestLLMInjectionGuardrailCaching:
    def test_second_call_with_same_text_uses_cache(self):
        guardrail = _make_injection_guardrail()
        guardrail._llm.call.return_value = "SAFE"

        result1 = guardrail.validate("Same output text.")
        result2 = guardrail.validate("Same output text.")

        assert result1["valid"] is True
        assert result2["valid"] is True
        # LLM should only be called once
        assert guardrail._llm.call.call_count == 1

    def test_different_text_calls_llm_twice(self):
        guardrail = _make_injection_guardrail()
        guardrail._llm.call.return_value = "SAFE"

        guardrail.validate("Output A.")
        guardrail.validate("Output B.")

        assert guardrail._llm.call.call_count == 2

    def test_injection_verdict_cached(self):
        guardrail = _make_injection_guardrail()
        guardrail._llm.call.return_value = "INJECTION"

        result1 = guardrail.validate("Suspicious output.")
        result2 = guardrail.validate("Suspicious output.")

        assert result1["valid"] is False
        assert result2["valid"] is False
        assert guardrail._llm.call.call_count == 1

    def test_cache_lru_eviction(self):
        guardrail = _make_injection_guardrail(cache_size=2)
        guardrail._llm.call.return_value = "SAFE"

        guardrail.validate("Text 1")
        guardrail.validate("Text 2")
        guardrail.validate("Text 3")  # evicts "Text 1"

        # Re-validate "Text 1" — should call LLM again (evicted)
        guardrail.validate("Text 1")
        assert guardrail._llm.call.call_count == 4

        # "Text 2" may or may not be evicted depending on access order
        # "Text 3" should still be cached
        guardrail.validate("Text 3")
        # "Text 3" was just added, not evicted yet
        # Total calls should still be 4 if Text 3 is cached
        assert guardrail._llm.call.call_count == 4

    def test_llm_failure_not_cached(self):
        guardrail = _make_injection_guardrail()
        guardrail._llm.call.side_effect = RuntimeError("API error")

        result1 = guardrail.validate("Failing output.")
        assert result1["valid"] is True  # fail-open

        # Second call should try LLM again (error was not cached)
        guardrail._llm.call.side_effect = None
        guardrail._llm.call.return_value = "SAFE"
        result2 = guardrail.validate("Failing output.")
        assert result2["valid"] is True
        assert guardrail._llm.call.call_count == 2

    def test_empty_string_not_cached(self):
        guardrail = _make_injection_guardrail()
        guardrail.validate("")
        guardrail.validate("")
        guardrail._llm.call.assert_not_called()
        assert len(guardrail._cache) == 0


class TestSelfReflectionGuardrailCaching:
    def test_second_call_with_same_text_uses_cache(self):
        guardrail = _make_self_reflection_guardrail()
        guardrail._llm.call.return_value = "PASS"

        result1 = guardrail.validate("Revenue was $10M.")
        result2 = guardrail.validate("Revenue was $10M.")

        assert result1["valid"] is True
        assert result2["valid"] is True
        assert guardrail._llm.call.call_count == 1

    def test_different_text_calls_llm_twice(self):
        guardrail = _make_self_reflection_guardrail()
        guardrail._llm.call.return_value = "PASS"

        guardrail.validate("Output A.")
        guardrail.validate("Output B.")

        assert guardrail._llm.call.call_count == 2

    def test_fail_verdict_cached(self):
        guardrail = _make_self_reflection_guardrail()
        guardrail._llm.call.return_value = "FAIL"

        result1 = guardrail.validate("Bad output.")
        result2 = guardrail.validate("Bad output.")

        assert result1["valid"] is False
        assert result2["valid"] is False
        assert guardrail._llm.call.call_count == 1

    def test_cache_includes_task_description(self):
        """Different task descriptions for same output should produce different cache keys."""
        g1 = _make_self_reflection_guardrail({
            "llm_model": "databricks-test-model",
            "task_description": "Task A",
        })
        g1._llm.call.return_value = "PASS"

        g2 = _make_self_reflection_guardrail({
            "llm_model": "databricks-test-model",
            "task_description": "Task B",
        })
        g2._llm.call.return_value = "PASS"

        g1.validate("Same output.")
        g2.validate("Same output.")

        # Both should have called LLM (different task descriptions = different cache keys)
        assert g1._llm.call.call_count == 1
        assert g2._llm.call.call_count == 1

    def test_llm_failure_not_cached(self):
        guardrail = _make_self_reflection_guardrail()
        guardrail._llm.call.side_effect = RuntimeError("timeout")

        result1 = guardrail.validate("Some output.")
        assert result1["valid"] is True

        guardrail._llm.call.side_effect = None
        guardrail._llm.call.return_value = "PASS"
        result2 = guardrail.validate("Some output.")
        assert result2["valid"] is True
        assert guardrail._llm.call.call_count == 2
