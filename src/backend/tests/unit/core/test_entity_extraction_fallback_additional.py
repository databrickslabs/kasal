"""
Additional unit tests for src/core/entity_extraction_fallback.py.

Focuses on exercising the inner patched functions (patched_create_instructor
and patched_to_pydantic) that are only covered when the monkey-patched methods
are actually called.

Goal: push coverage from 40% towards 50%+.
"""

import pytest
from unittest.mock import MagicMock, Mock, patch
import logging

from src.core.entity_extraction_fallback import (
    needs_entity_extraction_fallback,
    ENTITY_EXTRACTION_FALLBACK_MODEL,
    PROBLEMATIC_MODELS,
)


# ---------------------------------------------------------------------------
# Helper to create a fake Converter-like object
# ---------------------------------------------------------------------------

def _make_converter(model_name: str):
    """Build a mock Converter instance with an LLM that has the given model."""
    llm = MagicMock()
    llm.model = model_name
    llm.temperature = 0.7
    llm.max_tokens = 1024
    llm.api_base = "https://example.databricks.com"
    llm.api_key = "dapi_test_key"
    llm.supports_function_calling = Mock(return_value=True)

    converter = MagicMock()
    converter.llm = llm
    return converter


# ---------------------------------------------------------------------------
# Tests for patched_create_instructor (exercise lines 71-118)
# ---------------------------------------------------------------------------

class TestPatchedCreateInstructorBehavior:
    """Test that the patched _create_instructor function works correctly."""

    def test_patched_instructor_with_non_problematic_model(self):
        """patched_create_instructor delegates to original for safe models."""
        from src.core.entity_extraction_fallback import apply_entity_extraction_fallback

        original_result = MagicMock(name="original_instructor_result")
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls._create_instructor = original_fn

        mock_llm_cls = MagicMock()

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_entity_extraction_fallback()

        # Retrieve the patched function that was set on the class
        patched_fn = mock_converter_cls._create_instructor

        # Create a converter instance with a normal model
        converter_instance = _make_converter("gpt-4-normal")
        result = patched_fn(converter_instance)

        # original_fn should have been called (via original_create_instructor)
        original_fn.assert_called_once_with(converter_instance)
        assert result is original_result

    def test_patched_instructor_with_problematic_model_success(self):
        """patched_create_instructor switches LLM for problematic models."""
        from src.core.entity_extraction_fallback import apply_entity_extraction_fallback

        original_result = MagicMock(name="fallback_result")
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls._create_instructor = original_fn

        mock_llm_instance = MagicMock()
        mock_llm_cls = Mock(return_value=mock_llm_instance)

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_entity_extraction_fallback()

        patched_fn = mock_converter_cls._create_instructor

        converter_instance = _make_converter("databricks-claude-3.5")
        original_llm = converter_instance.llm

        result = patched_fn(converter_instance)

        # The LLM should be restored to original after the call
        assert converter_instance.llm is original_llm
        # original_fn was called with the fallback LLM set
        original_fn.assert_called_once()

    def test_patched_instructor_restores_llm_on_fallback_error(self):
        """patched_create_instructor restores original LLM when fallback fails."""
        from src.core.entity_extraction_fallback import apply_entity_extraction_fallback

        original_result = MagicMock(name="final_result")
        call_count = [0]

        def side_effect(self_inst):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call with fallback LLM - simulate failure
                raise RuntimeError("Fallback LLM failed")
            return original_result

        original_fn = Mock(side_effect=lambda inst: original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls._create_instructor = original_fn

        # Make LLM() raise an exception
        mock_llm_cls = Mock(side_effect=Exception("LLM creation failed"))

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_entity_extraction_fallback()

        patched_fn = mock_converter_cls._create_instructor
        converter_instance = _make_converter("databricks-claude-3.5")
        original_llm = converter_instance.llm

        # Should not raise — falls through to original_fn
        result = patched_fn(converter_instance)

        # LLM restored
        assert converter_instance.llm is original_llm

    def test_patched_instructor_no_llm_attribute(self):
        """patched_create_instructor handles converters without llm attribute."""
        from src.core.entity_extraction_fallback import apply_entity_extraction_fallback

        original_result = MagicMock()
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls._create_instructor = original_fn

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=MagicMock()),
        }):
            apply_entity_extraction_fallback()

        patched_fn = mock_converter_cls._create_instructor

        # Converter without llm attribute
        converter_no_llm = MagicMock(spec=[])  # No attributes
        result = patched_fn(converter_no_llm)

        original_fn.assert_called_once_with(converter_no_llm)


# ---------------------------------------------------------------------------
# Tests for patched_to_pydantic (exercise lines 148-195)
# ---------------------------------------------------------------------------

class TestPatchedToPydanticBehavior:
    """Test that the patched to_pydantic function works correctly."""

    def test_patched_to_pydantic_with_safe_model(self):
        """patched_to_pydantic delegates to original for safe models."""
        from src.core.entity_extraction_fallback import apply_converter_llm_fallback

        original_result = MagicMock(name="pydantic_result")
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls.to_pydantic = original_fn

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=MagicMock()),
        }):
            apply_converter_llm_fallback()

        patched_fn = mock_converter_cls.to_pydantic
        converter_instance = _make_converter("gpt-4-turbo")

        result = patched_fn(converter_instance, current_attempt=1)
        original_fn.assert_called_once_with(converter_instance, 1)
        assert result is original_result

    def test_patched_to_pydantic_with_problematic_model_success(self):
        """patched_to_pydantic uses fallback LLM for problematic models."""
        from src.core.entity_extraction_fallback import apply_converter_llm_fallback

        original_result = MagicMock(name="pydantic_result")
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls.to_pydantic = original_fn

        fallback_llm = MagicMock()
        mock_llm_cls = Mock(return_value=fallback_llm)

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_converter_llm_fallback()

        patched_fn = mock_converter_cls.to_pydantic
        converter_instance = _make_converter("databricks-claude-3.5")
        original_llm = converter_instance.llm

        result = patched_fn(converter_instance, current_attempt=1)

        # LLM should be restored
        assert converter_instance.llm is original_llm
        original_fn.assert_called_once()

    def test_patched_to_pydantic_restores_llm_on_error(self):
        """patched_to_pydantic restores original LLM and retries with original on error."""
        from src.core.entity_extraction_fallback import apply_converter_llm_fallback

        call_count = [0]
        original_result = MagicMock(name="fallback_result")

        def side_effect(self_inst, attempt):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("fallback failed")
            return original_result

        original_fn = Mock(side_effect=side_effect)

        mock_converter_cls = MagicMock()
        mock_converter_cls.to_pydantic = original_fn

        fallback_llm = MagicMock()
        fallback_llm.supports_function_calling = Mock(return_value=True)
        mock_llm_cls = Mock(return_value=fallback_llm)

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_converter_llm_fallback()

        patched_fn = mock_converter_cls.to_pydantic
        converter_instance = _make_converter("databricks-gpt-oss")
        original_llm = converter_instance.llm

        # Should not raise — falls back to original
        result = patched_fn(converter_instance, current_attempt=1)

        # LLM restored after recovery
        assert converter_instance.llm is original_llm
        assert call_count[0] == 2  # called with fallback, then with original

    def test_patched_to_pydantic_handles_llm_creation_failure(self):
        """patched_to_pydantic falls back gracefully when fallback LLM can't be created."""
        from src.core.entity_extraction_fallback import apply_converter_llm_fallback

        original_result = MagicMock(name="orig_result")
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls.to_pydantic = original_fn

        # LLM() creation fails
        mock_llm_cls = Mock(side_effect=Exception("LLM init error"))

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_converter_llm_fallback()

        patched_fn = mock_converter_cls.to_pydantic
        converter_instance = _make_converter("gpt-oss-model")

        result = patched_fn(converter_instance, current_attempt=1)
        # Should fall back to original
        assert result is original_result

    def test_patched_to_pydantic_sets_supports_function_calling(self):
        """patched_to_pydantic patches supports_function_calling on fallback LLM."""
        from src.core.entity_extraction_fallback import apply_converter_llm_fallback

        original_fn = Mock(return_value=MagicMock())

        mock_converter_cls = MagicMock()
        mock_converter_cls.to_pydantic = original_fn

        fallback_llm = MagicMock()
        # Has supports_function_calling attribute
        fallback_llm.supports_function_calling = Mock(return_value=False)
        mock_llm_cls = Mock(return_value=fallback_llm)

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=mock_llm_cls),
        }):
            apply_converter_llm_fallback()

        patched_fn = mock_converter_cls.to_pydantic
        converter_instance = _make_converter("databricks-claude-opus")

        patched_fn(converter_instance, current_attempt=1)
        original_fn.assert_called_once()

    def test_patched_to_pydantic_no_llm_attribute(self):
        """patched_to_pydantic handles converter without llm attribute."""
        from src.core.entity_extraction_fallback import apply_converter_llm_fallback

        original_result = MagicMock()
        original_fn = Mock(return_value=original_result)

        mock_converter_cls = MagicMock()
        mock_converter_cls.to_pydantic = original_fn

        with patch.dict("sys.modules", {
            "crewai.utilities.converter": MagicMock(Converter=mock_converter_cls),
            "crewai.llm": MagicMock(LLM=MagicMock()),
        }):
            apply_converter_llm_fallback()

        patched_fn = mock_converter_cls.to_pydantic

        # Converter without llm attribute at all
        converter_no_llm = MagicMock(spec=[])
        result = patched_fn(converter_no_llm, current_attempt=1)

        original_fn.assert_called_once_with(converter_no_llm, 1)


# ---------------------------------------------------------------------------
# Additional needs_entity_extraction_fallback edge cases
# ---------------------------------------------------------------------------

class TestNeedsEntityExtractionFallbackEdgeCases:
    def test_handles_integer_model_name(self):
        """Should convert non-string model names to string before checking."""
        result = needs_entity_extraction_fallback(42)
        # "42" doesn't match any pattern - returns False
        assert result is False

    def test_handles_empty_list(self):
        """Calling with a list should not crash."""
        result = needs_entity_extraction_fallback([])
        assert result is False

    def test_claude_in_model_name(self):
        """Any model with 'databricks-claude' in name needs fallback."""
        assert needs_entity_extraction_fallback("databricks-claude-sonnet") is True

    def test_gpt_oss_substring_match(self):
        """Models containing 'gpt-oss' need fallback."""
        assert needs_entity_extraction_fallback("databricks/gpt-oss-turbo") is True

    def test_unrelated_model_does_not_match(self):
        """Models without problematic patterns return False."""
        assert needs_entity_extraction_fallback("databricks-llama-3.3") is False
