"""Tests for the Databricks JSON-schema sanitizer in crewai_instructor_patch.

The Databricks AI Gateway rejects JSON schemas that put numeric range keywords
(minimum/maximum/…) on number/integer types — which is exactly what instructor
emits for Pydantic ``Field(ge=, le=)`` constraints (e.g. CrewAI memory
save-analysis' ``importance: float = Field(ge=0, le=1)``). The sanitizer strips
those keywords from structured-output requests, but ONLY for Databricks models.
"""

import litellm
import instructor

from src.core.llm_handlers.crewai_instructor_patch import (
    strip_numeric_range_keywords,
    sanitize_databricks_request_kwargs,
    _instructor_mode_for_llm,
)


class _FakeLLM:
    def __init__(self, model, is_litellm=True):
        self.model = model
        self.is_litellm = is_litellm


def _memory_analysis_tools():
    """Shape instructor produces for MemoryAnalysis.importance (ge=0, le=1)."""
    return [
        {
            "type": "function",
            "function": {
                "name": "MemoryAnalysis",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "importance": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0,
                        },
                        "scope": {"type": "string"},
                    },
                },
            },
        }
    ]


class TestStripNumericRangeKeywords:
    def test_strips_keywords_recursively(self):
        schema = {
            "type": "object",
            "properties": {
                "a": {"type": "number", "minimum": 0, "maximum": 5, "multipleOf": 2},
                "b": {"type": "integer", "exclusiveMinimum": 1, "exclusiveMaximum": 9},
                "nested": {
                    "type": "array",
                    "items": {"type": "number", "minimum": -1},
                },
            },
        }
        strip_numeric_range_keywords(schema)
        props = schema["properties"]
        assert props["a"] == {"type": "number"}
        assert props["b"] == {"type": "integer"}
        assert props["nested"]["items"] == {"type": "number"}

    def test_leaves_unrelated_keys_intact(self):
        schema = {"type": "string", "description": "keep me", "enum": ["x", "y"]}
        strip_numeric_range_keywords(schema)
        assert schema == {"type": "string", "description": "keep me", "enum": ["x", "y"]}


class TestSanitizeDatabricksRequestKwargs:
    def test_strips_for_databricks_model_in_tools(self):
        kwargs = {"model": "databricks/databricks-claude-haiku-4-5", "tools": _memory_analysis_tools()}
        sanitize_databricks_request_kwargs(kwargs)
        imp = kwargs["tools"][0]["function"]["parameters"]["properties"]["importance"]
        assert imp == {"type": "number"}

    def test_strips_for_databricks_model_in_response_format(self):
        kwargs = {
            "model": "databricks/databricks-llama-4-maverick",
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "MemoryAnalysis",
                    "schema": {
                        "type": "object",
                        "properties": {"importance": {"type": "number", "minimum": 0.0, "maximum": 1.0}},
                    },
                },
            },
        }
        sanitize_databricks_request_kwargs(kwargs)
        imp = kwargs["response_format"]["json_schema"]["schema"]["properties"]["importance"]
        assert imp == {"type": "number"}

    def test_noop_for_non_databricks_model(self):
        kwargs = {"model": "gpt-4o", "tools": _memory_analysis_tools()}
        sanitize_databricks_request_kwargs(kwargs)
        imp = kwargs["tools"][0]["function"]["parameters"]["properties"]["importance"]
        assert imp == {"type": "number", "minimum": 0.0, "maximum": 1.0}

    def test_noop_when_model_missing_or_not_str(self):
        kwargs = {"tools": _memory_analysis_tools()}
        sanitize_databricks_request_kwargs(kwargs)  # no model → no-op, no crash
        imp = kwargs["tools"][0]["function"]["parameters"]["properties"]["importance"]
        assert imp.get("minimum") == 0.0

    def test_noop_when_no_tools_or_response_format(self):
        kwargs = {"model": "databricks/databricks-claude-haiku-4-5", "messages": [{"role": "user", "content": "hi"}]}
        sanitize_databricks_request_kwargs(kwargs)  # nothing to strip, no crash
        assert "tools" not in kwargs


def test_litellm_completion_is_patched():
    """Importing the patch wraps litellm.completion/acompletion once."""
    assert getattr(litellm, "_kasal_schema_sanitizer_applied", False) is True


class TestInstructorModeForLlm:
    """Databricks litellm models coerce structured output via MD_JSON to avoid
    instructor's "multiple tool calls" crash; everything else keeps the default."""

    def test_databricks_litellm_uses_md_json(self):
        llm = _FakeLLM("databricks/databricks-claude-opus-4-8")
        assert _instructor_mode_for_llm(llm) is instructor.Mode.MD_JSON

    def test_databricks_llama_uses_md_json(self):
        llm = _FakeLLM("databricks/databricks-llama-4-maverick")
        assert _instructor_mode_for_llm(llm) is instructor.Mode.MD_JSON

    def test_non_databricks_litellm_keeps_default(self):
        # Other providers keep instructor's default mode (unchanged behavior).
        assert _instructor_mode_for_llm(_FakeLLM("gpt-4o")) is None

    def test_non_litellm_keeps_default(self):
        # e.g. the codex Responses handler (is_litellm=False) never uses instructor.
        assert _instructor_mode_for_llm(_FakeLLM("databricks-gpt-5-3-codex", is_litellm=False)) is None

    def test_string_llm_keeps_default(self):
        assert _instructor_mode_for_llm("databricks/whatever") is None

    def test_none_llm_keeps_default(self):
        assert _instructor_mode_for_llm(None) is None
