"""Tests for DAX LLM fallback module."""
import json
import pytest
from collections import OrderedDict
from unittest.mock import AsyncMock, patch, MagicMock
from src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback import (
    _content_hash,
    _build_user_prompt,
    _parse_response,
    _validate_sql,
    translate_with_llm,
    translate_batch_with_llm,
)
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TranslationResult


class TestHelpers:
    def test_content_hash_deterministic(self):
        h1 = _content_hash("SUM(Sales[Amount])")
        h2 = _content_hash("SUM(Sales[Amount])")
        assert h1 == h2

    def test_content_hash_different(self):
        h1 = _content_hash("SUM(Sales[Amount])")
        h2 = _content_hash("SUM(Sales[Cost])")
        assert h1 != h2

    def test_build_user_prompt(self):
        prompt = _build_user_prompt(
            "Total Sales", "SUM(Sales[Amount])",
            {"base_a", "base_b"}, {"Total Sales": "total_sales"}
        )
        assert "Total Sales" in prompt
        assert "SUM(Sales[Amount])" in prompt
        assert "MEASURE()" in prompt

    def test_parse_response_valid_json(self):
        resp = '{"success": true, "sql_expr": "SUM(source.amount)", "confidence": "high"}'
        parsed = _parse_response(resp)
        assert parsed["success"] is True
        assert parsed["sql_expr"] == "SUM(source.amount)"

    def test_parse_response_markdown_wrapped(self):
        resp = '```json\n{"success": true, "sql_expr": "SUM(source.x)"}\n```'
        parsed = _parse_response(resp)
        assert parsed["success"] is True

    def test_parse_response_invalid(self):
        parsed = _parse_response("not json at all")
        assert parsed["success"] is False

    def test_validate_sql_clean(self):
        assert _validate_sql("SUM(source.amount) / NULLIF(SUM(source.count), 0)") is True

    def test_validate_sql_has_dax(self):
        assert _validate_sql("SELECTEDVALUE(Table[Col])") is False
        assert _validate_sql("ALLSELECTED(Table)") is False

    def test_validate_sql_measure_ref_ok(self):
        assert _validate_sql("MEASURE(total_sales) - MEASURE(total_cost)") is True


class TestTranslateWithLLM:
    @pytest.mark.asyncio
    async def test_llm_failure_returns_unchanged(self):
        m = TranslationResult(
            measure_name="test", original_name="Test",
            sql_expr=None, is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression="ALLSELECTED(T[col])",
            confidence="none", category="unassigned",
        )
        with patch("src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback._call_llm",
                   new_callable=AsyncMock, return_value={"content": None, "error": "unavailable"}):
            result = await translate_with_llm(m, "fact_test", set(), {})
        assert result.is_translatable is False

    @pytest.mark.asyncio
    async def test_successful_translation(self):
        m = TranslationResult(
            measure_name="test", original_name="Test",
            sql_expr=None, is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression="ALLSELECTED(T[col])",
            confidence="none", category="unassigned",
        )
        mock_response = {
            "content": json.dumps({
                "success": True,
                "sql_expr": "SUM(source.col)",
                "confidence": "medium",
                "explanation": "Translated ALLSELECTED to SUM"
            }),
            "usage": {"total_tokens": 100},
        }
        with patch("src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback._call_llm",
                   new_callable=AsyncMock, return_value=mock_response):
            result = await translate_with_llm(m, "fact_test", {"base_a"}, {})
        assert result.is_translatable is True
        assert result.sql_expr == "SUM(source.col)"
        assert result.category == "llm_translated"

    @pytest.mark.asyncio
    async def test_llm_returns_dax_constructs_rejected(self):
        m = TranslationResult(
            measure_name="test", original_name="Test",
            sql_expr=None, is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression="complex DAX",
            confidence="none", category="unassigned",
        )
        mock_response = {
            "content": json.dumps({
                "success": True,
                "sql_expr": "SELECTEDVALUE(source.col)",
                "confidence": "low",
            }),
            "usage": {},
        }
        with patch("src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback._call_llm",
                   new_callable=AsyncMock, return_value=mock_response):
            result = await translate_with_llm(m, "fact_test", set(), {})
        assert result.is_translatable is False  # Rejected by validation

    @pytest.mark.asyncio
    async def test_cache_hit(self):
        m = TranslationResult(
            measure_name="test", original_name="Test",
            sql_expr=None, is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression="SUM(T[x])",
            confidence="none", category="unassigned",
        )
        # Pre-populate a run-scoped cache
        run_cache: OrderedDict[str, dict] = OrderedDict()
        cache_key = _content_hash("SUM(T[x])")
        run_cache[cache_key] = {"success": True, "sql_expr": "SUM(source.x)", "confidence": "high"}

        result = await translate_with_llm(
            m, "fact_test", set(), {},
            cache=run_cache,
        )
        assert result.is_translatable is True
        assert result.sql_expr == "SUM(source.x)"


class TestTranslateBatchWithLLM:
    @pytest.mark.asyncio
    async def test_skips_artifacts(self):
        measures = [
            TranslationResult(
                measure_name="fmt", original_name="Fmt",
                sql_expr=None, is_translatable=False,
                skip_reason="FORMAT function (display-only)",
                dax_expression="FORMAT(x, '#')",
                confidence="none", category="unassigned",
            ),
        ]
        result = await translate_batch_with_llm(measures, "fact_test", set(), {})
        assert result[0].is_translatable is False

    @pytest.mark.asyncio
    async def test_llm_error_leaves_measures_unchanged(self):
        measures = [
            TranslationResult(
                measure_name="test", original_name="Test",
                sql_expr=None, is_translatable=False,
                skip_reason="No matching pattern",
                dax_expression="complex DAX",
                confidence="none", category="unassigned",
            ),
        ]
        with patch("src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback._call_llm",
                   new_callable=AsyncMock, return_value={"content": None, "error": "unavailable"}):
            result = await translate_batch_with_llm(measures, "fact_test", set(), {})
        assert result[0].is_translatable is False
