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


class TestBatchConcurrency:
    """translate_batch_with_llm runs in bounded-concurrency chunks (not one-at-a-time).

    Regression: the old sequential loop could exceed the flow's crew timeout on
    models with hundreds of measures. Chunked concurrency cuts wall-time while
    preserving cross-measure MEASURE() reference resolution between chunks.
    """

    def _mk(self, i):
        return TranslationResult(
            original_name=f"M{i}", measure_name=f"m{i}",
            dax_expression=f"SUM(t[c{i}])", sql_expr="", is_translatable=False,
            skip_reason="", confidence="", category="",
        )

    def test_all_translated_and_runs_concurrently(self):
        import asyncio, time
        from src.engines.crewai.tools.custom.metric_view_utils import dax_llm_fallback as d
        measures = [self._mk(i) for i in range(14)]  # 3 chunks at concurrency=6

        async def fake_call(prompt, sys, model):
            await asyncio.sleep(0.05)
            return {'content': json.dumps({"success": True, "sql_expr": "SUM(source.c)", "confidence": "high"})}

        async def go():
            with patch.object(d, "_call_llm", new=fake_call):
                t = time.monotonic()
                out = await d.translate_batch_with_llm(measures, "tbl", set(), {}, model="m")
                return out, time.monotonic() - t

        out, dur = asyncio.run(go())
        assert sum(1 for m in out if m.is_translatable) == 14
        # Sequential would be ~14*0.05=0.70s; chunked(6) is ~3*0.05=0.15s.
        assert dur < 0.45, f"expected concurrent execution, got {dur:.2f}s"

    def test_artifacts_skipped(self):
        import asyncio
        from src.engines.crewai.tools.custom.metric_view_utils import dax_llm_fallback as d
        m = self._mk(1)
        m.skip_reason = "FORMAT string artifact"
        called = False

        async def fake_call(*a):
            nonlocal called; called = True
            return {'content': '{"success": true, "sql_expr": "x"}'}

        async def go():
            with patch.object(d, "_call_llm", new=fake_call):
                return await d.translate_batch_with_llm([m], "tbl", set(), {}, model="m")

        asyncio.run(go())
        assert called is False  # artifact never sent to the LLM


class TestLLMFirstCorpus:
    """LLM-first translator: skill corpus + dax_class + cached system prompt."""

    def test_corpus_loaded_and_in_prompt(self):
        from src.engines.crewai.tools.custom.metric_view_utils import dax_llm_fallback as d
        # The 10 skill files are vendored → corpus non-empty and in the system prompt.
        assert len(d._SKILL_CORPUS) > 1000
        assert 'PATTERNS' in d._SYSTEM_PROMPT
        assert 'dax_class' in d._SYSTEM_PROMPT  # 7-category contract present

    def test_system_message_is_cache_marked_when_corpus_present(self):
        from src.engines.crewai.tools.custom.metric_view_utils import dax_llm_fallback as d
        msg = d._system_message(d._SYSTEM_PROMPT)
        # With a corpus, the system content is a structured block with cache_control.
        assert isinstance(msg['content'], list)
        assert msg['content'][0]['cache_control'] == {'type': 'ephemeral'}

    def test_dax_class_captured_on_success(self):
        import asyncio, json
        from src.engines.crewai.tools.custom.metric_view_utils import dax_llm_fallback as d
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TranslationResult
        m = TranslationResult(original_name='M', measure_name='m', dax_expression='SUM(t[c])',
                              sql_expr='', is_translatable=False, skip_reason='', confidence='', category='')
        fake = json.dumps({"success": True, "sql_expr": "SUM(source.c)",
                           "dax_class": "translatable_direct", "confidence": "high"})

        async def go():
            with patch.object(d, "_call_llm", new=AsyncMock(return_value={"content": fake, "usage": {}})):
                return await d.translate_with_llm(m, "t", set(), {}, model="x")
        out = asyncio.run(go())
        assert out.is_translatable and out.dax_class == "translatable_direct"
        assert out.category == "llm_translated"  # emission routing unchanged

    def test_topo_priority_orders_batch(self):
        import asyncio, json
        from src.engines.crewai.tools.custom.metric_view_utils import dax_llm_fallback as d
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TranslationResult
        # child references parent; topo puts parent first so child resolves.
        parent = TranslationResult(original_name='Parent', measure_name='parent', dax_expression='SUM(t[a])',
                                   sql_expr='', is_translatable=False, skip_reason='', confidence='', category='')
        child = TranslationResult(original_name='Child', measure_name='child', dax_expression='[Parent]*2',
                                  sql_expr='', is_translatable=False, skip_reason='', confidence='', category='')
        seen_order = []

        async def fake_call(prompt, sysp, model):
            # record which measure ran (prompt carries the name)
            seen_order.append('Parent' if 'Parent' in prompt and 'Child' not in prompt else 'Child')
            return {"content": json.dumps({"success": True, "sql_expr": "SUM(source.a)", "dax_class": "translatable_direct"}), "usage": {}}

        async def go():
            with patch.object(d, "_call_llm", new=fake_call):
                # child listed first, but topo_priority puts parent (rank 0) before child (rank 1)
                await d.translate_batch_with_llm([child, parent], "t", set(), {}, model="x",
                                                 topo_priority={'Parent': 0, 'Child': 1})
        asyncio.run(go())
        # parent must be attempted before child
        assert seen_order.index('Parent') < seen_order.index('Child')
