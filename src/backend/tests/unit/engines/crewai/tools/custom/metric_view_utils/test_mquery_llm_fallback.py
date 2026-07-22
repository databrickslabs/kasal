"""Tests for the M-Query → SQL LLM fallback module."""
import json
import asyncio
from collections import OrderedDict
from unittest.mock import AsyncMock, patch

import pytest

from src.engines.crewai.tools.custom.metric_view_utils import mquery_llm_fallback as mllm
from src.engines.crewai.tools.custom.metric_view_utils.mquery_llm_fallback import (
    _content_hash,
    _parse_response,
    _validate_source_sql,
    translate_mquery_to_sql,
    recover_sources_with_llm,
)
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import (
    looks_like_raw_mquery,
)

RAW_M = (
    'let\n  Source = Sql.Database("srv","db"),\n'
    '  t = Source{[Schema="sales",Item="fact_sales"]}[Data]\nin t'
)
EMBEDDED_SQL = "SELECT region, SUM(amount) AS amount FROM sales.fact GROUP BY region"


class TestLooksLikeRawMQuery:
    def test_detects_let_in_block(self):
        assert looks_like_raw_mquery(RAW_M) is True

    def test_detects_connector_functions(self):
        assert looks_like_raw_mquery('Source = Sql.Database("s","d")') is True

    def test_plain_sql_is_not_m(self):
        assert looks_like_raw_mquery(EMBEDDED_SQL) is False
        assert looks_like_raw_mquery("SELECT * FROM a.b") is False

    def test_empty_and_none(self):
        assert looks_like_raw_mquery("") is False
        assert looks_like_raw_mquery(None) is False


class TestValidateSourceSql:
    def test_valid_select(self):
        assert _validate_source_sql("SELECT * FROM dbo.fact") is True

    def test_rejects_m_leftovers(self):
        assert _validate_source_sql("let x = 1 in x") is False
        assert _validate_source_sql('Sql.Database("s","d")') is False

    def test_rejects_no_from(self):
        assert _validate_source_sql("SELECT 1") is False

    def test_rejects_empty(self):
        assert _validate_source_sql("") is False


class TestParseResponse:
    def test_plain_json(self):
        p = _parse_response('{"success":true,"source_sql":"SELECT * FROM a.b"}')
        assert p["success"] and p["source_sql"] == "SELECT * FROM a.b"

    def test_markdown_fenced(self):
        p = _parse_response('```json\n{"success":true,"source_sql":"SELECT * FROM a.b"}\n```')
        assert p["success"]

    def test_garbage_is_failure(self):
        p = _parse_response("not json")
        assert p["success"] is False


class TestTranslateMQueryToSql:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_success_path(self):
        fake = json.dumps({
            "success": True, "source_sql": "SELECT * FROM sales.fact_sales",
            "source_table": "sales.fact_sales", "confidence": "high",
        })
        with patch.object(mllm, "_call_llm", new=AsyncMock(return_value={"content": fake})):
            res = self._run(translate_mquery_to_sql("FT_Sales", RAW_M))
        assert res["success"] and res["source_sql"] == "SELECT * FROM sales.fact_sales"

    def test_llm_output_with_m_leftover_rejected(self):
        fake = json.dumps({"success": True, "source_sql": "let x in x"})
        with patch.object(mllm, "_call_llm", new=AsyncMock(return_value={"content": fake})):
            res = self._run(translate_mquery_to_sql("T", RAW_M))
        assert res["success"] is False

    def test_llm_failure_is_fail_open(self):
        with patch.object(mllm, "_call_llm", new=AsyncMock(return_value={"content": None, "error": "boom"})):
            res = self._run(translate_mquery_to_sql("T", RAW_M))
        assert res["success"] is False

    def test_cache_hit_skips_second_call(self):
        fake = json.dumps({"success": True, "source_sql": "SELECT * FROM a.b", "source_table": "a.b"})
        cache = OrderedDict()
        mock = AsyncMock(return_value={"content": fake})
        with patch.object(mllm, "_call_llm", new=mock):
            self._run(translate_mquery_to_sql("T", RAW_M, cache=cache))
            self._run(translate_mquery_to_sql("T", RAW_M, cache=cache))
        assert mock.call_count == 1


class TestRecoverSourcesWithLlm:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_only_raw_m_entries_are_translated(self):
        entries = [
            {"table_name": "FT_Sales", "transpiled_sql": RAW_M, "validation_passed": "Yes"},
            {"table_name": "FT_SQL", "transpiled_sql": EMBEDDED_SQL, "validation_passed": "Yes"},
        ]
        fake = json.dumps({"success": True, "source_sql": "SELECT * FROM sales.fact_sales", "source_table": "sales.fact_sales"})
        with patch.object(mllm, "_call_llm", new=AsyncMock(return_value={"content": fake})):
            out, n = self._run(recover_sources_with_llm(entries))
        assert n == 1
        # raw-M entry rewritten, SQL entry untouched
        assert out[0]["transpiled_sql"] == "SELECT * FROM sales.fact_sales"
        assert out[1]["transpiled_sql"] == EMBEDDED_SQL

    def test_untranslatable_entry_left_unchanged(self):
        entries = [{"table_name": "FT", "transpiled_sql": RAW_M, "validation_passed": "Yes"}]
        with patch.object(mllm, "_call_llm", new=AsyncMock(return_value={"content": None, "error": "x"})):
            out, n = self._run(recover_sources_with_llm(entries))
        assert n == 0
        assert out[0]["transpiled_sql"] == RAW_M

    def test_empty_input(self):
        out, n = self._run(recover_sources_with_llm([]))
        assert out == [] and n == 0
