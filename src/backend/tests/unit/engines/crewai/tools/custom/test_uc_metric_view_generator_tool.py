"""Tests for UCMetricViewGeneratorTool."""
import json
import pytest
from src.engines.crewai.tools.custom.uc_metric_view_generator_tool import UCMetricViewGeneratorTool


class TestUCMetricViewGeneratorTool:
    def test_initialization(self):
        tool = UCMetricViewGeneratorTool()
        assert tool.name == "UC Metric View Generator"

    def test_basic_generation(self):
        tool = UCMetricViewGeneratorTool()
        measures = [
            {
                'measure_name': 'total_sales',
                'original_name': 'Total Sales',
                'dax_expression': 'SUM(fact_sales[amount])',
                'proposed_allocation': 'fact_sales',
            }
        ]
        mquery = [
            {
                'table_name': 'fact_sales',
                'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM cat.sch.sales GROUP BY region',
                'validation_passed': 'Yes',
            }
        ]
        result = tool._run(
            measures_json=json.dumps(measures),
            mquery_json=json.dumps(mquery),
            catalog='test_cat',
            schema_name='test_sch',
        )
        data = json.loads(result)
        assert 'yaml' in data
        assert 'sql' in data
        assert 'stats' in data

    def test_empty_input(self):
        tool = UCMetricViewGeneratorTool()
        result = tool._run(measures_json='[]', mquery_json='[]')
        data = json.loads(result)
        assert data['stats'] == {} or isinstance(data['stats'], dict)

    def test_invalid_json(self):
        tool = UCMetricViewGeneratorTool()
        result = tool._run(measures_json='bad json')
        data = json.loads(result)
        assert 'error' in data


# ===========================================================================
# Durable raw DAX persistence (conversion_history / Lakebase)
# ===========================================================================

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch, MagicMock, AsyncMock


class TestBuildRawDaxExtract:
    """_build_raw_dax_extract collects full untruncated DAX per measure."""

    def test_collects_untruncated_dax(self):
        long_dax = "CALCULATE(SUM(fact[amount]), " + ("X" * 2000) + ")"
        measures = [{
            'measure_name': 'total_sales',
            'original_name': 'Total Sales',
            'dax_expression': long_dax,
            'proposed_allocation': 'fact_sales',
        }]
        extract = UCMetricViewGeneratorTool._build_raw_dax_extract(measures)
        assert len(extract) == 1
        assert extract[0]['dax_expression'] == long_dax
        assert len(extract[0]['dax_expression']) > 500
        assert extract[0]['measure_name'] == 'total_sales'
        assert extract[0]['proposed_allocation'] == 'fact_sales'

    def test_multiple_measures(self):
        measures = [
            {'measure_name': 'm1', 'dax_expression': 'SUM(a)'},
            {'measure_name': 'm2', 'dax_expression': 'AVG(b)'},
        ]
        extract = UCMetricViewGeneratorTool._build_raw_dax_extract(measures)
        assert len(extract) == 2
        assert {e['measure_name'] for e in extract} == {'m1', 'm2'}

    def test_non_list_returns_empty(self):
        assert UCMetricViewGeneratorTool._build_raw_dax_extract(None) == []
        assert UCMetricViewGeneratorTool._build_raw_dax_extract("not a list") == []

    def test_skips_non_dict_entries(self):
        extract = UCMetricViewGeneratorTool._build_raw_dax_extract(
            [{'measure_name': 'ok', 'dax_expression': 'SUM(a)'}, "garbage", 42]
        )
        assert len(extract) == 1
        assert extract[0]['measure_name'] == 'ok'

    def test_missing_dax_becomes_empty_string(self):
        extract = UCMetricViewGeneratorTool._build_raw_dax_extract([{'measure_name': 'm'}])
        assert extract[0]['dax_expression'] == ''


class TestSaveDaxToConversionHistory:
    """_save_dax_to_conversion_history persists to conversion_history, fail-open."""

    def _run(self, coro):
        return asyncio.run(coro)

    def test_persists_record_with_raw_dax(self):
        tool = UCMetricViewGeneratorTool()
        raw_dax = [{'measure_name': 'total_sales', 'original_name': 'Total Sales',
                    'dax_expression': 'SUM(fact[amount])', 'proposed_allocation': 'fact'}]

        captured = {}
        mock_repo = MagicMock()
        mock_record = SimpleNamespace(id=7, group_id=None)

        async def _create(data):
            captured["data"] = data
            return mock_record
        mock_repo.create = AsyncMock(side_effect=_create)
        mock_repo.session = MagicMock()
        mock_repo.session.commit = AsyncMock()

        @asynccontextmanager
        async def _repo_ctx():
            yield mock_repo

        with patch(
            "src.engines.crewai.tools.tool_session_provider.ToolSessionProvider.conversion_repo",
            _repo_ctx,
        ):
            self._run(tool._save_dax_to_conversion_history(
                raw_dax=raw_dax, yaml_output={"v": "yaml"}, sql_output={"v": "sql"},
                workspace_id="ws-1", dataset_id="ds-1", catalog="main", schema="default",
            ))

        data = captured["data"]
        assert data["source_format"] == "powerbi_dax"
        assert data["target_format"] == "uc_metrics"
        assert data["input_data"]["dax_raw"] == raw_dax
        assert data["input_data"]["workspace_id"] == "ws-1"
        assert data["measure_count"] == 1
        mock_repo.session.commit.assert_awaited_once()

    def test_json_mode_counts_views_not_zero(self):
        """JSON/flow mode: raw_dax empty but views generated → count = views, not 0.

        Regression: a successful JSON-mode run reported 'measure_count: 0' and
        'Generated UC metric views for 0 measure(s)', which read as a failure.
        """
        tool = UCMetricViewGeneratorTool()
        captured = {}
        mock_repo = MagicMock()
        mock_repo.create = AsyncMock(side_effect=lambda d: captured.update({"data": d}) or SimpleNamespace(id=9, group_id=None))
        mock_repo.session = MagicMock()
        mock_repo.session.commit = AsyncMock()

        @asynccontextmanager
        async def _repo_ctx():
            yield mock_repo

        yaml_out = {"fact_a": "version: '1.1'", "fact_b": "...", "fact_c": "..."}
        with patch(
            "src.engines.crewai.tools.tool_session_provider.ToolSessionProvider.conversion_repo",
            _repo_ctx,
        ):
            self._run(tool._save_dax_to_conversion_history(
                raw_dax=[], yaml_output=yaml_out, sql_output=yaml_out,
                workspace_id="ws", dataset_id="ds", catalog="main", schema="default",
            ))

        data = captured["data"]
        assert data["measure_count"] == 3  # views, not 0
        assert "3 UC metric view(s)" in data["output_summary"]

    def test_fail_open_on_repo_error(self):
        tool = UCMetricViewGeneratorTool()

        @asynccontextmanager
        async def _boom_ctx():
            raise RuntimeError("db down")
            yield  # pragma: no cover

        with patch(
            "src.engines.crewai.tools.tool_session_provider.ToolSessionProvider.conversion_repo",
            _boom_ctx,
        ):
            result = self._run(tool._save_dax_to_conversion_history(
                raw_dax=[], yaml_output={}, sql_output={},
                workspace_id=None, dataset_id=None, catalog="main", schema="default",
            ))
        assert result is None


# ---------------------------------------------------------------------------
# Measure DAX fallback (TMDL → SP) for API-mode extraction under a Service Account
# ---------------------------------------------------------------------------

from unittest.mock import patch, MagicMock


class TestTmdlTablesToMeasures:
    def test_maps_shape_with_dax(self):
        tables = {"Fact_Sales": {"columns": [], "mquery_expression": "", "measures": [
            {"name": "Total Revenue", "expression": "SUM(x)"},
            {"name": "Margin", "expression": "DIVIDE(a,b)"},
        ]}}
        out = UCMetricViewGeneratorTool._tmdl_tables_to_measures(tables)
        assert len(out) == 2
        assert out[0]["measure_name"] == "Total Revenue"
        assert out[0]["dax_expression"] == "SUM(x)"
        assert out[0]["proposed_allocation"] == "Fact_Sales"

    def test_skips_nameless_and_empty(self):
        assert UCMetricViewGeneratorTool._tmdl_tables_to_measures({}) == []
        tables = {"T": {"measures": [{"name": "", "expression": "X"}]}}
        assert UCMetricViewGeneratorTool._tmdl_tables_to_measures(tables) == []


class TestExtractMeasuresFallback:
    """_extract_measures_fallback recovers DAX via TMDL (SA first, then SP)."""

    def _fake_gen(self, tmdl_parts_ok=True):
        gen = MagicMock()
        gen.get_fabric_token.return_value = "fab-token"
        gen.fetch_tmdl_parts.return_value = (
            [{"path": "definition/tables/T.tmdl", "payload": "x"}] if tmdl_parts_ok else None
        )
        gen.parse_tmdl_to_admin_tables.return_value = {
            "Fact": {"columns": [], "mquery_expression": "", "measures": [
                {"name": "Rev", "expression": "SUM(a)"}]}
        }
        return gen

    def test_sa_tmdl_recovers_measures(self):
        tool = UCMetricViewGeneratorTool()
        gen = self._fake_gen()
        with patch.object(UCMetricViewGeneratorTool, "_import_generate_config", return_value=gen):
            out = tool._extract_measures_fallback(
                workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c",
                client_secret=None, username="sa@x.com", password="pw",
            )
        assert len(out) == 1
        assert out[0]["measure_name"] == "Rev"
        assert out[0]["dax_expression"] == "SUM(a)"
        # SA branch used username/password
        _, kw = gen.get_fabric_token.call_args
        assert kw["username"] == "sa@x.com"

    def test_falls_through_to_sp_when_no_sa(self):
        tool = UCMetricViewGeneratorTool()
        gen = self._fake_gen()
        with patch.object(UCMetricViewGeneratorTool, "_import_generate_config", return_value=gen):
            out = tool._extract_measures_fallback(
                workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c",
                client_secret="sp-secret", username=None, password=None,
            )
        assert len(out) == 1
        # SP branch passes the secret positionally (3rd arg)
        args, _ = gen.get_fabric_token.call_args
        assert "sp-secret" in args

    def test_returns_empty_when_tmdl_unavailable_and_no_creds(self):
        tool = UCMetricViewGeneratorTool()
        gen = self._fake_gen(tmdl_parts_ok=False)
        with patch.object(UCMetricViewGeneratorTool, "_import_generate_config", return_value=gen):
            out = tool._extract_measures_fallback(
                workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c",
                client_secret=None, username=None, password=None,
            )
        assert out == []


# ---------------------------------------------------------------------------
# MQuery TMDL fallback (Admin Scanner blocked for Service Accounts)
# ---------------------------------------------------------------------------

class TestTmdlTablesToMquery:
    def test_maps_source_and_skips_empty(self):
        tables = {
            "Fact_Sales": {"columns": [], "mquery_expression": "let S = X in S", "measures": []},
            "Empty": {"columns": [], "mquery_expression": "", "measures": []},
        }
        out = UCMetricViewGeneratorTool._tmdl_tables_to_mquery(tables)
        assert len(out) == 1
        assert out[0]["table_name"] == "Fact_Sales"
        assert out[0]["transpiled_sql"] == "let S = X in S"
        # MUST be "Yes": MQueryParser.parse_json drops entries whose
        # validation_passed does not start with "Yes" (unless SUM+GROUP BY),
        # so "No" here silently discarded every TMDL-recovered table → 0 views.
        assert out[0]["validation_passed"] == "Yes"


class TestExtractMqueryFallback:
    def _fake_gen(self, tmdl_ok=True):
        gen = MagicMock()
        gen.get_fabric_token.return_value = "fab"
        gen.fetch_tmdl_parts.return_value = (
            [{"path": "definition/tables/T.tmdl", "payload": "x"}] if tmdl_ok else None
        )
        gen.parse_tmdl_to_admin_tables.return_value = {
            "Fact": {"columns": [], "mquery_expression": "let S=X in S", "measures": []}
        }
        return gen

    def test_sa_recovers_mquery(self):
        tool = UCMetricViewGeneratorTool()
        gen = self._fake_gen()
        with patch.object(UCMetricViewGeneratorTool, "_import_generate_config", return_value=gen):
            out = tool._extract_mquery_fallback(
                workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c",
                client_secret=None, username="sa@x.com", password="pw",
            )
        assert len(out) == 1 and out[0]["table_name"] == "Fact"
        _, kw = gen.get_fabric_token.call_args
        assert kw["username"] == "sa@x.com"

    def test_sp_fallback_when_no_sa(self):
        tool = UCMetricViewGeneratorTool()
        gen = self._fake_gen()
        with patch.object(UCMetricViewGeneratorTool, "_import_generate_config", return_value=gen):
            out = tool._extract_mquery_fallback(
                workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c",
                client_secret="sp-secret", username=None, password=None,
            )
        assert len(out) == 1
        args, _ = gen.get_fabric_token.call_args
        assert "sp-secret" in args

    def test_empty_when_no_creds_and_no_tmdl(self):
        tool = UCMetricViewGeneratorTool()
        gen = self._fake_gen(tmdl_ok=False)
        with patch.object(UCMetricViewGeneratorTool, "_import_generate_config", return_value=gen):
            out = tool._extract_mquery_fallback(
                workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c",
                client_secret=None, username=None, password=None,
            )
        assert out == []


class TestBuildFallbackExtract:
    """Worst-case fallback: per-table M-Query + measures/DAX table.

    When no views generate (all-raw-M model), the run still yields the extracted
    source material grouped per table so the customer has something usable.
    """

    def test_groups_mquery_and_measures_per_table(self):
        measures = [
            {"measure_name": "Total", "dax_expression": "SUM(DCC[Amt])", "proposed_allocation": "DCC_Sales"},
            {"measure_name": "Cnt", "expression": "COUNTROWS(DCC)", "table_name": "DCC_Sales"},
        ]
        mquery = [{"table_name": "DCC_Sales", "transpiled_sql": "let S in S"}]
        rows = UCMetricViewGeneratorTool._build_fallback_extract(measures, mquery)
        dcc = next(r for r in rows if r["table_name"] == "DCC_Sales")
        assert dcc["measure_count"] == 2
        assert dcc["has_mquery"] is True
        # alt keys (expression) are read
        assert any(m["dax_expression"] == "COUNTROWS(DCC)" for m in dcc["measures"])

    def test_table_with_mquery_but_no_measures_included(self):
        rows = UCMetricViewGeneratorTool._build_fallback_extract(
            [], [{"table_name": "dim_Country", "transpiled_sql": "let S in S"}]
        )
        dim = next(r for r in rows if r["table_name"] == "dim_Country")
        assert dim["measure_count"] == 0 and dim["has_mquery"] is True

    def test_measure_without_allocation_goes_unassigned(self):
        rows = UCMetricViewGeneratorTool._build_fallback_extract(
            [{"measure_name": "X", "dax_expression": "BLANK()"}], []
        )
        assert rows[0]["table_name"] == "__unassigned__"
        assert rows[0]["measure_count"] == 1

    def test_empty_inputs(self):
        assert UCMetricViewGeneratorTool._build_fallback_extract([], []) == []
        assert UCMetricViewGeneratorTool._build_fallback_extract(None, None) == []

    def test_sorted_measures_first(self):
        rows = UCMetricViewGeneratorTool._build_fallback_extract(
            [{"measure_name": "M", "dax_expression": "d", "proposed_allocation": "HasMeasures"}],
            [{"table_name": "NoMeasures", "transpiled_sql": "let S in S"},
             {"table_name": "HasMeasures", "transpiled_sql": "let S in S"}],
        )
        # table with measures sorts before the one without
        assert rows[0]["table_name"] == "HasMeasures"
