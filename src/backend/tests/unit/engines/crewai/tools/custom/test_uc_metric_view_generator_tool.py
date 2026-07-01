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
