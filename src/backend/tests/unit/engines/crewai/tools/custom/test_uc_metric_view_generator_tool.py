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
