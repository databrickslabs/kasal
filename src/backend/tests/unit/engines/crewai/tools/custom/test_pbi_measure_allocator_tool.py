"""Tests for PbiMeasureAllocatorTool."""
import json
import pytest
from src.engines.crewai.tools.custom.pbi_measure_allocator_tool import PbiMeasureAllocatorTool


class TestPbiMeasureAllocatorTool:
    def test_initialization(self):
        tool = PbiMeasureAllocatorTool()
        assert tool.name == "PBI Measure Allocator"

    def test_basic_allocation(self):
        tool = PbiMeasureAllocatorTool()
        measures = [
            {'measure_name': 'total', 'dax_expression': 'SUM(fact_sales[amount])', 'original_name': 'Total'},
            {'measure_name': 'pct', 'dax_expression': 'DIVIDE(SUM(fact_sales[a]), SUM(fact_sales[b]))', 'original_name': 'Pct'},
        ]
        mquery = [
            {
                'table_name': 'fact_sales',
                'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM cat.sch.sales GROUP BY region',
                'validation_passed': 'Yes',
            }
        ]
        result = tool._run(measures_json=json.dumps(measures), mquery_json=json.dumps(mquery))
        data = json.loads(result)
        assert data['summary']['total'] == 2
        assert data['summary']['allocated'] >= 1

    def test_unassigned_measures(self):
        tool = PbiMeasureAllocatorTool()
        measures = [
            {'measure_name': 'x', 'dax_expression': 'IF(TRUE, 1, 0)', 'original_name': 'X'},
        ]
        mquery = [
            {
                'table_name': 'fact_sales',
                'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM cat.sch.sales GROUP BY region',
                'validation_passed': 'Yes',
            }
        ]
        result = tool._run(measures_json=json.dumps(measures), mquery_json=json.dumps(mquery))
        data = json.loads(result)
        assert data['summary']['unassigned'] >= 1
