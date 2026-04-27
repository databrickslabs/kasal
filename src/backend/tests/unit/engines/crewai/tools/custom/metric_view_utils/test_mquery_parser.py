"""Tests for MQueryParser."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser


@pytest.fixture
def parser():
    return MQueryParser()


class TestParseJson:
    def test_basic_fact_table(self, parser):
        entries = [{
            'table_name': 'fact_sales',
            'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM catalog.schema.sales GROUP BY region',
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert 'fact_sales' in tables
        info = tables['fact_sales']
        assert info.is_fact is True
        assert info.source_table == 'catalog.schema.sales'
        assert len(info.aggregate_columns) == 1
        assert info.aggregate_columns[0]['name'] == 'amount'
        assert 'region' in info.group_by_columns

    def test_dim_table_no_aggregates(self, parser):
        entries = [{
            'table_name': 'dim_region',
            'transpiled_sql': 'SELECT code, name FROM catalog.schema.regions',
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert 'dim_region' in tables
        assert tables['dim_region'].is_fact is False

    def test_skips_failed_validation(self, parser):
        entries = [{
            'table_name': 'bad_table',
            'transpiled_sql': 'SELECT * FROM catalog.schema.x',
            'validation_passed': 'No',
        }]
        tables = parser.parse_json(entries)
        assert 'bad_table' not in tables

    def test_accepts_list_input(self, parser):
        entries = [{
            'table_name': 'test',
            'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert 'test' in tables

    def test_extracts_where_filters(self, parser):
        entries = [{
            'table_name': 'fact',
            'transpiled_sql': "SELECT col, SUM(val) AS val FROM cat.sch.tbl WHERE status = 'active' GROUP BY col",
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert len(tables['fact'].static_filters) >= 1

    def test_extracts_left_joins(self, parser):
        sql = (
            "SELECT t.col, SUM(t.val) AS val "
            "FROM cat.sch.fact t "
            "LEFT JOIN cat.sch.dim d ON t.key = d.key "
            "GROUP BY t.col"
        )
        entries = [{'table_name': 'fact', 'transpiled_sql': sql, 'validation_passed': 'Yes'}]
        tables = parser.parse_json(entries)
        assert 'd' in tables['fact'].dim_source_tables


class TestGroupByAll:
    def test_infer_group_by_all(self, parser):
        sql = "SELECT region, country, SUM(amount) AS amount FROM cat.sch.tbl GROUP BY ALL"
        entries = [{'table_name': 'test', 'transpiled_sql': sql, 'validation_passed': 'Yes'}]
        tables = parser.parse_json(entries)
        gb = tables['test'].group_by_columns
        assert 'region' in gb
        assert 'country' in gb
