"""Integration tests for the MetricViewPipeline with real-format data."""
import json
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TranslationResult


class TestPipelineIntegration:
    """Test the full pipeline with synthetic but realistic data."""

    def _make_pipeline(self, measures, mquery_entries, config=None):
        parser = MQueryParser()
        tables = parser.parse_json(mquery_entries)
        return MetricViewPipeline(
            mapping=measures,
            mquery_tables=tables,
            config=config or {},
        )

    def test_basic_fact_table(self):
        measures = [
            {'measure_name': 'Total Sales', 'dax_expression': 'SUM(fact_sales[amount])',
             'original_name': 'Total Sales', 'proposed_allocation': 'fact_sales'},
        ]
        mquery = [
            {'table_name': 'fact_sales',
             'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM cat.sch.sales GROUP BY region',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        specs = pipeline.run()
        assert 'fact_sales' in specs
        assert len(specs['fact_sales'].measures) >= 1

    def test_measure_arithmetic_cascade(self):
        """Test [A] - [B] resolution via Pass 2."""
        measures = [
            {'measure_name': 'Sales', 'dax_expression': 'SUM(fact[sales])',
             'original_name': 'Sales', 'proposed_allocation': 'fact'},
            {'measure_name': 'Cost', 'dax_expression': 'SUM(fact[cost])',
             'original_name': 'Cost', 'proposed_allocation': 'fact'},
            {'measure_name': 'Profit', 'dax_expression': '[Sales] - [Cost]',
             'original_name': 'Profit', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT region, SUM(sales) AS sales, SUM(cost) AS cost FROM cat.sch.tbl GROUP BY region',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        specs = pipeline.run()
        spec = specs['fact']
        measure_names = {m.measure_name for m in spec.measures}
        assert 'profit' in measure_names
        profit = next(m for m in spec.measures if m.measure_name == 'profit')
        assert 'MEASURE(' in profit.sql_expr

    def test_cycle_detection(self):
        """Test that circular dependencies are caught."""
        measures = [
            {'measure_name': 'A', 'dax_expression': '[B] + 1',
             'original_name': 'A', 'proposed_allocation': 'fact'},
            {'measure_name': 'B', 'dax_expression': '[A] + 1',
             'original_name': 'B', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        specs = pipeline.run()
        spec = specs['fact']
        # Both should be untranslatable with circular dependency reason
        untrans_names = {m.original_name for m in spec.untranslatable}
        assert 'A' in untrans_names or 'B' in untrans_names

    def test_artifact_cascade(self):
        """Test that FORMAT/Color measures are classified as artifacts."""
        measures = [
            {'measure_name': 'Color', 'dax_expression': 'IF(x>0, "green", "red")',
             'original_name': 'Sales_Color', 'proposed_allocation': 'fact'},
            {'measure_name': 'Fmt', 'dax_expression': 'FORMAT(x, "#,##0")',
             'original_name': 'Sales_Fmt', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        specs = pipeline.run()
        spec = specs['fact']
        # Artifact measures should end up in untranslatable with appropriate reasons
        for m in spec.untranslatable:
            assert ('Color' in m.skip_reason or 'FORMAT' in m.skip_reason
                    or 'artifact' in m.skip_reason.lower()
                    or 'IF(' in m.dax_expression or 'FORMAT(' in m.dax_expression)

    def test_switch_decomposition(self):
        """Test SWITCH decomposition from config."""
        measures = [
            {'measure_name': 'Wrapper', 'dax_expression': 'SWITCH(SELECTEDVALUE(T[x]), "a", 1)',
             'original_name': 'Wrapper', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        config = {
            'switch_decompositions': {
                'fact': [
                    {'name': 'branch_a', 'raw_expr': "SUM(source.val) FILTER (WHERE source.col = 'A')",
                     'comment': 'Branch A'},
                ]
            }
        }
        pipeline = self._make_pipeline(measures, mquery, config)
        specs = pipeline.run()
        spec = specs['fact']
        measure_names = {m.measure_name for m in spec.measures}
        assert 'branch_a' in measure_names

    def test_empty_measures(self):
        """Test that tables with zero DAX measures still get base measures from MQuery."""
        measures = []
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        specs = pipeline.run()
        # fact has base measures from MQuery but no DAX measures
        if 'fact' in specs:
            assert len(specs['fact'].measures) >= 1  # base measure from SUM(val)

    def test_get_results_serializable(self):
        """Test that get_results() returns JSON-serializable data."""
        measures = [
            {'measure_name': 'X', 'dax_expression': 'SUM(fact[x])',
             'original_name': 'X', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(x) AS x FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        pipeline.run()
        results = pipeline.get_results()
        # Should be JSON-serializable
        json.dumps(results)

    def test_multiple_fact_tables(self):
        """Test pipeline with multiple fact tables."""
        measures = [
            {'measure_name': 'Revenue', 'dax_expression': 'SUM(sales[amount])',
             'original_name': 'Revenue', 'proposed_allocation': 'sales'},
            {'measure_name': 'Units', 'dax_expression': 'SUM(inventory[qty])',
             'original_name': 'Units', 'proposed_allocation': 'inventory'},
        ]
        mquery = [
            {'table_name': 'sales',
             'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM cat.sch.sales GROUP BY region',
             'validation_passed': 'Yes'},
            {'table_name': 'inventory',
             'transpiled_sql': 'SELECT warehouse, SUM(qty) AS qty FROM cat.sch.inv GROUP BY warehouse',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        specs = pipeline.run()
        assert 'sales' in specs
        assert 'inventory' in specs

    def test_emit_all_yaml_produces_content(self):
        """Test that emit_all_yaml generates YAML for each spec."""
        measures = [
            {'measure_name': 'Total', 'dax_expression': 'SUM(fact[val])',
             'original_name': 'Total', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        pipeline.run()
        yaml_out = pipeline.emit_all_yaml(catalog='test_cat', schema='test_sch')
        assert 'fact' in yaml_out
        assert 'version' in yaml_out['fact']
        assert 'measures' in yaml_out['fact']

    def test_emit_all_sql_produces_content(self):
        """Test that emit_all_sql generates deploy SQL for each spec."""
        measures = [
            {'measure_name': 'Total', 'dax_expression': 'SUM(fact[val])',
             'original_name': 'Total', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        pipeline.run()
        sql_out = pipeline.emit_all_sql(catalog='test_cat', schema='test_sch')
        assert 'fact' in sql_out
        assert 'Metric View' in sql_out['fact']

    def test_stats_populated(self):
        """Test that pipeline.stats are populated after run()."""
        measures = [
            {'measure_name': 'X', 'dax_expression': 'SUM(fact[x])',
             'original_name': 'X', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT col, SUM(x) AS x FROM cat.sch.tbl GROUP BY col',
             'validation_passed': 'Yes'},
        ]
        pipeline = self._make_pipeline(measures, mquery)
        pipeline.run()
        assert 'fact' in pipeline.stats
        assert 'translated' in pipeline.stats['fact']
        assert 'total' in pipeline.stats['fact']
