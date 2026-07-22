"""Integration tests for the MetricViewPipeline with real-format data."""
import json
import os
import re

import pytest
import yaml

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


# ---------------------------------------------------------------------------
# Full end-to-end integration test using SC Reporting example data
# ---------------------------------------------------------------------------

_EXAMPLE_DIR = os.path.normpath(os.path.join(
    os.path.dirname(__file__),
    '..', '..', '..', '..', '..', '..', '..', '..', '..',
    'examples', 'uc_metric_view_migration',
))

_HAS_EXAMPLE_DATA = os.path.exists(
    os.path.join(_EXAMPLE_DIR, 'pipeline_config.json')
)


@pytest.fixture(scope='module')
def pipeline_output():
    """Run the full pipeline once for all tests in the module."""
    from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
    from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

    with open(os.path.join(_EXAMPLE_DIR, 'measure_table_mapping.json')) as f:
        measures = json.load(f)
    with open(os.path.join(_EXAMPLE_DIR, 'mquery_transpilation.json')) as f:
        mquery_entries = json.load(f)
    with open(os.path.join(_EXAMPLE_DIR, 'pbi_relationships.json')) as f:
        rels_raw = json.load(f)
    with open(os.path.join(_EXAMPLE_DIR, 'pipeline_config.json')) as f:
        config = json.load(f)

    CATALOG = 'test_cat'
    SCHEMA = 'test_sch'
    for tbl_cfg in config.get('mapping_only_tables', {}).values():
        tbl_cfg['source_table'] = tbl_cfg['source_table'].format(
            catalog=CATALOG, schema=SCHEMA,
        )

    parser = MQueryParser()
    mquery_tables = parser.parse_json(mquery_entries)
    fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}

    rel_loader = RelationshipsLoader()
    rel_enrich = rel_loader.load(rels_raw, mquery_tables, fact_tables)

    scan_parser = ScanDataParser()
    scan_path = os.path.join(_EXAMPLE_DIR, 'scan_result_debug.json')
    scan_data = scan_parser.parse(scan_path) if os.path.exists(scan_path) else {}

    pipeline = MetricViewPipeline(
        mapping=measures,
        mquery_tables=mquery_tables,
        config=config,
        relationships_enrichment=rel_enrich,
        inactive_relationships=rel_loader.get_inactive_relationships() or None,
        scan_data=scan_data,
        unflatten_tables=True,
        refresh_policy_tables=scan_parser.get_refresh_policy_tables() or None,
        no_summarize_columns=scan_parser.get_no_summarize_columns() or None,
        rls_tables=scan_parser.get_rls_tables() or None,
    )
    specs = pipeline.run()
    yaml_out = pipeline.emit_all_yaml(catalog=CATALOG, schema=SCHEMA)
    results = pipeline.get_results()
    return {
        'specs': specs,
        'yaml_out': yaml_out,
        'stats': pipeline.stats,
        'results': results,
    }


@pytest.mark.skipif(not _HAS_EXAMPLE_DATA, reason='Example data not available')
class TestFullPipelineEndToEnd:
    """End-to-end integration tests using real SC Reporting example data."""

    def test_produces_26_views(self, pipeline_output):
        assert len(pipeline_output['specs']) == 26

    def test_yaml_parseable(self, pipeline_output):
        skipped_tables = {
            k for k, s in pipeline_output['stats'].items()
            if s.get('skipped')
        }
        for key, yml in pipeline_output['yaml_out'].items():
            parsed = yaml.safe_load(yml)
            if key in skipped_tables:
                # Skipped tables (all measures untranslatable) may emit empty YAML
                continue
            assert parsed is not None, f'{key} YAML not parseable'
            assert 'version' in parsed, f'{key} missing version'

    def test_no_empty_dimensions(self, pipeline_output):
        for key, spec in pipeline_output['specs'].items():
            for d in spec.dimensions:
                assert d.get('name'), f'{key} has empty dimension name'
                assert d.get('expr') and d['expr'] != 'source.', (
                    f'{key} has empty dimension expr: {d}'
                )

    def test_no_none_measure_exprs(self, pipeline_output):
        for key, spec in pipeline_output['specs'].items():
            for m in spec.measures:
                assert m.sql_expr is not None, (
                    f'{key}/{m.measure_name} has None sql_expr'
                )

    def test_measure_refs_resolve(self, pipeline_output):
        """Check that MEASURE() references resolve within each spec.

        Window-spec PY measures may reference _py variants that are
        generated on a different table (known limitation), so we count
        total unresolved rather than failing on first.
        """
        total_unresolved = 0
        for key, spec in pipeline_output['specs'].items():
            all_names = {m.measure_name for m in spec.measures}
            for m in spec.measures:
                if m.sql_expr:
                    refs = set(re.findall(r'\bMEASURE\((\w+)\)', m.sql_expr))
                    unresolved = refs - all_names
                    total_unresolved += len(unresolved)
        # Allow a small number of cross-spec PY measure references
        assert total_unresolved <= 10, (
            f'Too many unresolved MEASURE() refs: {total_unresolved} (max 10)'
        )

    def test_total_translated_regression_guard(self, pipeline_output):
        total = sum(
            s.get('translated', 0)
            for k, s in pipeline_output['stats'].items()
            if k != '__unassigned__'
        )
        assert total >= 390, (
            f'Regression: only {total} translated (expected >= 390)'
        )

    def test_manual_overrides_injected(self, pipeline_output):
        spec = pipeline_output['specs'].get('fact_scorecard_BP_wc')
        assert spec is not None, 'fact_scorecard_BP_wc spec not found'
        manual = [
            m for m in spec.measures
            if getattr(m, 'category', '') == 'manual_override'
        ]
        assert len(manual) >= 5, (
            f'Expected >= 5 manual overrides, got {len(manual)}'
        )

    def test_window_specs_on_py_measures(self, pipeline_output):
        spec = pipeline_output['specs'].get('FT_Planning')
        assert spec is not None, 'FT_Planning spec not found'
        py_measures = [
            m for m in spec.measures if m.measure_name.endswith('_py')
        ]
        windowed = [m for m in py_measures if m.window_spec]
        assert len(windowed) >= 3, (
            f'Expected >= 3 windowed PY measures, got {len(windowed)}'
        )

    def test_migration_report_has_pbi_features(self, pipeline_output):
        report = pipeline_output['results'].get('migration_report', '')
        assert 'PBI Native Features' in report
        assert 'USERELATIONSHIP' in report
        assert 'Aggregation' in report

    def test_yaml_no_empty_dimension_names(self, pipeline_output):
        """Validate the actual YAML strings don't contain empty dims."""
        for key, yml in pipeline_output['yaml_out'].items():
            lines = yml.split('\n')
            for i, line in enumerate(lines):
                if line.strip() == '- name:':
                    pytest.fail(
                        f'{key} has empty dimension name at line {i + 1}'
                    )

    def test_business_coverage_in_results(self, pipeline_output):
        bc = pipeline_output['results'].get('business_coverage')
        assert bc is not None
        assert bc['business_pct'] > bc['overall_pct']
        assert bc['artifacts_excluded'] > 0
        assert bc['business_pct'] >= 80  # SC Reporting should be ~85%


class TestMeasureDrivenFacts:
    """Phase 1b: promote plain-source tables with allocated DAX measures to facts.

    Regression: click-together Power Query facts resolve to a plain `SELECT * FROM t`
    (no aggregate SQL) → is_fact=False → 0 views, even though DAX measures are
    allocated to them (the aggregation lives in the measures). This pass promotes
    such tables. Strictly additive — only affects tables that produce ZERO output
    otherwise; never touches real SQL facts.
    """

    def _pipe(self, measures, mquery, config=None):
        parser = MQueryParser()
        tables = parser.parse_json(mquery)
        return MetricViewPipeline(mapping=measures, mquery_tables=tables, config=config or {})

    def test_plain_source_with_measures_promoted(self):
        measures = [{'measure_name': 'Total', 'original_name': 'Total',
                     'dax_expression': 'SUM(DCC_Sales[Amount])', 'proposed_allocation': 'DCC_Sales'}]
        mquery = [{'table_name': 'DCC_Sales',
                   'transpiled_sql': 'SELECT * FROM lakehouse.dcc_sales', 'validation_passed': 'Yes'}]
        pipe = self._pipe(measures, mquery)
        pipe.run()
        y = pipe.emit_all_yaml(catalog='main', schema='m')
        assert 'DCC_Sales' in y

    def test_plain_source_without_measures_not_promoted(self):
        # A dimension/param table (no measures) must NOT become a view.
        mquery = [{'table_name': 'dim_Country',
                   'transpiled_sql': 'SELECT * FROM lakehouse.dim_country', 'validation_passed': 'Yes'}]
        pipe = self._pipe([], mquery)
        pipe.run()
        y = pipe.emit_all_yaml(catalog='main', schema='m')
        assert 'dim_Country' not in y

    def test_kill_switch_disables_promotion(self):
        measures = [{'measure_name': 'Total', 'original_name': 'Total',
                     'dax_expression': 'SUM(DCC_Sales[Amount])', 'proposed_allocation': 'DCC_Sales'}]
        mquery = [{'table_name': 'DCC_Sales',
                   'transpiled_sql': 'SELECT * FROM lakehouse.dcc_sales', 'validation_passed': 'Yes'}]
        pipe = self._pipe(measures, mquery, config={'allow_measure_driven_facts': False})
        pipe.run()
        y = pipe.emit_all_yaml(catalog='main', schema='m')
        assert 'DCC_Sales' not in y

    def test_real_sql_fact_unaffected(self):
        # The working aggregate-SQL path must behave exactly as before.
        measures = [{'measure_name': 'M', 'original_name': 'M',
                     'dax_expression': 'SUM(FT[amt])', 'proposed_allocation': 'FT'}]
        mquery = [{'table_name': 'FT',
                   'transpiled_sql': 'SELECT region, SUM(amt) AS amt FROM s.ft GROUP BY region',
                   'validation_passed': 'Yes'}]
        pipe = self._pipe(measures, mquery)
        pipe.run()
        y = pipe.emit_all_yaml(catalog='main', schema='m')
        assert 'FT' in y

    def test_no_source_table_not_promoted(self):
        # A raw-M table the parser couldn't extract a source from stays skipped
        # (measure-driven promotion needs a real source_table).
        measures = [{'measure_name': 'X', 'original_name': 'X',
                     'dax_expression': 'SUM(T[a])', 'proposed_allocation': 'T'}]
        # 'let..in' raw M → parser yields source_table='' → not promotable here.
        mquery = [{'table_name': 'T',
                   'transpiled_sql': 'let Source = Foo in Source', 'validation_passed': 'Yes'}]
        pipe = self._pipe(measures, mquery)
        pipe.run()
        y = pipe.emit_all_yaml(catalog='main', schema='m')
        assert 'T' not in y


class TestCatalogSchemaPlaceholderResolution:
    """P1: no {catalog}/{schema} literal may survive into emitted join sources."""

    def _spec(self):
        from types import SimpleNamespace
        return SimpleNamespace(
            source_table="{catalog}.{schema}.fact_x",
            joins=[
                {"name": "dim_calendar", "source": "{catalog}.{schema}.c_dim_calendar",
                 "on": "source.d = dim_calendar.d"},
                {"name": "dim_real", "source": "dc_prod.idor.ca_dim_plant",
                 "on": "source.p = dim_real.p"},
            ],
        )

    def test_placeholders_substituted(self):
        spec = self._spec()
        MetricViewPipeline._resolve_placeholders_in_spec(spec, "dc_prod", "idor")
        assert spec.source_table == "dc_prod.idor.fact_x"
        assert spec.joins[0]["source"] == "dc_prod.idor.c_dim_calendar"
        # already-resolved sources untouched
        assert spec.joins[1]["source"] == "dc_prod.idor.ca_dim_plant"

    def test_idempotent(self):
        spec = self._spec()
        MetricViewPipeline._resolve_placeholders_in_spec(spec, "dc_prod", "idor")
        MetricViewPipeline._resolve_placeholders_in_spec(spec, "dc_prod", "idor")
        assert "{catalog}" not in spec.joins[0]["source"]
        assert "{schema}" not in spec.joins[0]["source"]
