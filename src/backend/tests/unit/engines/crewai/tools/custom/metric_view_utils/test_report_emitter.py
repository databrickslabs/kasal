"""Tests for migration report emitter."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.report_emitter import emit_migration_report
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MetricViewSpec, TranslationResult


class TestReportEmitter:
    def test_basic_report(self):
        spec = MetricViewSpec(
            fact_table_key='fact_test',
            source_table='cat.sch.tbl',
            view_name='mv_test',
            comment='Test',
            joins=[],
            dimensions=[{'name': 'col', 'expr': 'source.col', 'comment': 'Col'}],
            measures=[
                TranslationResult(
                    measure_name='total', original_name='Total',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='', dax_expression='SUM(T[val])',
                    confidence='high', category='base',
                )
            ],
            untranslatable=[],
            base_measure_count=1,
        )
        stats = {
            'fact_test': {'total': 1, 'translated': 1, 'untranslatable': 0,
                         'artifacts': 0, 'base': 1, 'dax': 0, 'switch': 0},
        }
        report = emit_migration_report({'fact_test': spec}, stats)
        assert '# UC Metric View Migration Report' in report
        assert 'Executive Summary' in report
        assert 'Per-Table Results' in report
        assert 'fact_test' in report

    def test_empty_report(self):
        report = emit_migration_report({}, {})
        assert '# UC Metric View Migration Report' in report
        assert '0' in report  # zero totals

    def test_report_includes_untranslatable(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='mv',
            comment='',
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[
                TranslationResult(
                    measure_name='bad', original_name='Bad Measure',
                    sql_expr=None, is_translatable=False,
                    skip_reason='FORMAT function',
                    dax_expression='FORMAT(x)', confidence='none', category='unassigned',
                )
            ],
        )
        stats = {'fact': {'total': 1, 'translated': 0, 'untranslatable': 1, 'artifacts': 1}}
        report = emit_migration_report({'fact': spec}, stats)
        assert 'Bad Measure' in report
        assert 'FORMAT' in report


class TestReportM2NSection:
    def test_m2n_section_rendered(self):
        """M:N relationships appear in the report when limitations are provided."""
        limitations = {
            'm2n_relationships': [
                {'from_table': 'Orders', 'from_column': 'product_id',
                 'to_table': 'Products', 'to_column': 'order_id'},
            ]
        }
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '## M:N Relationships (Not Migrated)' in report
        assert 'Orders' in report
        assert 'Products' in report
        assert 'product_id' in report
        assert 'bridge table' in report

    def test_m2n_section_absent_when_no_limitations(self):
        """M:N detail section is not rendered when there are no limitations."""
        report = emit_migration_report({}, {})
        assert '## M:N Relationships (Not Migrated)' not in report

    def test_m2n_section_absent_when_empty_list(self):
        """M:N detail section is not rendered when the list is empty."""
        limitations = {'m2n_relationships': []}
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '## M:N Relationships (Not Migrated)' not in report

    def test_m2n_multiple_rows(self):
        """Multiple M:N relationships are all rendered."""
        limitations = {
            'm2n_relationships': [
                {'from_table': 'A', 'from_column': 'k1',
                 'to_table': 'B', 'to_column': 'k1'},
                {'from_table': 'C', 'from_column': 'k2',
                 'to_table': 'D', 'to_column': 'k2'},
            ]
        }
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '| A |' in report
        assert '| C |' in report
        assert '| B |' in report
        assert '| D |' in report


class TestReportRLSSection:
    def test_rls_section_rendered(self):
        """RLS warning appears when rls_tables are provided."""
        limitations = {'rls_tables': ['Sales', 'Budget']}
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '## Row-Level Security Warning' in report
        assert '- Budget' in report
        assert '- Sales' in report
        assert 'Databricks row filters' in report

    def test_rls_section_absent_when_no_limitations(self):
        """RLS detail section is not rendered when there are no limitations."""
        report = emit_migration_report({}, {})
        assert '## Row-Level Security Warning' not in report

    def test_rls_section_absent_when_empty(self):
        """RLS detail section is not rendered when the list is empty."""
        limitations = {'rls_tables': []}
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '## Row-Level Security Warning' not in report

    def test_rls_tables_sorted(self):
        """RLS tables are rendered in sorted order."""
        limitations = {'rls_tables': ['Zebra', 'Alpha', 'Middle']}
        report = emit_migration_report({}, {}, limitations=limitations)
        alpha_pos = report.index('- Alpha')
        middle_pos = report.index('- Middle')
        zebra_pos = report.index('- Zebra')
        assert alpha_pos < middle_pos < zebra_pos


class TestReportAggregationSection:
    def test_aggregation_section_rendered(self):
        """Aggregation warnings appear when provided."""
        limitations = {
            'aggregation_warnings': [{
                'table': 'fact_agg',
                'storage_mode': 'Import',
                'warning': 'Import-mode table may be an aggregation table. Verify source grain.',
            }]
        }
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '## Aggregation Table Warnings' in report
        assert 'fact_agg' in report
        assert 'Import storage mode' in report

    def test_aggregation_section_absent_when_no_limitations(self):
        """Aggregation detail section is not rendered when there are no limitations."""
        report = emit_migration_report({}, {})
        assert '## Aggregation Table Warnings' not in report

    def test_aggregation_section_absent_when_empty(self):
        """Aggregation detail section is not rendered when the list is empty."""
        limitations = {'aggregation_warnings': []}
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '## Aggregation Table Warnings' not in report

    def test_aggregation_multiple_warnings(self):
        """Multiple aggregation warnings are all rendered."""
        limitations = {
            'aggregation_warnings': [
                {'table': 'agg_a', 'storage_mode': 'Import', 'warning': 'Warning A'},
                {'table': 'agg_b', 'storage_mode': 'Import', 'warning': 'Warning B'},
            ]
        }
        report = emit_migration_report({}, {}, limitations=limitations)
        assert '**agg_a**' in report
        assert '**agg_b**' in report
        assert 'Warning A' in report
        assert 'Warning B' in report
