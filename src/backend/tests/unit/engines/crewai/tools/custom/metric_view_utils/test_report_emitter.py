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
