"""Tests for PBI limitation detection and reporting."""
from src.engines.crewai.tools.custom.metric_view_utils.report_emitter import emit_migration_report
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MetricViewSpec, TranslationResult


def _make_spec(key='fact'):
    return MetricViewSpec(
        fact_table_key=key, source_table='s', view_name='v',
        comment='c', joins=[], dimensions=[],
        measures=[TranslationResult(
            measure_name='x', original_name='X', sql_expr='SUM(source.x)',
            is_translatable=True, skip_reason='', dax_expression='',
            confidence='high', category='base',
        )],
        untranslatable=[], base_measure_count=1,
    )


class TestLimitationsReport:
    def test_all_limitations_in_summary_table(self):
        limitations = {
            'inactive_relationships': [{'from_table': 'f', 'from_column': 'c', 'to_table': 'd', 'to_column': 'k'}],
            'm2n_relationships': [{'from_table': 'a', 'from_column': 'k', 'to_table': 'b', 'to_column': 'k'}],
            'rls_tables': {'Sales'},
            'aggregation_warnings': [{'table': 'agg', 'warning': 'test'}],
            'refresh_policies': [{'table_name': 'fact'}],
            'summarization_warnings': [{'table_name': 'f', 'column_name': 'id'}],
            'calculation_groups_expanded': [{'expanded_count': 5}],
            'perspectives': [{'name': 'Sales View'}],
            'field_parameters': [{'name': 'Measure Selector'}],
        }
        report = emit_migration_report(
            {'fact': _make_spec()},
            {'fact': {'total': 1, 'translated': 1, 'untranslatable': 0, 'artifacts': 0}},
            limitations=limitations,
        )
        assert 'PBI Native Features' in report
        assert 'USERELATIONSHIP' in report
        assert 'M:N' in report
        assert 'Row-Level Security' in report
        assert 'Aggregation' in report
        assert 'Incremental Refresh' in report
        assert 'Summarization' in report
        assert 'Calculation Groups' in report
        assert 'Perspectives' in report
        assert 'Field Parameters' in report
        assert 'Conditional Formatting' in report

    def test_no_limitations_still_has_table(self):
        report = emit_migration_report(
            {'fact': _make_spec()},
            {'fact': {'total': 1, 'translated': 1, 'untranslatable': 0, 'artifacts': 0}},
        )
        assert 'PBI Native Features' in report
        assert 'N/A' in report

    def test_perspectives_section(self):
        limitations = {'perspectives': [{'name': 'Sales'}, {'name': 'Finance'}]}
        report = emit_migration_report(
            {'fact': _make_spec()},
            {'fact': {'total': 1, 'translated': 1, 'untranslatable': 0}},
            limitations=limitations,
        )
        assert 'Perspectives (Not Migrated)' in report
        assert 'Sales' in report
        assert 'Finance' in report

    def test_field_parameters_section(self):
        limitations = {'field_parameters': [{'name': 'Measure Picker'}]}
        report = emit_migration_report(
            {'fact': _make_spec()},
            {'fact': {'total': 1, 'translated': 1, 'untranslatable': 0}},
            limitations=limitations,
        )
        assert 'Field Parameters (Not Migrated)' in report
        assert 'Measure Picker' in report
