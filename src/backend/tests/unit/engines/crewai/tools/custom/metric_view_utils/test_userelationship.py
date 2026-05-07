"""Tests for USERELATIONSHIP handling."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo


def _make_table(name, source, group_by, agg=None, is_fact=True):
    return TableInfo(
        table_name=name,
        source_table=source,
        aggregate_columns=agg if agg is not None else [{'name': 'v', 'source_col': 'v'}],
        group_by_columns=group_by,
        calculated_columns=[],
        is_fact=is_fact,
        full_sql='',
    )


class TestInactiveRelationships:
    def test_inactive_rels_collected(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['order_date', 'ship_date']),
            'Calendar': _make_table('Calendar', 'cat.sch.calendar', ['date'],
                                    agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'order_date', 'from_cardinality': 'Many',
             'to_table': 'Calendar', 'to_column': 'date', 'to_cardinality': 'One',
             'is_active': True},
            {'from_table': 'fact', 'from_column': 'ship_date', 'from_cardinality': 'Many',
             'to_table': 'Calendar', 'to_column': 'date', 'to_cardinality': 'One',
             'is_active': False},
        ]
        loader.load(rels, tables, {'fact'})
        inactive = loader.get_inactive_relationships()
        assert len(inactive) == 1
        assert inactive[0]['from_column'] == 'ship_date'
        assert inactive[0]['to_table'] == 'Calendar'
        assert inactive[0]['to_column'] == 'date'
        assert inactive[0]['from_table'] == 'fact'

    def test_no_inactive_when_all_active(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'dim': _make_table('dim', 'cat.sch.dim', ['key', 'label'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'key', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'key', 'to_cardinality': 'One',
             'is_active': True},
        ]
        loader.load(rels, tables, {'fact'})
        assert loader.get_inactive_relationships() == []

    def test_inactive_default_empty(self):
        loader = RelationshipsLoader()
        assert loader.get_inactive_relationships() == []

    def test_active_still_creates_enrichment(self):
        """Active relationships should still produce enrichment joins."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['order_date', 'ship_date']),
            'Calendar': _make_table('Calendar', 'cat.sch.calendar', ['date'],
                                    agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'order_date', 'from_cardinality': 'Many',
             'to_table': 'Calendar', 'to_column': 'date', 'to_cardinality': 'One',
             'is_active': True},
            {'from_table': 'fact', 'from_column': 'ship_date', 'from_cardinality': 'Many',
             'to_table': 'Calendar', 'to_column': 'date', 'to_cardinality': 'One',
             'is_active': False},
        ]
        result = loader.load(rels, tables, {'fact'})
        # Active rel should produce enrichment, inactive should not
        assert 'fact' in result
        assert len(result['fact']) == 1
        assert result['fact'][0]['join_on'] == 'source.order_date = calendar.date'

    def test_multiple_inactive_rels(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['d1', 'd2']),
            'DimA': _make_table('DimA', 'cat.sch.dima', ['key'],
                                agg=[], is_fact=False),
            'DimB': _make_table('DimB', 'cat.sch.dimb', ['key'],
                                agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'd1', 'from_cardinality': 'Many',
             'to_table': 'DimA', 'to_column': 'key', 'to_cardinality': 'One',
             'is_active': False},
            {'from_table': 'fact', 'from_column': 'd2', 'from_cardinality': 'Many',
             'to_table': 'DimB', 'to_column': 'key', 'to_cardinality': 'One',
             'is_active': False},
        ]
        loader.load(rels, tables, {'fact'})
        inactive = loader.get_inactive_relationships()
        assert len(inactive) == 2


class TestUseRelationshipDAX:
    def test_pattern_matches(self):
        translator = DaxTranslator()
        result = translator.translate(
            {'measure_name': 'ShipSales', 'original_name': 'Ship Sales',
             'dax_expression': 'CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipDate], Calendar[Date]))'},
            'fact_test',
        )
        # Should be matched by the userelationship pattern, not quick-rejected
        assert result.is_translatable or 'USERELATIONSHIP' not in result.skip_reason

    def test_pattern_without_userelationship(self):
        translator = DaxTranslator()
        result = translator.translate(
            {'measure_name': 'Total', 'original_name': 'Total',
             'dax_expression': 'SUM(Sales[Amount])'},
            'fact_test',
        )
        assert result.is_translatable

    def test_match_returns_components(self):
        translator = DaxTranslator()
        match = translator._match_userelationship(
            'CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipDate], Calendar[Date]))',
            'test',
        )
        assert match is not None
        assert match['inner_expr'] == 'SUM(Sales[Amount])'
        assert match['fact_table'] == 'Sales'
        assert match['fact_col'] == 'ShipDate'
        assert match['dim_table'] == 'Calendar'
        assert match['dim_col'] == 'Date'

    def test_match_returns_none_without_userelationship(self):
        translator = DaxTranslator()
        match = translator._match_userelationship(
            'SUM(Sales[Amount])',
            'test',
        )
        assert match is None

    def test_translate_userelationship_simple_sum(self):
        translator = DaxTranslator()
        result = translator.translate(
            {'measure_name': 'ShipAmount', 'original_name': 'Ship Amount',
             'dax_expression': 'CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipDate], Calendar[Date]))'},
            'Sales',
        )
        # Inner expression SUM(Sales[Amount]) should be translatable
        assert result.is_translatable
        assert 'SUM(source.Amount)' in result.sql_expr


class TestReportEmitterInactiveRelationships:
    def test_inactive_relationships_in_report(self):
        from src.engines.crewai.tools.custom.metric_view_utils.report_emitter import emit_migration_report
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MetricViewSpec

        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.fact',
            view_name='mv_fact',
            comment='test',
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[],
        )
        limitations = {
            'inactive_relationships': [
                {'from_table': 'fact', 'from_column': 'ship_date',
                 'to_table': 'Calendar', 'to_column': 'date'},
            ],
        }
        report = emit_migration_report(
            {'fact': spec}, {'fact': {'total': 0, 'translated': 0, 'artifacts': 0,
                                      'untranslatable': 0, 'base': 0, 'dax': 0, 'switch': 0}},
            limitations=limitations,
        )
        assert '## Inactive Relationships (USERELATIONSHIP)' in report
        assert 'calendar_ship_date' in report
        assert 'ship_date' in report

    def test_no_section_without_inactive_rels(self):
        from src.engines.crewai.tools.custom.metric_view_utils.report_emitter import emit_migration_report
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MetricViewSpec

        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.fact',
            view_name='mv_fact',
            comment='test',
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[],
        )
        report = emit_migration_report(
            {'fact': spec}, {'fact': {'total': 0, 'translated': 0, 'artifacts': 0,
                                      'untranslatable': 0, 'base': 0, 'dax': 0, 'switch': 0}},
        )
        assert '## Inactive Relationships' not in report
