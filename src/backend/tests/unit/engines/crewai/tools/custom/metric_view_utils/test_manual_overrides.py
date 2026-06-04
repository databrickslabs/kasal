"""Tests for the manual_overrides pipeline config feature."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo


class TestManualOverrides:
    """Verify that manual_overrides in config inject measures correctly."""

    @staticmethod
    def _make_pipeline(measures, mquery_entries, config=None):
        parser = MQueryParser()
        tables = parser.parse_json(mquery_entries)
        return MetricViewPipeline(
            mapping=measures,
            mquery_tables=tables,
            config=config or {},
        )

    def test_manual_overrides_injected(self):
        """Manual overrides from config should appear as translated measures."""
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(val) AS val FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        config = {
            'manual_overrides': {
                'fact': [
                    {'name': 'custom_metric', 'expr': 'SUM(source.val) / NULLIF(COUNT(*), 0)', 'comment': 'Custom'},
                    {'name': 'with_window', 'expr': 'MEASURE(val)', 'comment': 'PY metric',
                     'window': {'order': 'key', 'range': 'trailing 12 month', 'semiadditive': 'last'}},
                ]
            }
        }
        pipeline = self._make_pipeline([], mquery, config)
        specs = pipeline.run()
        spec = specs['fact']
        names = {m.measure_name for m in spec.measures}
        assert 'custom_metric' in names
        assert 'with_window' in names
        # Check window spec
        windowed = [m for m in spec.measures if m.measure_name == 'with_window'][0]
        assert windowed.window_spec is not None
        assert windowed.window_spec['semiadditive'] == 'last'

    def test_manual_override_does_not_duplicate(self):
        """If a regex already translated the measure, the override should not duplicate it."""
        measures = [
            {'measure_name': 'Revenue', 'dax_expression': 'SUM(fact[revenue])',
             'original_name': 'Revenue', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(revenue) AS revenue FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        config = {
            'manual_overrides': {
                'fact': [
                    # 'revenue' is already a base measure
                    {'name': 'revenue', 'expr': 'SUM(source.revenue) * 2', 'comment': 'Should be skipped'},
                ]
            }
        }
        pipeline = self._make_pipeline(measures, mquery, config)
        specs = pipeline.run()
        spec = specs['fact']
        rev_measures = [m for m in spec.measures if m.measure_name == 'revenue']
        assert len(rev_measures) == 1  # Not duplicated
        assert 'SUM(source.revenue)' in rev_measures[0].sql_expr  # Original, not override

    def test_manual_override_unlocks_pass2(self):
        """Manual override should register in base_names so Pass 2 measure-refs can resolve."""
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(val) AS val FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        # override_a is a manual override, measure_b references it
        config = {
            'manual_overrides': {
                'fact': [
                    {'name': 'override_a', 'expr': 'SUM(source.val) * 100', 'comment': 'Manual'},
                ]
            }
        }
        mapping = [
            {'measure_name': 'Measure_B', 'proposed_allocation': 'fact',
             'dax_expression': '[override_a] - SUM(fact[val])',
             'original_name': 'Measure_B'},
        ]
        pipeline = self._make_pipeline(mapping, mquery, config)
        specs = pipeline.run()
        spec = specs['fact']
        names = {m.measure_name for m in spec.measures}
        # override_a injected + measure_b should resolve via Pass 2 since override_a is in base_names
        assert 'override_a' in names
        assert 'measure_b' in names

    def test_manual_override_category(self):
        """Manual overrides should have category='manual_override'."""
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(val) AS val FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        config = {
            'manual_overrides': {
                'fact': [
                    {'name': 'custom_m', 'expr': 'SUM(source.val)', 'comment': 'Test'},
                ]
            }
        }
        pipeline = self._make_pipeline([], mquery, config)
        specs = pipeline.run()
        spec = specs['fact']
        custom = [m for m in spec.measures if m.measure_name == 'custom_m']
        assert len(custom) == 1
        assert custom[0].category == 'manual_override'

    def test_manual_override_stats(self):
        """Stats should include manual_override count."""
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(val) AS val FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        config = {
            'manual_overrides': {
                'fact': [
                    {'name': 'custom_m', 'expr': 'SUM(source.val)', 'comment': 'Test'},
                ]
            }
        }
        pipeline = self._make_pipeline([], mquery, config)
        pipeline.run()
        assert 'manual_override' in pipeline.stats['fact']
        assert pipeline.stats['fact']['manual_override'] == 1

    def test_manual_override_removes_from_untranslatable(self):
        """If a measure was untranslatable, a manual override should rescue it."""
        measures = [
            {'measure_name': 'Hard_One', 'dax_expression': 'CALCULATE(SUM(fact[val]), USERELATIONSHIP(x, y))',
             'original_name': 'Hard_One', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(val) AS val FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        config = {
            'manual_overrides': {
                'fact': [
                    {'name': 'hard_one', 'expr': 'SUM(source.val) * 42', 'comment': 'Manually handled'},
                ]
            }
        }
        pipeline = self._make_pipeline(measures, mquery, config)
        specs = pipeline.run()
        spec = specs['fact']
        translated_names = {m.measure_name for m in spec.measures}
        untrans_names = {m.measure_name for m in spec.untranslatable}
        assert 'hard_one' in translated_names
        assert 'hard_one' not in untrans_names

    def test_empty_manual_overrides_no_effect(self):
        """Empty manual_overrides dict should not affect pipeline output."""
        measures = [
            {'measure_name': 'Sales', 'dax_expression': 'SUM(fact[val])',
             'original_name': 'Sales', 'proposed_allocation': 'fact'},
        ]
        mquery = [
            {'table_name': 'fact',
             'transpiled_sql': 'SELECT key, SUM(val) AS val FROM cat.sch.fact GROUP BY key',
             'validation_passed': 'Yes'},
        ]
        config_empty = {'manual_overrides': {}}
        config_none = {}
        p1 = self._make_pipeline(measures, mquery, config_empty)
        p2 = self._make_pipeline(measures, mquery, config_none)
        s1 = p1.run()
        s2 = p2.run()
        assert len(s1['fact'].measures) == len(s2['fact'].measures)
