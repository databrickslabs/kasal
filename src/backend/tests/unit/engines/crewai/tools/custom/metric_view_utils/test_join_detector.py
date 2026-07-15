"""Tests for join detector."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.join_detector import (
    JoinDetector, _sanitize_alias,
)
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo


def _make_table(name, source, group_by, agg=None, is_fact=True, full_sql='',
                calculated_columns=None, static_filters=None):
    return TableInfo(
        table_name=name,
        source_table=source,
        aggregate_columns=agg or [{'name': 'val', 'source_col': 'val'}],
        group_by_columns=group_by,
        calculated_columns=calculated_columns or [],
        is_fact=is_fact,
        full_sql=full_sql,
        static_filters=static_filters or [],
    )


class TestSanitizeAlias:
    def test_valid_alias(self):
        assert _sanitize_alias("dim_geo") == "dim_geo"

    def test_valid_underscore_start(self):
        assert _sanitize_alias("_private") == "_private"

    def test_invalid_space(self):
        with pytest.raises(ValueError):
            _sanitize_alias("dim geo")

    def test_invalid_semicolon(self):
        with pytest.raises(ValueError):
            _sanitize_alias("dim;DROP")

    def test_invalid_dot(self):
        with pytest.raises(ValueError):
            _sanitize_alias("schema.table")

    def test_valid_alphanumeric(self):
        assert _sanitize_alias("dim123") == "dim123"


class TestJoinDetectorNoConfig:
    def test_no_joins_without_config(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['col'])}
        detector = JoinDetector(tables)
        measures = [{'dax_expression': 'SUM(fact[val])'}]
        joins = detector.detect('fact', measures, tables['fact'])
        assert joins == []

    def test_empty_measures(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['col'])}
        detector = JoinDetector(tables)
        joins = detector.detect('fact', [], tables['fact'])
        assert joins == []


class TestJoinDetectorDimJoins:
    def test_dim_join_detected(self):
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'Dim_X': _make_table('Dim_X', 'cat.sch.dim_x', ['key', 'label'],
                                 agg=[], is_fact=False),
        }
        config = {
            'join_key_map': {
                'Dim_X': {
                    'alias': 'dim_x',
                    'join_key': 'key',
                    'dim_columns': ['label'],
                },
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': 'SUM(fact[val]) + Dim_X[label]'}]
        joins = detector.detect('fact', measures, tables['fact'])
        assert len(joins) == 1
        assert joins[0]['name'] == 'dim_x'
        assert 'source.key = dim_x.key' in joins[0]['join_on']

    def test_dim_join_with_alt_key(self):
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['plant', 'wkctr']),
            'Dim_Y': _make_table('Dim_Y', 'cat.sch.dim_y', ['composite_key', 'name'],
                                 agg=[], is_fact=False),
        }
        config = {
            'join_key_map': {
                'Dim_Y': {
                    'alias': 'dim_y',
                    'join_key': 'composite_key',
                    'alt_join_keys': ['plant', 'wkctr'],
                    'alt_join_on': 'source.plant = dim_y.plant AND source.wkctr = dim_y.wkctr',
                    'dim_columns': ['name'],
                },
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': 'Dim_Y[name]'}]
        joins = detector.detect('fact', measures, tables['fact'])
        assert len(joins) == 1
        assert 'source.plant = dim_y.plant' in joins[0]['join_on']

    def test_dim_join_missing_key_skipped(self):
        """If the join key is not in group_by_columns, the join is skipped."""
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['other_col']),
            'Dim_Z': _make_table('Dim_Z', 'cat.sch.dim_z', ['key', 'label'],
                                 agg=[], is_fact=False),
        }
        config = {
            'join_key_map': {
                'Dim_Z': {
                    'alias': 'dim_z',
                    'join_key': 'key',
                    'dim_columns': ['label'],
                },
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': 'Dim_Z[label]'}]
        joins = detector.detect('fact', measures, tables['fact'])
        assert joins == []

    def test_dim_table_no_source_skipped(self):
        """If the dim table has no source_table, it is skipped."""
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'Dim_A': _make_table('Dim_A', '', ['key', 'label'],
                                 agg=[], is_fact=False),
        }
        config = {
            'join_key_map': {
                'Dim_A': {
                    'alias': 'dim_a',
                    'join_key': 'key',
                    'dim_columns': ['label'],
                },
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': 'Dim_A[label]'}]
        joins = detector.detect('fact', measures, tables['fact'])
        assert joins == []

    def test_inner_join_type(self):
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'Dim_B': _make_table('Dim_B', 'cat.sch.dim_b', ['key', 'label'],
                                 agg=[], is_fact=False),
        }
        config = {
            'join_key_map': {
                'Dim_B': {
                    'alias': 'dim_b',
                    'join_key': 'key',
                    'dim_columns': ['label'],
                },
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': 'Dim_B[label]'}]
        joins = detector.detect('fact', measures, tables['fact'], inner_dim_joins=True)
        assert len(joins) == 1
        assert joins[0]['join_type'] == 'inner'


class TestJoinDetectorGetDimDimensions:
    def test_get_dim_dimensions_string_cols(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['key'])}
        config = {
            'join_key_map': {
                'Dim_X': {
                    'alias': 'dim_x',
                    'join_key': 'key',
                    'dim_columns': ['label', 'name'],
                },
            }
        }
        detector = JoinDetector(tables, config)
        joins = [{'name': 'dim_x', 'source': 'cat.sch.dim_x',
                  'join_on': 'source.key = dim_x.key'}]
        dims = detector.get_dim_dimensions(joins, tables['fact'])
        assert len(dims) == 2
        assert dims[0]['name'] == 'label'
        assert dims[0]['expr'] == 'dim_x.label'
        assert dims[1]['name'] == 'name'

    def test_get_dim_dimensions_dict_cols(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['key'])}
        config = {
            'join_key_map': {
                'Dim_X': {
                    'alias': 'dim_x',
                    'join_key': 'key',
                    'dim_columns': [
                        {'name': 'full_name', 'expr': "CONCAT(first, ' ', last)"},
                    ],
                },
            }
        }
        detector = JoinDetector(tables, config)
        joins = [{'name': 'dim_x', 'source': 'cat.sch.dim_x',
                  'join_on': 'source.key = dim_x.key'}]
        dims = detector.get_dim_dimensions(joins, tables['fact'])
        assert len(dims) == 1
        assert dims[0]['name'] == 'full_name'
        assert "dim_x.CONCAT" in dims[0]['expr']

    def test_get_dim_dimensions_no_match(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['key'])}
        config = {'join_key_map': {}}
        detector = JoinDetector(tables, config)
        joins = [{'name': 'dim_x', 'source': 'cat.sch.dim_x',
                  'join_on': 'source.key = dim_x.key'}]
        dims = detector.get_dim_dimensions(joins, tables['fact'])
        assert dims == []

    def test_get_dim_dimensions_empty_joins(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['key'])}
        detector = JoinDetector(tables)
        dims = detector.get_dim_dimensions([], tables['fact'])
        assert dims == []


class TestJoinDetectorFactJoins:
    def test_no_fact_joins_without_config(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['key'])}
        detector = JoinDetector(tables)
        measures = [{'dax_expression': 'SUM(fact[val])', 'direct_fact_refs': []}]
        joins = detector.detect_fact_joins('fact', measures, tables['fact'])
        assert joins == []

    def test_fact_join_with_join_key(self):
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'other_fact': _make_table('other_fact', 'cat.sch.other', ['key', 'val']),
        }
        config = {
            'fact_join_map': {
                'other_fact': {
                    'alias': 'of',
                    'join_key': 'key',
                },
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': '', 'direct_fact_refs': ['other_fact']}]
        joins = detector.detect_fact_joins('fact', measures, tables['fact'])
        assert len(joins) == 1
        assert joins[0]['name'] == 'of'
        assert 'source.key = of.key' in joins[0]['join_on']

    def test_fact_join_self_ref_ignored(self):
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['key'])}
        config = {
            'fact_join_map': {
                'fact': {'alias': 'f2', 'join_key': 'key'},
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': '', 'direct_fact_refs': ['fact']}]
        joins = detector.detect_fact_joins('fact', measures, tables['fact'])
        assert joins == []

    def test_fact_join_missing_key_skipped(self):
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['other_col']),
            'kbi': _make_table('kbi', 'cat.sch.kbi', ['key', 'val']),
        }
        config = {
            'fact_join_map': {
                'kbi': {'alias': 'kbi_j', 'join_key': 'key'},
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': '', 'direct_fact_refs': ['kbi']}]
        joins = detector.detect_fact_joins('fact', measures, tables['fact'])
        assert joins == []

    def test_fact_join_composite_keys(self):
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k1', 'k2']),
            'other': _make_table('other', 'cat.sch.other', ['k1', 'k2', 'val']),
        }
        config = {
            'fact_join_map': {
                'other': {'alias': 'oth', 'join_key': ['k1', 'k2']},
            }
        }
        detector = JoinDetector(tables, config)
        measures = [{'dax_expression': '', 'direct_fact_refs': ['other']}]
        joins = detector.detect_fact_joins('fact', measures, tables['fact'])
        assert len(joins) == 1
        assert 'source.k1 = oth.k1' in joins[0]['join_on']
        assert 'source.k2 = oth.k2' in joins[0]['join_on']


class TestP4RichJoinSql:
    """P4: config-driven QUALIFY dedup subqueries + zero-pad key alignment."""

    def _detector(self, join_key_map, extra_cfg=None):
        cfg = {'join_key_map': join_key_map}
        cfg.update(extra_cfg or {})
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['comp_code'])}
        return JoinDetector(tables, cfg), tables

    def test_pad_key_generates_lpad(self):
        jkm = {
            'Dim_Geo': {
                'alias': 'dim_geo', 'join_key': 'comp_code', 'dim_key': 'comp_code',
                'source_table': 'cat.sch.dim_geo',
                'pad_key': {'len': 4, 'char': '0'},
            }
        }
        det, tables = self._detector(jkm)
        joins = det.detect('fact', [{'dax_expression': 'SUM(Dim_Geo[x])'}], tables['fact'])
        assert len(joins) == 1
        assert joins[0]['join_on'] == "LPAD(source.comp_code, 4, '0') = dim_geo.comp_code"

    def test_no_pad_when_not_configured(self):
        jkm = {
            'Dim_Geo': {
                'alias': 'dim_geo', 'join_key': 'comp_code', 'dim_key': 'comp_code',
                'source_table': 'cat.sch.dim_geo',
            }
        }
        det, tables = self._detector(jkm)
        joins = det.detect('fact', [{'dax_expression': 'SUM(Dim_Geo[x])'}], tables['fact'])
        assert joins[0]['join_on'] == "source.comp_code = dim_geo.comp_code"
        assert 'LPAD' not in joins[0]['join_on']

    def test_dedup_dim_wraps_source_in_qualify_subquery(self):
        jkm = {
            'Dim_Geo': {
                'alias': 'dim_geo', 'join_key': 'comp_code', 'dim_key': 'comp_code',
                'source_table': 'cat.sch.dim_geo',
                'dedup_dim': True,
            }
        }
        det, tables = self._detector(jkm)
        joins = det.detect('fact', [{'dax_expression': 'SUM(Dim_Geo[x])'}], tables['fact'])
        src = joins[0]['source']
        assert src.startswith('(SELECT * FROM cat.sch.dim_geo')
        assert 'QUALIFY ROW_NUMBER() OVER (PARTITION BY comp_code ORDER BY comp_code) = 1' in src

    def test_global_dedup_flag_applies_to_all(self):
        jkm = {
            'Dim_Geo': {
                'alias': 'dim_geo', 'join_key': 'comp_code', 'dim_key': 'comp_code',
                'source_table': 'cat.sch.dim_geo',
            }
        }
        det, tables = self._detector(jkm, {'dedup_dim_joins': True})
        joins = det.detect('fact', [{'dax_expression': 'SUM(Dim_Geo[x])'}], tables['fact'])
        assert 'QUALIFY ROW_NUMBER()' in joins[0]['source']

    def test_no_dedup_by_default(self):
        jkm = {
            'Dim_Geo': {
                'alias': 'dim_geo', 'join_key': 'comp_code', 'dim_key': 'comp_code',
                'source_table': 'cat.sch.dim_geo',
            }
        }
        det, tables = self._detector(jkm)
        joins = det.detect('fact', [{'dax_expression': 'SUM(Dim_Geo[x])'}], tables['fact'])
        assert joins[0]['source'] == 'cat.sch.dim_geo'
