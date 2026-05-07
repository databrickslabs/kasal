"""Tests for relationships loader."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
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


class TestRelationshipsLoaderBasic:
    def test_basic_many_to_one(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'dim': _make_table('dim', 'cat.sch.dim', ['key', 'label'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'key', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'key', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert 'fact' in result
        assert len(result['fact']) == 1
        assert result['fact'][0]['name'] == 'dim'
        assert 'source.key = dim.key' in result['fact'][0]['join_on']

    def test_one_to_many_reversed(self):
        """One:Many is reversed to Many:One for correct join direction."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['key']),
            'dim': _make_table('dim', 'cat.sch.dim', ['key', 'label'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'dim', 'from_column': 'key', 'from_cardinality': 'One',
             'to_table': 'fact', 'to_column': 'fk', 'to_cardinality': 'Many',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert 'fact' in result
        assert result['fact'][0]['join_on'] == 'source.fk = dim.key'


class TestRelationshipsLoaderFiltering:
    def test_skip_inactive(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': False}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result == {}

    def test_skip_many_to_many(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 's', ['k']),
            'dim': _make_table('dim', 'd', ['k'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'Many',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result == {}

    def test_skip_system_tables(self):
        """LocalDateTable and DateTableTemplate relationships are skipped."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['date_key']),
            'LocalDateTable_abc': _make_table('LocalDateTable_abc', 'cat.sch.dates',
                                               ['date_key'], agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'date_key',
             'from_cardinality': 'Many',
             'to_table': 'LocalDateTable_abc', 'to_column': 'date_key',
             'to_cardinality': 'One', 'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result == {}

    def test_skip_non_fact_table(self):
        """Only fact tables (in the fact_tables set) get enrichment joins."""
        loader = RelationshipsLoader()
        tables = {
            'dim_a': _make_table('dim_a', 'cat.sch.dim_a', ['k'],
                                 agg=[], is_fact=False),
            'dim_b': _make_table('dim_b', 'cat.sch.dim_b', ['k'],
                                 agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'dim_a', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim_b', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})  # dim_a not in fact_tables
        assert result == {}

    def test_skip_dim_without_source(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', '', ['k'], agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result == {}

    def test_skip_same_source_table(self):
        """If fact and dim have the same source table, the relationship is skipped."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.same', ['k']),
            'dim': _make_table('dim', 'cat.sch.same', ['k', 'label'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result == {}

    def test_skip_missing_columns(self):
        """Relationships with empty from_column or to_column are skipped."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': '', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result == {}


class TestRelationshipsLoaderAlias:
    def test_c_dim_prefix_stripped(self):
        """Alias strips leading 'c_' from 'c_dim_*' table names."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'c_dim_region': _make_table('c_dim_region', 'cat.sch.dim_region',
                                         ['k', 'name'], agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'c_dim_region', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert result['fact'][0]['name'] == 'dim_region'


class TestRelationshipsLoaderRawPbiFormat:
    def test_parse_raw_pbi_format(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k', 'label'],
                               agg=[], is_fact=False),
        }
        raw = {
            "results": [{"tables": [{"rows": [
                {"[ID]": 1, "[FromTable]": "fact", "[FromColumn]": "k",
                 "[FromCardinality]": "Many", "[ToTable]": "dim", "[ToColumn]": "k",
                 "[ToCardinality]": "One", "[IsActive]": True}
            ]}]}]
        }
        result = loader.load(raw, tables, {'fact'})
        assert 'fact' in result
        assert len(result['fact']) == 1
        assert result['fact'][0]['name'] == 'dim'

    def test_deduplicate_raw_pbi_rows(self):
        """Duplicate [ID] rows should be deduplicated."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k', 'label'],
                               agg=[], is_fact=False),
        }
        raw = {
            "results": [{"tables": [{"rows": [
                {"[ID]": 1, "[FromTable]": "fact", "[FromColumn]": "k",
                 "[FromCardinality]": "Many", "[ToTable]": "dim", "[ToColumn]": "k",
                 "[ToCardinality]": "One", "[IsActive]": True},
                {"[ID]": 1, "[FromTable]": "fact", "[FromColumn]": "k",
                 "[FromCardinality]": "Many", "[ToTable]": "dim", "[ToColumn]": "k",
                 "[ToCardinality]": "One", "[IsActive]": True},
            ]}]}]
        }
        result = loader.load(raw, tables, {'fact'})
        assert len(result['fact']) == 1

    def test_unknown_format_returns_empty(self):
        loader = RelationshipsLoader()
        tables = {'fact': _make_table('fact', 'cat.sch.fact', ['k'])}
        result = loader.load({"unknown": "format"}, tables, {'fact'})
        assert result == {}


class TestRelationshipsLoaderMultipleJoins:
    def test_multiple_dim_joins(self):
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['region_key', 'product_key']),
            'dim_region': _make_table('dim_region', 'cat.sch.dim_region',
                                       ['region_key', 'region_name'],
                                       agg=[], is_fact=False),
            'dim_product': _make_table('dim_product', 'cat.sch.dim_product',
                                        ['product_key', 'product_name'],
                                        agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'region_key',
             'from_cardinality': 'Many',
             'to_table': 'dim_region', 'to_column': 'region_key',
             'to_cardinality': 'One', 'is_active': True},
            {'from_table': 'fact', 'from_column': 'product_key',
             'from_cardinality': 'Many',
             'to_table': 'dim_product', 'to_column': 'product_key',
             'to_cardinality': 'One', 'is_active': True},
        ]
        result = loader.load(rels, tables, {'fact'})
        assert len(result['fact']) == 2
        aliases = {j['name'] for j in result['fact']}
        assert aliases == {'dim_region', 'dim_product'}

    def test_one_to_one_accepted(self):
        """One:One relationships are accepted (no reversal needed)."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k', 'label'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'One',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        assert 'fact' in result
        assert len(result['fact']) == 1

    def test_dim_columns_preserved(self):
        """The dim_columns list comes from the dim table's group_by_columns."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k', 'col_a', 'col_b'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'fact'})
        join_entry = result['fact'][0]
        assert 'k' in join_entry['dim_columns']
        assert 'col_a' in join_entry['dim_columns']
        assert 'col_b' in join_entry['dim_columns']
        assert join_entry['_auto'] is True


class TestM2NTracking:
    def test_m2n_tracked_not_silently_dropped(self):
        """M:N relationships are tracked via get_skipped_m2n(), not silently dropped."""
        loader = RelationshipsLoader()
        tables = {
            'A': _make_table('A', 's_a', ['k']),
            'B': _make_table('B', 's_b', ['k'], agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'A', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'B', 'to_column': 'k', 'to_cardinality': 'Many',
             'is_active': True}
        ]
        result = loader.load(rels, tables, {'A'})
        assert result == {}  # Still not included as a join
        m2n = loader.get_skipped_m2n()
        assert len(m2n) == 1
        assert m2n[0]['from_table'] == 'A'
        assert m2n[0]['to_table'] == 'B'
        assert m2n[0]['from_column'] == 'k'
        assert m2n[0]['to_column'] == 'k'

    def test_no_m2n_returns_empty(self):
        """When no M:N relationships exist, get_skipped_m2n() returns empty list."""
        loader = RelationshipsLoader()
        tables = {
            'fact': _make_table('fact', 'cat.sch.fact', ['k']),
            'dim': _make_table('dim', 'cat.sch.dim', ['k', 'label'],
                               agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'fact', 'from_column': 'k', 'from_cardinality': 'Many',
             'to_table': 'dim', 'to_column': 'k', 'to_cardinality': 'One',
             'is_active': True}
        ]
        loader.load(rels, tables, {'fact'})
        assert loader.get_skipped_m2n() == []

    def test_m2n_initial_state_empty(self):
        """Before load() is called, get_skipped_m2n() returns empty list."""
        loader = RelationshipsLoader()
        assert loader.get_skipped_m2n() == []

    def test_multiple_m2n_tracked(self):
        """Multiple M:N relationships are all tracked."""
        loader = RelationshipsLoader()
        tables = {
            'A': _make_table('A', 's_a', ['k1', 'k2']),
            'B': _make_table('B', 's_b', ['k1'], agg=[], is_fact=False),
            'C': _make_table('C', 's_c', ['k2'], agg=[], is_fact=False),
        }
        rels = [
            {'from_table': 'A', 'from_column': 'k1', 'from_cardinality': 'Many',
             'to_table': 'B', 'to_column': 'k1', 'to_cardinality': 'Many',
             'is_active': True},
            {'from_table': 'A', 'from_column': 'k2', 'from_cardinality': 'Many',
             'to_table': 'C', 'to_column': 'k2', 'to_cardinality': 'Many',
             'is_active': True},
        ]
        loader.load(rels, tables, {'A'})
        m2n = loader.get_skipped_m2n()
        assert len(m2n) == 2
        from_tables = {r['from_table'] for r in m2n}
        to_tables = {r['to_table'] for r in m2n}
        assert from_tables == {'A'}
        assert to_tables == {'B', 'C'}
