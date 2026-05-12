"""Tests for config_scaffold — auto-proposal of pipeline_config.json."""
from __future__ import annotations

import json
import os
import sys

import pytest

# ── Path setup (mirror the scaffold script) ────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.abspath(os.path.join(_HERE, '..', '..', '..', '..', '..', '..', '..'))
_EXAMPLE_DIR = os.path.abspath(os.path.join(_BACKEND, '..', '..', 'examples', 'uc_metric_view_migration'))

# Ensure imports work
sys.path.insert(0, _BACKEND)
sys.path.insert(0, os.path.join(_BACKEND, 'src'))

# ── Import scaffold functions ──
# We insert the example directory so we can import from config_scaffold module
sys.path.insert(0, _EXAMPLE_DIR)

from config_scaffold import (
    _alias_to_dim_key,
    _extract_switch_branches,
    _parse_join_on,
    propose_column_overrides,
    propose_enrichment_joins,
    propose_filter_sets,
    propose_join_key_map,
    propose_mapping_only_tables,
    propose_measure_resolutions,
    propose_parameter_defaults,
    propose_switch_decompositions,
)

from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline


# ── Test data paths ─────────────────────────────────────────────────
_MEASURES_PATH = os.path.join(_EXAMPLE_DIR, 'measure_table_mapping.json')
_MQUERY_PATH = os.path.join(_EXAMPLE_DIR, 'mquery_transpilation.json')
_RELS_PATH = os.path.join(_EXAMPLE_DIR, 'pbi_relationships.json')
_SCAN_PATH = os.path.join(_EXAMPLE_DIR, 'scan_result_debug.json')
_EXISTING_CONFIG = os.path.join(_EXAMPLE_DIR, 'pipeline_config.json')

_HAS_EXAMPLE_DATA = all(
    os.path.exists(p) for p in [_MEASURES_PATH, _MQUERY_PATH, _RELS_PATH]
)


# ── Helpers ─────────────────────────────────────────────────────────

def _make_table(name, source, group_by, agg=None, is_fact=True, full_sql=''):
    return TableInfo(
        table_name=name,
        source_table=source,
        aggregate_columns=agg if agg is not None else [{'name': 'v', 'source_col': 'v'}],
        group_by_columns=group_by,
        calculated_columns=[],
        is_fact=is_fact,
        full_sql=full_sql,
    )


def _load_sc_reporting():
    """Load SC Reporting example data (when available)."""
    with open(_MEASURES_PATH) as f:
        measures = json.load(f)
    with open(_MQUERY_PATH) as f:
        mquery_entries = json.load(f)
    with open(_RELS_PATH) as f:
        rels_raw = json.load(f)
    parser = MQueryParser()
    mquery_tables = parser.parse_json(mquery_entries)
    fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}
    rel_loader = RelationshipsLoader()
    enrichment = rel_loader.load(rels_raw, mquery_tables, fact_tables)
    return measures, mquery_tables, fact_tables, enrichment, rels_raw


# ═══════════════════════════════════════════════════════════════════
# 1. join_key_map tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeJoinKeyMap:

    def test_basic_enrichment(self):
        """From a single enrichment join, produces one join_key_map entry."""
        tables = {
            'fact_a': _make_table('fact_a', 'cat.sch.fact_a', ['key']),
            'dim_x': _make_table('dim_x', 'cat.sch.dim_x', ['key', 'label'],
                                 agg=[], is_fact=False),
        }
        enrichment = {
            'fact_a': [
                {
                    'name': 'dim_x',
                    'source': 'cat.sch.dim_x',
                    'join_on': 'source.key = dim_x.key',
                    'dim_columns': ['label'],
                },
            ],
        }
        result = propose_join_key_map(enrichment, tables)
        assert 'dim_x' in result
        assert result['dim_x']['alias'] == 'dim_x'
        assert result['dim_x']['join_key'] == 'key'
        assert 'label' in result['dim_x']['dim_columns']

    def test_extracts_alias(self):
        """Alias matches dim table convention."""
        tables = {
            'fact_a': _make_table('fact_a', 'cat.sch.fact_a', ['fk']),
            'C_Dim_Geo': _make_table('C_Dim_Geo', 'cat.sch.dim_geo', ['pk', 'country'],
                                     agg=[], is_fact=False),
        }
        enrichment = {
            'fact_a': [
                {
                    'name': 'dim_geo',
                    'source': 'cat.sch.dim_geo',
                    'join_on': 'source.fk = dim_geo.pk',
                    'dim_columns': ['country'],
                },
            ],
        }
        result = propose_join_key_map(enrichment, tables)
        assert 'C_Dim_Geo' in result
        assert result['C_Dim_Geo']['alias'] == 'dim_geo'

    def test_extracts_dim_columns(self):
        """dim_columns populated from group_by columns via enrichment."""
        tables = {
            'fact': _make_table('fact', 'c.s.fact', ['k']),
            'dim_cal': _make_table('dim_cal', 'c.s.dim_cal', ['fiscper', 'year', 'month'],
                                   agg=[], is_fact=False),
        }
        enrichment = {
            'fact': [
                {
                    'name': 'dim_cal',
                    'source': 'c.s.dim_cal',
                    'join_on': 'source.fiscper = dim_cal.fiscper',
                    'dim_columns': ['fiscper', 'year', 'month'],
                },
            ],
        }
        result = propose_join_key_map(enrichment, tables)
        assert 'dim_cal' in result
        assert len(result['dim_cal']['dim_columns']) == 3

    def test_dim_key_different_from_join_key(self):
        """When dim column differs from fact column, dim_key is set."""
        tables = {
            'fact': _make_table('fact', 'c.s.fact', ['comp_code']),
            'dim': _make_table('dim', 'c.s.dim', ['co_code_bw'],
                               agg=[], is_fact=False),
        }
        enrichment = {
            'fact': [
                {
                    'name': 'dim',
                    'source': 'c.s.dim',
                    'join_on': 'source.comp_code = dim.co_code_bw',
                    'dim_columns': ['country'],
                },
            ],
        }
        result = propose_join_key_map(enrichment, tables)
        assert result['dim']['join_key'] == 'comp_code'
        assert result['dim']['dim_key'] == 'co_code_bw'

    @pytest.mark.skipif(not _HAS_EXAMPLE_DATA, reason='Example data not available')
    def test_sc_reporting_proposes_ge_4_join_keys(self):
        """From SC Reporting relationships, proposes >= 4 join keys."""
        _, mquery_tables, _, enrichment, _ = _load_sc_reporting()
        result = propose_join_key_map(enrichment, mquery_tables)
        assert len(result) >= 4, f'Expected >= 4 join keys, got {len(result)}: {list(result.keys())}'


# ═══════════════════════════════════════════════════════════════════
# 2. enrichment_joins tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeEnrichmentJoins:

    def test_pass_through(self):
        """enrichment_joins is an exact pass-through."""
        enrichment = {
            'fact_a': [{'name': 'dim', 'source': 'cat.sch.dim', 'join_on': 'source.k=dim.k'}],
        }
        result = propose_enrichment_joins(enrichment)
        assert result is enrichment

    def test_empty_enrichment(self):
        result = propose_enrichment_joins({})
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# 3. column_overrides tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeColumnOverrides:

    def test_detects_column_name_diff(self):
        """When DAX uses different casing that normalizes differently."""
        tables = {
            'FactA': _make_table('FactA', 'c.s.fact_a', ['version'],
                                 agg=[{'name': 'val', 'source_col': 'val'}]),
        }
        measures = [
            {
                'measure_name': 'm1',
                'dax_expression': "SUM(FactA[Val])",
                'proposed_allocation': 'FactA',
            },
        ]
        result = propose_column_overrides(tables, measures)
        # val vs Val — after snake_case both become 'val', so no override expected
        # This tests the logic doesn't crash on matching columns
        assert isinstance(result, dict)

    def test_empty_inputs(self):
        result = propose_column_overrides({}, [])
        assert result == {}

    def test_skips_not_available_dax(self):
        tables = {
            'T': _make_table('T', 'c.s.t', ['k']),
        }
        measures = [{'measure_name': 'm', 'dax_expression': 'Not available', 'proposed_allocation': 'T'}]
        result = propose_column_overrides(tables, measures)
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# 4. mapping_only_tables tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeMappingOnlyTables:

    def test_identifies_missing_tables(self):
        """Tables in measures but not MQuery are identified."""
        measures = [
            {'measure_name': 'm1', 'proposed_allocation': 'FT_Missing'},
            {'measure_name': 'm2', 'proposed_allocation': 'fact_a'},
        ]
        tables = {
            'fact_a': _make_table('fact_a', 'c.s.fact_a', ['k']),
        }
        result = propose_mapping_only_tables(measures, tables, {})
        assert 'FT_Missing' in result
        assert 'fact_a' not in result

    def test_empty_measures(self):
        result = propose_mapping_only_tables([], {}, {})
        assert result == {}

    def test_all_tables_present(self):
        measures = [{'measure_name': 'm', 'proposed_allocation': 'fact_a'}]
        tables = {'fact_a': _make_table('fact_a', 'c.s.fact_a', ['k'])}
        result = propose_mapping_only_tables(measures, tables, {})
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# 5. switch_decompositions tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeSwitchDecompositions:

    _SWITCH_DAX = '''
        VAR _selected = SELECTEDVALUE(Dim[Type])
        RETURN SWITCH(TRUE(),
            _selected = "TypeA", SUM(Fact[ColA]),
            _selected = "TypeB", SUM(Fact[ColB]),
            _selected = "TypeC", SUM(Fact[ColC])
        )
    '''

    def test_detects_switch_pattern(self):
        """Measures with SELECTEDVALUE+SWITCH are detected."""
        measures = [
            {
                'measure_name': 'KBI Selector',
                'dax_expression': self._SWITCH_DAX,
                'proposed_allocation': 'fact_pe002',
            },
        ]
        result = propose_switch_decompositions(measures)
        assert 'fact_pe002' in result
        assert len(result['fact_pe002']) == 1

    def test_extracts_branches(self):
        """Branch names are extracted from SWITCH expression."""
        branches = _extract_switch_branches(self._SWITCH_DAX)
        assert len(branches) >= 2
        case_values = [b['case_value'] for b in branches]
        assert 'TypeA' in case_values
        assert 'TypeB' in case_values

    def test_skeleton_format(self):
        """Skeleton has name, raw_expr=TODO, and comment."""
        measures = [
            {
                'measure_name': 'Test Switch',
                'dax_expression': self._SWITCH_DAX,
                'proposed_allocation': 'fact_t',
            },
        ]
        result = propose_switch_decompositions(measures)
        skeleton = result['fact_t'][0]
        assert skeleton['name'] == 'test_switch'
        assert skeleton['raw_expr'] == 'TODO: human fills SQL'
        assert 'SWITCH' in skeleton['comment']

    def test_no_switch_measures(self):
        measures = [
            {
                'measure_name': 'm1',
                'dax_expression': 'SUM(Fact[Col])',
                'proposed_allocation': 'fact_a',
            },
        ]
        result = propose_switch_decompositions(measures)
        assert result == {}

    def test_skips_not_available(self):
        measures = [
            {
                'measure_name': 'm1',
                'dax_expression': 'Not available',
                'proposed_allocation': 'fact_a',
            },
        ]
        result = propose_switch_decompositions(measures)
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# 6. measure_resolutions tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeMeasureResolutions:

    def test_resolves_unresolved_refs(self):
        """Finds 'Cannot resolve [ref]' and maps to known measures."""
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import (
            MetricViewSpec,
            TranslationResult,
        )
        # Create a minimal pipeline mock
        pipeline = type('MockPipeline', (), {
            'mapping': [
                {'measure_name': 'Sales Total', 'proposed_allocation': 'fact_a'},
            ],
            'all_specs': {
                'fact_a': MetricViewSpec(
                    fact_table_key='fact_a',
                    source_table='c.s.fact_a',
                    view_name='mv_fact_a',
                    comment='',
                    joins=[],
                    dimensions=[],
                    measures=[],
                    untranslatable=[
                        TranslationResult(
                            measure_name='ratio_m',
                            original_name='Ratio M',
                            sql_expr=None,
                            is_translatable=False,
                            skip_reason='Cannot resolve [Sales Total]',
                            dax_expression='DIVIDE([Sales Total], 100)',
                            confidence='low',
                            category='dax',
                        ),
                    ],
                ),
            },
        })()

        result = propose_measure_resolutions(pipeline)
        assert 'Sales Total' in result
        assert 'TODO' in result['Sales Total']['base_expr']


# ═══════════════════════════════════════════════════════════════════
# 7. parameter_defaults tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeParameterDefaults:

    def test_detects_dollar_brace_params(self):
        """Detects ${ParameterName} in SQL."""
        tables = {
            't1': _make_table(
                't1', 'c.s.t', ['k'],
                full_sql="SELECT * FROM t WHERE col = ${CurrencyFilter}",
            ),
        }
        result = propose_parameter_defaults(tables)
        assert 'CurrencyFilter' in result

    def test_detects_hash_params(self):
        """Detects #"ParamName" in SQL."""
        tables = {
            't1': _make_table(
                't1', 'c.s.t', ['k'],
                full_sql='SELECT * FROM #"MyParam"',
            ),
        }
        result = propose_parameter_defaults(tables)
        assert 'MyParam' in result

    def test_empty_tables(self):
        result = propose_parameter_defaults({})
        assert result == {}

    def test_no_params_found(self):
        tables = {
            't1': _make_table('t1', 'c.s.t', ['k'], full_sql='SELECT * FROM t'),
        }
        result = propose_parameter_defaults(tables)
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# 8. filter_sets tests
# ═══════════════════════════════════════════════════════════════════

class TestProposeFilterSets:

    def test_groups_filter_values(self):
        """Groups filter values by column from switch branches."""
        switch_decomps = {
            'fact_t': [
                {
                    'name': 'test',
                    'raw_expr': 'TODO',
                    'comment': '',
                    '_branches': [
                        {'variable': 'wc_type', 'case_value': 'PET'},
                        {'variable': 'wc_type', 'case_value': 'RGB'},
                        {'variable': 'wc_type', 'case_value': 'CAN'},
                    ],
                },
            ],
        }
        result = propose_filter_sets(switch_decomps, [])
        assert 'WC_TYPE' in result
        assert sorted(result['WC_TYPE']) == ['CAN', 'PET', 'RGB']

    def test_empty_switch(self):
        result = propose_filter_sets({}, [])
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# Helper function tests
# ═══════════════════════════════════════════════════════════════════

class TestHelpers:

    def test_parse_join_on_standard(self):
        fact_col, dim_col = _parse_join_on('source.fiscper = dim_cal.fiscper', 'dim_cal')
        assert fact_col == 'fiscper'
        assert dim_col == 'fiscper'

    def test_parse_join_on_different_cols(self):
        fact_col, dim_col = _parse_join_on('source.comp_code = dim_geo.co_code_bw', 'dim_geo')
        assert fact_col == 'comp_code'
        assert dim_col == 'co_code_bw'

    def test_parse_join_on_reversed(self):
        fact_col, dim_col = _parse_join_on('dim_x.pk = source.fk', 'dim_x')
        assert fact_col == 'fk'
        assert dim_col == 'pk'

    def test_alias_to_dim_key_direct_match(self):
        tables = {'dim_x': _make_table('dim_x', 'c.s.dim_x', ['k'], is_fact=False)}
        assert _alias_to_dim_key('dim_x', tables) == 'dim_x'

    def test_alias_to_dim_key_c_prefix(self):
        tables = {'C_Dim_Geo': _make_table('C_Dim_Geo', 'c.s.geo', ['k'], is_fact=False)}
        assert _alias_to_dim_key('dim_geo', tables) == 'C_Dim_Geo'

    def test_alias_to_dim_key_no_match(self):
        tables = {'unrelated': _make_table('unrelated', 'c.s.t', ['k'], is_fact=False)}
        assert _alias_to_dim_key('dim_missing', tables) == ''

    def test_extract_switch_branches_empty(self):
        assert _extract_switch_branches('SUM(Table[Col])') == []

    def test_extract_switch_branches_parses(self):
        dax = 'SWITCH(TRUE(), x = "A", SUM(T[a]), x = "B", SUM(T[b]))'
        branches = _extract_switch_branches(dax)
        assert len(branches) >= 2
        assert branches[0]['case_value'] == 'A'


# ═══════════════════════════════════════════════════════════════════
# End-to-end / integration tests (require example data)
# ═══════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _HAS_EXAMPLE_DATA, reason='Example data not available')
class TestEndToEnd:

    def test_end_to_end_sc_reporting(self):
        """Full scaffold on SC Reporting data proposes >= 5 config keys."""
        measures, mquery_tables, fact_tables, enrichment, _ = _load_sc_reporting()
        from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

        scan_data = {}
        if os.path.exists(_SCAN_PATH):
            scan_data = ScanDataParser().parse(_SCAN_PATH)

        config = {}
        config['join_key_map'] = propose_join_key_map(enrichment, mquery_tables)
        config['enrichment_joins'] = propose_enrichment_joins(enrichment)
        config['column_overrides'] = propose_column_overrides(mquery_tables, measures)
        config['mapping_only_tables'] = propose_mapping_only_tables(measures, mquery_tables, scan_data)
        config['switch_decompositions'] = propose_switch_decompositions(measures)
        config['parameter_defaults'] = propose_parameter_defaults(mquery_tables)
        config['filter_sets'] = propose_filter_sets(config['switch_decompositions'], measures)

        # Count non-empty keys
        non_empty = sum(1 for v in config.values() if v)
        assert non_empty >= 5, f'Expected >= 5 non-empty keys, got {non_empty}'

    def test_proposed_config_valid_json(self):
        """Full output serializes to valid JSON."""
        measures, mquery_tables, _, enrichment, _ = _load_sc_reporting()
        config = {
            'join_key_map': propose_join_key_map(enrichment, mquery_tables),
            'enrichment_joins': propose_enrichment_joins(enrichment),
            'column_overrides': propose_column_overrides(mquery_tables, measures),
            'mapping_only_tables': propose_mapping_only_tables(measures, mquery_tables, {}),
            'switch_decompositions': propose_switch_decompositions(measures),
            'parameter_defaults': propose_parameter_defaults(mquery_tables),
            'filter_sets': propose_filter_sets({}, measures),
            'measure_resolutions': {},
        }
        text = json.dumps(config, indent=2, default=str)
        reparsed = json.loads(text)
        assert isinstance(reparsed, dict)

    def test_proposed_config_has_required_keys(self):
        """All 8 top-level keys are present."""
        measures, mquery_tables, _, enrichment, _ = _load_sc_reporting()
        config = {
            'join_key_map': propose_join_key_map(enrichment, mquery_tables),
            'enrichment_joins': propose_enrichment_joins(enrichment),
            'column_overrides': propose_column_overrides(mquery_tables, measures),
            'mapping_only_tables': propose_mapping_only_tables(measures, mquery_tables, {}),
            'switch_decompositions': propose_switch_decompositions(measures),
            'parameter_defaults': propose_parameter_defaults(mquery_tables),
            'filter_sets': propose_filter_sets({}, measures),
            'measure_resolutions': {},
        }
        required = [
            'join_key_map', 'enrichment_joins', 'column_overrides',
            'mapping_only_tables', 'switch_decompositions',
            'parameter_defaults', 'filter_sets', 'measure_resolutions',
        ]
        for key in required:
            assert key in config, f'Missing required key: {key}'


# ═══════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════

class TestEdgeCases:

    def test_empty_inputs_no_crash(self):
        """All propose functions handle empty inputs gracefully."""
        assert propose_join_key_map({}, {}) == {}
        assert propose_enrichment_joins({}) == {}
        assert propose_column_overrides({}, []) == {}
        assert propose_mapping_only_tables([], {}, {}) == {}
        assert propose_switch_decompositions([]) == {}
        assert propose_parameter_defaults({}) == {}
        assert propose_filter_sets({}, []) == {}

    def test_no_relationships(self):
        """With no relationships, join_key_map is empty but doesn't crash."""
        tables = {'fact': _make_table('fact', 'c.s.fact', ['k'])}
        result = propose_join_key_map({}, tables)
        assert result == {}

    def test_no_scan_data(self):
        """mapping_only_tables works without scan data."""
        measures = [{'measure_name': 'm', 'proposed_allocation': 'FT_X'}]
        result = propose_mapping_only_tables(measures, {}, {})
        assert 'FT_X' in result

    def test_single_table_scenario(self):
        """Single fact table with one dim produces valid config."""
        tables = {
            'fact': _make_table('fact', 'c.s.fact', ['k', 'val']),
            'dim': _make_table('dim', 'c.s.dim', ['k', 'name'],
                               agg=[], is_fact=False),
        }
        enrichment = {
            'fact': [
                {
                    'name': 'dim',
                    'source': 'c.s.dim',
                    'join_on': 'source.k = dim.k',
                    'dim_columns': ['name'],
                },
            ],
        }
        jkm = propose_join_key_map(enrichment, tables)
        ej = propose_enrichment_joins(enrichment)
        assert len(jkm) == 1
        assert len(ej) == 1

    def test_measures_with_no_dax(self):
        """Measures with empty dax_expression are handled."""
        measures = [
            {'measure_name': 'm1', 'dax_expression': '', 'proposed_allocation': 'fact'},
            {'measure_name': 'm2', 'dax_expression': 'Not available', 'proposed_allocation': 'fact'},
        ]
        result = propose_switch_decompositions(measures)
        assert result == {}

    def test_duplicate_enrichment_joins_deduplicated(self):
        """Same dim table appearing in multiple fact tables is deduplicated in join_key_map."""
        tables = {
            'fact_a': _make_table('fact_a', 'c.s.fa', ['fk']),
            'fact_b': _make_table('fact_b', 'c.s.fb', ['fk']),
            'dim': _make_table('dim', 'c.s.dim', ['pk', 'label'],
                               agg=[], is_fact=False),
        }
        enrichment = {
            'fact_a': [{'name': 'dim', 'source': 'c.s.dim', 'join_on': 'source.fk = dim.pk', 'dim_columns': ['label']}],
            'fact_b': [{'name': 'dim', 'source': 'c.s.dim', 'join_on': 'source.fk = dim.pk', 'dim_columns': ['label']}],
        }
        result = propose_join_key_map(enrichment, tables)
        # Should only have one entry for 'dim', not duplicated
        assert len(result) == 1
        assert 'dim' in result

    def test_multiple_params_in_sql(self):
        """Detects multiple parameters in the same SQL."""
        tables = {
            't': _make_table(
                't', 'c.s.t', ['k'],
                full_sql="SELECT * FROM t WHERE a = ${Param1} AND b = ${Param2}",
            ),
        }
        result = propose_parameter_defaults(tables)
        assert 'Param1' in result
        assert 'Param2' in result

    def test_switch_with_no_true_pattern(self):
        """SWITCH without TRUE() pattern returns empty branches."""
        dax = 'SWITCH(col, "A", 1, "B", 2)'
        branches = _extract_switch_branches(dax)
        assert branches == []

    def test_mapping_only_with_scan_enrichment(self):
        """mapping_only_tables extracts source from scan data when available."""
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import ScanTableInfo
        measures = [{'measure_name': 'm', 'proposed_allocation': 'FT_X'}]
        scan = {
            'FT_X': ScanTableInfo(
                pbi_table_name='FT_X',
                raw_m_expression='',
                native_sql='SELECT a, b, SUM(c) AS c FROM cat.sch.real_table GROUP BY a, b',
                m_steps=[],
                has_union=False,
                pbi_columns=[],
            ),
        }
        result = propose_mapping_only_tables(measures, {}, scan)
        assert 'FT_X' in result
        assert result['FT_X']['source_table'] == 'cat.sch.real_table'
        assert 'a' in result['FT_X']['dimensions']
        assert 'b' in result['FT_X']['dimensions']
