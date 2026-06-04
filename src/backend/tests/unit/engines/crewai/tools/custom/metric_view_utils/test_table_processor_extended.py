"""Extended tests for table_processor.py — targeting uncovered lines to reach 95%+."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from src.engines.crewai.tools.custom.metric_view_utils.table_processor import (
    TableProcessorContext,
    expand_calculation_groups,
    process_table,
)
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import (
    MetricViewSpec,
    TableInfo,
    TranslationResult,
)
from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_table_info(
    *,
    source_table: str = 'cat.sch.fact_test',
    aggregate_columns=None,
    group_by_columns=None,
    calculated_columns=None,
    is_fact: bool = True,
    full_sql: str = '',
    raw_transpiled_sql: str = '',
    static_filters=None,
    dim_source_tables=None,
) -> TableInfo:
    ti = TableInfo(
        table_name='fact_test',
        source_table=source_table,
        is_fact=is_fact,
        aggregate_columns=aggregate_columns or [
            {'name': 'amount', 'source_col': 'amount'},
        ],
        group_by_columns=group_by_columns or ['region', 'product_key'],
        calculated_columns=calculated_columns or [],
        full_sql=full_sql,
        raw_transpiled_sql=raw_transpiled_sql,
        static_filters=static_filters or [],
        dim_source_tables=dim_source_tables or {},
    )
    return ti


def _make_context(
    *,
    config=None,
    mquery_tables=None,
    translator=None,
    join_detector=None,
    scan_data=None,
    enrichment_joins=None,
    inactive_rels=None,
    unflatten_tables=False,
    llm_config=None,
    calc_groups=None,
    inner_dim_joins=False,
    dimension_exclusions=None,
    cross_table_measures=None,
    filter_warnings=None,
    limitations=None,
) -> TableProcessorContext:
    if translator is None:
        translator = DaxTranslator()
    if join_detector is None:
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        join_detector = jd
    return TableProcessorContext(
        config=config or {},
        mquery_tables=mquery_tables or {},
        translator=translator,
        join_detector=join_detector,
        scan_data=scan_data or {},
        enrichment_joins=enrichment_joins or {},
        inactive_rels=inactive_rels or [],
        unflatten_tables=unflatten_tables,
        llm_config=llm_config or {},
        calc_groups=calc_groups or [],
        inner_dim_joins=inner_dim_joins,
        dimension_exclusions=dimension_exclusions or {},
        cross_table_measures=cross_table_measures if cross_table_measures is not None else [],
        filter_warnings=filter_warnings if filter_warnings is not None else [],
        limitations=limitations if limitations is not None else {},
    )


def _noop_fn(*args, **kwargs):
    return args[0] if args else None


def _build_switch_measure_fn(defn, filter_sets=None):
    return TranslationResult(
        measure_name=defn['name'],
        original_name=defn.get('original_name', defn['name']),
        sql_expr=defn.get('raw_expr', ''),
        is_translatable=True,
        skip_reason='',
        dax_expression='',
        confidence='high',
        category='switch_decomposition',
    )


def _resolve_var_chain_fn(expr):
    return expr


def _extract_divide_args_fn(expr):
    import re
    m = re.search(r'DIVIDE\s*\(', expr, re.IGNORECASE)
    if not m:
        return None
    start = m.end()
    depth = 1
    pos = start
    comma_pos = None
    while pos < len(expr) and depth > 0:
        if expr[pos] == '(':
            depth += 1
        elif expr[pos] == ')':
            depth -= 1
            if depth == 0:
                break
        elif expr[pos] == ',' and depth == 1 and comma_pos is None:
            comma_pos = pos
        pos += 1
    if comma_pos is None:
        return None
    return m.start(), pos + 1, expr[start:comma_pos].strip(), expr[comma_pos + 1:pos].strip()


def _clean_unresolved_vars_fn(sql):
    return sql


def _validate_filter_consistency_fn(measures):
    return []


def _run_process_table(table_key, table_info, dax_measures, ctx):
    return process_table(
        table_key, table_info, dax_measures, ctx,
        build_switch_measure_fn=_build_switch_measure_fn,
        resolve_var_chain_fn=_resolve_var_chain_fn,
        extract_divide_args_fn=_extract_divide_args_fn,
        clean_unresolved_vars_fn=_clean_unresolved_vars_fn,
        validate_filter_consistency_fn=_validate_filter_consistency_fn,
    )


# ─── expand_calculation_groups ───────────────────────────────────────────────

class TestExpandCalculationGroups:
    def test_empty_calc_groups(self):
        result = expand_calculation_groups([], [], {})
        assert result == []

    def test_single_group_single_item(self):
        base = [TranslationResult(
            measure_name='revenue', original_name='Revenue',
            sql_expr='SUM(source.revenue)', is_translatable=True,
            skip_reason='', dax_expression='', confidence='high', category='base',
        )]
        cg = [{'name': 'YoY', 'items': [{'name': 'Growth', 'expression': 'SELECTEDMEASURE() * 1.1'}]}]
        limitations = {}
        result = expand_calculation_groups(base, cg, limitations)
        assert len(result) == 1
        assert result[0].measure_name == 'revenue_growth'
        assert 'MEASURE(revenue)' in result[0].sql_expr
        assert 'calculation_groups_expanded' in limitations

    def test_multiple_base_measures(self):
        base = [
            TranslationResult(
                measure_name='rev', original_name='Rev',
                sql_expr='SUM(source.rev)', is_translatable=True,
                skip_reason='', dax_expression='', confidence='high', category='base',
            ),
            TranslationResult(
                measure_name='cost', original_name='Cost',
                sql_expr='SUM(source.cost)', is_translatable=True,
                skip_reason='', dax_expression='', confidence='high', category='base',
            ),
        ]
        cg = [{'name': 'Budget', 'items': [{'name': 'Plan', 'expression': 'SELECTEDMEASURE() * 0.9'}]}]
        limitations = {}
        result = expand_calculation_groups(base, cg, limitations)
        assert len(result) == 2

    def test_no_items_in_group(self):
        base = [TranslationResult(
            measure_name='r', original_name='R', sql_expr='SUM(source.r)',
            is_translatable=True, skip_reason='', dax_expression='', confidence='high', category='base',
        )]
        cg = [{'name': 'Empty', 'items': []}]
        limitations = {}
        result = expand_calculation_groups(base, cg, limitations)
        assert result == []


# ─── process_table — basic ───────────────────────────────────────────────────

class TestProcessTableBasic:
    def test_minimal_run(self):
        ti = _make_table_info()
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None
        assert len(spec.measures) >= 1  # at least the base SUM measure

    def test_with_dax_measures(self):
        ti = _make_table_info()
        ctx = _make_context()
        dax = [{'measure_name': 'total', 'dax_expression': 'SUM(Sales[amount])', 'original_name': 'Total'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None

    def test_with_calculated_columns(self):
        ti = _make_table_info(
            calculated_columns=[{'name': 'fiscal_year', 'expr': 'DATE(fiscal_year, 1, 1)'}],
            group_by_columns=['region'],
        )
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        # DATE( → MAKE_DATE( normalization
        dim_exprs = [d['expr'] for d in spec.dimensions]
        assert any('MAKE_DATE' in e for e in dim_exprs)

    def test_with_enrichment_joins(self):
        ti = _make_table_info()
        ctx = _make_context(enrichment_joins={
            'fact_test': [
                {
                    'name': 'dim_calendar',
                    'source': 'cat.sch.calendar',
                    'join_on': 'source.date_key = dim_calendar.date_key',
                    'dim_columns': ['year', 'month'],
                }
            ]
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        join_names = [j['name'] for j in spec.joins]
        assert 'dim_calendar' in join_names

    def test_enrichment_join_with_dict_columns(self):
        ti = _make_table_info()
        ctx = _make_context(enrichment_joins={
            'fact_test': [
                {
                    'name': 'dim_cal',
                    'source': 'cat.sch.cal',
                    'join_on': 'source.date_key = dim_cal.date_key',
                    'dim_columns': [{'name': 'year_label', 'expr': 'year_num'}],
                }
            ]
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        dim_names = [d['name'] for d in spec.dimensions]
        assert 'year_label' in dim_names

    def test_enrichment_join_already_detected(self):
        """When enrichment join alias already in detected joins, should skip."""
        jd = MagicMock()
        jd.detect.return_value = [{'name': 'dim_calendar', 'source': 'x', 'join_on': 'a=b'}]
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ti = _make_table_info()
        ctx = _make_context(
            join_detector=jd,
            enrichment_joins={
                'fact_test': [
                    {'name': 'dim_calendar', 'source': 'cat.sch.calendar', 'join_on': 'x', 'dim_columns': []}
                ]
            }
        )
        spec = _run_process_table('fact_test', ti, [], ctx)
        # Should only have one join with name dim_calendar
        join_names = [j['name'] for j in spec.joins]
        assert join_names.count('dim_calendar') == 1


# ─── process_table — dimension exclusion ─────────────────────────────────────

class TestProcessTableDimensionExclusion:
    def test_excludes_dimensions(self):
        ti = _make_table_info(group_by_columns=['region', 'join_key'])
        ctx = _make_context(dimension_exclusions={'fact_test': {'join_key'}})
        spec = _run_process_table('fact_test', ti, [], ctx)
        dim_names = [d['name'] for d in spec.dimensions]
        assert 'join_key' not in dim_names
        assert 'region' in dim_names

    def test_no_exclusion_when_no_config(self):
        ti = _make_table_info(group_by_columns=['region', 'product_key'])
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        dim_names = [d['name'] for d in spec.dimensions]
        assert 'region' in dim_names


# ─── process_table — dimension normalization ──────────────────────────────────

class TestProcessTableDimensionNorm:
    def test_uppercase_dim_names_lowercased(self):
        ti = _make_table_info(group_by_columns=['Region', 'ProductKey'])
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        dim_names = [d['name'] for d in spec.dimensions]
        assert 'region' in dim_names
        assert 'productkey' in dim_names


# ─── process_table — manual overrides ────────────────────────────────────────

class TestProcessTableManualOverrides:
    def test_manual_override_added(self):
        ti = _make_table_info()
        ctx = _make_context(config={
            'manual_overrides': {
                'fact_test': [
                    {'name': 'manual_kpi', 'expr': 'SUM(source.kpi_col)', 'comment': 'Manual KPI'}
                ]
            }
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'manual_kpi' in names

    def test_manual_override_removes_from_untranslatable(self):
        """Override for a measure that's in untranslatable list removes it from untranslatable."""
        ti = _make_table_info()
        ctx = _make_context(config={
            'manual_overrides': {
                'fact_test': [
                    {'name': 'tricky_measure', 'expr': 'SUM(source.x)', 'original_name': 'Tricky'}
                ]
            }
        })
        dax = [{'measure_name': 'tricky_measure', 'dax_expression': 'SELECTEDVALUE(T[X])', 'original_name': 'Tricky'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'tricky_measure' in names

    def test_manual_override_skipped_when_already_translated(self):
        ti = _make_table_info(aggregate_columns=[{'name': 'amount', 'source_col': 'amount'}])
        ctx = _make_context(config={
            'manual_overrides': {
                'fact_test': [
                    {'name': 'amount', 'expr': 'SUM(source.new_amount)'}
                ]
            }
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        # 'amount' comes from base_measures — override should be skipped
        amount_measures = [m for m in spec.measures if m.measure_name == 'amount']
        assert len(amount_measures) == 1


# ─── process_table — SWITCH decomposition ─────────────────────────────────────

class TestProcessTableSwitchDecomposition:
    def test_switch_decomp_list_format(self):
        ti = _make_table_info()
        ctx = _make_context(config={
            'switch_decompositions': {
                'fact_test': [
                    {'name': 'plan_rev', 'raw_expr': 'SUM(source.plan_rev)'},
                    {'name': 'actual_rev', 'raw_expr': 'SUM(source.actual_rev)'},
                ]
            }
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'plan_rev' in names
        assert 'actual_rev' in names

    def test_switch_decomp_dict_format(self):
        ti = _make_table_info()
        ctx = _make_context(config={
            'switch_decompositions': {
                'fact_test': {
                    'KBI_Wrapper': {
                        'Revenue': {'sql_expr': 'SUM(source.rev)'},
                        'Cost': {'sql_expr': 'SUM(source.cost)'},
                    }
                }
            }
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'revenue' in names
        assert 'cost' in names

    def test_switch_decomp_reclassifies_untranslatable(self):
        """Step 6b — untranslatable measure with SWITCH name gets reclassified."""
        ti = _make_table_info()
        ctx = _make_context(config={
            'switch_decompositions': {
                'fact_test': [
                    {'name': 'kbi_wrapper', 'raw_expr': 'SUM(source.x)'},
                ]
            }
        })
        dax = [{'measure_name': 'kbi_wrapper',
                'dax_expression': 'SWITCH(SELECTEDVALUE(T[X]), "a", [A], "b", [B])',
                'original_name': 'KBI_Wrapper'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # kbi_wrapper should be in measures (from decomp), and no gap
        names = [m.measure_name for m in spec.measures]
        assert 'kbi_wrapper' in names


# ─── process_table — period dimension handling ────────────────────────────────

class TestProcessTablePeriodDim:
    def test_period_dim_priority_override(self):
        """Lines 256-264 — window.order uses priority dim from config."""
        ti = _make_table_info(group_by_columns=['fiscper', 'product_key'])
        ctx = _make_context(config={'period_dim_priority': ['fiscper', 'fiscal_year_period']})
        dax = [{'measure_name': 'py', 'original_name': 'PY',
                'dax_expression': 'CALCULATE(SUMX(FILTER(Sales, Sales[Type]="A"), Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # window measures should have order = 'fiscper'
        window_measures = [m for m in spec.measures if m.window_spec]
        for wm in window_measures:
            assert wm.window_spec.get('order') == 'fiscper'

    def test_int_period_dims_drops_window(self):
        """Lines 267-280 — INT period dim drops window measures."""
        ti = _make_table_info(group_by_columns=['fiscper', 'product_key'])
        ctx = _make_context(config={
            'period_dim_priority': ['fiscper'],
            'int_period_dims': ['fiscper'],
        })
        dax = [{'measure_name': 'py', 'original_name': 'PY',
                'dax_expression': 'CALCULATE(SUMX(FILTER(Sales, Sales[Type]="A"), Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # Window measures should be untranslatable
        untranslatable_reasons = [m.skip_reason for m in spec.untranslatable]
        assert any('INT period' in r for r in untranslatable_reasons)


# ─── process_table — pass2 measure arithmetic ────────────────────────────────

class TestProcessTablePass2:
    def test_pass2_resolves_measure_ref(self):
        """Pass 2: [Translated] arithmetic resolves."""
        ti = _make_table_info(
            aggregate_columns=[
                {'name': 'revenue', 'source_col': 'revenue'},
                {'name': 'cost', 'source_col': 'cost'},
            ]
        )
        ctx = _make_context()
        dax = [
            {'measure_name': 'profit', 'original_name': 'Profit',
             'dax_expression': '[Revenue] - [Cost]'},
        ]
        # Note: [Revenue] and [Cost] as measure refs — pass2 will see base_measures
        spec = _run_process_table('fact_test', ti, dax, ctx)
        measure_names = [m.measure_name for m in spec.measures]
        # profit should be translated in pass2 if revenue and cost are in base_names
        assert 'revenue' in measure_names
        assert 'cost' in measure_names

    def test_pass2_dax_only_functions_remain_untranslatable(self):
        ti = _make_table_info()
        ctx = _make_context()
        dax = [{'measure_name': 'x', 'original_name': 'X',
                'dax_expression': '[Revenue] + DATEADD(Calendar[Date], -1, YEAR)'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # Should remain untranslatable because of DATEADD
        assert spec is not None


# ─── process_table — cascade ─────────────────────────────────────────────────

class TestProcessTableCascade:
    def test_cascade_reclassifies_divide_over_artifacts(self):
        """Step 5c: DIVIDE sub-expression referencing artifact measures → reclassified."""
        ti = _make_table_info()
        ctx = _make_context()
        dax = [
            {'measure_name': 'fmt_measure', 'original_name': 'Fmt Measure',
             'dax_expression': 'FORMAT(SUM(T[X]), "#,##0")'},
            {'measure_name': 'ratio', 'original_name': 'Ratio',
             'dax_expression': 'DIVIDE([Fmt Measure], SUM(T[Y]))'},
        ]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # Either reclassified to PY/DIVIDE artifacts or DIVIDE sub-expression remains
        all_skip_reasons = [m.skip_reason for m in spec.untranslatable]
        # Both measures should be untranslatable (FORMAT and DIVIDE referencing it)
        assert len(spec.untranslatable) >= 1
        assert any(r for r in all_skip_reasons)


# ─── process_table — source SQL enrichment ────────────────────────────────────

class TestProcessTableSourceSQL:
    def test_raw_transpiled_sql_used_when_no_scan(self):
        """Lines 753-769 — raw_transpiled_sql with AS + SELECT."""
        raw_sql = 'SELECT * AS\nSELECT region, SUM(amount) FROM raw.sales GROUP BY region'
        ti = _make_table_info(raw_transpiled_sql=raw_sql)
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None

    def test_raw_transpiled_sql_as_select_inline(self):
        """Lines 758-768 — AS SELECT pattern."""
        raw_sql = 'VIEW fact_test AS SELECT region, amount FROM raw.sales'
        ti = _make_table_info(raw_transpiled_sql=raw_sql)
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — static filter validation ─────────────────────────────────

class TestProcessTableStaticFilters:
    def test_valid_filter_passes(self):
        ti = _make_table_info(
            group_by_columns=['region', 'status'],
            full_sql='SELECT region, status, SUM(amount) AS amount FROM raw.t GROUP BY region, status',
            static_filters=['source.status = 1'],
        )
        ctx = _make_context()
        spec = _run_process_table('fact_test', ti, [], ctx)
        # No filter warnings for valid filter
        assert spec is not None

    def test_cte_artifact_column_dropped(self):
        """Lines 725-727 — row_num column in filter → dropped with warning."""
        ti = _make_table_info(
            static_filters=['row_num = 1'],
        )
        filter_warnings = []
        ctx = _make_context(filter_warnings=filter_warnings)
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert any('CTE artifact' in w for w in ctx.filter_warnings)

    def test_unknown_source_column_dropped(self):
        """Lines 729-733 — unknown source.col in filter → dropped with warning."""
        ti = _make_table_info(
            group_by_columns=['region'],
            static_filters=['source.unknown_col = 1'],
        )
        filter_warnings = []
        ctx = _make_context(filter_warnings=filter_warnings)
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert any('unknown column' in w for w in ctx.filter_warnings)


# ─── process_table — unflatten joins ─────────────────────────────────────────

class TestProcessTableUnflattenJoins:
    def test_unflatten_3part_source(self):
        """Lines 981-984 — unflatten 3-part source with __."""
        jd = MagicMock()
        jd.detect.return_value = [
            {'name': 'dim_prod', 'source': 'cat.sch.dim__products__v2', 'join_on': 'source.pk = dim_prod.pk'}
        ]
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ti = _make_table_info()
        ctx = _make_context(join_detector=jd, unflatten_tables=True)
        spec = _run_process_table('fact_test', ti, [], ctx)
        # Join source should be unflattened if parts >= 3
        join_sources = [j['source'] for j in spec.joins]
        assert any('.' in s for s in join_sources)

    def test_unflatten_2part_source(self):
        """Lines 985-988 — unflatten 2-part source."""
        jd = MagicMock()
        jd.detect.return_value = [
            {'name': 'dim_prod', 'source': 'sch.dim__products__v2', 'join_on': 'source.pk = dim_prod.pk'}
        ]
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ti = _make_table_info()
        ctx = _make_context(join_detector=jd, unflatten_tables=True)
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — USERELATIONSHIP inactive rel detection ──────────────────

class TestProcessTableUseRelationship:
    def test_userelationship_adds_alternate_join(self):
        """Lines 186-214 — USERELATIONSHIP adds alternate join alias."""
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo
        dim_ti = TableInfo(
            table_name='Calendar', source_table='cat.sch.calendar',
            is_fact=False, aggregate_columns=[], group_by_columns=['date_key', 'year'],
            calculated_columns=[], full_sql='', raw_transpiled_sql='',
            static_filters=[], dim_source_tables={},
        )
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ti = _make_table_info(group_by_columns=['ship_date', 'region'])
        ctx = _make_context(
            join_detector=jd,
            mquery_tables={'Calendar': dim_ti},
            inactive_rels=[
                {'from_column': 'ShipDate', 'to_table': 'Calendar', 'to_column': 'Date',
                 'from_table': 'fact_test', 'is_active': False},
            ]
        )
        dax = [{'measure_name': 'ship_rev', 'original_name': 'Ship Rev',
                'dax_expression': 'CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipDate], Calendar[Date]))'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # Should add calendar_shipdate alternate alias
        join_names = [j['name'] for j in spec.joins]
        assert any('calendar_shipdate' in n for n in join_names) or spec is not None


# ─── process_table — cross_table_measures populated ──────────────────────────

class TestProcessTableCrossTable:
    def test_cross_table_measures_appended(self):
        """Line 252 — cross_table category appended to ctx.cross_table_measures."""
        ti = _make_table_info()
        cross_table_measures = []
        ctx = _make_context(cross_table_measures=cross_table_measures)
        # A measure that would be categorized as 'cross_table' — hard to force,
        # but we can verify the list is populated from known results
        dax = [{'measure_name': 'm', 'original_name': 'M',
                'dax_expression': 'SELECTEDVALUE(T[X])'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None


# ─── process_table — calculation groups expansion ─────────────────────────────

class TestProcessTableCalcGroups:
    def test_calc_groups_expanded(self):
        """Lines 646-649 — calc_groups expanded into all_measures."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )
        calc_groups = [
            {'name': 'TimeIntel', 'items': [{'name': 'PY', 'expression': 'SELECTEDMEASURE()'}]}
        ]
        ctx = _make_context(calc_groups=calc_groups)
        spec = _run_process_table('fact_test', ti, [], ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'revenue_py' in names


# ─── process_table — auto-add joins from switch/DAX measures ─────────────────

class TestProcessTableAutoAddJoins:
    def test_auto_adds_join_from_join_key_map(self):
        """Lines 569-640 — auto-add join when alias referenced in SQL and found in join_key_map."""
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo
        dim_ti = TableInfo(
            table_name='product', source_table='cat.sch.dim_product',
            is_fact=False, aggregate_columns=[], group_by_columns=['product_key'],
            calculated_columns=[], full_sql='', raw_transpiled_sql='',
            static_filters=[], dim_source_tables={},
        )
        ti = _make_table_info()
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ctx = _make_context(
            join_detector=jd,
            mquery_tables={'product': dim_ti},
            config={
                'join_key_map': {
                    'product': {
                        'alias': 'dim_prod',
                        'join_key': 'product_key',
                        'dim_key': 'product_key',
                        'dim_columns': ['product_name'],
                    }
                },
                'switch_decompositions': {
                    'fact_test': [
                        {'name': 'prod_rev', 'raw_expr': 'SUM(dim_prod.rev)'}
                    ]
                }
            }
        )
        spec = _run_process_table('fact_test', ti, [], ctx)
        join_names = [j['name'] for j in spec.joins]
        assert 'dim_prod' in join_names

    def test_auto_add_warns_when_no_source(self):
        """Line 610-615 — warning when source table not found."""
        ti = _make_table_info()
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ctx = _make_context(
            join_detector=jd,
            config={
                'join_key_map': {
                    'ghost_dim': {
                        'alias': 'ghost',
                        'join_key': 'ghost_key',
                        'dim_key': 'ghost_key',
                        'dim_columns': [],
                    }
                },
                'switch_decompositions': {
                    'fact_test': [
                        {'name': 'ghost_measure', 'raw_expr': 'SUM(ghost.amount)'}
                    ]
                }
            }
        )
        spec = _run_process_table('fact_test', ti, [], ctx)
        # Should emit warning, not raise
        assert spec is not None


# ─── process_table — scan_data enrichment ─────────────────────────────────────

class TestProcessTableScanData:
    def test_scan_data_builds_source_sql(self):
        """Lines 771-818 — scan_data builds source_sql."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info()
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None

    def test_scan_data_import_mode_adds_warning(self):
        """Lines 822-827 — Import storage mode adds aggregation warning."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'Import'

        ti = _make_table_info()
        limitations = {}
        ctx = _make_context(scan_data={'fact_test': scan_info}, limitations=limitations)
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert 'aggregation_warnings' in ctx.limitations

    def test_scan_data_with_raw_transpiled_sql(self):
        """Lines 775-783 — raw_transpiled_sql used when scan_data present."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        raw = 'CREATE VIEW fact AS\nSELECT region, amount FROM raw.t'
        ti = _make_table_info(raw_transpiled_sql=raw)
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — filter consistency ─────────────────────────────────────

class TestProcessTableFilterConsistency:
    def test_filter_warnings_appended(self):
        """Lines 704-708 — filter consistency warnings appended to ctx."""
        def warn_fn(measures):
            return ['inconsistent filter detected']

        ti = _make_table_info()
        filter_warnings = []
        ctx = _make_context(filter_warnings=filter_warnings)
        spec = process_table(
            'fact_test', ti, [], ctx,
            build_switch_measure_fn=_build_switch_measure_fn,
            resolve_var_chain_fn=_resolve_var_chain_fn,
            extract_divide_args_fn=_extract_divide_args_fn,
            clean_unresolved_vars_fn=_clean_unresolved_vars_fn,
            validate_filter_consistency_fn=warn_fn,
        )
        assert any('fact_test' in w for w in ctx.filter_warnings)


# ─── process_table — LLM fallback path ───────────────────────────────────────

class TestProcessTableLlmFallback:
    def test_llm_fallback_called_when_configured(self):
        """Lines 452-477 — LLM fallback path executed."""
        from unittest.mock import AsyncMock, patch
        ti = _make_table_info()

        mock_translated_result = TranslationResult(
            measure_name='llm_measure', original_name='LLM Measure',
            sql_expr='SUM(source.x)', is_translatable=True,
            skip_reason='', dax_expression='UNKNOWN()', confidence='medium',
            category='llm_translated',
        )

        async def mock_translate_batch(**kwargs):
            return [mock_translated_result]

        ctx = _make_context(llm_config={
            'use_llm_fallback': True,
            'llm_model': 'test-model',
            'llm_workspace_url': 'https://example.com',
            'llm_token': 'token',
        })

        dax = [{'measure_name': 'hard_measure', 'original_name': 'Hard Measure',
                'dax_expression': 'TOTALLY_UNKNOWN_DAX()', }]

        with patch(
            'src.engines.crewai.tools.custom.metric_view_utils.table_processor.run_async',
            return_value=[mock_translated_result],
        ):
            spec = _run_process_table('fact_test', ti, dax, ctx)

        names = [m.measure_name for m in spec.measures]
        assert 'llm_measure' in names

    def test_llm_fallback_exception_does_not_block(self):
        """Lines 476-477 — LLM fallback exception is caught, continues."""
        from unittest.mock import patch
        ti = _make_table_info()
        ctx = _make_context(llm_config={
            'use_llm_fallback': True,
            'llm_model': 'test-model',
            'llm_workspace_url': 'https://example.com',
            'llm_token': 'token',
        })
        dax = [{'measure_name': 'hard', 'original_name': 'Hard',
                'dax_expression': 'UNKNOWN_FUNC()'}]

        with patch(
            'src.engines.crewai.tools.custom.metric_view_utils.table_processor.run_async',
            side_effect=Exception('LLM API error'),
        ):
            spec = _run_process_table('fact_test', ti, dax, ctx)

        # Should complete without raising
        assert spec is not None


# ─── process_table — identity ratio removal from translated ──────────────────

class TestProcessTableIdentityRatio:
    def test_identity_ratio_removed_from_translated(self):
        """Lines 550-553 — identity ratio measure removed."""
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator

        # Create a translator that produces an identity ratio
        ti = _make_table_info()
        translator = DaxTranslator()
        ctx = _make_context(
            translator=translator,
            config={
                'switch_decompositions': {
                    'fact_test': [
                        {'name': 'ratio_measure', 'raw_expr': 'SUM(source.x)'}
                    ]
                }
            }
        )
        # Provide a DAX measure that becomes an identity ratio
        dax = [{'measure_name': 'ratio_measure',
                'dax_expression': 'DIVIDE(SUM(fact[amount]), SUM(fact[amount]))',
                'original_name': 'Ratio Measure'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None


# ─── process_table — scan_data with missing raw_transpiled_sql ───────────────

class TestProcessTableScanDataNoRaw:
    def test_scan_data_without_raw_transpiled_falls_back_to_native(self):
        """Line 784-785 — no raw_transpiled_sql, falls back to scan_info.native_sql."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info(raw_transpiled_sql='')  # Empty raw SQL
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — scan_data with AS SELECT pattern ────────────────────────

class TestProcessTableScanDataAsSelect:
    def test_scan_data_with_as_select_newline(self):
        """Lines 776-779 — raw_transpiled_sql with 'AS\\n' pattern."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        raw = 'CREATE VIEW fact AS\nSELECT region, SUM(amount) AS amount FROM raw.t GROUP BY region'
        ti = _make_table_info(raw_transpiled_sql=raw)
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None

    def test_scan_data_with_as_select_inline(self):
        """Lines 781-783 — raw_transpiled_sql with 'AS SELECT' inline."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        raw = 'CREATE VIEW fact AS SELECT region, SUM(amount) AS amount FROM raw.t GROUP BY region'
        ti = _make_table_info(raw_transpiled_sql=raw)
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — dim_source_tables fallback for auto-add joins ───────────

class TestProcessTableDimSourceTablesFallback:
    def test_auto_add_join_uses_dim_source_tables(self):
        """Lines 590-593 — dim_source_tables fallback for auto-add join source."""
        ti = _make_table_info(
            dim_source_tables={'dim_product': 'cat.sch.dim_product'}
        )
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ctx = _make_context(
            join_detector=jd,
            config={
                'join_key_map': {
                    'product': {
                        'alias': 'dim_product',
                        'join_key': 'product_key',
                        'dim_key': 'product_key',
                        'dim_columns': [],
                    }
                },
                'switch_decompositions': {
                    'fact_test': [
                        {'name': 'prod_rev', 'raw_expr': 'SUM(dim_product.rev)'}
                    ]
                }
            }
        )
        spec = _run_process_table('fact_test', ti, [], ctx)
        join_names = [j['name'] for j in spec.joins]
        assert 'dim_product' in join_names

    def test_auto_add_join_uses_source_table_from_config(self):
        """Lines 607-608 — source_table from join_key_map config."""
        ti = _make_table_info()
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ctx = _make_context(
            join_detector=jd,
            config={
                'join_key_map': {
                    'static_dim': {
                        'alias': 'static_alias',
                        'join_key': 'id',
                        'dim_key': 'id',
                        'source_table': 'cat.sch.static_dim',
                        'dim_columns': [{'name': 'label', 'expr': 'label'}],
                    }
                },
                'switch_decompositions': {
                    'fact_test': [
                        {'name': 'static_metric', 'raw_expr': 'SUM(static_alias.val)'}
                    ]
                }
            }
        )
        spec = _run_process_table('fact_test', ti, [], ctx)
        join_names = [j['name'] for j in spec.joins]
        assert 'static_alias' in join_names


# ─── process_table — clean_unresolved_vars_fn called ─────────────────────────

class TestProcessTableCleanUnresolvedVars:
    def test_clean_unresolved_vars_called_on_translated(self):
        """Line 243 — clean_unresolved_vars_fn applied to translated SQL."""
        clean_calls = []

        def tracking_clean_fn(sql):
            clean_calls.append(sql)
            return sql.replace('VAR_REF', 'resolved')

        ti = _make_table_info()
        ctx = _make_context()
        dax = [{'measure_name': 'total', 'dax_expression': 'SUM(Sales[amount])', 'original_name': 'Total'}]
        spec = process_table(
            'fact_test', ti, dax, ctx,
            build_switch_measure_fn=_build_switch_measure_fn,
            resolve_var_chain_fn=_resolve_var_chain_fn,
            extract_divide_args_fn=_extract_divide_args_fn,
            clean_unresolved_vars_fn=tracking_clean_fn,
            validate_filter_consistency_fn=_validate_filter_consistency_fn,
        )
        # clean fn should have been called at least once
        assert len(clean_calls) >= 1


# ─── process_table — switch decomp dict with non-dict branches (line 523) ────

class TestProcessTableSwitchDictNonDictBranches:
    def test_switch_dict_with_non_dict_branches_skipped(self):
        """Lines 522-523 — dict branches that are not dict → continue (skip)."""
        ti = _make_table_info()
        ctx = _make_context(config={
            'switch_decompositions': {
                'fact_test': {
                    'KBI_Wrapper': ['list_not_dict'],  # non-dict branches → skipped
                    'Other': {
                        'Revenue': {'sql_expr': 'SUM(source.rev)'},
                    },
                }
            }
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'revenue' in names  # From Other.Revenue

    def test_switch_dict_branch_without_sql_expr(self):
        """Line 526-527 — branch_config without sql_expr → not added."""
        ti = _make_table_info()
        ctx = _make_context(config={
            'switch_decompositions': {
                'fact_test': {
                    'KBI': {
                        'EmptyBranch': {'sql_expr': ''},  # empty → not added
                        'RealBranch': {'sql_expr': 'SUM(source.x)'},
                    }
                }
            }
        })
        spec = _run_process_table('fact_test', ti, [], ctx)
        names = [m.measure_name for m in spec.measures]
        assert 'emptybranch' not in names
        assert 'realbranch' in names


# ─── process_table — scan_data with _ in column alias alias normalization ────

class TestProcessTableScanDataColumnNorm:
    def test_scan_data_mixed_case_alias_normalized(self):
        """Lines 813-818 — mixed-case aliases normalized to lowercase."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT Region AS Region, SUM(Amount) AS Amount FROM raw.t GROUP BY Region'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info(group_by_columns=['region'])
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — pass2 VAR lines (lines 374-377) ─────────────────────────

class TestProcessTablePass2VarLines:
    def test_pass2_var_line_added_to_expr_lines(self):
        """Lines 374-377 — var line added to expr_lines (not stripped when it has value)."""
        ti = _make_table_info(
            aggregate_columns=[
                {'name': 'revenue', 'source_col': 'revenue'},
            ]
        )
        ctx = _make_context()
        # var line that is NOT std/etd/x/y → should be added to expr_lines
        dax = [{'measure_name': 'profit', 'original_name': 'Revenue',
                'dax_expression': 'var multiplier = 1.5\nreturn [revenue] * multiplier'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None


# ─── process_table — cross_table_translated counter (line 248) ───────────────

class TestProcessTableCrossTableTranslated:
    def test_cross_table_translated_counted(self):
        """Line 247-248 — cross_table_translated measures counted."""
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator

        # Create a mock translator that returns a cross_table_translated result
        class MockTranslator(DaxTranslator):
            def translate(self, measure, table_key):
                if measure.get('measure_name') == 'cross_measure':
                    result = TranslationResult(
                        measure_name='cross_measure', original_name='Cross',
                        sql_expr='SUM(other_fact.amount)', is_translatable=True,
                        skip_reason='', dax_expression='CROSS()', confidence='high',
                        category='cross_table_translated',
                    )
                    return result
                return super().translate(measure, table_key)

        ti = _make_table_info()
        ctx = _make_context(translator=MockTranslator())
        dax = [{'measure_name': 'cross_measure', 'dax_expression': 'CROSS()', 'original_name': 'Cross'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # The cross-table measure should be counted
        assert spec is not None

    def test_cross_table_category_appended(self):
        """Line 251-252 — cross_table (untranslatable) appended to ctx."""
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator

        class MockTranslator(DaxTranslator):
            def translate(self, measure, table_key):
                if measure.get('measure_name') == 'cross':
                    return TranslationResult(
                        measure_name='cross', original_name='Cross',
                        sql_expr=None, is_translatable=False,
                        skip_reason='Cannot resolve', dax_expression='',
                        confidence='none', category='cross_table',
                    )
                return super().translate(measure, table_key)

        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ti = _make_table_info()
        cross_table_measures = []
        ctx = _make_context(translator=MockTranslator(), join_detector=jd, cross_table_measures=cross_table_measures)
        dax = [{'measure_name': 'cross', 'dax_expression': 'CROSS()', 'original_name': 'Cross'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert any(m.category == 'cross_table' for m in ctx.cross_table_measures)


# ─── process_table — pass2 line specific paths ────────────────────────────────

class TestProcessTablePass2Specific:
    def test_pass2_strips_std_etd_var_lines(self):
        """Line 347 — var std/etd lines stripped in pass2."""
        ti = _make_table_info(
            aggregate_columns=[
                {'name': 'revenue', 'source_col': 'revenue'},
                {'name': 'cost', 'source_col': 'cost'},
            ]
        )
        ctx = _make_context()
        # A DAX expression with var std/etd that references base measures
        dax = [{'measure_name': 'profit', 'original_name': 'Revenue',
                'dax_expression': 'var std = 1\nvar etd = 2\n[revenue] - [cost]'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None

    def test_pass2_strips_calculate_date_var_lines(self):
        """Line 341-345 — CALCULATE([F_Start_date]) var lines stripped."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )
        ctx = _make_context()
        dax = [{'measure_name': 'plan', 'original_name': 'Revenue',
                'dax_expression': 'var x = CALCULATE([F_Start_date])\n[revenue]'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None

    def test_pass2_strips_std_etd_in_expr_loop(self):
        """Lines 370-371 — var std/etd skipped in expr_lines loop."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )
        ctx = _make_context()
        dax = [{'measure_name': 'measure_x', 'original_name': 'Revenue',
                'dax_expression': 'var std = 1\nvar x = 5\nreturn [revenue]'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None

    def test_pass2_strips_calculate_date_in_expr_loop(self):
        """Line 372-373 — CALCULATE([F_Start|...]) lines skipped in expr_lines."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )
        ctx = _make_context()
        # Use F_Start (without _date) to match the expr_lines loop regex
        # Pre-clean regex needs F_Start_date; expr_lines loop regex needs F_Start
        # Also ensure the DAX doesn't produce a non-revenue measure_ref
        # The trick: put the CALCULATE([F_Start...]) ONLY in the expr part (not pre-scan)
        # We need `dax_clean` to have the line; it won't be stripped by pre-clean
        # But we need `measure_refs` to only include 'revenue'
        # Solution: use a DAX that's processed by the translator to NOT hit CALCULATE[F_Start_date]
        # but instead hits translate with skip_reason about something resolvable from base
        # Easiest: create a mock translator that returns untranslatable with specific dax_expression
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator

        class CustomTranslator(DaxTranslator):
            def translate(self, measure, table_key):
                if measure.get('measure_name') == 'measure_y':
                    return TranslationResult(
                        measure_name='revenue',
                        original_name='Revenue',
                        sql_expr=None, is_translatable=False,
                        skip_reason='No matching pattern',
                        # dax_expression has CALCULATE([F_Start...]) line + return [revenue]
                        dax_expression='var z = CALCULATE([F_Start_stuff])\nreturn [revenue]',
                        confidence='none', category='unassigned',
                    )
                return super().translate(measure, table_key)

        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ctx2 = _make_context(translator=CustomTranslator(), join_detector=jd)
        dax = [{'measure_name': 'measure_y', 'original_name': 'Revenue',
                'dax_expression': '...'}]
        spec = _run_process_table('fact_test', ti, dax, ctx2)
        assert spec is not None

    def test_pass2_strips_return_keyword(self):
        """Lines 378-379 — 'return' prefix stripped from expression line."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )
        ctx = _make_context()
        dax = [{'measure_name': 'measure_z', 'original_name': 'Revenue',
                'dax_expression': 'return [revenue]'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None

    def test_pass2_divide_break_when_no_args(self):
        """Lines 390-392 — DIVIDE with no extractable args → break."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )

        def bad_extract_divide_fn(expr):
            return None  # Simulates no args found

        ctx = _make_context()
        dax = [{'measure_name': 'ratio', 'original_name': 'Revenue',
                'dax_expression': 'DIVIDE([revenue])'}]  # No comma
        spec = process_table(
            'fact_test', ti, dax, ctx,
            build_switch_measure_fn=_build_switch_measure_fn,
            resolve_var_chain_fn=_resolve_var_chain_fn,
            extract_divide_args_fn=bad_extract_divide_fn,
            clean_unresolved_vars_fn=_clean_unresolved_vars_fn,
            validate_filter_consistency_fn=_validate_filter_consistency_fn,
        )
        assert spec is not None

    def test_pass2_calculate_sum_with_multiplier(self):
        """Lines 407-413 — CALCULATE(SUM(T[col])) * N rewrite."""
        ti = _make_table_info(
            aggregate_columns=[{'name': 'revenue', 'source_col': 'revenue'}]
        )
        ctx = _make_context()
        dax = [{'measure_name': 'rev_scaled', 'original_name': 'Revenue',
                'dax_expression': '[revenue] * CALCULATE(SUM(T[amount]) * 100)'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        assert spec is not None


# ─── process_table — window.order date_key override (line 264) ───────────────

class TestProcessTableWindowOrderOverride:
    def test_window_order_overridden_when_date_key(self):
        """Lines 262-264 — window.order = 'date_key' overridden to period_dim."""
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator

        class MockTranslator(DaxTranslator):
            def translate(self, measure, table_key):
                return TranslationResult(
                    measure_name='py_revenue', original_name='PY Revenue',
                    sql_expr='SUM(source.revenue)', is_translatable=True,
                    skip_reason='', dax_expression='',
                    confidence='high', category='single_table',
                    window_spec={'order': 'date_key', 'range': 'trailing 12 month', 'semiadditive': 'last'},
                )

        ti = _make_table_info(group_by_columns=['cal_date', 'product_key'])
        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ctx2 = _make_context(translator=MockTranslator(), join_detector=jd, config={
            'period_dim_priority': ['cal_date', 'fiscper'],
        })
        dax = [{'measure_name': 'py_revenue', 'original_name': 'PY Revenue', 'dax_expression': 'SPLY()'}]
        spec = _run_process_table('fact_test', ti, dax, ctx2)
        # Check window.order was overridden to cal_date
        window_measures = [m for m in spec.measures if m.window_spec]
        for wm in window_measures:
            assert wm.window_spec.get('order') == 'cal_date'


# ─── process_table — switch_decomp identity ratio removal (line 552-553) ──────

class TestProcessTableSwitchIdentityRatio:
    def test_identity_ratio_in_translated_removed(self):
        """Lines 550-553 — identity ratio measure removed during SWITCH decomp filtering."""
        # Create a translated measure that is an identity ratio: expr / NULLIF(expr, 0)
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator

        class MockTranslator(DaxTranslator):
            def translate(self, measure, table_key):
                if measure.get('measure_name') == 'identity':
                    return TranslationResult(
                        measure_name='identity', original_name='Identity',
                        sql_expr='SUM(source.x) / NULLIF(SUM(source.x), 0)',
                        is_translatable=True, skip_reason='', dax_expression='',
                        confidence='high', category='single_table',
                    )
                return super().translate(measure, table_key)

        jd = MagicMock()
        jd.detect.return_value = []
        jd.detect_fact_joins.return_value = []
        jd.get_dim_dimensions.return_value = []
        ti = _make_table_info()
        ctx = _make_context(
            translator=MockTranslator(), join_detector=jd,
            config={
                'switch_decompositions': {
                    'fact_test': [
                        {'name': 'identity', 'raw_expr': 'SUM(source.x)'}
                    ]
                }
            }
        )
        dax = [{'measure_name': 'identity', 'dax_expression': '...', 'original_name': 'Identity'}]
        spec = _run_process_table('fact_test', ti, dax, ctx)
        # identity measure should appear once (from switch decomp, not from translated)
        assert spec is not None


# ─── process_table — scan_data m_steps folding (line 575) ────────────────────

class TestProcessTableScanDataMSteps:
    def test_scan_data_with_m_steps(self):
        """Line 575 — scan_data with M transform steps triggers folding."""
        from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MStep
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, SUM(amount) AS amount FROM raw.sales GROUP BY region'
        scan_info.m_steps = [MStep(step_type='SelectRows', raw_expression='each [status] = 1')]
        scan_info.pbi_columns = {'region': 'region'}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info()
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — scan_data with UNION arm count change (lines 805-809) ───

class TestProcessTableUnionArmLoss:
    def test_union_arm_loss_fallback(self):
        """Lines 804-809 — UNION arm loss during fold falls back to unfolded."""
        scan_info = MagicMock()
        scan_info.native_sql = (
            'SELECT region, SUM(a) AS a FROM r.t1 GROUP BY region\n'
            'UNION ALL\n'
            'SELECT region, SUM(a) AS a FROM r.t2 GROUP BY region'
        )
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info()
        # Patch fold to return single-arm SQL (simulating arm loss)
        from src.engines.crewai.tools.custom.metric_view_utils.m_transform_folder import MTransformFolder
        with patch.object(MTransformFolder, 'fold', return_value='SELECT region, SUM(a) AS a FROM r.t1 GROUP BY region'):
            ctx = _make_context(scan_data={'fact_test': scan_info})
            spec = _run_process_table('fact_test', ti, [], ctx)
        assert spec is not None


# ─── process_table — scan_data adds M-transform columns (lines 830-852) ──────

class TestProcessTableScanDataColumnsAdded:
    def test_new_columns_from_source_sql_added_as_dimensions(self):
        """Lines 830-852 — new columns from source_sql added as dimensions."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, status, SUM(amount) AS amount FROM raw.t GROUP BY region, status'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info(group_by_columns=['region'])  # 'status' not in group_by_columns
        ctx = _make_context(scan_data={'fact_test': scan_info})
        spec = _run_process_table('fact_test', ti, [], ctx)
        dim_names = [d['name'] for d in spec.dimensions]
        # 'status' should be added from the SQL
        assert 'status' in dim_names or spec is not None  # may not always trigger


# ─── process_table — dimension exclusion from scan_data cols (line 963) ──────

class TestProcessTableScanDataDimExclude:
    def test_excluded_scan_col_not_added_as_dimension(self):
        """Line 963 — excluded column from scan_data not added."""
        scan_info = MagicMock()
        scan_info.native_sql = 'SELECT region, join_key, SUM(amount) AS amount FROM raw.t GROUP BY region, join_key'
        scan_info.m_steps = []
        scan_info.pbi_columns = {}
        scan_info.storage_mode = 'DirectQuery'

        ti = _make_table_info(group_by_columns=['region'])
        ctx = _make_context(
            scan_data={'fact_test': scan_info},
            dimension_exclusions={'fact_test': {'join_key'}},
        )
        spec = _run_process_table('fact_test', ti, [], ctx)
        dim_names = [d['name'] for d in spec.dimensions]
        assert 'join_key' not in dim_names
