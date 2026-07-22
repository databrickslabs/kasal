"""Tests for DAX→SQL translator — core pattern matching."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator


@pytest.fixture
def translator():
    return DaxTranslator()


class TestQuickReject:
    def test_format_rejected(self, translator):
        result = translator.translate(
            {'measure_name': 'fmt', 'dax_expression': 'FORMAT(Sales, "#,##0")', 'original_name': 'fmt'},
            'fact_test')
        assert result.is_translatable is False
        assert 'FORMAT' in result.skip_reason

    def test_color_rejected(self, translator):
        result = translator.translate(
            {'measure_name': 'KBI_Color', 'dax_expression': 'IF(x>0, "green", "red")', 'original_name': 'KBI_Color'},
            'fact_test')
        assert result.is_translatable is False
        assert 'Color' in result.skip_reason

    def test_isblank_blank_rejected(self, translator):
        result = translator.translate(
            {'measure_name': 'guard', 'dax_expression': 'IF(ISBLANK(x), BLANK(), x)', 'original_name': 'guard'},
            'fact_test')
        assert result.is_translatable is False

    def test_blank_placeholder(self, translator):
        result = translator.translate(
            {'measure_name': 'blank', 'dax_expression': 'BLANK()', 'original_name': 'blank'},
            'fact_test')
        assert result.is_translatable is False
        assert 'BLANK' in result.skip_reason

    def test_selectedvalue_switch(self, translator):
        result = translator.translate(
            {'measure_name': 'sw', 'dax_expression': 'SWITCH(SELECTEDVALUE(Table[Col]), "a", 1, 2)', 'original_name': 'sw'},
            'fact_test')
        assert result.is_translatable is False

    def test_empty_dax(self, translator):
        result = translator.translate(
            {'measure_name': 'empty', 'dax_expression': '', 'original_name': 'empty'},
            'fact_test')
        assert result.is_translatable is False


class TestSimpleSum:
    def test_basic_sum(self, translator):
        result = translator.translate(
            {'measure_name': 'total', 'dax_expression': 'SUM(Sales[Amount])', 'original_name': 'Total'},
            'fact_test')
        assert result.is_translatable is True
        assert result.sql_expr == 'SUM(source.Amount)'

    def test_calculate_sum(self, translator):
        result = translator.translate(
            {'measure_name': 'total', 'dax_expression': 'CALCULATE(SUM(Sales[Amount]))', 'original_name': 'Total'},
            'fact_test')
        assert result.is_translatable is True
        assert 'SUM(source.Amount)' in result.sql_expr


class TestSimpleSumx:
    def test_sumx_without_filter(self, translator):
        result = translator.translate(
            {'measure_name': 'total', 'dax_expression': 'SUMX(Sales, Sales[Amount])', 'original_name': 'Total'},
            'fact_test')
        assert result.is_translatable is True
        assert result.sql_expr == 'SUM(source.Amount)'


class TestDivide:
    def test_simple_divide(self, translator):
        result = translator.translate(
            {'measure_name': 'ratio', 'dax_expression': 'DIVIDE(SUM(T[A]), SUM(T[B]))', 'original_name': 'Ratio'},
            'fact_test')
        assert result.is_translatable is True
        assert 'NULLIF' in result.sql_expr
        assert 'SUM(source.A)' in result.sql_expr


class TestCountxFilter:
    def test_countx_filter(self, translator):
        result = translator.translate(
            {'measure_name': 'cnt', 'dax_expression': 'COUNTX(FILTER(Sales, Sales[Status]="Active"), Sales[ID])', 'original_name': 'Count'},
            'fact_test')
        assert result.is_translatable is True
        assert 'COUNT(source.ID)' in result.sql_expr
        assert 'FILTER' in result.sql_expr


class TestAveragexFilter:
    def test_averagex_filter(self, translator):
        result = translator.translate(
            {'measure_name': 'avg', 'dax_expression': 'AVERAGEX(FILTER(Sales, Sales[Type]="A"), Sales[Value])', 'original_name': 'Avg'},
            'fact_test')
        assert result.is_translatable is True
        assert 'AVG(source.Value)' in result.sql_expr


class TestDistinctCountNoBlank:
    def test_simple(self, translator):
        result = translator.translate(
            {'measure_name': 'dc', 'dax_expression': 'DISTINCTCOUNTNOBLANK(Sales[Customer])', 'original_name': 'DC'},
            'fact_test')
        assert result.is_translatable is True
        assert 'COUNT(DISTINCT source.Customer)' == result.sql_expr


class TestSameperiodlastyear:
    def test_spely_with_sumx_filter(self, translator):
        dax = 'CALCULATE(SUMX(FILTER(Sales, Sales[Type]="A"), Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))'
        result = translator.translate(
            {'measure_name': 'py', 'dax_expression': dax, 'original_name': 'PY'},
            'fact_test')
        assert result.is_translatable is True
        assert result.window_spec is not None
        assert result.window_spec['order'] == 'fiscper'

    def test_spely_simple_deferred(self, translator):
        dax = 'CALCULATE([Total], SAMEPERIODLASTYEAR(Cal[Date]))'
        result = translator.translate(
            {'measure_name': 'py', 'dax_expression': dax, 'original_name': 'PY'},
            'fact_test')
        assert result.is_translatable is False
        assert 'SAMEPERIODLASTYEAR' in result.skip_reason
        # P6: the stub must be actionable (names the date_py / window workaround)
        assert 'TODO' in result.skip_reason
        assert 'date_py' in result.skip_reason or 'window' in result.skip_reason.lower()


class TestDivideThirdArg:
    """PROP-2: DIVIDE(num, den, alt_result) must not leak the 3rd arg into NULLIF."""

    def test_two_arg_divide(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        assert DaxTranslator._extract_divide_args('DIVIDE(SUM(a), SUM(b))') == ('SUM(a)', 'SUM(b)')

    def test_three_arg_divide_drops_alt(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        # denominator must be SUM(b), NOT "SUM(b), 0"
        assert DaxTranslator._extract_divide_args('DIVIDE(SUM(a), SUM(b), 0)') == ('SUM(a)', 'SUM(b)')

    def test_three_arg_blank_alt(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        assert DaxTranslator._extract_divide_args('DIVIDE([x], [y], BLANK())') == ('[x]', '[y]')

    def test_nested_parens_preserved(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        num, den = DaxTranslator._extract_divide_args('DIVIDE(SUM(a)+SUM(c), NULLIF(SUM(b),0), 0)')
        assert num == 'SUM(a)+SUM(c)'
        assert den == 'NULLIF(SUM(b),0)'  # inner comma not mistaken for arg separator


class TestMeasureRefRatioFilters:
    """Bug B: DIVIDE(CALCULATE([M], pred_num), CALCULATE([M], pred_den)) must
    preserve DIFFERENT per-side filter predicates — not collapse to num==denom.

    Two sub-bugs were fixed:
      1. parenthesized predicates `([col] in {...})` failed the anchored regex
         → filters parsed as [] → both sides identical.
      2. single-line `var num = ... var denom = ... return DIVIDE(...)` parsing
         captured only the first var → expression collapsed.
    """

    def _t(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        t = DaxTranslator()
        t._measure_resolutions = {
            'Base_KBI': {'base_expr': 'SUM(source.kbi_value)',
                         'base_filters': ["bic_chversion = '0000'"]},
        }
        return t

    def test_var_chain_ratio_keeps_distinct_filters(self):
        t = self._t()
        dax = ('var num = CALCULATE([Base_KBI], (FT_QSE[bic_csubkbi] in {"A1","A2"})) '
               'var denom = CALCULATE([Base_KBI], (FT_QSE[bic_csubkbi] in {"B1","B2"})) '
               'return DIVIDE(num, denom)')
        m = t._match_divide_calculate_measure_ref(dax, 'x')
        sql, _ = t._translate_divide_calculate_measure_ref(m, dax, 'FT_QSE')
        assert sql is not None
        assert "IN ('A1', 'A2')" in sql   # numerator predicate preserved
        assert "IN ('B1', 'B2')" in sql   # denominator predicate preserved
        # and the two sides are NOT identical (no 1.0 collapse)
        import re
        num, den = re.split(r'/\s*NULLIF', sql, maxsplit=1)
        assert num.strip() != den.strip()

    def test_inline_divide_ratio_keeps_distinct_filters(self):
        t = self._t()
        dax = ('DIVIDE(CALCULATE([Base_KBI], FT_QSE[bic_csubkbi] in {"A1","A2"}),'
               'CALCULATE([Base_KBI], FT_QSE[bic_csubkbi] in {"B1","B2"}))')
        m = t._match_divide_calculate_measure_ref(dax, 'x')
        sql, _ = t._translate_divide_calculate_measure_ref(m, dax, 'FT_QSE')
        assert sql is not None
        assert "IN ('A1', 'A2')" in sql and "IN ('B1', 'B2')" in sql

    def test_parenthesized_predicate_parsed(self):
        # The core sub-bug: a wrapped `(table[col] in {...})` predicate must parse.
        t = self._t()
        parsed = t._parse_calculate_filters('(FT_QSE[bic_csubkbi] in {"A1","A2"})', 'FT_QSE')
        assert parsed == ["source.bic_csubkbi IN ('A1', 'A2')"]

    def test_scaffolding_refs_not_treated_as_measure_refs(self):
        """Fix #4: [F_Start_date]/[F_End_date] date-window scaffolding must NOT be
        picked up as measure-refs. Otherwise a var-chain join-alias DIVIDE (e.g.
        fact_pe002 EPL_Actual) is claimed by the measure-ref matcher and fails on
        'Cannot resolve [F_Start_date]' — dropping the measure instead of routing
        it to the LLM var-chain path."""
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        t = DaxTranslator()
        dax = (
            'var std = CALCULATE([F_Start_date]) var etd= CALCULATE([F_End_date]) '
            'var a = CALCULATE(SUMX(fact_pe002,fact_pe002[epl]),'
            'FILTER(Dim_wkctr,Dim_wkctr[bic_cwc_type] in {"APET","CAN"})) '
            'var b = CALCULATE(SUMX(fact_pe002,fact_pe002[paid_hours]),'
            'FILTER(Dim_wkctr,Dim_wkctr[bic_cwc_type] in {"APET","CAN"})) return DIVIDE(a,b)'
        )
        # scaffolding-only refs → matcher declines (falls through to LLM), it must
        # NOT claim-and-fail
        assert t._match_divide_calculate_measure_ref(dax, 'epl') is None
        assert t._match_calculate_measure_ref(dax, 'epl') is None
        # end-to-end llm_first: untranslatable (routed to LLM), NOT wrong SQL
        res = t.translate({'measure_name': 'epl', 'original_name': 'EPL_Actual',
                           'dax_expression': dax}, 'fact_pe002', trivial_only=True)
        assert res.is_translatable is False
        assert res.sql_expr is None

    def test_real_measure_ref_still_detected(self):
        # Regression: a genuine measure-ref (not scaffolding) is still found.
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        t = DaxTranslator()
        refs = t._find_calculate_measure_refs('CALCULATE([Real_Measure], T[a]="1")')
        assert [r['ref_name'] for r in refs] == ['Real_Measure']


class TestPrecleanDax:
    """PROP-6: strip DAX comments + normalize inline BLANK() before translation."""

    def _P(self, dax):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        return DaxTranslator._preclean_dax(dax)

    def test_strips_double_slash_comment(self):
        assert '//' not in self._P('SUM(x) // note')

    def test_strips_double_dash_comment(self):
        # the -- Denominator leak that commented out SQL
        out = self._P('SUM(a) / NULLIF(SUM(b),0) -- Denominator')
        assert '--' not in out and 'SUM(a)' in out

    def test_strips_block_comment(self):
        assert '/*' not in self._P('SUM(/* x */ a)')

    def test_inline_blank_to_null(self):
        assert self._P('IF(x, BLANK(), y)') == 'IF(x, NULL, y)'

    def test_standalone_blank_preserved_for_reject(self):
        # a pure BLANK() measure must stay BLANK() so quick-reject can drop it
        assert self._P('BLANK()').strip().upper() == 'BLANK()'


class TestFragileMatchersRouteToLLM:
    """The fragile multi-var matchers must NOT be in the llm_first fast-path, so
    complex ratios route to the LLM instead of being botched by regex."""

    def test_multivar_matchers_excluded_from_fast_path(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        fp = DaxTranslator._TRIVIAL_FAST_PATH
        for pat in ('calculate_sumx_vars_divide',
                    'calculate_sumx_filter_inner',
                    'calculate_sumx_filter_outer'):
            assert pat not in fp, f"{pat} should route to LLM, not the regex fast-path"

    def test_simple_converters_stay_in_fast_path(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        fp = DaxTranslator._TRIVIAL_FAST_PATH
        for pat in ('simple_sum', 'divide', 'sumx_filter', 'calculate_equality_filter'):
            assert pat in fp

    def test_trivial_only_skips_multivar_divide(self):
        # in trivial_only (llm_first) mode, a multi-var DIVIDE returns untranslatable
        # (so the caller routes it to the LLM) rather than a botched regex result
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        t = DaxTranslator({})
        dax = ('var a = CALCULATE(SUMX(FILTER(f, f[mg] in {"1"}), f[target])) '
               'var b = CALCULATE(SUMX(FILTER(f, f[mg] in {"1"}), f[issued])) '
               'var c = CALCULATE(SUMX(FILTER(f, f[mg] in {"1"}), f[received])) '
               'return DIVIDE(a, b-c)')
        res = t.translate({'measure_name': 'y', 'dax_expression': dax, 'original_name': 'Y'},
                          'f', trivial_only=True)
        assert res.is_translatable is False  # -> routed to LLM


class TestSameMeasureRatioKeepsPerSideFilters:
    """DIVIDE(CALCULATE([base], A), CALCULATE([base], B)) must keep the DIFFERENT
    per-side filters (A in num, B in denom) — not collapse to num==denom==1.0."""

    def _t(self, base_filters):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        return DaxTranslator({'measure_resolutions': {
            'Base': {'base_expr': 'SUM(source.kbi_value)', 'base_filters': base_filters}}})

    def _sql(self, t):
        dax = ('DIVIDE(CALCULATE([Base], T[csubkbi] in {"A"}), '
               'CALCULATE([Base], T[csubkbi] in {"B"}))')
        return t.translate({'measure_name': 'r', 'original_name': 'R',
                            'dax_expression': dax}, 'T').sql_expr

    def test_per_side_filters_preserved_no_base_filter(self):
        sql = self._sql(self._t([]))
        num, den = sql.split('/ NULLIF', 1)
        assert "'A'" in num and "'B'" in den    # different per side
        assert num.strip() != den.strip()        # NOT num==denom

    def test_per_side_filters_preserved_with_base_filter(self):
        sql = self._sql(self._t(["bic_chversion = '0000'"]))
        num, den = sql.split('/ NULLIF', 1)
        assert "'A'" in num and "'B'" in den
        assert "bic_chversion = '0000'" in num and "bic_chversion = '0000'" in den


class TestDateWindowScaffoldingStrip:
    """Strip slicer-scalar var std/etd = CALCULATE([F_Start_date]) so the ratio
    underneath becomes translatable (was blocking ~36 reference ratios)."""

    def _P(self, dax):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        return DaxTranslator._preclean_dax(dax)

    def test_strips_fstart_scaffolding_keeps_ratio(self):
        dax = ('var std = CALCULATE([F_Start_date])\n'
               'var etd= CALCULATE([F_End_date])\n'
               'var a = CALCULATE(SUMX(FILTER(f,f[mg] in {"1"}),f[target]))\n'
               'var b = CALCULATE(SUMX(FILTER(f,f[mg] in {"1"}),f[issued]))\n'
               'return DIVIDE(a, b)')
        out = self._P(dax)
        assert 'F_Start_date' not in out and 'F_End_date' not in out
        assert 'DIVIDE' in out
        assert 'var a =' in out and 'var b =' in out   # real aggregate vars kept
        # no dangling std/etd tokens
        import re
        assert not re.search(r'(?<![\w.])(std|etd)(?![\w.])', out)

    def test_regex_translates_cleaned_ratio(self):
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        t = DaxTranslator({})
        dax = ('var std = CALCULATE([F_Start_date])\n'
               'var a = CALCULATE(SUMX(FILTER(f,f[mg] in {"1"}),f[target]))\n'
               'var b = CALCULATE(SUMX(FILTER(f,f[mg] in {"1"}),f[issued]))\n'
               'var c = CALCULATE(SUMX(FILTER(f,f[mg] in {"1"}),f[received]))\n'
               'return DIVIDE(a, b-c)')
        r = t.translate({'measure_name': 'y', 'original_name': 'Y', 'dax_expression': dax},
                        'f', trivial_only=False)
        assert r.is_translatable
        assert '/ NULLIF(' in r.sql_expr
        assert 'received' in r.sql_expr  # denominator's c term present

    def test_normal_dax_untouched(self):
        # a measure WITHOUT the window pickers must be unchanged
        dax = 'CALCULATE(SUM(f[val]), f[ver]="0000")'
        assert self._P(dax) == dax
