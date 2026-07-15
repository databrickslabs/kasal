"""Tests for the P5 SQL measure sanitizer."""
from src.engines.crewai.tools.custom.metric_view_utils.sql_measure_sanitizer import (
    sanitize_measure_sql, strip_nullif_one, detect_self_division, coalesce_wrap_base,
)


class TestStripNullifOne:
    def test_removes_noop_division(self):
        assert strip_nullif_one("SUM(source.x) / NULLIF(1, 0)") == "SUM(source.x)"

    def test_leaves_real_denominator(self):
        s = "SUM(source.a) / NULLIF(SUM(source.b), 0)"
        assert strip_nullif_one(s) == s

    def test_none_safe(self):
        assert strip_nullif_one(None) is None


class TestSelfDivision:
    def test_detects_self_division(self):
        assert detect_self_division("SUM(source.x) / NULLIF(SUM(source.x), 0)")

    def test_detects_when_numerator_parenthesized(self):
        assert detect_self_division("(SUM(source.x)) / NULLIF(SUM(source.x), 0)")

    def test_real_ratio_not_flagged(self):
        assert not detect_self_division("SUM(source.a) / NULLIF(SUM(source.b), 0)")

    def test_no_ratio_not_flagged(self):
        assert not detect_self_division("SUM(source.x)")


class TestCoalesceWrap:
    def test_wraps_bare_sum(self):
        assert coalesce_wrap_base("SUM(source.sales)") == "SUM(COALESCE(source.sales, 0))"

    def test_skips_already_wrapped(self):
        s = "SUM(COALESCE(source.sales, 0))"
        assert coalesce_wrap_base(s) == s

    def test_wraps_multiple(self):
        out = coalesce_wrap_base("SUM(source.a) - SUM(source.b)")
        assert out == "SUM(COALESCE(source.a, 0)) - SUM(COALESCE(source.b, 0))"


class TestSanitizeMeasureSql:
    def test_base_measure_coalesce_and_noop_strip(self):
        sql, note = sanitize_measure_sql("SUM(source.sales) / NULLIF(1, 0)", is_base=True)
        assert sql == "SUM(COALESCE(source.sales, 0))"
        assert note is None

    def test_self_division_flagged(self):
        sql, note = sanitize_measure_sql(
            "SUM(source.x) / NULLIF(SUM(source.x), 0)", is_base=False)
        assert note and "self-division" in note

    def test_non_base_not_coalesced(self):
        sql, _ = sanitize_measure_sql(
            "SUM(source.a) / NULLIF(SUM(source.b), 0)", is_base=False)
        assert "COALESCE" not in sql

    def test_none_safe(self):
        assert sanitize_measure_sql(None, is_base=True) == (None, None)

    def test_idempotent(self):
        s = "SUM(source.sales)"
        once, _ = sanitize_measure_sql(s, is_base=True)
        twice, _ = sanitize_measure_sql(once, is_base=True)
        assert once == twice


class TestDetectSilentWrong:
    """PROP-3a/4a: measures that would be silently wrong/invalid are flagged."""

    def _D(self, sql):
        from src.engines.crewai.tools.custom.metric_view_utils.sql_measure_sanitizer import (
            detect_silent_wrong,
        )
        return detect_silent_wrong(sql)

    def test_empty_ratio_flagged(self):
        assert self._D("/ NULLIF(, 0)")

    def test_unresolved_measure_ref_flagged(self):
        assert self._D("SUM(a) / NULLIF(EDGE_Measure[Edge All DPMO], 0)")

    def test_todo_literal_flagged(self):
        assert self._D("SUM(a) / NULLIF(TODO: fill SQL expression, 0)")

    def test_prior_year_flagged(self):
        assert self._D("SUM(source.x) FILTER (WHERE SAMEPERIODLASTYEAR(cal))")

    def test_raw_sumx_flagged(self):
        assert self._D("SUMX(FILTER(fact, fact[x]=1), val)")

    # These MUST stay clean (no false positives on the good run's measures)
    def test_clean_aggregate_ok(self):
        assert self._D("SUM(source.kbi_value)") is None

    def test_clean_filtered_aggregate_ok(self):
        assert self._D(
            "SUM(source.kbi_value) FILTER (WHERE bic_chversion = '0000' "
            "AND fis_code IN ('DCC3', 'DCC1', 'DCCE'))") is None

    def test_clean_ratio_ok(self):
        assert self._D("SUM(source.a) / NULLIF(SUM(source.b), 0)") is None

    def test_clean_subtraction_ok(self):
        assert self._D(
            "(SUM(source.value) FILTER (WHERE fis_code_parent IN ('DCD2','DHF2'))) "
            "- (SUM(source.value) FILTER (WHERE fis_code_parent IN ('DHHX')))") is None

    def test_measure_composition_ok(self):
        assert self._D(
            "MEASURE(rpet_flake_gram) * MEASURE(sales_pet) / 1000000") is None


class TestLostDaxComponent:
    """Detect DAX components silently dropped from otherwise-clean SQL."""

    def _L(self, dax, sql):
        from src.engines.crewai.tools.custom.metric_view_utils.sql_measure_sanitizer import (
            detect_lost_dax_component,
        )
        return detect_lost_dax_component(dax, sql)

    def test_prior_year_dropped_flagged(self):
        dax = 'CALCULATE(SUM(t[nsr]), SAMEPERIODLASTYEAR(cal[date_id]))'
        sql = "SUM(source.nsr) FILTER (WHERE bic_chversion = '0000')"
        assert self._L(dax, sql)  # PY shift not in SQL -> flagged

    def test_prior_year_with_window_not_flagged(self):
        dax = 'CALCULATE(SUM(t[nsr]), SAMEPERIODLASTYEAR(cal[date_id]))'
        sql = "SUM(source.nsr) OVER (ORDER BY fiscper)"
        assert self._L(dax, sql) is None  # has a window -> faithful enough

    def test_ratio_denominator_dropped_flagged(self):
        dax = 'DIVIDE(SUMX(FILTER(f, f[mg] in {"1"}), f[target]), issued - received)'
        sql = "SUM(source.target_value) FILTER (WHERE matl_group IN ('1'))"
        assert self._L(dax, sql)  # DIVIDE in DAX, no / in SQL

    def test_ratio_present_not_flagged(self):
        dax = 'DIVIDE(SUM(t[a]), SUM(t[b]))'
        sql = "SUM(source.a) / NULLIF(SUM(source.b), 0)"
        assert self._L(dax, sql) is None

    def test_exclusion_dropped_flagged(self):
        dax = 'DIVIDE(SUM(t[a]), SUM(t[b]) FILTER(geo[company_code] <> "0307"))'
        sql = "SUM(source.a) / NULLIF(SUM(source.b), 0)"
        assert self._L(dax, sql)  # <> exclusion in DAX, none in SQL

    def test_exclusion_present_not_flagged(self):
        dax = 'CALCULATE(SUM(t[a]), t[comp_code] <> "0307")'
        sql = "SUM(source.a) FILTER (WHERE comp_code <> '0307')"
        assert self._L(dax, sql) is None

    def test_clean_filtered_aggregate_never_flagged(self):
        dax = 'CALCULATE(SUM(t[kbi_value]), t[fis_code] IN {"DCC3"})'
        sql = "SUM(source.kbi_value) FILTER (WHERE fis_code IN ('DCC3'))"
        assert self._L(dax, sql) is None

    # ── Additive multi-block collapse (a - b / a + b outside DIVIDE) ──────────

    def test_additive_subtraction_collapse_flagged(self):
        # cost_to_supply: DAX is `a - b` (two CALCULATE blocks), SQL emitted only a.
        dax = (
            'var std = CALCULATE([F_Start_date]) '
            'var a = CALCULATE(SUMX(FILTER(FT_BPC003, FT_BPC003[bic_chversion]="0000" '
            '&& FT_BPC003[fis_code_parent] IN {"DCD2","DHF2"}), FT_BPC003[value])) '
            'var b = CALCULATE(SUMX(FILTER(FT_BPC003, FT_BPC003[bic_chversion]="0000" '
            '&& FT_BPC003[fis_code_parent] IN {"DHHX"}), FT_BPC003[value])) '
            'return a - b'
        )
        sql = "SUM(source.value) FILTER (WHERE bic_chversion = '0000' AND fis_code_parent IN ('DCD2', 'DHF2'))"
        assert self._L(dax, sql)  # -b term dropped -> flagged

    def test_additive_sum_collapse_flagged(self):
        # total_nsr: DAX is `a + b`, SQL emitted only a.
        dax = (
            'var a = CALCULATE(SUMX(FILTER(Fact_CO012, Fact_CO012[bic_chversion]="0000"), Fact_CO012[net_sales_revenue])) '
            'var b = CALCULATE(SUMX(FILTER(Fact_CO012, Fact_CO012[bic_chversion]="0000"), Fact_CO012[other])) '
            'return a + b'
        )
        sql = "SUM(source.net_sales_revenue) FILTER (WHERE bic_chversion = '0000')"
        assert self._L(dax, sql)  # +b term dropped -> flagged

    def test_additive_both_terms_emitted_not_flagged(self):
        # Faithful a - b: both filtered SUMs present -> not flagged.
        dax = (
            'var a = CALCULATE(SUMX(FILTER(t, t[x]="0000"), t[v])) '
            'var b = CALCULATE(SUMX(FILTER(t, t[x]="DHHX"), t[v])) return a - b'
        )
        sql = "SUM(source.v) FILTER (WHERE x = '0000') - SUM(source.v) FILTER (WHERE x = 'DHHX')"
        assert self._L(dax, sql) is None

    def test_single_block_filtered_aggregate_not_flagged_by_additive(self):
        # Single CALCULATE block, no arithmetic -> additive check must not fire.
        dax = 'var a = CALCULATE(SUMX(FILTER(t, t[x]="RE" && t[fis_code] IN {"DHG2"}), t[v])) return a'
        sql = "SUM(source.value) FILTER (WHERE bic_chversion = 'RE' AND fis_code IN ('DHG2'))"
        assert self._L(dax, sql) is None


class TestDanglingMultiLetterVar:
    """detect_silent_wrong catches dangling res1/res2/std var names."""

    def _S(self, sql):
        from src.engines.crewai.tools.custom.metric_view_utils.sql_measure_sanitizer import (
            detect_silent_wrong,
        )
        return detect_silent_wrong(sql)

    def test_res1_flagged(self):
        assert self._S("(res1+SUM(fact.val) FILTER (WHERE x = 1)) / NULLIF(res2, 0)")

    def test_clean_not_flagged(self):
        assert self._S("SUM(source.a) / NULLIF(SUM(source.b), 0)") is None
