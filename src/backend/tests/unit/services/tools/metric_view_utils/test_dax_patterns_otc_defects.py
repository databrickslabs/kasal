"""Regression tests for the IDOR/DQA OTC-Management post-mortem defect classes
(M1–M5, M8, M9, M11).

Fixtures are the REAL OTC DAX (from ``pipeline_config_full.json``'s
``measures_json``); the expected SQL is the customer ground-truth ``new_expr``
from ``measure_comparison.csv``. OTC is only the fixture — the translator logic is
generic (no OTC names are hardcoded in the source).

Where Kasal's canonical output form differs from the CSV but is SEMANTICALLY
equivalent, that is called out in the test docstring:
- M4/M5 use ``SUM(CASE WHEN … THEN col END)`` (identical to the CSV form).
- M3 emits the GENERIC NULL-aware inequality ``(col IS NULL OR col <> 'X')`` (the
  faithful DAX-BLANK translation and the fix for the "always 0" defect), rather
  than the CSV's ``col IS NULL`` — the latter relies on the tenant-specific fact
  that the string sentinel 'NotNull' maps to physical NULL, which a generic
  translator cannot and must not infer.
"""

import re

import pytest

from src.services.tools.metric_view_utils.dax_translator import DaxTranslator
from src.services.tools.metric_view_utils.sql_measure_sanitizer import (
    sanitize_measure_name,
)
from src.services.tools.metric_view_utils.utils import to_snake_case

# Measure resolutions the pipeline builds from the base measures. Realistic:
# these three are plain base sums / distinct counts defined on their facts.
_RESOLUTIONS = {
    "Sum OTC FLTP": {"base_expr": "SUM(source.fltp)"},
    "TotalInvoices": {"base_expr": "SUM(source.global_no_of_invoices)"},
    "DistinctCountCG AGG": {"base_expr": "COUNT(DISTINCT source.cg_id)"},
}


@pytest.fixture
def translator():
    return DaxTranslator({"measure_resolutions": _RESOLUTIONS})


def _sql(translator, dax, table_key="Fact_NPS", name="m"):
    r = translator.translate(
        {"measure_name": name, "dax_expression": dax, "original_name": name}, table_key
    )
    return r


# ── M1 / M2 — SUMMARIZE + DISTINCTCOUNT group-then-aggregate (NPS) ────────────

_NPS_GROUP = (
    "VAR _GroupTable = CALCULATETABLE ( ADDCOLUMNS ( SUMMARIZE ( 'Fact_NPS', [nps] ), "
    "\"@Count\", CALCULATE ( DISTINCTCOUNT ( 'Fact_NPS'[cg_id] ) ) ) )\n"
)
_PAIR = "concat(CAST(source.nps AS STRING), '|', CAST(source.cg_id AS STRING))"


class TestNpsGroupThenAggregate:
    def test_promoters_distinct_pair(self, translator):
        dax = _NPS_GROUP + (
            "VAR _Promoters = SUMX ( FILTER ( _GroupTable, [nps] > 8 ), [@Count] )\n"
            "RETURN _Promoters"
        )
        r = _sql(translator, dax)
        assert r.is_translatable
        assert (
            r.sql_expr == f"COUNT(DISTINCT CASE WHEN source.nps > 8 THEN {_PAIR} END)"
        )

    def test_detractors_distinct_pair(self, translator):
        dax = _NPS_GROUP + (
            "VAR _Detractors = SUMX ( FILTER ( _GroupTable, [nps] < 7 ), [@Count] )\n"
            "RETURN _Detractors"
        )
        r = _sql(translator, dax)
        assert (
            r.sql_expr == f"COUNT(DISTINCT CASE WHEN source.nps < 7 THEN {_PAIR} END)"
        )

    def test_passives_in_set(self, translator):
        dax = _NPS_GROUP + (
            "VAR Passives = SUMX ( FILTER ( _GroupTable, [nps] IN {7,8} ), [@Count] )\n"
            "RETURN Passives"
        )
        r = _sql(translator, dax)
        assert (
            r.sql_expr
            == f"COUNT(DISTINCT CASE WHEN source.nps IN (7, 8) THEN {_PAIR} END)"
        )

    def test_nps_total_ratio_over_distinct_respondents(self, translator):
        """NPS Total: denominator is [DistinctCountCG AGG] = COUNT(DISTINCT cg_id).
        Exact match to CSV new_expr (fixes 35,901,235 → 74.37)."""
        dax = _NPS_GROUP + (
            "VAR _Responses = [DistinctCountCG AGG]\n"
            "VAR _Promoters = SUMX ( FILTER ( _GroupTable, [nps] > 8 ), [@Count] )\n"
            "VAR _Detractors = SUMX ( FILTER ( _GroupTable, [nps] < 7 ), [@Count] )\n"
            "RETURN DIVIDE ( ( _Promoters - _Detractors ), _Responses ) * 100"
        )
        expected = (
            f"(COUNT(DISTINCT CASE WHEN source.nps > 8 THEN {_PAIR} END) - "
            f"COUNT(DISTINCT CASE WHEN source.nps < 7 THEN {_PAIR} END)) / "
            f"NULLIF(COUNT(DISTINCT source.cg_id), 0) * 100"
        )
        assert _sql(translator, dax).sql_expr == expected

    def test_nps_agg_ratio_over_total_pairs(self, translator):
        """NPS AGG: denominator is SUMX(_GroupTable,[@Count]) = total distinct
        pairs. Exact match to CSV new_expr."""
        dax = _NPS_GROUP + (
            "VAR _Responses = SUMX(_GroupTable, [@Count])\n"
            "VAR _Promoters = SUMX(FILTER(_GroupTable,[nps] > 8), [@Count])\n"
            "VAR _Detractors = SUMX(FILTER(_GroupTable,[nps] < 7), [@Count])\n"
            "RETURN DIVIDE((_Promoters - _Detractors),_Responses) * 100"
        )
        expected = (
            f"(COUNT(DISTINCT CASE WHEN source.nps > 8 THEN {_PAIR} END) - "
            f"COUNT(DISTINCT CASE WHEN source.nps < 7 THEN {_PAIR} END)) / "
            f"NULLIF(COUNT(DISTINCT {_PAIR}), 0) * 100"
        )
        assert _sql(translator, dax).sql_expr == expected

    def test_no_row_level_sum_and_no_count_product(self, translator):
        """The defect produced row-level SUM / a product of counts. Assert the
        fixed output has neither."""
        dax = _NPS_GROUP + (
            "VAR _Promoters = SUMX ( FILTER ( _GroupTable, [nps] > 8 ), [@Count] )\n"
            "RETURN _Promoters"
        )
        sql = _sql(translator, dax).sql_expr
        assert "SUM(" not in sql  # distinct-pair count, not a row-level sum
        assert "*" not in sql  # no count-product nonsense


# ── M4 — DAX VAR/RETURN 1 - DIVIDE (OTC zero-adjust KBIs) ─────────────────────


class TestOtcVarReturnRatio:
    def test_dsd_zero_adjst_one_minus_divide(self, translator):
        """M4: `1 - DIVIDE` kept; VAR names fully inlined; exact CSV new_expr."""
        dax = (
            "VAR _Code1Sum = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = \"KDST01215\")\n"
            "VAR _Code2Sum = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = \"KDST01216\")\n"
            "VAR _Div = DIVIDE(_Code2Sum,_Code1Sum)\n"
            "RETURN IF( ISBLANK(_Div) , BLANK(), 1 - _Div )"
        )
        r = _sql(translator, dax, table_key="Fact_OTC")
        assert r.is_translatable
        assert r.sql_expr == (
            "1 - SUM(CASE WHEN source.bic_csubkbi = 'KDST01216' THEN source.fltp END) "
            "/ NULLIF(SUM(CASE WHEN source.bic_csubkbi = 'KDST01215' THEN source.fltp END), 0)"
        )
        # No DAX VAR identifiers leaked into the SQL.
        assert "_Code1Sum" not in r.sql_expr and "_Div" not in r.sql_expr


# ── M5 — CALCULATE filter args (incl. IN {…}) preserved ──────────────────────


class TestOtcCalculateFilterPreserved:
    def test_orders_auto_billed_in_set(self, translator):
        """M5: IN {…} filter preserved (was dropped → SUM(fltp)/SUM(fltp)=1).
        Exact CSV new_expr."""
        dax = (
            'VAR _Code1Sum = CALCULATE([Sum OTC FLTP], \'Fact_OTC\'[bic_csubkbi] in {"KSMB01001", "KSMB01002"})\n'
            "VAR _Code2Sum = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = \"KSMB01003\")\n"
            "RETURN DIVIDE(_Code1Sum,_Code2Sum)"
        )
        r = _sql(translator, dax, table_key="Fact_OTC")
        assert r.sql_expr == (
            "SUM(CASE WHEN source.bic_csubkbi IN ('KSMB01001', 'KSMB01002') THEN source.fltp END) "
            "/ NULLIF(SUM(CASE WHEN source.bic_csubkbi = 'KSMB01003' THEN source.fltp END), 0)"
        )

    def test_not_self_division(self, translator):
        dax = (
            'VAR _Code1Sum = CALCULATE([Sum OTC FLTP], \'Fact_OTC\'[bic_csubkbi] in {"KSMB01001", "KSMB01002"})\n'
            "VAR _Code2Sum = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = \"KSMB01003\")\n"
            "RETURN DIVIDE(_Code1Sum,_Code2Sum)"
        )
        num, den = _sql(translator, dax, table_key="Fact_OTC").sql_expr.split(" / ", 1)
        assert num != den.replace("NULLIF(", "").rstrip(", 0)")


# ── M3 / M9 — BLANK inequality (NULL-aware) + qualified filter columns ───────


class TestBlankSemanticsAndQualification:
    def test_totalzru_qualified_filter(self, translator):
        """M9: quoted table filter column is source-qualified. Exact CSV new_expr."""
        dax = (
            "CALCULATE( SUM('AI_Invoice-DataBricks SQL'[zru]), "
            "'AI_Invoice-DataBricks SQL'[bill_type] = \"ZRU\" )"
        )
        r = _sql(translator, dax, table_key="AI_Invoice-DataBricks SQL")
        assert r.sql_expr == "SUM(source.zru) FILTER (WHERE source.bill_type = 'ZRU')"

    def test_totalzru_ftrr_null_aware_inequality(self, translator):
        """M3: `<> "NotNull"` must let NULL rows pass (DAX BLANK semantics), else
        the measure is always 0. Generic NULL-aware form; columns source-qualified."""
        dax = (
            "CALCULATE( SUM('AI_Invoice-DataBricks SQL'[zru]), "
            "'AI_Invoice-DataBricks SQL'[bill_type] = \"ZRU\", "
            "'AI_Invoice-DataBricks SQL'[bic_ccrednum] <> \"NotNull\" )"
        )
        r = _sql(translator, dax, table_key="AI_Invoice-DataBricks SQL")
        assert r.sql_expr == (
            "SUM(source.zru) FILTER (WHERE source.bill_type = 'ZRU' AND "
            "(source.bic_ccrednum IS NULL OR source.bic_ccrednum <> 'NotNull'))"
        )
        # The core defect: NULL rows are no longer silently dropped.
        assert "IS NULL" in r.sql_expr
        # M9: no unqualified bare column.
        assert "WHERE bill_type" not in r.sql_expr

    def test_ftrr_hidden_measure_ref_agg(self, translator):
        """M3 on a measure-ref aggregate ([TotalInvoices])."""
        dax = (
            "CALCULATE( [TotalInvoices], "
            "'AI_Invoice-DataBricks SQL'[bic_ccrednum] <> \"NotNull\" )"
        )
        r = _sql(translator, dax, table_key="AI_Invoice-DataBricks SQL")
        assert r.sql_expr == (
            "SUM(source.global_no_of_invoices) FILTER (WHERE "
            "(source.bic_ccrednum IS NULL OR source.bic_ccrednum <> 'NotNull'))"
        )

    def test_bareword_single_equality_left_to_existing_pattern(self, translator):
        """The new calc-filters matcher must NOT hijack the bareword
        single-equality case (handled by calculate_equality_filter as CASE WHEN)."""
        dax = 'CALCULATE(SUM(Fact[amount]), Fact[status] = "OPEN")'
        r = _sql(translator, dax, table_key="Fact")
        assert (
            r.sql_expr == "SUM(CASE WHEN source.status = 'OPEN' THEN source.amount END)"
        )


# ── M11 — latest-week semi-additive ratio (SKU snapshot) ─────────────────────


class TestLatestWeekSemiAdditive:
    def test_sku_inactive_ratio_and_window(self, translator):
        dax = (
            "VAR LatestYear = MAX(Calendar445[Year])\n"
            "VAR LatestMonth = CALCULATE(MAX(Calendar445[Month]), Calendar445[Year] = LatestYear)\n"
            "VAR LatestYearWeek = CALCULATE(MAX('SKU Active Or NotActive'[YearWeeks]), Calendar445[Year] = LatestYear, Calendar445[Month] = LatestMonth)\n"
            "VAR FilteredData = FILTER('SKU Active Or NotActive', 'SKU Active Or NotActive'[YearWeeks] = LatestYearWeek)\n"
            "VAR Inactive = CALCULATE(SUM('SKU Active Or NotActive'[Inactive]), FilteredData)\n"
            "VAR Last6Month = CALCULATE(SUM('SKU Active Or NotActive'[Activity last 6 months, no DP&Stock]), FilteredData)\n"
            "VAR Total = CALCULATE(SUM('SKU Active Or NotActive'[Grand Total]), FilteredData)\n"
            "RETURN DIVIDE(Inactive + Last6Month, Total, 0)"
        )
        r = _sql(translator, dax, table_key="SKU Active Or NotActive")
        assert r.is_translatable
        # Body matches CSV new_expr exactly (physical col names via to_snake_case).
        assert r.sql_expr == (
            "COALESCE((COALESCE(SUM(source.inactive), 0) + "
            "COALESCE(SUM(source.activity_last_6_months_no_dp_stock), 0)) / "
            "NULLIF(SUM(source.grand_total), 0), 0)"
        )
        # Latest-week logic → a semi-additive window (M11).
        assert r.window_spec is not None
        assert r.window_spec["semiadditive"] == "last"
        assert r.window_spec["range"] == "current"
        assert r.window_spec["order"]  # the period/week order column


# ── M8 — measure-name sanitization to [a-z0-9_] ──────────────────────────────


class TestMeasureNameSanitization:
    # The invalid PBI names from the CSV `renamed` rows.
    _INVALID = [
        "cycle_time(order intake to settlement)",
        "FTL ship. dispatched automatically %",
        "DSD Orders executed & settled via Osapiens %",
        "Touchless settlement, billing and invoicing %",
        "% of DSD shipments dispatched automatically (via automatic dispatching)",
    ]

    @pytest.mark.parametrize("raw", _INVALID)
    def test_sanitize_produces_valid_identifier(self, raw):
        out = sanitize_measure_name(raw)
        assert re.fullmatch(r"[a-z0-9_]+", out), f"invalid identifier: {out!r}"

    def test_ampersand_becomes_and(self):
        assert (
            sanitize_measure_name("DSD executed & settled")
            == "dsd_executed_and_settled"
        )

    def test_idempotent_on_valid_name(self):
        valid = "orders_delivered_on_time_pct"
        assert sanitize_measure_name(valid) == valid

    @pytest.mark.parametrize("raw", _INVALID)
    def test_wired_translate_path_yields_valid_name(self, translator, raw):
        """The full to_snake_case → sanitize path (as used in translate())."""
        r = translator.translate(
            {
                "measure_name": raw,
                "dax_expression": "SUM('Fact_OTC'[fltp])",
                "original_name": raw,
            },
            "Fact_OTC",
        )
        assert re.fullmatch(r"[a-z0-9_]+", r.measure_name)
        assert r.measure_name == sanitize_measure_name(to_snake_case(raw))


# ── llm_first (trivial_only) — the defect matchers run deterministically ─────


class TestFastPathMode:
    def test_defect_matchers_run_in_trivial_only(self, translator):
        dax = (
            "VAR _Code1Sum = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = \"KDST01215\")\n"
            "VAR _Code2Sum = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = \"KDST01216\")\n"
            "VAR _Div = DIVIDE(_Code2Sum,_Code1Sum)\n"
            "RETURN IF( ISBLANK(_Div) , BLANK(), 1 - _Div )"
        )
        r = translator.translate(
            {"measure_name": "m", "dax_expression": dax, "original_name": "m"},
            "Fact_OTC",
            trivial_only=True,
        )
        assert r.is_translatable
        assert "bic_csubkbi = 'KDST01216'" in r.sql_expr
