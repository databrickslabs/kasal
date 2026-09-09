"""Tests for recovery_recommender (KASAL_FIXES Gaps 4 & 5)."""

from src.services.tools.metric_view_utils.recovery_recommender import (
    draft_source_view,
    recommend,
)


def test_draft_crossfact_unions_the_facts():
    dax = (
        "var a = SUMX(fact_pe005, fact_pe005[val]) "
        "var b = SUMX(fact_scorecard_Actuals_wc, fact_scorecard_Actuals_wc[val]) RETURN a+b"
    )
    d = draft_source_view(dax, fact_table="fact_pe005")
    assert d and "DRAFT · UNVERIFIED" in d
    assert "UNION ALL" in d and "fact_pe005" in d and "fact_scorecard_Actuals_wc" in d
    assert "CREATE OR REPLACE VIEW" in d


def test_draft_multistage_precompute_view():
    dax = (
        "VAR t = SUMMARIZE(FILTER('F', TRUE()), 'F'[k], \"x\", 1) "
        "RETURN AVERAGEX(t, [x])"
    )
    d = draft_source_view(dax, fact_table="survey_responses", measure_name="adc")
    assert d and "DRAFT · UNVERIFIED" in d
    assert "GROUP BY <grain_cols>" in d and "SEPARATE UCMV" in d


def test_draft_none_for_simple_measure():
    assert draft_source_view("SUM('Fact'[amount])", fact_table="Fact") is None


def test_gap5_multistage_aggregation():
    dax = (
        "VAR _u = UNION(SUMMARIZE(FILTER('Fact', TRUE()), 'Fact'[Profile], \"AvgDC\", "
        "AVERAGEX(CURRENTGROUP(), [x]))) RETURN AVERAGEX(_u, [AvgDC])"
    )
    rec = recommend(dax)
    assert rec and "Multi-stage aggregation" in rec and "SEPARATE metric view" in rec


def test_gap4_manyside_filter_gets_exists_recipe():
    dax = (
        "CALCULATE(COUNT('Interactions - Fact'[_SK_INTERACTION]), "
        "'Termination Reason'[Reason] = \"Unmet Legal Requirements\")"
    )
    rec = recommend(
        dax,
        fact_table="Interactions - Fact",
        m2n_tables={"Termination Reason"},
        join_tables={"dim_interaction"},
    )
    assert rec and "EXISTS" in rec and "Termination Reason" in rec


def test_gap4_zero_match_flags_not_exists():
    dax = (
        "VAR p = FILTER(VALUES('Fact'[Profile Id]), "
        "CALCULATE(COUNTROWS('Fact'), 'Termination Reason'[Reason]=\"x\") = 0) "
        "RETURN CALCULATE(DISTINCTCOUNT('Fact'[Profile Id]), p)"
    )
    rec = recommend(dax, m2n_tables={"Termination Reason"})
    assert rec and "NOT EXISTS" in rec


def test_no_recipe_for_plain_filter():
    # a filter on an already-joined dim → not a Gap 4/5 case
    dax = "CALCULATE(COUNT('Fact'[id]), 'Dim'[flag] = \"y\")"
    assert recommend(dax, m2n_tables=set(), join_tables={"Dim"}) is None


def test_no_recipe_for_simple_measure():
    assert recommend("SUM('Fact'[amount])") is None


def test_generic_zero_match_without_known_m2n():
    dax = (
        "CALCULATE(DISTINCTCOUNT('Fact'[Profile Id]), "
        "FILTER(VALUES('Fact'[Profile Id]), CALCULATE(COUNTROWS('Fact'), 'Q'[id]=\"5.4\")=0))"
    )
    rec = recommend(dax)
    assert rec and "NOT-EXISTS" in rec.upper()
