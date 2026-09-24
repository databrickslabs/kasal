"""
Unit tests for pbi_validation.py

All tests use injected ``evaluate_fn`` mocks — no live PBI or UC credentials
are required.  Fixtures are derived from the OTC Management post-mortem:

* NPS Total measure (PBI correct: 74.37, wrong old version: 35,901,235)
* Dim_Country row-count validation (31 rows)
* Mismatch detection with and without country dimension
"""

import pytest

from src.services.tools.metric_view_utils.pbi_validation import (
    ColumnMismatch,
    EvaluateFn,
    MeasureSpec,
    MeasureValidationResult,
    ValidationStatus,
    ViewValidationResult,
    _compare_period_country,
    _compare_period_sums,
    _normalise_rows,
    _strip_pbi_prefix,
    _to_float,
    validate_measure,
    validate_view,
)


# ---------------------------------------------------------------------------
# Helper: build a mock evaluate_fn
# ---------------------------------------------------------------------------


def _make_evaluate_fn(pbi_rows: list, uc_rows: list) -> EvaluateFn:
    """Return an evaluate_fn that routes EVALUATE → pbi_rows, else → uc_rows."""

    def fn(query: str) -> list:
        if query.strip().upper().startswith("EVALUATE"):
            return list(pbi_rows)
        return list(uc_rows)

    return fn


def _make_error_fn(pbi_error: bool = False, uc_error: bool = False) -> EvaluateFn:
    def fn(query: str) -> list:
        if query.strip().upper().startswith("EVALUATE"):
            if pbi_error:
                raise RuntimeError("PBI connection refused")
            return [{"fiscper_date": "2026-08-15", "val": 74.37}]
        if uc_error:
            raise RuntimeError("UC warehouse timeout")
        return [{"fiscper_date": "2026-08-15", "val": 74.37}]

    return fn


# ---------------------------------------------------------------------------
# Internal helper tests
# ---------------------------------------------------------------------------


def test_to_float_none():
    assert _to_float(None) is None


def test_to_float_int():
    assert _to_float(42) == 42.0


def test_to_float_string_numeric():
    assert _to_float("74.37") == pytest.approx(74.37)


def test_to_float_string_non_numeric():
    assert _to_float("n/a") is None


def test_strip_pbi_prefix_bracket_form():
    assert _strip_pbi_prefix("Fact_NPS[country_id]") == "country_id"


def test_strip_pbi_prefix_table_bracket():
    assert _strip_pbi_prefix("'Fact_NPS'[fiscper_date]") == "fiscper_date"


def test_strip_pbi_prefix_plain():
    assert _strip_pbi_prefix("country_id") == "country_id"


def test_normalise_rows_strips_prefix():
    rows = [{"'Fact_NPS'[country_id]": "IE", "'Fact_NPS'[val]": 74.37}]
    normalised = _normalise_rows(rows)
    assert "country_id" in normalised[0]
    assert "val" in normalised[0]
    assert normalised[0]["country_id"] == "IE"


# ---------------------------------------------------------------------------
# _compare_period_sums
# ---------------------------------------------------------------------------


def test_compare_period_sums_match():
    pbi = [
        {"period": "2026-08", "amount": 100.0},
        {"period": "2026-09", "amount": 200.0},
    ]
    uc = [
        {"period": "2026-08", "amount": 100.0},
        {"period": "2026-09", "amount": 200.0},
    ]
    mismatches = _compare_period_sums(pbi, uc, "period", ["amount"], 1e-4)
    assert mismatches == []


def test_compare_period_sums_mismatch():
    pbi = [{"period": "2026-08", "amount": 100.0}]
    uc = [{"period": "2026-08", "amount": 99.0}]
    mismatches = _compare_period_sums(pbi, uc, "period", ["amount"], 1e-4)
    assert len(mismatches) == 1
    assert mismatches[0].abs_diff == pytest.approx(1.0)


def test_compare_period_sums_missing_uc_period():
    """Period present in PBI but absent in UC → mismatch."""
    pbi = [{"period": "2026-07", "amount": 50.0}, {"period": "2026-08", "amount": 100.0}]
    uc = [{"period": "2026-08", "amount": 100.0}]
    mismatches = _compare_period_sums(pbi, uc, "period", ["amount"], 1e-4)
    assert any(m.period == "2026-07" for m in mismatches)


def test_compare_period_sums_tolerance():
    """Differences within tolerance are not flagged."""
    pbi = [{"period": "2026-08", "amount": 100.0}]
    uc = [{"period": "2026-08", "amount": 100.00005}]
    mismatches = _compare_period_sums(pbi, uc, "period", ["amount"], tolerance=1e-3)
    assert mismatches == []


# ---------------------------------------------------------------------------
# _compare_period_country
# ---------------------------------------------------------------------------


def test_compare_period_country_both_match():
    pbi = [
        {"period": "2026-08", "country": "IE", "val": 74.37},
        {"period": "2026-08", "country": "PL", "val": 68.0},
    ]
    uc = list(pbi)
    mismatches = _compare_period_country(pbi, uc, "period", "country", "val", 1e-4)
    assert mismatches == []


def test_compare_period_country_one_wrong():
    pbi = [{"period": "2026-08", "country": "IE", "val": 74.37}]
    uc = [{"period": "2026-08", "country": "IE", "val": 35_901_235.0}]
    mismatches = _compare_period_country(pbi, uc, "period", "country", "val", 1e-4)
    assert len(mismatches) == 1
    mm = mismatches[0]
    assert mm.pbi_value == pytest.approx(74.37)
    assert mm.generated_value == pytest.approx(35_901_235.0)


def test_compare_period_country_no_country_col():
    pbi = [{"period": "2026-08", "val": 74.37}]
    uc = [{"period": "2026-08", "val": 74.37}]
    mismatches = _compare_period_country(pbi, uc, "period", None, "val", 1e-4)
    assert mismatches == []


# ---------------------------------------------------------------------------
# validate_view: happy path
# ---------------------------------------------------------------------------


def test_validate_view_verified_row_count_match():
    """Identical row counts → VERIFIED (no period_col provided)."""
    rows = [{"country_id": "IE"}, {"country_id": "PL"}]
    fn = _make_evaluate_fn(pbi_rows=rows, uc_rows=rows)
    result = validate_view("Dim_Country", "SELECT * FROM dim_country", fn)

    assert isinstance(result, ViewValidationResult)
    assert result.status == ValidationStatus.VERIFIED
    assert result.pbi_row_count == 2
    assert result.generated_row_count == 2
    assert result.row_count_match is True
    assert result.period_mismatches == []


def test_validate_view_unverified_row_count_mismatch():
    """Row count differs → UNVERIFIED."""
    pbi = [{"id": i} for i in range(31)]  # OTC: 31 Dim_Country rows
    uc = [{"id": i} for i in range(29)]   # 2 rows missing
    fn = _make_evaluate_fn(pbi_rows=pbi, uc_rows=uc)
    result = validate_view("Dim_Country", "SELECT * FROM dim_country_view", fn)

    assert result.status == ValidationStatus.UNVERIFIED
    assert result.pbi_row_count == 31
    assert result.generated_row_count == 29
    assert result.row_count_match is False


def test_validate_view_verified_with_period_col():
    """Row counts match and per-period sums match → VERIFIED."""
    pbi = [
        {"fiscper_date": "2026-08-15", "CustomerPulsedTotal": 1000.0},
        {"fiscper_date": "2026-07-15", "CustomerPulsedTotal": 950.0},
    ]
    fn = _make_evaluate_fn(pbi_rows=pbi, uc_rows=pbi)
    result = validate_view(
        "Fact_CustomerExp",
        "SELECT * FROM fact_ce",
        fn,
        period_col="fiscper_date",
        numeric_cols=["CustomerPulsedTotal"],
    )
    assert result.status == ValidationStatus.VERIFIED
    assert result.period_mismatches == []


def test_validate_view_unverified_sum_mismatch():
    """Row counts match but per-period sums differ → UNVERIFIED."""
    pbi = [
        {"fiscper_date": "2026-08-15", "amount": 1000.0},
    ]
    uc = [
        {"fiscper_date": "2026-08-15", "amount": 800.0},  # 200 off
    ]
    fn = _make_evaluate_fn(pbi_rows=pbi, uc_rows=uc)
    result = validate_view(
        "FactTable",
        "SELECT * FROM fact_view",
        fn,
        period_col="fiscper_date",
        numeric_cols=["amount"],
    )
    assert result.status == ValidationStatus.UNVERIFIED
    assert len(result.period_mismatches) == 1
    assert result.period_mismatches[0].abs_diff == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# validate_view: error paths
# ---------------------------------------------------------------------------


def test_validate_view_pbi_error():
    fn = _make_error_fn(pbi_error=True)
    result = validate_view("Dim_Country", "SELECT * FROM v", fn)
    assert result.status == ValidationStatus.ERROR
    assert "PBI" in result.error
    assert "PBI connection refused" in result.error


def test_validate_view_uc_error():
    fn = _make_error_fn(uc_error=True)
    result = validate_view("Dim_Country", "SELECT * FROM v", fn)
    assert result.status == ValidationStatus.ERROR
    assert "UC" in result.error
    assert "UC warehouse timeout" in result.error


# ---------------------------------------------------------------------------
# validate_measure: happy path
# ---------------------------------------------------------------------------

NPS_SPEC = MeasureSpec(
    pbi_measure_name="NPS Total",
    pbi_table_name="Fact_NPS",
    metric_view_fqn="`dc_adb-landing-zone-002`.`idor`.`otc_ucm_mv_fact_nps`",
    measure_sql_name="nps_total",
)


def test_validate_measure_verified():
    """OTC NPS Total = 74.37 per period, per country → VERIFIED."""
    rows = [
        {"fiscper_date": "2026-08-15", "country_id": "IE", "val": 74.37},
        {"fiscper_date": "2026-08-15", "country_id": "PL", "val": 68.5},
    ]
    fn = _make_evaluate_fn(pbi_rows=rows, uc_rows=rows)
    result = validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn)

    assert isinstance(result, MeasureValidationResult)
    assert result.status == ValidationStatus.VERIFIED
    assert result.mismatches == []
    assert result.pbi_total == pytest.approx(74.37 + 68.5)
    assert result.generated_total == pytest.approx(74.37 + 68.5)


def test_validate_measure_unverified_classic_nps_bug():
    """Reproduce the M1 bug: old Kasal generated 35,901,235 instead of 74.37."""
    pbi_rows = [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 74.37}]
    uc_rows = [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 35_901_235.0}]
    fn = _make_evaluate_fn(pbi_rows=pbi_rows, uc_rows=uc_rows)
    result = validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn)

    assert result.status == ValidationStatus.UNVERIFIED
    assert len(result.mismatches) == 1
    mm = result.mismatches[0]
    assert mm.pbi_value == pytest.approx(74.37)
    assert mm.generated_value == pytest.approx(35_901_235.0)
    assert mm.period == "2026-08-15"
    assert mm.country == "IE"


def test_validate_measure_no_country_col():
    """validate_measure without country_col groups by period only."""
    pbi_rows = [{"fiscper_date": "2026-08-15", "val": 74.37}]
    uc_rows = [{"fiscper_date": "2026-08-15", "val": 74.37}]
    fn = _make_evaluate_fn(pbi_rows=pbi_rows, uc_rows=uc_rows)
    result = validate_measure(NPS_SPEC, "fiscper_date", None, fn)
    assert result.status == ValidationStatus.VERIFIED


def test_validate_measure_pbi_query_contains_evaluate():
    """Verify the evaluate_fn is called with an EVALUATE... query for PBI."""
    captured = []

    def fn(query: str) -> list:
        captured.append(query)
        return [{"fiscper_date": "2026-08-15", "val": 74.37}]

    validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn)
    assert any(q.strip().startswith("EVALUATE") for q in captured)


def test_validate_measure_uc_query_uses_measure_syntax():
    """The UC SQL should use MEASURE() and FROM the metric view fqn."""
    captured = []

    def fn(query: str) -> list:
        captured.append(query)
        return [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 74.37}]

    validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn)
    uc_queries = [q for q in captured if not q.strip().startswith("EVALUATE")]
    assert uc_queries
    uc_sql = uc_queries[0]
    assert "MEASURE(nps_total)" in uc_sql
    assert "`dc_adb-landing-zone-002`.`idor`.`otc_ucm_mv_fact_nps`" in uc_sql
    assert "GROUP BY ALL" in uc_sql


# ---------------------------------------------------------------------------
# validate_measure: error paths
# ---------------------------------------------------------------------------


def test_validate_measure_pbi_error():
    fn = _make_error_fn(pbi_error=True)
    result = validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn)
    assert result.status == ValidationStatus.ERROR
    assert "PBI evaluate failed" in result.error


def test_validate_measure_uc_error():
    fn = _make_error_fn(uc_error=True)
    result = validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn)
    assert result.status == ValidationStatus.ERROR
    assert "UC evaluate failed" in result.error


# ---------------------------------------------------------------------------
# Tolerance
# ---------------------------------------------------------------------------


def test_validate_measure_within_tolerance():
    pbi_rows = [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 74.37}]
    uc_rows = [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 74.3700001}]
    fn = _make_evaluate_fn(pbi_rows=pbi_rows, uc_rows=uc_rows)
    result = validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn, tolerance=1e-4)
    assert result.status == ValidationStatus.VERIFIED


def test_validate_measure_outside_tolerance():
    pbi_rows = [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 74.37}]
    uc_rows = [{"fiscper_date": "2026-08-15", "country_id": "IE", "val": 73.0}]
    fn = _make_evaluate_fn(pbi_rows=pbi_rows, uc_rows=uc_rows)
    result = validate_measure(NPS_SPEC, "fiscper_date", "country_id", fn, tolerance=0.01)
    assert result.status == ValidationStatus.UNVERIFIED


# ---------------------------------------------------------------------------
# PBI column-name normalisation in validate_view
# ---------------------------------------------------------------------------


def test_validate_view_normalises_pbi_column_names():
    """PBI returns columns like 'Fact_NPS[country_id]'; validate_view should handle them."""
    pbi_rows = [
        {"Fact_NPS[country_id]": "IE", "Fact_NPS[fltp]": 100.0},
        {"Fact_NPS[country_id]": "PL", "Fact_NPS[fltp]": 200.0},
    ]
    uc_rows = [
        {"country_id": "IE", "fltp": 100.0},
        {"country_id": "PL", "fltp": 200.0},
    ]
    fn = _make_evaluate_fn(pbi_rows=pbi_rows, uc_rows=uc_rows)
    result = validate_view(
        "Fact_OTC",
        "SELECT * FROM v",
        fn,
        period_col="country_id",
        numeric_cols=["fltp"],
    )
    assert result.status == ValidationStatus.VERIFIED
