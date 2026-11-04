"""Tests for cell groups, the statistics-only report and the scale check.

All inputs are synthetic. The distinctive values below are used to prove that no
PBI or UC value reaches a statistics-only report.
"""

import pandas as pd

from src.services.tools.metric_view_utils.reconciliation import (
    CELL_BOTH_EMPTY,
    CELL_COMPARABLE,
    CELL_EXTRA_IN_UC,
    CELL_MISSING_IN_UC,
    build_scale_query,
    classify_cells,
    run_scale_check,
    scale_verdict,
    statistics_by_measure,
)
from src.services.tools.metric_view_utils.reconciliation.mapping import (
    load_mapping_dict,
)

# Distinctive numbers: if any of them appears in a report, values leaked.
UC_LEAK = 7654321.125
PBI_LEAK = 0.31415926


def _long(rows):
    """Long frame with the columns the statistics read."""
    return pd.DataFrame(
        rows,
        columns=[
            "ucmv_kbi_name",
            "dimension",
            "period",
            "ucmv_value",
            "pbi_value",
            "within_tolerance",
        ],
    )


def test_classify_cells_groups_are_exclusive():
    ucmv = pd.Series([1.0, 0.0, None, 5.0, 0.0001])
    pbi = pd.Series([2.0, 3.0, 4.0, None, 0.0])
    within = pd.Series([False, False, False, False, True])
    groups = list(classify_cells(ucmv, pbi, within))
    assert groups == [
        CELL_COMPARABLE,
        CELL_MISSING_IN_UC,
        CELL_MISSING_IN_UC,
        CELL_EXTRA_IN_UC,
        CELL_BOTH_EMPTY,
    ]


def test_statistics_counts_and_relative_difference_only():
    long_df = _long(
        [
            ("m", "AT", "2025001", 10.0, 10.0, True),
            ("m", "AT", "2025002", 10.0, 9.5, False),
            ("m", "DE", "2025001", 0.0, 4.0, False),
            ("m", "DE", "2025002", 3.0, 0.0, False),
            ("m", "FR", "2025001", None, 0.0, True),
        ]
    )
    stats = statistics_by_measure(long_df)["m"]
    assert stats["cells"] == 5
    assert stats["aligned"] == 2
    assert stats["comparable"] == 2
    assert stats["comparable_aligned"] == 1
    assert stats["missing_in_uc"] == 1
    assert stats["extra_in_uc"] == 1
    assert stats["both_empty"] == 1
    assert abs(stats["max_relative_difference"] - 0.05) < 1e-9
    assert stats["periods_failing"] == 2
    assert stats["dims_failing"] == 2
    assert {p["period"] for p in stats["periods"]} == {"2025001", "2025002"}


def test_statistics_report_contains_no_values_or_absolute_deltas():
    long_df = _long(
        [
            ("m", "AT", "2025001", UC_LEAK, PBI_LEAK, False),
            ("m", "AT", "2025002", UC_LEAK, UC_LEAK + 1.0, False),
            ("m", "DE", "2025001", 1.0, 1.0, True),
        ]
    )
    stats = statistics_by_measure(long_df)
    rendered = repr(stats)
    assert str(UC_LEAK) not in rendered
    assert str(PBI_LEAK) not in rendered
    assert "max_abs_delta" not in rendered
    assert "delta" not in stats["m"]
    assert "ucmv_value" not in stats["m"]


def test_empty_frame_gives_empty_report():
    assert statistics_by_measure(_long([])) == {}


def test_scale_verdicts():
    assert scale_verdict(0, 0) == "no_data"
    assert scale_verdict(10, 1) == "fraction"
    assert scale_verdict(10, 9) == "0-100"


class _CountExecutor:
    """Returns one scale row built from a fixed count list (no values)."""

    def __init__(self, row):
        self.row = row
        self.sql = None

    def execute(self, sql):
        self.sql = sql
        return pd.DataFrame([self.row]), None


def _mapping():
    return load_mapping_dict(
        {
            "ucmv_table": "cat.sch.fact_sc",
            "pbi_semantic_model_id": "model",
            "pbi_fact_table": "Fact",
            "time_dimension": {
                "grain": "month",
                "ucmv": {"mode": "column", "name": "fiscper"},
                "pbi_column": "fiscper",
                "pbi_table": "Cal",
            },
            "default_dimension": "country",
            "dimensions": {
                "country": {
                    "ucmv_column": "country_group",
                    "pbi_dim_table": "Geo",
                    "pbi_dim_column": "country",
                }
            },
            "measures": [
                {
                    "ucmv_measure": "fill_pct",
                    "pbi_kind": "direct",
                    "pbi_measure": "Fill",
                },
                {"ucmv_measure": "cost", "pbi_kind": "direct", "pbi_measure": "Cost"},
            ],
        }
    )


def test_scale_query_counts_per_measure_and_groups_by_dimension():
    sql = build_scale_query("cat.sch.fact_sc", "country_group", ["fill_pct", "cost"])
    assert "GROUP BY `country_group`" in sql
    assert "MEASURE(`fill_pct`)" in sql and "MEASURE(`cost`)" in sql
    assert "COUNT_IF(ABS(m0) > 1.5)" in sql


def test_run_scale_check_flags_a_0_100_measure_by_counts_only():
    # groups=10; fill_pct: 10 non-null, 10 above 1.5 -> 0-100;
    # cost: 10 non-null, 0 above -> fraction
    executor = _CountExecutor([10, 10, 10, 10, 0])
    verdicts = run_scale_check(executor, _mapping(), "country")
    assert verdicts["fill_pct"]["verdict"] == "0-100"
    assert verdicts["cost"]["verdict"] == "fraction"
    assert set(verdicts["fill_pct"]) == {
        "groups",
        "non_null",
        "above_threshold",
        "threshold",
        "verdict",
    }
