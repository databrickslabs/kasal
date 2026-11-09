"""Statistics-only reconciliation report (pure, pandas).

For an account that must not see KPI values. The report holds only counts, shares
and the largest RELATIVE difference of comparable cells. It carries no PBI or UC
value, no absolute difference, and no delta.

Values are read in exactly one place, :func:`value_free_cells`, which reduces the
long comparison frame to flags, the group of each cell, and the relative
difference. Everything downstream works on that value-free frame, so a value
cannot reach the report by accident.

Periods and dimension members appear in the report as labels (they are keys, not
figures).
"""

from __future__ import annotations

from typing import TypedDict

import pandas as pd

from .cells import CELL_COMPARABLE, CELL_EXTRA_IN_UC, CELL_MISSING_IN_UC, classify_cells

# Failing periods / members listed per measure; the rest is counted.
BREAKDOWN_LIMIT = 40

_MEASURE_KEY = "ucmv_kbi_name"


class MeasureStatistics(TypedDict):
    """One measure's value-free statistics (every field is a count, a share, a
    label or the relative difference)."""

    cells: int
    aligned: int
    pct_aligned: float
    comparable: int
    comparable_aligned: int
    missing_in_uc: int
    extra_in_uc: int
    both_empty: int
    max_relative_difference: float | None
    n_periods: int
    periods_failing: int
    n_dims: int
    dims_failing: int
    failing_periods: list[str]
    periods: list[dict[str, object]]
    periods_omitted: int
    members: list[dict[str, object]]
    members_omitted: int


def value_free_cells(long_df: pd.DataFrame) -> pd.DataFrame:
    """Reduce the long comparison frame to value-free columns.

    Reads the values to classify each cell and to compute the relative difference
    of comparable cells, then keeps only keys, flags and that relative difference.
    """
    ucmv = pd.to_numeric(long_df["ucmv_value"], errors="coerce")
    pbi = pd.to_numeric(long_df["pbi_value"], errors="coerce")
    group = classify_cells(
        long_df["ucmv_value"], long_df["pbi_value"], long_df["within_tolerance"]
    )
    scale = pd.concat([ucmv.abs(), pbi.abs()], axis=1).max(axis=1)
    relative = ((ucmv - pbi).abs() / scale).where(group == CELL_COMPARABLE)

    frame = long_df[[_MEASURE_KEY, "dimension", "period"]].copy()
    frame["is_aligned"] = long_df["within_tolerance"].fillna(False).astype(bool)
    frame["is_comparable"] = group == CELL_COMPARABLE
    frame["is_missing"] = group == CELL_MISSING_IN_UC
    frame["is_extra"] = group == CELL_EXTRA_IN_UC
    frame["is_empty"] = ~(
        frame["is_comparable"] | frame["is_missing"] | frame["is_extra"]
    )
    frame["relative_difference"] = relative
    return frame


def _axis_failures(
    frame: pd.DataFrame, axis: str, label: str
) -> dict[str, list[dict[str, object]]]:
    """Per failing axis value (period or member) of one measure: its counts."""
    grouped = frame.groupby([_MEASURE_KEY, axis], dropna=False).agg(
        cells=("is_aligned", "size"),
        aligned=("is_aligned", "sum"),
        missing_in_uc=("is_missing", "sum"),
        extra_in_uc=("is_extra", "sum"),
    )
    failing = grouped[grouped["aligned"] < grouped["cells"]]
    rows: dict[str, list[dict[str, object]]] = {}
    for index, row in failing.iterrows():
        measure, value = index if isinstance(index, tuple) else (index, None)
        rows.setdefault(str(measure), []).append(
            {
                label: str(value),
                "cells": int(row["cells"]),
                "aligned": int(row["aligned"]),
                "missing_in_uc": int(row["missing_in_uc"]),
                "extra_in_uc": int(row["extra_in_uc"]),
            }
        )
    return rows


def statistics_by_measure(long_df: pd.DataFrame) -> dict[str, MeasureStatistics]:
    """Per measure: cell counts per group, shares, the largest relative difference
    of comparable cells, and the failing periods and members with their counts.

    Keys are ``ucmv_kbi_name``. The per-measure ``n_periods`` / ``periods_failing``
    / ``n_dims`` / ``dims_failing`` / ``failing_periods`` fields are the inputs
    the mismatch classifier needs.
    """
    if long_df.empty:
        return {}
    frame = value_free_cells(long_df)
    frame["_aligned_comparable"] = frame["is_comparable"] & frame["is_aligned"]
    per_measure = frame.groupby(_MEASURE_KEY, dropna=False).agg(
        cells=("is_aligned", "size"),
        aligned=("is_aligned", "sum"),
        comparable=("is_comparable", "sum"),
        comparable_aligned=("_aligned_comparable", "sum"),
        missing_in_uc=("is_missing", "sum"),
        extra_in_uc=("is_extra", "sum"),
        both_empty=("is_empty", "sum"),
        max_relative_difference=("relative_difference", "max"),
    )
    by_period = _axis_failures(frame, "period", "period")
    by_member = _axis_failures(frame, "dimension", "member")

    out: dict[str, MeasureStatistics] = {}
    for measure, row in per_measure.iterrows():
        name = str(measure)
        periods = frame.loc[frame[_MEASURE_KEY] == measure, "period"]
        members = frame.loc[frame[_MEASURE_KEY] == measure, "dimension"]
        failing_periods = by_period.get(name, [])
        failing_members = by_member.get(name, [])
        cells = int(row["cells"])
        aligned = int(row["aligned"])
        comparable = int(row["comparable"])
        max_rel = row["max_relative_difference"]
        out[name] = {
            "cells": cells,
            "aligned": aligned,
            "pct_aligned": round(100 * aligned / cells, 1) if cells else 0.0,
            "comparable": comparable,
            "comparable_aligned": int(row["comparable_aligned"]),
            "missing_in_uc": int(row["missing_in_uc"]),
            "extra_in_uc": int(row["extra_in_uc"]),
            "both_empty": int(row["both_empty"]),
            "max_relative_difference": (None if pd.isna(max_rel) else float(max_rel)),
            "n_periods": int(periods.nunique()),
            "periods_failing": len(failing_periods),
            "n_dims": int(members.nunique()),
            "dims_failing": len(failing_members),
            "failing_periods": sorted(str(p["period"]) for p in failing_periods),
            "periods": failing_periods[:BREAKDOWN_LIMIT],
            "periods_omitted": max(0, len(failing_periods) - BREAKDOWN_LIMIT),
            "members": failing_members[:BREAKDOWN_LIMIT],
            "members_omitted": max(0, len(failing_members) - BREAKDOWN_LIMIT),
        }
    return out
