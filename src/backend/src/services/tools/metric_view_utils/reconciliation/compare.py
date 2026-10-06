"""Join / delta / tolerance / summary logic (pure, pandas).

Joins the UCMV-side and PBI-side results on (dimension, period), computes a
per-cell delta, decides ``within_tolerance`` (absolute or relative), and rolls
up to a per-measure summary. Output is a long, one-row-per-cell frame plus a
per-measure summary — the shape the "431/467 at 100%, 99.84% of cells" headline
is computed from.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from .mapping import UCMVMapping
from .periods import is_closed_period, matches_reference_years, normalize_snapshot_date

LONG_FORMAT_COLUMNS = [
    "run_id",
    "run_timestamp",
    "ucmv_table",
    "pbi_semantic_model_id",
    "period",
    "dimension_type",
    "dimension",
    "ucmv_kbi_name",
    "pbi_kbi_name",
    "pbi_sub_kbi_name",
    "pbi_sub_kbi_name2",
    "ucmv_value",
    "pbi_value",
    "delta",
    "within_tolerance",
]


def compare(
    ucmv_df: pd.DataFrame,
    pbi_df: pd.DataFrame,
    mapping: UCMVMapping,
    dimension_name: str,
    reference_years: list,
) -> pd.DataFrame:
    """Wide comparison: one row per (dimension, period), one
    ``<measure>_ucmv``/``_pbi``/``_delta`` triple per measure."""
    snapshot = mapping.binding.time_dimension.grain == "snapshot"
    if snapshot:
        ucmv_df, pbi_df = ucmv_df.copy(), pbi_df.copy()
        for frame in (ucmv_df, pbi_df):
            frame["period"] = frame["period"].map(normalize_snapshot_date)
            if frame.duplicated([dimension_name, "period"]).any():
                raise ValueError(
                    "Duplicate dimension/snapshot keys would multiply comparison rows."
                )
        ucmv_df = ucmv_df[
            ucmv_df["period"].apply(
                lambda p: matches_reference_years(p, reference_years)
            )
        ]
        periods = set(ucmv_df["period"])
        pbi_df = pbi_df[pbi_df["period"].isin(periods)]
    else:
        ucmv_df = ucmv_df[ucmv_df["period"].apply(is_closed_period)].reset_index(
            drop=True
        )
        pbi_df = pbi_df[
            pbi_df["period"].apply(
                lambda p: matches_reference_years(p, reference_years)
                and is_closed_period(p)
            )
        ].reset_index(drop=True)

    measures = mapping.resolved_measures()
    merged = ucmv_df.merge(
        pbi_df,
        on=[dimension_name, "period"],
        how="outer",
        suffixes=("_ucmv", "_pbi"),
        indicator=True,
    )
    for m in measures:
        ucmv_col = f"{m.ucmv_measure}_ucmv"
        pbi_col = f"{m.ucmv_measure}_pbi"
        delta_col = f"{m.ucmv_measure}_delta"
        # A measure absent from one side entirely (KeyError-safe): create as NA.
        if ucmv_col not in merged:
            merged[ucmv_col] = pd.NA
        if pbi_col not in merged:
            merged[pbi_col] = pd.NA
        if snapshot:
            ucmv_blank = merged[ucmv_col].isna() | (
                m.null_equals_zero & merged[ucmv_col].eq(0).fillna(False)
            )
            pbi_blank = merged[pbi_col].isna() | (
                m.null_equals_zero & merged[pbi_col].eq(0).fillna(False)
            )
            matching_blank = ucmv_blank & pbi_blank & merged["_merge"].eq("both")
            merged[delta_col] = (merged[ucmv_col] - merged[pbi_col]).mask(
                matching_blank, 0
            )
        else:
            merged[delta_col] = (
                merged[ucmv_col].fillna(0) - merged[pbi_col].fillna(0)
            ).round(6)

    cols = [dimension_name, "period"] + [
        f"{m.ucmv_measure}{s}" for m in measures for s in ("_ucmv", "_pbi", "_delta")
    ]
    return merged[cols].sort_values([dimension_name, "period"]).reset_index(drop=True)


def _within_tolerance(sub: pd.DataFrame, m) -> pd.Series:
    if m.tolerance_kind == "absolute":
        return sub["delta"].abs() <= m.tolerance_value
    denom = pd.to_numeric(sub["ucmv_value"], errors="coerce").abs()
    rel_ok = (sub["delta"].abs() / denom.replace(0, pd.NA)) <= m.tolerance_value
    # A zero/blank ucmv_value with an exactly-zero delta means PBI is blank too —
    # an exact match, not an undefined ratio.
    zero_exact = (denom.isna() | (denom == 0)) & (sub["delta"].abs() <= 1e-9)
    return rel_ok.fillna(False) | zero_exact


def to_long_format(
    wide_df: pd.DataFrame,
    mapping: UCMVMapping,
    dimension_name: str,
    run_id: Optional[str] = None,
    run_timestamp: Optional[datetime] = None,
) -> pd.DataFrame:
    """Melt the wide comparison into one row per (dimension x period x measure),
    with ``within_tolerance`` decided per cell."""
    run_id = run_id or str(uuid.uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    binding = mapping.binding

    rows = []
    for m in mapping.resolved_measures():
        ucmv_col = f"{m.ucmv_measure}_ucmv"
        pbi_col = f"{m.ucmv_measure}_pbi"
        delta_col = f"{m.ucmv_measure}_delta"
        sub = wide_df[[dimension_name, "period", ucmv_col, pbi_col, delta_col]].copy()
        sub = sub.rename(
            columns={
                dimension_name: "dimension",
                ucmv_col: "ucmv_value",
                pbi_col: "pbi_value",
                delta_col: "delta",
            }
        )
        sub["run_id"] = run_id
        sub["run_timestamp"] = run_timestamp
        sub["ucmv_table"] = binding.ucmv_table
        sub["pbi_semantic_model_id"] = binding.pbi_semantic_model_id
        sub["dimension_type"] = dimension_name
        sub["ucmv_kbi_name"] = m.ucmv_measure
        sub["pbi_kbi_name"] = m.pbi_field_name
        sub["pbi_sub_kbi_name"] = m.switch_selector_value
        sub["pbi_sub_kbi_name2"] = m.switch_selector2_value
        sub["within_tolerance"] = _within_tolerance(sub, m)
        rows.append(sub)

    long_df = (
        pd.concat(rows, ignore_index=True)
        if rows
        else pd.DataFrame(columns=LONG_FORMAT_COLUMNS)
    )
    return long_df[LONG_FORMAT_COLUMNS]


_SUMMARY_ID_COLUMNS = [
    "ucmv_table",
    "ucmv_kbi_name",
    "pbi_kbi_name",
    "pbi_sub_kbi_name",
    "pbi_sub_kbi_name2",
]
_SUMMARY_COLUMNS = _SUMMARY_ID_COLUMNS + [
    "total",
    "aligned",
    "pct_aligned",
    "max_abs_delta",
    "n_periods",
    "periods_failing",
    "pct_periods_failing",
    "n_dims",
    "dims_failing",
    "pct_dims_failing",
]


def _axis_fail_breakdown(long_df: pd.DataFrame, axis_col: str) -> pd.DataFrame:
    per_axis = (
        long_df.groupby(_SUMMARY_ID_COLUMNS + [axis_col], dropna=False)[
            "within_tolerance"
        ]
        .agg(_cells="size", _aligned="sum")
        .reset_index()
    )
    per_axis["_axis_failing"] = per_axis["_aligned"] < per_axis["_cells"]
    return (
        per_axis.groupby(_SUMMARY_ID_COLUMNS, dropna=False)
        .agg(_n=(axis_col, "size"), _failing=("_axis_failing", "sum"))
        .reset_index()
    )


def summarize(long_df: pd.DataFrame) -> pd.DataFrame:
    """Per-measure alignment-rate summary, worst-aligned first, with a roll-up of
    the period/dimension failure breakdown."""
    if long_df.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)

    grouped = long_df.groupby(_SUMMARY_ID_COLUMNS, dropna=False)
    out = grouped.agg(
        total=("delta", "size"),
        aligned=("within_tolerance", "sum"),
        max_abs_delta=("delta", lambda s: s.abs().max()),
    ).reset_index()
    out["pct_aligned"] = (100 * out["aligned"] / out["total"]).round(1)

    per_period = _axis_fail_breakdown(long_df, "period").rename(
        columns={"_n": "n_periods", "_failing": "periods_failing"}
    )
    per_dim = _axis_fail_breakdown(long_df, "dimension").rename(
        columns={"_n": "n_dims", "_failing": "dims_failing"}
    )
    out = out.merge(per_period, on=_SUMMARY_ID_COLUMNS, how="left").merge(
        per_dim, on=_SUMMARY_ID_COLUMNS, how="left"
    )
    out["pct_periods_failing"] = (
        100 * out["periods_failing"] / out["n_periods"]
    ).round(1)
    out["pct_dims_failing"] = (100 * out["dims_failing"] / out["n_dims"]).round(1)

    return out.sort_values(["pct_aligned"], ascending=[True])[
        _SUMMARY_COLUMNS
    ].reset_index(drop=True)


def _summarize_by(long_df: pd.DataFrame, extra_col: str) -> pd.DataFrame:
    cols = [
        "ucmv_table",
        "ucmv_kbi_name",
        "pbi_sub_kbi_name",
        "pbi_sub_kbi_name2",
        extra_col,
    ]
    if long_df.empty:
        return pd.DataFrame(
            columns=cols + ["total", "aligned", "pct_aligned", "max_abs_delta"]
        )
    grouped = long_df.groupby(cols, dropna=False)
    out = grouped.agg(
        total=("delta", "size"),
        aligned=("within_tolerance", "sum"),
        max_abs_delta=("delta", lambda s: s.abs().max()),
    ).reset_index()
    out["pct_aligned"] = (100 * out["aligned"] / out["total"]).round(1)
    return out.sort_values(["ucmv_kbi_name", extra_col]).reset_index(drop=True)


def summarize_by_period(long_df: pd.DataFrame) -> pd.DataFrame:
    """Alignment rate per (measure, period) — separates a trailing-edge data-lag
    pattern from a consistent failure."""
    return _summarize_by(long_df, "period")


def summarize_by_dimension(long_df: pd.DataFrame) -> pd.DataFrame:
    """Alignment rate per (measure, dimension value) — separates a
    dimension-specific pattern from an even failure."""
    return _summarize_by(long_df, "dimension")
