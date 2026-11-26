"""The JSON-ready report of one reconciled view (pure, pandas).

Turns the long comparison frame of one view into the dict the tool returns:
per measure its alignment, classification and cell counts; the summary; the
measures the PBI side could not compare (timed out); the periods left out per
measure (calendar label NULL); and, when asked, the per-measure cross breakdown.

Two shapes: the default one with sample mismatches (values), and the
statistics-only one, which carries no values at all (see ``statistics.py``).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from .classify import MismatchStats, classify
from .compare import summarize
from .cross import cross_breakdown
from .mapping import UCMVMapping
from .statistics import statistics_by_measure

# Cap the number of mismatching cells reported per measure.
MAX_SAMPLE_MISMATCHES = 10


@dataclass(frozen=True)
class ViewOptions:
    """Output options of one view (operator-set, never LLM-set)."""

    statistics_only: bool = False
    cross_breakdown: bool = False
    pbi_refresh_period: Optional[str] = None
    mv_refresh_period: Optional[str] = None


@dataclass
class ViewRunInfo:
    """What the PBI side could not compare in this view."""

    timed_out: list[str] = field(default_factory=list)
    periods_skipped: dict[str, int] = field(default_factory=dict)
    cells_left_out: dict[str, int] = field(default_factory=dict)


def num(value: object) -> object:
    """Coerce a pandas/NumPy cell into a JSON-friendly number or None."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (float, np.floating)) and math.isnan(float(value)):
        return None
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def build_view_report(
    long_df: pd.DataFrame,
    mapping: UCMVMapping,
    dimension_name: str,
    options: ViewOptions,
    run: ViewRunInfo,
) -> dict[str, object]:
    """The report of one view. ``long_df`` holds only the compared cells."""
    if options.statistics_only:
        body = _statistics_body(long_df, mapping, options)
    else:
        body = _values_body(long_df, mapping, options)
    _count_run_info(body.measures, run)
    report: dict[str, object] = {"dimension": dimension_name}
    if options.statistics_only:
        report["statistics_only"] = True
    report["measures"] = body.measures
    report["summary"] = _summary(long_df, body.measures_at_100, len(body.measures), run)
    report["timed_out_measures"] = sorted(run.timed_out)
    if options.cross_breakdown:
        report["cross_breakdown"] = cross_breakdown(long_df)
    return report


@dataclass
class _Body:
    """Per-measure entries of one report shape, and how many reached 100 %."""

    measures: dict[str, dict[str, object]]
    measures_at_100: int


def _values_body(
    long_df: pd.DataFrame, mapping: UCMVMapping, options: ViewOptions
) -> _Body:
    summary = summarize(long_df)
    measures_out: dict[str, dict[str, object]] = {}
    at_100 = 0
    for _, row in summary.iterrows():
        name = row["ucmv_kbi_name"]
        measure = mapping.measure(name)
        cells = long_df[long_df["ucmv_kbi_name"] == name]
        failing = cells[~cells["within_tolerance"].astype(bool)]
        failing_periods = sorted(failing["period"].astype(str).unique().tolist())
        stats = MismatchStats(
            pct_aligned=float(row["pct_aligned"]),
            n_periods=int(row["n_periods"]),
            periods_failing=int(row["periods_failing"]),
            n_dims=int(row["n_dims"]),
            dims_failing=int(row["dims_failing"]),
            failing_periods=failing_periods,
            value_format=measure.value_format,
        )
        samples = [
            {
                "dimension": s["dimension"],
                "period": s["period"],
                "ucmv_value": num(s["ucmv_value"]),
                "pbi_value": num(s["pbi_value"]),
                "delta": num(s["delta"]),
            }
            for _, s in failing.head(MAX_SAMPLE_MISMATCHES).iterrows()
        ]
        pct_aligned = float(row["pct_aligned"])
        if pct_aligned >= 100:
            at_100 += 1
        measures_out[name] = {
            "pct_aligned": pct_aligned,
            "cells_total": int(row["total"]),
            "cells_aligned": int(row["aligned"]),
            "classification": classify(
                stats,
                pbi_refresh_period=options.pbi_refresh_period,
                mv_refresh_period=options.mv_refresh_period,
            ),
            "sample_mismatches": samples,
        }
    return _Body(measures=measures_out, measures_at_100=at_100)


def _statistics_body(
    long_df: pd.DataFrame, mapping: UCMVMapping, options: ViewOptions
) -> _Body:
    """Counts, shares and the largest RELATIVE difference per measure. No values,
    no absolute deltas."""
    measures_out: dict[str, dict[str, object]] = {}
    at_100 = 0
    for name, stats in statistics_by_measure(long_df).items():
        measure = mapping.measure(name)
        classification = classify(
            MismatchStats(
                pct_aligned=stats["pct_aligned"],
                n_periods=stats["n_periods"],
                periods_failing=stats["periods_failing"],
                n_dims=stats["n_dims"],
                dims_failing=stats["dims_failing"],
                failing_periods=stats["failing_periods"],
                value_format=measure.value_format,
            ),
            pbi_refresh_period=options.pbi_refresh_period,
            mv_refresh_period=options.mv_refresh_period,
        )
        if stats["pct_aligned"] >= 100:
            at_100 += 1
        # failing_periods is the classifier's input; the report lists periods once.
        entry: dict[str, object] = {
            key: value for key, value in stats.items() if key != "failing_periods"
        }
        entry["classification"] = classification
        measures_out[name] = entry
    return _Body(measures=measures_out, measures_at_100=at_100)


def _count_run_info(
    measures_out: dict[str, dict[str, object]], run: ViewRunInfo
) -> None:
    """Per measure: how many periods the PBI side skipped and how many cells left
    the comparison for that reason (zero when none)."""
    for name, entry in measures_out.items():
        entry["periods_skipped"] = run.periods_skipped.get(name, 0)
        entry["cells_left_out"] = run.cells_left_out.get(name, 0)


def _summary(
    long_df: pd.DataFrame, at_100: int, total_measures: int, run: ViewRunInfo
) -> dict[str, object]:
    cells_total = int(long_df.shape[0])
    cells_aligned = int(long_df["within_tolerance"].astype(bool).sum())
    return {
        "measures_at_100": at_100,
        "total_measures": total_measures,
        "measures_timed_out": len(run.timed_out),
        "cells_total": cells_total,
        "cells_aligned": cells_aligned,
        "overall_cell_pct": (
            round(100 * cells_aligned / cells_total, 2) if cells_total else 0.0
        ),
    }
