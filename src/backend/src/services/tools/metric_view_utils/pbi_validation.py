"""
PBI-vs-UC artefact validation for the Power BI → Unity Catalog Metric View pipeline.

Rec #4 from the IDOR/DQA post-mortem (the highest-impact item): validate every
generated artefact against the live Power BI model automatically as part of the
pipeline.  Anything that doesn't match is flagged ``unverified`` rather than
emitted as final.

Two entry points
----------------
``validate_view``
    Compare a generated UC view (or materialized view) against the corresponding
    PBI table: row counts + per-period numeric-column sums.

``validate_measure``
    Compare a generated UC metric-view measure against the live PBI measure per
    fiscal period and (optionally) country.

Credential injection via ``evaluate_fn``
-----------------------------------------
Neither function makes any network calls itself.  The caller supplies a single
callback::

    def evaluate_fn(query: str) -> list[dict]:
        if query.upper().lstrip().startswith("EVALUATE"):
            return pbi_dax_executor.execute(model_id, query)
        else:
            return list(spark.sql(query).toLocalIterator())

This design lets the module run in unit tests with a simple mock function and in
production with the real PBI executeQueries + Spark endpoints.  If the callback
raises, the result is ``ValidationStatus.ERROR`` with the exception message.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

Row = Dict[str, Any]
Rows = List[Row]
EvaluateFn = Callable[[str], Rows]
"""Callback that executes either a DAX query (starting with ``EVALUATE``) or a
SQL query and returns a list of ``{column: value}`` dicts."""


# ---------------------------------------------------------------------------
# Enums and result dataclasses
# ---------------------------------------------------------------------------


class ValidationStatus(str, Enum):
    """Outcome of one validation check."""

    VERIFIED = "verified"
    """Row counts and all sampled values agree within tolerance."""

    UNVERIFIED = "unverified"
    """One or more values differ beyond tolerance, or row counts diverge."""

    ERROR = "error"
    """``evaluate_fn`` raised an exception; validation could not run."""

    SKIPPED = "skipped"
    """Validation was not attempted (e.g. ``evaluate_fn`` was not supplied)."""


@dataclass
class ColumnMismatch:
    """A single per-period/country value that differs between PBI and UC."""

    period: Any
    country: Optional[Any]
    pbi_value: Optional[float]
    generated_value: Optional[float]
    abs_diff: Optional[float] = None
    pct_diff: Optional[float] = None


@dataclass
class ViewValidationResult:
    """Result of :func:`validate_view`."""

    view_name: str
    status: ValidationStatus
    pbi_row_count: Optional[int] = None
    generated_row_count: Optional[int] = None
    row_count_match: Optional[bool] = None
    period_mismatches: List[ColumnMismatch] = field(default_factory=list)
    """Per-period sum differences for numeric columns (empty when row counts
    already differ or no ``period_col`` was supplied)."""
    error: Optional[str] = None


@dataclass
class MeasureValidationResult:
    """Result of :func:`validate_measure`."""

    measure_name: str
    status: ValidationStatus
    mismatches: List[ColumnMismatch] = field(default_factory=list)
    """Per-(period, country) rows where PBI value ≠ UC value."""
    pbi_total: Optional[float] = None
    generated_total: Optional[float] = None
    error: Optional[str] = None


@dataclass
class MeasureSpec:
    """Describes one PBI measure and its generated UC equivalent.

    Parameters
    ----------
    pbi_measure_name:
        The DAX measure name as it appears in PBI, e.g. ``"NPS Total"``.
    pbi_table_name:
        The PBI table the measure lives on, e.g. ``"Fact_NPS"``.
    metric_view_fqn:
        Fully-qualified UC metric view name, e.g.
        ``"`catalog`.`schema`.`mv_fact_nps`"``.
    measure_sql_name:
        The measure name inside the metric view, e.g. ``"nps_total"``.
    """

    pbi_measure_name: str
    pbi_table_name: str
    metric_view_fqn: str
    measure_sql_name: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_NUMERIC = (int, float)


def _to_float(v: Any) -> Optional[float]:
    """Convert a value to float; return None for NULL / non-numeric."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _diff(pbi: Optional[float], gen: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """Return (abs_diff, pct_diff) for two nullable floats."""
    if pbi is None or gen is None:
        return (None, None)
    abs_d = abs(pbi - gen)
    pct_d = abs_d / abs(pbi) if pbi != 0.0 else (0.0 if gen == 0.0 else None)
    return (abs_d, pct_d)


def _strip_pbi_prefix(column: str) -> str:
    """Remove the ``'TableName'[ColumnName]`` prefix DAX results often carry."""
    m = re.search(r"\[([^\]]+)\]$", column)
    return m.group(1) if m else column


def _normalise_rows(rows: Rows) -> List[Row]:
    """Strip PBI column-name prefixes from all row keys."""
    return [{_strip_pbi_prefix(k): v for k, v in row.items()} for row in rows]


def _period_sums(
    rows: Rows,
    period_col: str,
    numeric_cols: List[str],
) -> Dict[Any, Dict[str, float]]:
    """Aggregate numeric columns by period_col.

    Returns ``{period: {col: sum, ...}}``.
    """
    agg: Dict[Any, Dict[str, float]] = {}
    for row in rows:
        period = row.get(period_col)
        if period not in agg:
            agg[period] = {c: 0.0 for c in numeric_cols}
        for c in numeric_cols:
            v = _to_float(row.get(c))
            if v is not None:
                agg[period][c] += v
    return agg


def _compare_period_sums(
    pbi_rows: Rows,
    uc_rows: Rows,
    period_col: str,
    numeric_cols: List[str],
    tolerance: float,
) -> List[ColumnMismatch]:
    """Compare per-period numeric sums; return mismatches outside tolerance."""
    pbi_agg = _period_sums(pbi_rows, period_col, numeric_cols)
    uc_agg = _period_sums(uc_rows, period_col, numeric_cols)
    all_periods = set(pbi_agg) | set(uc_agg)
    mismatches: List[ColumnMismatch] = []
    for period in sorted(all_periods, key=str):
        for col in numeric_cols:
            pbi_val = pbi_agg.get(period, {}).get(col)
            uc_val = uc_agg.get(period, {}).get(col)
            abs_d, pct_d = _diff(pbi_val, uc_val)
            if abs_d is None or abs_d > tolerance:
                mismatches.append(
                    ColumnMismatch(
                        period=period,
                        country=None,
                        pbi_value=pbi_val,
                        generated_value=uc_val,
                        abs_diff=abs_d,
                        pct_diff=pct_d,
                    )
                )
    return mismatches


def _compare_period_country(
    pbi_rows: Rows,
    uc_rows: Rows,
    period_col: str,
    country_col: Optional[str],
    value_col: str,
    tolerance: float,
) -> List[ColumnMismatch]:
    """Compare (period, country) → value pairs; return mismatches."""
    def _index(rows: Rows) -> Dict[Tuple, Optional[float]]:
        idx: Dict[Tuple, Optional[float]] = {}
        for row in rows:
            period = row.get(period_col)
            country = row.get(country_col) if country_col else None
            key = (period, country)
            idx[key] = _to_float(row.get(value_col))
        return idx

    pbi_idx = _index(pbi_rows)
    uc_idx = _index(uc_rows)
    all_keys = set(pbi_idx) | set(uc_idx)
    mismatches: List[ColumnMismatch] = []
    for key in sorted(all_keys, key=lambda k: (str(k[0]), str(k[1]))):
        pbi_val = pbi_idx.get(key)
        uc_val = uc_idx.get(key)
        abs_d, pct_d = _diff(pbi_val, uc_val)
        if abs_d is None or abs_d > tolerance:
            mismatches.append(
                ColumnMismatch(
                    period=key[0],
                    country=key[1],
                    pbi_value=pbi_val,
                    generated_value=uc_val,
                    abs_diff=abs_d,
                    pct_diff=pct_d,
                )
            )
    return mismatches


def _detect_numeric_cols(rows: Rows) -> List[str]:
    """Heuristic: return column names whose first non-None value is numeric."""
    sample = next((r for r in rows if r), None)
    if not sample:
        return []
    return [
        k for k, v in sample.items() if isinstance(v, _NUMERIC)
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_view(
    view_name: str,
    generated_sql: str,
    evaluate_fn: EvaluateFn,
    *,
    period_col: Optional[str] = None,
    numeric_cols: Optional[List[str]] = None,
    tolerance: float = 1e-4,
) -> ViewValidationResult:
    """Compare a generated UC view against the PBI table.

    Steps
    -----
    1. Call ``evaluate_fn("EVALUATE '<view_name>'")``.
    2. Call ``evaluate_fn(generated_sql)`` to get the UC rows.
    3. Compare row counts.
    4. If ``period_col`` is given, compare per-period sums of ``numeric_cols``
       (auto-detected when ``numeric_cols`` is omitted).

    Parameters
    ----------
    view_name:
        PBI table name used to build the ``EVALUATE`` DAX query.
    generated_sql:
        UC SQL (e.g. ``SELECT * FROM <view>``), passed as-is to the callback.
    evaluate_fn:
        Injected callback.  Routes ``EVALUATE …`` to PBI, plain SQL to UC.
    period_col:
        Optional column name to group numeric sums by (e.g. ``"fiscper_date"``).
    numeric_cols:
        Columns to sum per period.  Auto-detected from the first PBI row when
        omitted.
    tolerance:
        Absolute tolerance for numeric comparisons.
    """
    pbi_dax = f"EVALUATE '{view_name}'"
    try:
        pbi_rows = _normalise_rows(evaluate_fn(pbi_dax))
    except Exception as exc:  # pylint: disable=broad-except
        return ViewValidationResult(
            view_name=view_name,
            status=ValidationStatus.ERROR,
            error=f"PBI evaluate failed: {exc}",
        )
    try:
        uc_rows = _normalise_rows(evaluate_fn(generated_sql))
    except Exception as exc:  # pylint: disable=broad-except
        return ViewValidationResult(
            view_name=view_name,
            status=ValidationStatus.ERROR,
            error=f"UC evaluate failed: {exc}",
        )

    pbi_count = len(pbi_rows)
    uc_count = len(uc_rows)
    row_count_match = pbi_count == uc_count

    period_mismatches: List[ColumnMismatch] = []
    if row_count_match and period_col and pbi_rows:
        effective_cols = numeric_cols or _detect_numeric_cols(pbi_rows)
        if effective_cols:
            period_mismatches = _compare_period_sums(
                pbi_rows, uc_rows, period_col, effective_cols, tolerance
            )

    verified = row_count_match and not period_mismatches
    return ViewValidationResult(
        view_name=view_name,
        status=ValidationStatus.VERIFIED if verified else ValidationStatus.UNVERIFIED,
        pbi_row_count=pbi_count,
        generated_row_count=uc_count,
        row_count_match=row_count_match,
        period_mismatches=period_mismatches,
    )


def validate_measure(
    spec: MeasureSpec,
    period_col: str,
    country_col: Optional[str],
    evaluate_fn: EvaluateFn,
    *,
    tolerance: float = 1e-4,
) -> MeasureValidationResult:
    """Compare a UC metric-view measure against the live PBI measure.

    Builds two queries internally and calls ``evaluate_fn`` for each:

    * PBI side::

        EVALUATE SUMMARIZECOLUMNS(
            'Fact_NPS'[fiscper_date], 'Fact_NPS'[country_id],
            "val", [NPS Total]
        )

    * UC side::

        SELECT fiscper_date, country_id,
               MEASURE(nps_total) AS val
        FROM `catalog`.`schema`.`mv_fact_nps`
        GROUP BY ALL

    The caller's ``evaluate_fn`` must route ``EVALUATE …`` queries to the PBI
    executeQueries API and plain SQL to UC / Spark.

    Parameters
    ----------
    spec:
        Identifies the PBI measure and its UC equivalent.
    period_col:
        Dimension column to use as the time grain (e.g. ``"fiscper_date"``).
        Used both in the DAX ``SUMMARIZECOLUMNS`` and the SQL ``SELECT``.
    country_col:
        Optional second grouping dimension (e.g. ``"country_id"``).
    evaluate_fn:
        Injected callback.
    tolerance:
        Absolute tolerance for numeric comparisons.
    """
    # Build PBI DAX query
    group_dims = [f"'{spec.pbi_table_name}'[{period_col}]"]
    if country_col:
        group_dims.append(f"'{spec.pbi_table_name}'[{country_col}]")
    pbi_dax = (
        f"EVALUATE SUMMARIZECOLUMNS(\n"
        f"    {', '.join(group_dims)},\n"
        f'    "val", [{spec.pbi_measure_name}]\n'
        f")"
    )

    # Build UC SQL query using MEASURE() syntax
    select_cols = period_col
    if country_col:
        select_cols += f", {country_col}"
    uc_sql = (
        f"SELECT {select_cols},\n"
        f"       MEASURE({spec.measure_sql_name}) AS val\n"
        f"FROM {spec.metric_view_fqn}\n"
        f"GROUP BY ALL"
    )

    try:
        pbi_rows = _normalise_rows(evaluate_fn(pbi_dax))
    except Exception as exc:  # pylint: disable=broad-except
        return MeasureValidationResult(
            measure_name=spec.pbi_measure_name,
            status=ValidationStatus.ERROR,
            error=f"PBI evaluate failed: {exc}",
        )
    try:
        uc_rows = _normalise_rows(evaluate_fn(uc_sql))
    except Exception as exc:  # pylint: disable=broad-except
        return MeasureValidationResult(
            measure_name=spec.pbi_measure_name,
            status=ValidationStatus.ERROR,
            error=f"UC evaluate failed: {exc}",
        )

    mismatches = _compare_period_country(
        pbi_rows, uc_rows, period_col, country_col, "val", tolerance
    )

    pbi_total = sum(
        v for v in (_to_float(r.get("val")) for r in pbi_rows) if v is not None
    ) or None
    gen_total = sum(
        v for v in (_to_float(r.get("val")) for r in uc_rows) if v is not None
    ) or None

    return MeasureValidationResult(
        measure_name=spec.pbi_measure_name,
        status=ValidationStatus.VERIFIED if not mismatches else ValidationStatus.UNVERIFIED,
        mismatches=mismatches,
        pbi_total=pbi_total,
        generated_total=gen_total,
    )
