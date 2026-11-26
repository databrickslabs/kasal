"""Percentage scale check on the UC side (counts only).

A measure named as a percentage can be stored on the 0-100 scale. Grouping the
metric view by one dimension and counting how many groups exceed a threshold
(default 1.5) tells the two apart without reading any value:

- ``no_data``: no group has a value.
- ``0-100``: at least ``SCALE_HIGH_SHARE`` of the non-null groups are above the
  threshold. A fraction never exceeds 1.5 in nearly every group.
- ``fraction``: otherwise (few or no groups above the threshold).

The query returns three counts per measure (groups, non-null groups, groups above
the threshold). Only those counts leave this module. It runs through an injected
executor (``execute(sql) -> (DataFrame, err)``), so it adds no network code.
"""

from __future__ import annotations

from collections.abc import Sequence

from .mapping import UCMVMapping
from .runner import UcmvExecutor
from .ucmv_query import _quote_ident

DEFAULT_SCALE_THRESHOLD = 1.5

# Share of non-null groups above the threshold that makes a measure "0-100".
SCALE_HIGH_SHARE = 0.9


def scale_verdict(non_null: int, above: int) -> str:
    """Verdict for one measure from its non-null and above-threshold counts."""
    if non_null <= 0:
        return "no_data"
    if above / non_null >= SCALE_HIGH_SHARE:
        return "0-100"
    return "fraction"


def build_scale_query(
    view_fqn: str,
    dimension_column: str,
    measures: Sequence[str],
    threshold: float = DEFAULT_SCALE_THRESHOLD,
) -> str:
    """One SQL statement counting, per measure, the groups of ``dimension_column``.

    Output columns: ``groups``, then ``m<i>_non_null`` and ``m<i>_above`` for the
    i-th measure (index, not name, so no measure name reaches a column alias).
    """
    if not measures:
        raise ValueError("scale check needs at least one measure")
    dim = _quote_ident(dimension_column)
    inner_cols = [f"{dim} AS dim"] + [
        f"MEASURE({_quote_ident(m)}) AS m{i}" for i, m in enumerate(measures)
    ]
    from_target = ".".join(_quote_ident(p) for p in view_fqn.split("."))
    outer_cols = ["COUNT(*) AS groups"]
    for i in range(len(measures)):
        outer_cols.append(f"COUNT(m{i}) AS m{i}_non_null")
        outer_cols.append(f"COUNT_IF(ABS(m{i}) > {float(threshold)!r}) AS m{i}_above")
    return (
        "SELECT "
        + ",\n       ".join(outer_cols)
        + "\nFROM (\n    SELECT "
        + ",\n           ".join(inner_cols)
        + f"\n    FROM {from_target}\n    GROUP BY {dim}\n)"
    )


def _count(value: object) -> int:
    """A COUNT cell as returned by the SQL Statement API (text, int or None)."""
    if isinstance(value, (int, float, str)):
        return int(float(value)) if str(value).strip() else 0
    return 0


def parse_scale_row(
    row: Sequence[object], measures: Sequence[str], threshold: float
) -> dict[str, dict]:
    """Turn the single result row of :func:`build_scale_query` into per-measure
    verdicts (``{measure: {groups, non_null, above_threshold, verdict}}``)."""
    groups = _count(row[0])
    out: dict[str, dict] = {}
    for i, measure in enumerate(measures):
        non_null = _count(row[1 + 2 * i])
        above = _count(row[2 + 2 * i])
        out[measure] = {
            "groups": groups,
            "non_null": non_null,
            "above_threshold": above,
            "threshold": float(threshold),
            "verdict": scale_verdict(non_null, above),
        }
    return out


def run_scale_check(
    executor: UcmvExecutor,
    mapping: UCMVMapping,
    dimension_name: str,
    threshold: float = DEFAULT_SCALE_THRESHOLD,
) -> dict[str, dict]:
    """Run the scale check for every resolved measure of ``mapping``.

    ``executor`` is a UC SQL executor (``execute(sql) -> (DataFrame, err)``).
    """
    measures = [m.ucmv_measure for m in mapping.resolved_measures()]
    if not measures:
        return {}
    dimension = mapping.binding.dimensions[dimension_name]
    sql = build_scale_query(
        mapping.binding.ucmv_table, dimension.ucmv_column, measures, threshold
    )
    df, err = executor.execute(sql)
    if df is None or df.empty:
        raise RuntimeError(f"Scale check returned no row: {err}")
    row = df.iloc[0].tolist()
    return parse_scale_row(row, measures, threshold)
