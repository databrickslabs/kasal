"""Every measure of a metric view executes: one ``SELECT COUNT(*)`` per measure.

Only counts and timings are reported, never values. With ``group_by`` each measure runs
grouped by that dimension (window measures often fail only when grouped), and the group
count is reported. With ``timing`` the per-measure wall time is reported and measures slower
than the threshold are flagged ``slow``.
"""

from __future__ import annotations

import time

from .sql_helpers import SqlRunner, error_text, first_cell, quote_name


def measure_statement(view_fqn: str, measure: str, group_by: str | None) -> str:
    selected = f"MEASURE({quote_name(measure)})"
    if group_by:
        dimension = quote_name(group_by)
        inner = (
            f"SELECT {dimension}, {selected} AS m FROM {view_fqn} GROUP BY {dimension}"
        )
    else:
        inner = f"SELECT {selected} AS m FROM {view_fqn}"
    return f"SELECT COUNT(*) FROM ({inner})"


def check_view_measures(
    run_sql: SqlRunner,
    view_fqn: str,
    measures: list[str],
    group_by: str | None = None,
    timing: bool = False,
    slow_s: float = 30.0,
) -> dict[str, object]:
    """Run each measure once; return totals and a per-measure status map."""
    per_measure: dict[str, dict[str, object]] = {}
    failing = 0
    slow = 0
    for name in measures:
        started = time.perf_counter()
        outcome = run_sql(measure_statement(view_fqn, name, group_by))
        seconds = time.perf_counter() - started
        entry: dict[str, object] = {"status": "ok"}
        if not outcome.get("success"):
            failing += 1
            entry = {"status": "fail", "error": error_text(outcome)}
        elif group_by:
            groups = first_cell(outcome.get("data"))
            entry["groups"] = (
                int(groups) if groups is not None and groups.isdigit() else None
            )
        if outcome.get("success") and timing and seconds > slow_s:
            entry["status"] = "slow"
            slow += 1
        if timing:
            entry["seconds"] = round(seconds, 2)
        per_measure[name] = entry
    return {
        "measures": len(measures),
        "failing": failing,
        "slow": slow,
        "per_measure": per_measure,
    }
