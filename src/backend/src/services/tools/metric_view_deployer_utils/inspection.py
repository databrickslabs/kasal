"""Read-only inspection (``check`` mode) and the output summaries of both modes.

``check_one`` runs every measure of one view as a count; the summaries count statuses of the
per-view results so the tool's JSON output stays flat."""

from __future__ import annotations

from collections.abc import Mapping

from .definition import item_names
from .measure_check import check_view_measures
from .sql_helpers import SqlRunner


def check_one(
    runner: SqlRunner,
    view_name: str,
    spec: Mapping[str, object],
    group_by: str | None,
    timing: bool,
    slow_s: float,
) -> dict[str, object]:
    """Run every measure of one view. ``group_by`` skips the view when the dimension is absent."""
    if group_by and group_by not in item_names(spec, "dimensions"):
        return {
            "status": "skipped",
            "view_name": view_name,
            "message": f"no dimension {group_by!r}",
        }
    report = check_view_measures(
        runner,
        view_name,
        item_names(spec, "measures"),
        group_by=group_by,
        timing=timing,
        slow_s=slow_s,
    )
    status = "failing" if report["failing"] else "ok"
    return {"status": status, "view_name": view_name, **report}


_INSPECTION_STATUSES = (
    "verified",
    "differs",
    "not_metric_view",
    "missing",
    "ok",
    "failing",
    "skipped",
    "error",
)


def inspection_summary(
    results: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    statuses = [r.get("status") for r in results.values()]
    summary: dict[str, object] = {"total": len(results)}
    for status in _INSPECTION_STATUSES:
        summary[status] = statuses.count(status)
    measures_failing = 0
    for r in results.values():
        failing = r.get("failing")
        if isinstance(failing, int):
            measures_failing += failing
    summary["measures_failing"] = measures_failing
    return summary


def deploy_summary(
    results: Mapping[str, Mapping[str, object]], dry_run: bool
) -> dict[str, object]:
    def count(status: str) -> int:
        return sum(1 for r in results.values() if r.get("status") == status)

    return {
        "total": len(results),
        "validated": count("validated"),
        "deployed": count("deployed"),
        "deployed_incomplete": count("deployed_incomplete"),
        "deployed_grants_incomplete": count("deployed_grants_incomplete"),
        "errors": count("error"),
        "dry_run": dry_run,
    }
