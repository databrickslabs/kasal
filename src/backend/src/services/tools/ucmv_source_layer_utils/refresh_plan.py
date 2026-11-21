"""Refresh: reload the snapshots, then refresh the materialized views.

Order matters: views read the snapshot tables, so a failed snapshot load stops
the view refresh. The "unchanged" check needs no stored state: when every
selected view was last altered (refreshed) after Power BI's last completed
refresh, Power BI has nothing new and the run is skipped.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from src.services.tools.ucmv_source_layer_utils.names import parse_utc, quote_part
from src.services.tools.ucmv_source_layer_utils.sql_types import (
    SqlError,
    SqlFn,
    SqlResult,
)

ListViewsFn = Callable[[], dict[str, str]]
LastRefreshFn = Callable[[], str | None]


@dataclass(frozen=True)
class RefreshRequest:
    catalog: str
    schema: str
    views: tuple[str, ...] = ()  # explicit names (bare or catalog.schema.name)
    prefix: str = ""  # used when no explicit names are given
    skip_if_unchanged: bool = False


def list_views_sql(catalog: str, schema: str) -> str:
    """Materialized views of the schema with their last-altered time."""
    return (
        "SELECT table_name, CAST(last_altered AS STRING) "
        f"FROM {quote_part(catalog)}.information_schema.tables "
        f"WHERE table_schema = '{schema.replace(chr(39), chr(39) * 2)}' "
        "AND table_type = 'MATERIALIZED_VIEW' ORDER BY table_name"
    )


def parse_views(result: SqlResult) -> dict[str, str]:
    return {str(row[0]): str(row[1] or "") for row in result.rows if row and row[0]}


def refresh_decision(
    pbi_refresh: str | None, altered: dict[str, str]
) -> tuple[bool, str]:
    """(needed, reason). Needed unless every view is newer than Power BI's refresh."""
    if not pbi_refresh:
        return True, "no completed Power BI refresh found; refreshing"
    if not altered:
        return True, "no materialized views selected; nothing to compare, refreshing"
    refreshed_at = parse_utc(pbi_refresh)
    stale: list[str] = []
    for name, stamp in altered.items():
        try:
            if parse_utc(stamp) < refreshed_at:
                stale.append(name)
        except ValueError:
            stale.append(name)
    if stale:
        return (
            True,
            f"{len(stale)} view(s) were refreshed before Power BI's last refresh",
        )
    return False, "every selected view was refreshed after Power BI's last refresh"


def select_views(altered: dict[str, str], request: RefreshRequest) -> list[str]:
    """Explicit names (must be materialized views of the schema), else the prefix, else all."""
    if request.views:
        names = [
            v.strip().strip("`").split(".")[-1] for v in request.views if v.strip()
        ]
        unknown = sorted({n for n in names if n not in altered})
        if unknown:
            raise ValueError(
                f"not materialized views in {request.schema}: {', '.join(unknown)}"
            )
        return sorted(set(names))
    return sorted(n for n in altered if n.startswith(request.prefix))


def run_refresh(
    request: RefreshRequest,
    *,
    last_refresh: LastRefreshFn,
    list_views: ListViewsFn,
    load_snapshots: Callable[[str | None], dict[str, object]] | None,
    refresh_view: Callable[[str], None],
) -> dict[str, object]:
    """Decide, reload snapshots, refresh views. Reports what ran; no data."""
    pbi_refresh = last_refresh()
    altered_all = list_views()
    selected = select_views(altered_all, request)
    report: dict[str, object] = {
        "pbi_last_refresh_utc": pbi_refresh,
        "views_selected": selected,
    }
    if request.skip_if_unchanged:
        needed, reason = refresh_decision(
            pbi_refresh, {n: altered_all[n] for n in selected}
        )
        if not needed:
            report.update(
                {"skipped": True, "reason": reason, "views": [], "all_ok": True}
            )
            return report
        report["reason"] = reason
    else:
        report["reason"] = "refresh requested"
    report["skipped"] = False
    snapshots: dict[str, object] | None = None
    if load_snapshots is not None:
        snapshots = load_snapshots(pbi_refresh)
        report["snapshots"] = snapshots
        if not snapshots.get("all_loaded"):
            report.update(
                {"views": [], "all_ok": False, "views_skipped": "snapshot load failed"}
            )
            return report
    results: list[dict[str, object]] = []
    for name in selected:
        try:
            refresh_view(name)
            results.append({"view": name, "status": "refreshed"})
        except SqlError as exc:
            results.append({"view": name, "status": "failed", "error": str(exc)})
    report["views"] = results
    report["all_ok"] = all(r["status"] == "refreshed" for r in results)
    return report


def refresh_statement(fqn: str) -> str:
    return f"REFRESH MATERIALIZED VIEW {fqn}"


def runner(sql: SqlFn) -> Callable[[str], None]:
    """A view-refresh callable over a SQL executor."""

    def _refresh(fqn: str) -> None:
        sql(refresh_statement(fqn))

    return _refresh
