"""The review list of a verify-numbers run: one row per measure, with what a person has to check.

Every view is deployed as generated; what is in question is listed here instead of blocking
the run. A measure is one of:

- ``reconciled``: compared with Power BI and at or above the bar (default 90 % of cells);
- ``below_bar``: compared and below the bar;
- ``not_compared``: in the deployed view, but not compared (no Power BI counterpart in the
  mapping, the view has no period to compare by, the comparison failed or timed out);
- ``removed_on_deploy``: taken out so the view could deploy (deploy healing);
- ``not_deployed``: its view did not deploy;
- ``not_translated``: the DAX has no metric-view expression (with the reason).

The list carries statistics only (share of cells aligned, cell counts), never a measure value,
so it can leave the workspace. ``review_summary`` gives the same per view.

Pure functions: no I/O.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Mapping

import yaml

from ..raw_fields import RawMap, as_list, as_map

DEFAULT_BAR_PCT = 90.0
REVIEW_FIELDS = (
    "view",
    "measure",
    "power_bi",
    "status",
    "pct_aligned",
    "cells_aligned",
    "cells_total",
    "classification",
    "used_on_visuals",
    "reason",
)
_STATUS_ORDER = (
    "not_deployed",
    "removed_on_deploy",
    "below_bar",
    "not_compared",
    "not_translated",
    "reconciled",
)
_REASON_CHARS = 300
_HEALED_MEASURE = re.compile(r"^measure\s+(\S+)")
_USED_ON = re.compile(r"Used on (\d+) visual")


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _short(text: object) -> str:
    return " ".join(str(text or "").split())[:_REASON_CHARS]


def _view_measures(yaml_text: object) -> list[RawMap]:
    try:
        return [
            as_map(m)
            for m in as_list(
                as_map(yaml.safe_load(str(yaml_text or ""))).get("measures")
            )
        ]
    except yaml.YAMLError:
        return []


def _power_bi_name(mapped: RawMap) -> str:
    if mapped.get("pbi_measure"):
        return str(mapped["pbi_measure"])
    if mapped.get("pbi_column"):
        return f"{mapped.get('pbi_aggregation') or 'Sum'} of {mapped['pbi_column']}"
    return ""


def _row(view: str, measure: str, status: str, **fields: object) -> dict[str, object]:
    return {"view": view, "measure": measure, "status": status, **fields}


def _compared_rows(
    view: str,
    yaml_text: object,
    report: RawMap,
    mapped: Mapping[str, RawMap],
    bar: float,
) -> list[dict[str, object]]:
    """Rows for the measures of a view that was compared with Power BI."""
    results = as_map(report.get("measures"))
    timed_out = {str(m) for m in as_list(report.get("timed_out_measures"))}
    rows: list[dict[str, object]] = []
    for spec in _view_measures(yaml_text):
        name = str(spec.get("name") or "")
        used = _USED_ON.search(str(spec.get("comment") or ""))
        common: dict[str, object] = {
            "power_bi": _power_bi_name(as_map(mapped.get(name))),
            "used_on_visuals": int(used.group(1)) if used else 0,
        }
        result = as_map(results.get(name))
        pct = _number(result.get("pct_aligned"))
        if pct is None and _number(result.get("cells_total")):
            pct = (
                100.0
                * (_number(result.get("cells_aligned")) or 0.0)
                / float(str(result["cells_total"]))
            )
        if result and pct is not None:
            rows.append(
                _row(
                    view,
                    name,
                    "reconciled" if pct >= bar else "below_bar",
                    pct_aligned=round(pct, 1),
                    cells_aligned=result.get("cells_aligned"),
                    cells_total=result.get("cells_total"),
                    classification=result.get("classification") or "",
                    **common,
                )
            )
            continue
        reason = (
            "the Power BI query timed out"
            if name in timed_out
            else "no Power BI counterpart in the mapping"
        )
        rows.append(_row(view, name, "not_compared", reason=reason, **common))
    return rows


def _uncompared_rows(
    view: str, yaml_text: object, status: str, reason: str
) -> list[dict[str, object]]:
    return [
        _row(view, str(spec.get("name") or ""), status, reason=reason)
        for spec in _view_measures(yaml_text)
    ]


def build_review_list(
    output: Mapping[str, object], bar: float = DEFAULT_BAR_PCT
) -> list[dict[str, object]]:
    """Rows of the review list from a verify-numbers output (``yaml``, ``reconciliation``,
    ``pbi_ucmv_mapping``, ``cycles``, ``untranslatable_items``), most urgent status first.
    """
    yaml_by_view = as_map(output.get("yaml"))
    compared = as_map(as_map(output.get("reconciliation")).get("views"))
    mappings = as_map(output.get("pbi_ucmv_mapping"))
    last = (
        as_map(as_list(output.get("cycles"))[-1])
        if as_list(output.get("cycles"))
        else {}
    )
    failures = as_map(last.get("deploy_failures"))
    recon_errors = as_map(last.get("reconcile_errors"))
    rows: list[dict[str, object]] = []
    for view, yaml_text in yaml_by_view.items():
        report = as_map(compared.get(view))
        if report.get("measures"):
            mapped = {
                str(as_map(m).get("ucmv_measure")): as_map(m)
                for m in as_list(as_map(mappings.get(view)).get("measures"))
            }
            rows += _compared_rows(view, yaml_text, report, mapped, bar)
            continue
        deploy_error = str(failures.get(view) or "")
        if deploy_error.startswith("error"):
            rows += _uncompared_rows(
                view, yaml_text, "not_deployed", _short(deploy_error)
            )
            continue
        reason = (
            report.get("error")
            or recon_errors.get(view)
            or failures.get(f"mapping:{view}")
            or deploy_error
            or "not compared with Power BI"
        )
        rows += _uncompared_rows(view, yaml_text, "not_compared", _short(reason))
    for view, items in as_map(last.get("deploy_healed")).items():
        for item in as_list(items):
            removed = _HEALED_MEASURE.match(str(item))
            if removed:
                rows.append(
                    _row(
                        str(view),
                        removed.group(1),
                        "removed_on_deploy",
                        reason=_short(item),
                    )
                )
    for raw in as_list(output.get("untranslatable_items")):
        item = as_map(raw)
        rows.append(
            _row(
                str(item.get("table_key") or item.get("view_name") or ""),
                str(item.get("original_name") or ""),
                "not_translated",
                power_bi=str(item.get("original_name") or ""),
                used_on_visuals=len(as_list(item.get("used_in_visuals")))
                + len(as_list(item.get("indirect_visual_usage"))),
                reason=_short(item.get("skip_reason")),
            )
        )
    rows.sort(
        key=lambda r: (
            str(r["view"]),
            _STATUS_ORDER.index(str(r["status"])),
            str(r["measure"]),
        )
    )
    return rows


def review_summary(
    output: Mapping[str, object],
    rows: list[dict[str, object]],
    bar: float = DEFAULT_BAR_PCT,
) -> dict[str, dict[str, object]]:
    """Per view: where it stands against the bar, and how many measures have each status."""
    compared = as_map(as_map(output.get("reconciliation")).get("views"))
    built = as_map(output.get("yaml"))
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        by_status = counts.setdefault(str(row["view"]), {})
        by_status[str(row["status"])] = by_status.get(str(row["status"]), 0) + 1
    summary: dict[str, dict[str, object]] = {}
    for view, by_status in counts.items():
        pct = _number(
            as_map(as_map(compared.get(view)).get("summary")).get("overall_cell_pct")
        )
        if pct is not None:
            status = "at_or_above_bar" if pct >= bar else "below_bar"
        elif by_status.get("not_deployed"):
            status = "not_deployed"
        elif _view_measures(built.get(view)):
            status = "not_compared"
        else:
            status = "no_view"  # no measure was translated: no metric view to deploy
        summary[view] = {"status": status, "pct_aligned": pct, "measures": by_status}
    return summary


def with_review(
    output: Mapping[str, object], config: object = None
) -> dict[str, object]:
    """``output`` plus ``review_csv``, ``review_summary`` and the bar they were built with
    (``review_bar_pct`` in the pipeline config, default 90)."""
    bar = DEFAULT_BAR_PCT
    configured = _number(as_map(config).get("review_bar_pct"))
    if configured is not None and configured > 0:
        bar = configured
    rows = build_review_list(output, bar)
    return {
        **output,
        "review_bar_pct": bar,
        "review_summary": review_summary(output, rows, bar),
        "review_csv": review_csv(rows),
    }


def _cell(value: object) -> object:
    """A cell a spreadsheet opens as text: a measure name that starts with = + - @ would
    otherwise run as a formula."""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return "" if value is None else value


def review_csv(rows: list[dict[str, object]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=REVIEW_FIELDS, lineterminator="\n", extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(
        {key: _cell(row.get(key)) for key in REVIEW_FIELDS} for row in rows
    )
    return buffer.getvalue()
