"""Split Power BI report references (tool 78) into visuals to map, slicers and skipped visuals,
and resolve slicer columns to metric view dimensions (used by the PBI Visual-UCMV Mapper).

Nothing is dropped silently: decorative and AI visuals, and visuals without bound fields, are
returned as ``skipped`` with a status and a note, so the dashboard's coverage report lists them.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from .visual_types import rule_for

_NON_WORD = re.compile(r"[^a-z0-9]+")

Item = dict[str, object]


def norm(name: str) -> str:
    return _NON_WORD.sub("_", str(name).lower()).strip("_")


def _names(value: object) -> list[str]:
    return [str(v) for v in value if isinstance(v, str) and v] if isinstance(value, list) else []


def _raw_visuals(report_data: Mapping[str, object]) -> Iterable[tuple[Mapping[str, object], str]]:
    """``(visual, page_name)`` from the flat tool 78 format or the reports > pages format."""
    flat = report_data.get("visuals")
    if isinstance(flat, list) and flat:
        pages = report_data.get("pages")
        first = pages[0] if isinstance(pages, list) and pages and isinstance(pages[0], dict) else {}
        default_page = str(first.get("page_name") or "Page 1")
        for v in flat:
            if isinstance(v, dict):
                yield v, str(v.get("page_name") or default_page)
        return
    reports = report_data.get("reports")
    for report in reports if isinstance(reports, list) else []:
        pages = report.get("pages") if isinstance(report, dict) else None
        for page in pages if isinstance(pages, list) else []:
            if not isinstance(page, dict):
                continue
            page_name = str(page.get("page_display_name") or page.get("displayName") or page.get("page_name") or page.get("name") or "")
            visuals = page.get("visuals")
            for v in visuals if isinstance(visuals, list) else []:
                if isinstance(v, dict):
                    yield v, page_name


def split_report_items(report_data: Mapping[str, object]) -> tuple[list[Item], list[Item], list[Item]]:
    """``(visuals, slicers, skipped)``."""
    visuals: list[Item] = []
    slicers: list[Item] = []
    skipped: list[Item] = []
    for v, page in _raw_visuals(report_data):
        item: Item = {
            "visual_id": str(v.get("visual_id") or ""),
            "page_name": page,
            "visual_type": str(v.get("visual_type") or "tableEx"),
            "measures": _names(v.get("measures")),
            "tables": _names(v.get("tables")),
            "columns": _names(v.get("columns")),
        }
        rule = rule_for(str(item["visual_type"]))
        if rule.status == "filter":
            slicers.append(item)
        elif rule.status in ("decorative", "unsupported"):
            skipped.append({**item, "status": rule.status, "note": rule.note})
        elif not item["measures"] and not item["columns"]:
            skipped.append({**item, "status": "unmapped", "note": "no fields bound to the visual"})
        else:
            visuals.append(item)
    return visuals, slicers, skipped


def resolve_slicers(slicers: Iterable[Mapping[str, object]], ucmv_summaries: Iterable[Mapping[str, object]]) -> list[Item]:
    """Each slicer column -> the metric view dimension of the same (normalised) name and the views
    that have it. Tries ``column``, its last ``.`` part and ``table_column``."""
    by_dim: dict[str, dict[str, set[str]]] = {}
    for s in ucmv_summaries:
        view = str(s.get("view_name") or "")
        for dim in _names(s.get("dimensions")):
            by_dim.setdefault(norm(dim), {}).setdefault(dim, set()).add(view)
    out: list[Item] = []
    for slicer in slicers:
        fields: list[Item] = []
        tables = _names(slicer.get("tables"))
        for column in _names(slicer.get("columns")):
            short = column.split(".")[-1]
            keys = [norm(column), norm(short)] + [norm(f"{t}_{short}") for t in tables]
            match = next((by_dim[k] for k in keys if k in by_dim), None)
            if match:
                dim = sorted(match)[0]
                fields.append({"column": column, "dimension": dim, "views": sorted(match[dim])})
            else:
                fields.append({"column": column, "dimension": "", "views": []})
        out.append({**slicer, "fields": fields})
    return out
