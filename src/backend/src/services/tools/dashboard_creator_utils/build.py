"""Lakeview dashboard JSON + per-visual coverage report from the mapper output.

``build_dashboard`` returns ``(serialized_dashboard_dict, outcomes)``. One outcome per Power BI
visual (and per slicer column): ``visual_id``, ``page``, ``visual_type``, ``status``, ``widget``,
``note``. ``coverage`` summarises them; decorative visuals are counted but not in the share.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from src.services.tools.metric_view_utils.sql_guard import is_read_only_select

from .filters import filter_widget, slicer_fields
from .visual_types import rule_for
from .widgets import METRIC_VIEW, SQL, VisualFields, build_spec, label, widget_size

_COLUMNS = 6
_FILTER_SIZE = (2, 2)
_MIGRATED = ("mapped", "approximated", "table_fallback", "filter")
_NOT_COUNTED = ("decorative",)

Outcome = dict[str, str]
Dashboard = dict[str, list[dict[str, object]]]


def safe_name(s: str) -> str:
    """Alphanumeric + underscores only (widget / dataset / page names)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", s).strip("_")[:60] or "unnamed"


def _strings(value: object) -> list[str]:
    return [str(v) for v in value if isinstance(v, str) and v] if isinstance(value, list) else []


def _quote_dotted(name: str) -> str:
    return ".".join("`" + p.replace("`", "``") + "`" for p in name.split("."))


def _sql_for(view: str, dims: list[str], measures: list[str]) -> str:
    cols = [f"`{d.replace('`', '``')}`" for d in dims]
    cols += [f"MEASURE(`{m.replace('`', '``')}`) AS `{m.replace('`', '``')}`" for m in measures]
    sql = f"SELECT {', '.join(cols)} FROM {_quote_dotted(view)}"  # names back-quoted, gated below — nosemgrep: tmp.databricks.sql-injection.fstring-select
    return sql + " GROUP BY ALL" if dims else sql


class _Grid:
    """Shelf packing on the 6-column canvas: rows are as tall as their tallest widget."""

    def __init__(self) -> None:
        self.x = self.y = self.row_height = 0

    def place(self, width: int, height: int) -> dict[str, int]:
        if self.x + width > _COLUMNS:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0
        pos = {"x": self.x, "y": self.y, "width": width, "height": height}
        self.x += width
        self.row_height = max(self.row_height, height)
        return pos

    def new_row(self) -> None:
        if self.x:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0


def _outcome(m: Mapping[str, object], status: str, widget: str = "", note: str = "") -> Outcome:
    return {
        "visual_id": str(m.get("visual_id") or ""),
        "page": str(m.get("page_name") or "Default"),
        "visual_type": str(m.get("visual_type") or ""),
        "status": status,
        "widget": widget,
        "note": note,
    }


class _Datasets:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.items: list[dict[str, object]] = []
        self.by_view: dict[str, str] = {}

    def _name(self, base: str) -> str:
        name, n, taken = safe_name(base), 1, {str(d["name"]) for d in self.items}
        candidate = name
        while candidate in taken:
            candidate, n = f"{name}_{n}", n + 1
        return candidate

    def for_view(self, view: str) -> str:
        if view not in self.by_view:
            name = self._name(f"ds_{view.split('.')[-1]}")
            self.by_view[view] = name
            self.items.append({"name": name, "displayName": label(view.split(".")[-1]), "asset_name": view})
        return self.by_view[view]

    def for_visual(self, view: str, visual_id: str, dims: list[str], measures: list[str]) -> str:
        sql = _sql_for(view, dims, measures)
        if not is_read_only_select(sql):
            return ""
        name = self._name(f"ds_{view.split('.')[-1]}_{visual_id}")
        self.items.append({"name": name, "displayName": label(f"{view.split('.')[-1]} {visual_id}"), "query": sql})
        return name


def _widget_for(
    m: Mapping[str, object], datasets: _Datasets, grid: _Grid
) -> tuple[dict[str, object] | None, Outcome, str]:
    rule = rule_for(str(m.get("visual_type") or ""))
    view = str(m.get("ucmv_view") or "")
    if rule.status in ("decorative", "unsupported"):
        return None, _outcome(m, rule.status, note=rule.note), ""
    if not view:
        return None, _outcome(m, "unmapped", note="no metric view matched"), ""
    dims, measures = _strings(m.get("dimensions")), _strings(m.get("measures"))
    fields = VisualFields(dims, measures, datasets.mode)
    query_fields = fields.query_fields()
    title = str(m.get("chart_title") or f"{m.get('page_name', '')} - {m.get('visual_type', '')}")
    spec, status, note = build_spec(fields, rule, title)
    if not spec:
        return None, _outcome(m, status, note=note), ""
    if datasets.mode == METRIC_VIEW:
        ds = datasets.for_view(view)
    else:
        ds = datasets.for_visual(view, str(m.get("visual_id") or ""), dims, measures)
        if not ds:
            return None, _outcome(m, "unmapped", note="dataset SQL refused by the read-only check"), ""
    widget_type = str(spec["widgetType"])
    widget = {
        "name": safe_name(f"w_{m.get('visual_id') or m.get('page_name', '')}_{widget_type}"),
        "queries": [
            {"name": "main_query", "query": {"datasetName": ds, "fields": query_fields, "disaggregated": False}}
        ],
        "spec": spec,
    }
    layout = {"widget": widget, "position": grid.place(*widget_size(widget_type))}
    return layout, _outcome(m, status, widget_type, note), ds


def _filters_for_page(
    page: str,
    slicers: Iterable[Mapping[str, object]],
    page_views: Mapping[str, str],
    mode: str,
    grid: _Grid,
) -> tuple[list[dict[str, object]], list[Outcome]]:
    layout: list[dict[str, object]] = []
    outcomes: list[Outcome] = []
    seen: set[str] = set()
    for slicer in slicers:
        if str(slicer.get("page_name") or "Default") != page:
            continue
        for entry in slicer_fields(slicer):
            column, dim = str(entry.get("column") or ""), str(entry.get("dimension") or "")
            base = {**slicer, "visual_type": str(slicer.get("visual_type") or "slicer")}
            if not dim:
                outcomes.append(_outcome(base, "unmapped", note=f"slicer column {column!r}: no metric view dimension"))
                continue
            datasets = sorted({ds for view, ds in page_views.items() if view in _strings(entry.get("views"))})
            if mode != METRIC_VIEW:
                outcomes.append(_outcome(base, "unmapped", note=f"filter on {dim!r} needs metric-view datasets"))
            elif not datasets:
                outcomes.append(_outcome(base, "unmapped", note=f"no widget on this page reads a view with {dim!r}"))
            elif dim not in seen:
                seen.add(dim)
                name = safe_name(f"f_{page}_{dim}")
                widget = filter_widget(name, dim, datasets)
                layout.append({"widget": widget, "position": grid.place(*_FILTER_SIZE)})
                spec = widget["spec"]
                kind = str(spec["widgetType"]) if isinstance(spec, dict) else "filter"
                outcomes.append(_outcome(base, "filter", kind, f"filters {len(datasets)} dataset(s) on {dim!r}"))
    grid.new_row()
    return layout, outcomes


def build_dashboard(
    mappings: list[Mapping[str, object]],
    slicers: list[Mapping[str, object]] | None = None,
    *,
    dataset_mode: str = METRIC_VIEW,
) -> tuple[Dashboard, list[Outcome]]:
    mode = dataset_mode if dataset_mode in (METRIC_VIEW, SQL) else METRIC_VIEW
    datasets = _Datasets(mode)
    pages_map: dict[str, list[Mapping[str, object]]] = {}
    for m in mappings:
        pages_map.setdefault(str(m.get("page_name") or "Default"), []).append(m)
    pages: list[dict[str, object]] = []
    outcomes: list[Outcome] = []
    for page, visuals in pages_map.items():
        views = {str(m.get("ucmv_view")) for m in visuals if m.get("ucmv_view")}
        page_views = {v: datasets.for_view(v) for v in sorted(views)} if mode == METRIC_VIEW else {}
        grid = _Grid()
        layout, filter_outcomes = _filters_for_page(page, slicers or [], page_views, mode, grid)
        outcomes += filter_outcomes
        for m in visuals:
            entry, outcome, _ = _widget_for(m, datasets, grid)
            outcomes.append(outcome)
            if entry:
                layout.append(entry)
        if layout:
            pages.append(
                {"name": safe_name(f"page_{page}"), "displayName": page, "pageType": "PAGE_TYPE_CANVAS", "layout": layout}
            )
    return {"pages": pages, "datasets": datasets.items}, outcomes


def dashboard_counts(dashboard: Dashboard) -> tuple[int, int, int]:
    """``(pages, widgets, datasets)``."""
    pages = dashboard.get("pages", [])
    widgets = 0
    for page in pages:
        layout = page.get("layout")
        widgets += len(layout) if isinstance(layout, list) else 0
    return len(pages), widgets, len(dashboard.get("datasets", []))


def coverage(outcomes: list[Outcome]) -> dict[str, object]:
    counts: dict[str, int] = {}
    for o in outcomes:
        counts[o["status"]] = counts.get(o["status"], 0) + 1
    in_scope = sum(n for s, n in counts.items() if s not in _NOT_COUNTED)
    migrated = sum(n for s, n in counts.items() if s in _MIGRATED)
    return {
        "total": len(outcomes),
        "by_status": dict(sorted(counts.items())),
        "migrated_pct": round(100 * migrated / in_scope, 1) if in_scope else 0.0,
    }
