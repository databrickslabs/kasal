"""Lakeview widget specs for one mapped Power BI visual.

Two dataset modes:

* ``metric_view`` (default): the dataset IS the UC metric view (``asset_name``). Widget fields
  are ``MEASURE(`m`)`` and plain dimension columns, so the dashboard evaluates every measure
  at the grain of the widget and under every filter, which is the only way ratios, distinct
  counts and other non-additive measures stay right.
* ``sql``: one SQL dataset per visual, grouped exactly by the visual's dimensions
  (``MEASURE(`m`) AS `m```); widget fields re-aggregate one row per group with ``SUM``,
  which is exact at that grain.

Every builder returns ``(widget, status, note)``. ``status`` / ``note`` start from the visual
rule and are lowered when the visual lacks what the chart needs (e.g. a pie without a
category becomes a counter).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .visual_types import VisualRule

METRIC_VIEW = "metric_view"
SQL = "sql"


def quote(name: str) -> str:
    """Back-quote an identifier for a widget field expression."""
    return "`" + str(name).replace("`", "``") + "`"


def label(name: str) -> str:
    return str(name).replace("_", " ").strip().title() or str(name)


@dataclass
class VisualFields:
    dims: list[str]
    measures: list[str]
    mode: str = METRIC_VIEW
    names: dict[str, str] = field(default_factory=dict)  # measure -> field name

    def query_fields(self) -> list[dict[str, str]]:
        out = [{"name": d, "expression": quote(d)} for d in self.dims]
        for m in self.measures:
            name = m if m not in self.dims else f"{m}_value"
            self.names[m] = name
            expr = f"MEASURE({quote(m)})" if self.mode == METRIC_VIEW else f"SUM({quote(m)})"
            out.append({"name": name, "expression": expr})
        return out

    def measure_field(self, m: str) -> str:
        return self.names.get(m, m)


def _axis(field_name: str, scale: str, display: str) -> dict[str, object]:
    return {"fieldName": field_name, "scale": {"type": scale}, "displayName": display}


def _frame(title: str) -> dict[str, object]:
    return {"showTitle": True, "title": title}


def _table(f: VisualFields, title: str) -> dict[str, object]:
    columns = [{"fieldName": d, "displayName": label(d)} for d in f.dims]
    columns += [{"fieldName": f.measure_field(m), "displayName": label(m)} for m in f.measures]
    return {"version": 2, "widgetType": "table", "encodings": {"columns": columns}, "frame": _frame(title)}


def _counter(f: VisualFields, title: str) -> dict[str, object]:
    m = f.measures[0]
    return {
        "version": 2,
        "widgetType": "counter",
        "encodings": {"value": {"fieldName": f.measure_field(m), "displayName": label(m)}},
        "frame": _frame(title),
    }


def _chart(f: VisualFields, rule: VisualRule, title: str) -> dict[str, object]:
    category = f.dims[0]
    series = f.dims[1] if len(f.dims) > 1 else ""
    cat_axis = _axis(category, "categorical", label(category))
    if len(f.measures) > 1 and not series:
        value_axis: dict[str, object] = {
            "scale": {"type": "quantitative"},
            "fields": [{"fieldName": f.measure_field(m), "displayName": label(m)} for m in f.measures],
        }
    else:
        m = f.measures[0]
        value_axis = _axis(f.measure_field(m), "quantitative", label(m))
    x, y = (value_axis, cat_axis) if rule.horizontal else (cat_axis, value_axis)
    encodings: dict[str, object] = {"x": x, "y": y}
    if series:
        encodings["color"] = _axis(series, "categorical", label(series))
    spec: dict[str, object] = {
        "version": rule.version,
        "widgetType": rule.widget,
        "encodings": encodings,
        "frame": _frame(title),
    }
    if rule.widget == "bar" and rule.stacking == "group" and (series or len(f.measures) > 1):
        spec["mark"] = {"layout": "group"}
    return spec


def _pie(f: VisualFields, title: str) -> dict[str, object]:
    m, category = f.measures[0], f.dims[0]
    return {
        "version": 3,
        "widgetType": "pie",
        "encodings": {
            "angle": _axis(f.measure_field(m), "quantitative", label(m)),
            "color": _axis(category, "categorical", label(category)),
        },
        "frame": _frame(title),
    }


def _scatter(f: VisualFields, title: str) -> dict[str, object]:
    mx, my = f.measures[0], f.measures[1]
    encodings: dict[str, object] = {
        "x": _axis(f.measure_field(mx), "quantitative", label(mx)),
        "y": _axis(f.measure_field(my), "quantitative", label(my)),
    }
    if f.dims:
        encodings["color"] = _axis(f.dims[0], "categorical", label(f.dims[0]))
    return {"version": 3, "widgetType": "scatter", "encodings": encodings, "frame": _frame(title)}


def _join(*notes: str) -> str:
    return "; ".join(n for n in notes if n)


def build_spec(f: VisualFields, rule: VisualRule, title: str) -> tuple[dict[str, object], str, str]:
    """``(spec, status, note)`` for one visual; downgrades when required fields are missing."""
    status, note = rule.status, rule.note
    if not f.measures:
        if f.dims:
            return _table(f, title), "approximated", _join(note, "no measure: shown as a table of its columns")
        return {}, "unmapped", "no dimension or measure mapped"
    if rule.widget == "counter":
        return _counter(f, title), status, note
    if rule.widget == "table":
        return _table(f, title), status, note
    if rule.widget == "scatter" and len(f.measures) < 2:
        if f.dims:
            return _chart(f, VisualRule("bar", 3, "approximated"), title), "approximated", "scatter with one measure shown as bars"
        return _counter(f, title), "approximated", "scatter with one measure and no category shown as counter"
    if not f.dims:
        return _counter(f, title), "approximated", _join(note, "no category: shown as counter")
    if rule.widget == "pie":
        return _pie(f, title), status, note
    if rule.widget == "scatter":
        return _scatter(f, title), status, note
    spec = _chart(f, rule, title)
    if rule.stacking == "percent":
        status, note = "approximated", _join(note, "100 % stacking shown as stacked values")
    return spec, status, note


def widget_size(widget_type: str) -> tuple[int, int]:
    """Grid ``(width, height)`` on the 6-column canvas."""
    if widget_type == "counter":
        return 2, 3
    if widget_type == "table":
        return 6, 6
    return 3, 6
