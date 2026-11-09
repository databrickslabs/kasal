"""Power BI visual type -> Lakeview widget rule.

Every Power BI visual gets one rule and therefore one coverage status:

* ``mapped``: same chart family, same encodings.
* ``approximated``: rebuilt with a close widget; the note says what is lost.
* ``table_fallback``: no chart equivalent; the data is shown as a table.
* ``filter``: a slicer, rebuilt as a filter widget.
* ``decorative``: text, images, shapes, buttons, navigators; nothing to migrate.
* ``unsupported``: AI and script visuals; reported, not rebuilt.

Visual type ids are the PBIR / report-layout ``visualType`` values.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VisualRule:
    widget: str  # Lakeview widgetType ("" for filter / decorative / unsupported)
    version: int
    status: str
    horizontal: bool = False  # bars along the y axis (Power BI "bar" = horizontal)
    stacking: str = ""  # "group" | "stack" | "percent"
    note: str = ""


def _rule(
    widget: str,
    version: int,
    status: str = "mapped",
    *,
    horizontal: bool = False,
    stacking: str = "",
    note: str = "",
) -> VisualRule:
    return VisualRule(widget, version, status, horizontal, stacking, note)


_BAR, _LINE, _AREA, _PIE, _SCATTER = ("bar", 3), ("line", 3), ("area", 3), ("pie", 3), ("scatter", 3)
_COUNTER, _TABLE = ("counter", 2), ("table", 2)

RULES: dict[str, VisualRule] = {
    # bars: Power BI "bar" charts are horizontal, "column" charts vertical
    "barChart": _rule(*_BAR, horizontal=True, stacking="stack"),
    "clusteredBarChart": _rule(*_BAR, horizontal=True, stacking="group"),
    "hundredPercentStackedBarChart": _rule(*_BAR, horizontal=True, stacking="percent"),
    "columnChart": _rule(*_BAR, stacking="stack"),
    "clusteredColumnChart": _rule(*_BAR, stacking="group"),
    "hundredPercentStackedColumnChart": _rule(*_BAR, stacking="percent"),
    "ribbonChart": _rule(*_BAR, "approximated", stacking="stack", note="ribbons shown as stacked columns"),
    "waterfallChart": _rule(*_BAR, "approximated", note="waterfall shown as columns (no running total)"),
    "funnel": _rule(*_BAR, "approximated", horizontal=True, note="funnel shown as horizontal bars"),
    # lines and areas
    "lineChart": _rule(*_LINE),
    "areaChart": _rule(*_AREA),
    "stackedAreaChart": _rule(*_AREA, stacking="stack"),
    "hundredPercentStackedAreaChart": _rule(*_AREA, stacking="percent"),
    "lineClusteredColumnComboChart": _rule(
        *_BAR, "approximated", stacking="group", note="combo shown as columns; the line series is a column"
    ),
    "lineStackedColumnComboChart": _rule(
        *_BAR, "approximated", stacking="stack", note="combo shown as columns; the line series is a column"
    ),
    # parts of a whole
    "pieChart": _rule(*_PIE),
    "donutChart": _rule(*_PIE, "approximated", note="donut shown as pie"),
    "treemap": _rule(*_BAR, "approximated", horizontal=True, note="treemap shown as horizontal bars"),
    # relationships
    "scatterChart": _rule(*_SCATTER),
    # single values
    "card": _rule(*_COUNTER),
    "cardVisual": _rule(*_COUNTER),
    "kpi": _rule(*_COUNTER, "approximated", note="KPI shown as counter (no trend / target)"),
    "kpiVisual": _rule(*_COUNTER, "approximated", note="KPI shown as counter (no trend / target)"),
    "gauge": _rule(*_COUNTER, "approximated", note="gauge shown as counter (no min / max / target)"),
    "multiRowCard": _rule(*_TABLE, "approximated", note="multi-row card shown as table"),
    # tables
    "tableEx": _rule(*_TABLE),
    "table": _rule(*_TABLE),
    "matrix": _rule(*_TABLE, "approximated", note="matrix flattened to a table (no row / column hierarchy)"),
    "pivotTable": _rule(*_TABLE, "approximated", note="matrix flattened to a table (no row / column hierarchy)"),
    # maps: no geographic roles in the visual references, so the data is a table
    "map": _rule(*_TABLE, "table_fallback", note="map: geographic roles unknown, data shown as table"),
    "filledMap": _rule(*_TABLE, "table_fallback", note="map: geographic roles unknown, data shown as table"),
    "shapeMap": _rule(*_TABLE, "table_fallback", note="map: geographic roles unknown, data shown as table"),
    "azureMap": _rule(*_TABLE, "table_fallback", note="map: geographic roles unknown, data shown as table"),
    "decompositionTreeVisual": _rule(*_TABLE, "table_fallback", note="decomposition tree shown as table"),
    # slicers
    "slicer": _rule("", 2, "filter"),
    "advancedSlicerVisual": _rule("", 2, "filter"),
    # nothing to migrate
    **{
        t: _rule("", 0, "decorative")
        for t in (
            "textbox", "image", "shape", "basicShape", "actionButton",
            "pageNavigator", "bookmarkNavigator", "group",
        )
    },
    # not rebuilt
    **{
        t: _rule("", 0, "unsupported", note="AI / script visual, not rebuilt")
        for t in ("qnaVisual", "keyDriversVisual", "aiNarratives", "scriptVisual", "pythonVisual", "rVisual")
    },
}

_FALLBACK = _rule(*_TABLE, "table_fallback", note="visual type without a mapping, data shown as table")


def rule_for(visual_type: str) -> VisualRule:
    """The rule of a Power BI visual type; unknown types become a table."""
    return RULES.get(visual_type or "", _FALLBACK)
