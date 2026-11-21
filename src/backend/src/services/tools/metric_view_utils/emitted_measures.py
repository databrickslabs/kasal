"""Measures the YAML writer leaves out are reported, not lost.

The writer (``yaml_emitter.emit_yaml``) works on a copy of a view's specification and drops
what would not deploy: a measure on a column the fact does not have, a window whose order
dimension is not in the view. The pipeline's results are built from the specification, not
from the copy, so such a measure was counted as translated, was in no view, and was in no list
of open measures either.

``report_unemitted`` writes each view once, and moves every translated Power BI measure that
is not in the written view to the view's untranslated list, with the reason.

No I/O.
"""

from __future__ import annotations

from collections.abc import Callable

import yaml

from .data_classes import MetricViewSpec, TranslationResult

NOT_WRITTEN = (
    "not written to the view: a column, join or dimension it reads is not in the view"
)


def _reason(measure: TranslationResult, dimensions: set[str]) -> str:
    window = measure.window_spec or {}
    order = str(window.get("order") or "")
    if order and order not in dimensions:
        kind = "prior year: " if window.get("offset") is not None else ""
        return f"{kind}the view has no dimension {order} for the window to order by"
    return NOT_WRITTEN


def _written(text: str) -> tuple[set[str], set[str]] | None:
    """The measure and dimension names of a written view, None when it cannot be read."""
    if not text.strip():
        return set(), set()
    try:
        view = yaml.safe_load(text)
    except yaml.YAMLError:
        return None
    if not isinstance(view, dict):
        return None
    names = [
        {
            str(item.get("name"))
            for item in view.get(key) or []
            if isinstance(item, dict)
        }
        for key in ("measures", "dimensions")
    ]
    return names[0], names[1]


def report_unemitted(
    specs: dict[str, MetricViewSpec],
    write: Callable[[str, MetricViewSpec], str],
) -> dict[str, list[str]]:
    """Move the Power BI measures ``write(key, spec)`` leaves out of a view to
    ``spec.untranslatable``. Returns the moved measure names per view. Column sums (no
    DAX) are not Power BI measures and are left as they are."""
    moved: dict[str, list[str]] = {}
    for key, spec in specs.items():
        candidates = [m for m in spec.measures if m.dax_expression and m.sql_expr]
        if not candidates:
            continue
        written = _written(write(key, spec))
        if written is None:
            continue
        measures, dimensions = written
        for measure in candidates:
            if measure.measure_name in measures:
                continue
            measure.skip_reason = _reason(measure, dimensions)
            measure.is_translatable = False
            measure.sql_expr = None
            measure.window_spec = None
            spec.measures.remove(measure)
            spec.untranslatable.append(measure)
            moved.setdefault(key, []).append(measure.measure_name)
    return moved
