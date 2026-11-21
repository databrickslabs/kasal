"""Comparing measures that read another period than the one they are shown at (prior year).

Such a measure cannot be compared the way the others are:

- In Power BI it moves the dates of a calendar. Grouped by that calendar it returns the
  earlier value; grouped by the fact's own period column it returns the current one, because
  that column still filters the fact.
- In the metric view it is a window over one dimension (``prior_year.py``) and returns a
  value only when the query groups by that dimension.

So a mapping can carry a second time dimension, ``shifted_time_dimension``, and mark the
measures that use it (``time_shifted: true``): the metric view is grouped by the window's
dimension, Power BI by the calendar's period column. The reconciliation compares them in a
second pass and merges the result; when that pass fails, the other measures are still
compared and these are reported as not compared.

A measure is time-shifted when it has a window with an ``offset``, or reads such a measure
(``MEASURE(sales_py) / MEASURE(volume_py)``).

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import replace

import pandas as pd

from ..prior_year_dax import unshifted
from ..raw_fields import RawMap, as_list, as_map
from .mapping import UCMVMapping

_MEASURE_REF = re.compile(r"\bMEASURE\(\s*`?(\w+)`?\s*\)", re.IGNORECASE)
_KEY = re.compile(r"^\s*TRY_CAST\(\s*`?\w+`?\.`?(\w+)`?\s+AS\s+INT\s*\)\s*$", re.I)
_MONTH = re.compile(r"^\s*DATE_TRUNC\(\s*'MONTH'\s*,", re.I)
NOT_COMPARABLE = (
    "prior year: Power BI can only be grouped by the measure's calendar, and its "
    "period column is not known (no relationship ends on it)"
)


def _loose(name: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def shifted_measures(spec: RawMap) -> tuple[set[str], str]:
    """The time-shifted measures of a view and the dimension their windows order by
    (``(set(), "")`` when the view has none)."""
    measures = [as_map(m) for m in as_list(spec.get("measures"))]
    orders: Counter[str] = Counter()
    by_order: dict[str, set[str]] = {}
    for measure in measures:
        for raw in as_list(measure.get("window")):
            window = as_map(raw)
            if window.get("offset") is not None and window.get("order"):
                order = str(window["order"])
                orders[order] += 1
                by_order.setdefault(order, set()).add(str(measure.get("name")))
    if not orders:
        return set(), ""
    order = orders.most_common(1)[0][0]
    shifted = set(by_order[order])
    # A window on another dimension cannot share the query: left out of the comparison.
    other = {name for names in by_order.values() for name in names} - shifted
    grown = True
    while grown:
        grown = False
        for measure in measures:
            name = str(measure.get("name"))
            reads = set(_MEASURE_REF.findall(str(measure.get("expr") or "")))
            if name not in shifted and name not in other and reads & shifted:
                shifted.add(name)
                grown = True
    return shifted, order


def _calendar(resolved: object, shifted: set[str]) -> str:
    """The Power BI calendar the view's prior-year measures shift on."""
    for raw in as_list(resolved):
        entry = as_map(raw)
        if str(entry.get("measure_name")) in shifted:
            found = unshifted(entry.get("dax_expression"))
            if found is not None:
                return found.calendar
    return ""


def _calendar_column(
    calendar: str, wanted: set[str], relationships: object, joined: object
) -> str:
    """The calendar's column whose name is one of ``wanted``, as Power BI spells it."""
    for raw in as_list(relationships):
        rel = as_map(raw)
        if str(rel.get("to_table")) == calendar and _loose(rel.get("to_column")) in (
            wanted
        ):
            return str(rel["to_column"])
    entry = as_map(as_map(joined).get(calendar))
    for column in [entry.get("dim_key"), *as_list(entry.get("dim_columns"))]:
        if column and _loose(column) in wanted:
            return str(column)
    return ""


def shifted_time_dimension(
    spec: RawMap,
    order: str,
    resolved: object,
    shifted: set[str],
    relationships: object,
    joined: object,
) -> RawMap | None:
    """The time dimension the time-shifted measures of a view are compared by, None when
    Power BI cannot be grouped the same way."""
    calendar = _calendar(resolved, shifted)
    expr = next(
        (
            str(as_map(d).get("expr") or "")
            for d in as_list(spec.get("dimensions"))
            if str(as_map(d).get("name")) == order
        ),
        "",
    )
    if not calendar or not expr:
        return None
    ucmv: dict[str, object] = {"mode": "column", "name": order}
    key = _KEY.match(expr)
    if key:
        wanted = {_loose(key.group(1))}
    elif _MONTH.match(expr):
        # The month of a date: as YYYY0MM it is the calendar's fiscal period.
        wanted = {"fiscper"}
        ucmv["period_format"] = "date_to_fiscper"
    else:
        return None
    column = _calendar_column(calendar, wanted, relationships, joined)
    if not column:
        return None
    return {
        "grain": "month",
        "ucmv": ucmv,
        "pbi_table": calendar,
        "pbi_column": column,
    }


def split(mapping: UCMVMapping) -> tuple[UCMVMapping, UCMVMapping | None]:
    """``mapping`` without its time-shifted measures, and those measures with their own
    time dimension (None when there are none)."""
    td = mapping.binding.shifted_time_dimension
    shifted = [m for m in mapping.measures if m.time_shifted]
    if td is None or not shifted:
        return mapping, None
    plain = replace(
        mapping, measures=[m for m in mapping.measures if not m.time_shifted]
    )
    binding = replace(mapping.binding, time_dimension=td)
    return plain, replace(mapping, binding=binding, measures=shifted)


def merge_frames(
    first: pd.DataFrame, second: pd.DataFrame, dimension_name: str
) -> pd.DataFrame:
    """Two result frames keyed by (dimension, period) as one."""
    if first.empty:
        return second
    if second.empty:
        return first
    return first.merge(second, on=[dimension_name, "period"], how="outer")
