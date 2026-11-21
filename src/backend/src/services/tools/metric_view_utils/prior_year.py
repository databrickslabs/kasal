"""Prior-year measures in the metric view pipeline.

A metric view has the year shift as a window on a measure::

    - name: sales_py
      expr: MEASURE(sales)
      window:
        - order: fiscper_key
          range: current
          offset: -1000
          semiadditive: last

Three steps, around the translation the pipeline already does:

1. ``take_out_shifts`` (before allocation): every prior-year measure gets the DAX of the
   calculation it shifts (``prior_year_dax.unshifted``). From here on it is an ordinary measure:
   it is homed on the fact its columns are on and translated by the rules or the LLM.
2. ``follow_base_measures`` (after allocation): the prior year of one other measure lives on
   the table that measure lives on.
3. ``add_windows`` (after the tables are processed): each translated prior-year measure gets
   the window, the view gets the dimension the window orders by, and the measure gets its
   original DAX back.

The window orders by a number or a date, so the view needs one of:

- a period column whose values are year-first keys (``fiscper`` YYYYPPP, ``calmonth`` YYYYMM;
  more with ``prior_year_period_steps`` in the pipeline config): the view gets ``<period>_key``
  (the period as a number), and one year back is 1000 (or 100) keys back;
- the calendar the DAX shifts on, joined: the view gets ``<date>_month`` (the month of the
  calendar's date), and one year back is ``-1 year``.

A prior-year measure in a view with neither is not emitted (with the reason). Queried, a
prior-year measure returns a value when the query groups by that dimension and by no other
time dimension: the window moves along it and keeps every other grouped value fixed.

``prior_year_measures: false`` in the pipeline config turns all of this off.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from .data_classes import MetricViewSpec, TranslationResult
from .measure_allocator import (
    extract_column_refs,
    extract_measure_refs,
    measure_home,
    move_measure_home,
)
from .prior_year_dax import unshifted

MARK = "_prior_year"
# Mapping key on a helper measure (prior_year_mixed.py): the Power BI measure it shifts.
HELPER_OF = "_prior_year_helper_of"
# Year-first period keys, and how many keys one year is.
PERIOD_STEPS: dict[str, int] = {
    "fiscper": 1000,
    "calmonth": 100,
    "yearmonth": 100,
    "year_month": 100,
}
_MEASURE_REF = re.compile(r"\bMEASURE\(\s*`?(\w+)`?\s*\)", re.IGNORECASE)


def enabled(config: Mapping[str, object]) -> bool:
    return config.get("prior_year_measures") is not False


def take_out_shifts(mapping: list[dict], config: Mapping[str, object]) -> int:
    """Give every prior-year measure the DAX of what it shifts; the original and the calendar
    stay under ``MARK``. Returns how many measures were changed."""
    if not enabled(config):
        return 0
    changed = 0
    for entry in mapping:
        dax = entry.get("dax_expression")
        found = unshifted(dax)
        if found is None:
            continue
        entry[MARK] = {
            "dax": dax,
            "calendar": found.calendar,
            "column": found.column,
            "helper_of": entry.get(HELPER_OF),
        }
        entry["dax_expression"] = found.dax
        changed += 1
    return changed


def _key(name: object) -> str:
    return re.sub(r"\s+", "", str(name or "")).lower()


def follow_base_measures(mapping: list[dict]) -> None:
    """The prior year of one other measure (``[Sales]`` one year back) is homed where that
    measure is: only there can it be written as ``MEASURE(sales)``."""
    by_name = {
        _key(entry.get(field)): entry
        for entry in mapping
        for field in ("measure_name", "original_name")
        if entry.get(field)
    }
    for entry in mapping:
        if MARK not in entry:
            continue
        dax = str(entry.get("dax_expression") or "")
        references = extract_measure_refs(dax)
        if len(references) != 1 or extract_column_refs(dax):
            continue
        base = by_name.get(next(iter(references)))
        home = measure_home(base) if base is not None and base is not entry else None
        if home and home != measure_home(entry):
            move_measure_home(entry, home)


def _period_dimension(
    spec: MetricViewSpec, steps: Mapping[str, int]
) -> tuple[dict, int] | None:
    by_name = {str(d.get("name") or "").lower(): d for d in spec.dimensions}
    known = {str(c).lower() for c in spec.fact_columns}
    for name, step in steps.items():
        dimension = by_name.get(name.lower())
        if dimension is None or not dimension.get("expr"):
            continue
        # A period the fact is known not to have is not emitted (a phantom dimension).
        read = re.findall(r"\bsource\.`?(\w+)`?", str(dimension["expr"]))
        if known and any(column.lower() not in known for column in read):
            continue
        return dimension, step
    return None


def _loose(name: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _calendar_date(
    spec: MetricViewSpec, info: Mapping[str, object], config: Mapping[str, object]
) -> str:
    """``alias.column`` of the shift's calendar date in this view, "" when the view does not
    join that calendar or the column's name there is not known."""
    joined = config.get("join_key_map")
    entry = joined.get(info.get("calendar")) if isinstance(joined, Mapping) else None
    if not isinstance(entry, Mapping):
        return ""
    alias = str(entry.get("alias") or "")
    if not alias or not any(j.get("name") == alias for j in spec.joins):
        return ""
    wanted = _loose(info.get("column"))
    columns = [entry.get("dim_key"), *(entry.get("dim_columns") or [])]
    for column in columns:
        if column and _loose(column) == wanted:
            return f"{alias}.{column}"
    return ""


def _free_name(spec: MetricViewSpec, wanted: str, expr: str) -> str:
    """``wanted``, or a variant no other dimension or measure of the view has."""
    taken = {str(d.get("name")): str(d.get("expr")) for d in spec.dimensions}
    taken.update({m.measure_name: "" for m in spec.measures})
    name = wanted
    while name in taken and taken[name] != expr:
        name += "_py"
    return name


class _Order:
    """The dimension a view's prior-year windows order by: found once per calendar, and
    added to the view when the first measure needs it."""

    def __init__(self, spec: MetricViewSpec, config: Mapping[str, object]) -> None:
        self.spec = spec
        self.config = config
        self._found: dict[str, tuple[str, object] | None] = {}

    def _add(self, after: dict | None, wanted: str, expr: str, text: str) -> str:
        name = _free_name(self.spec, wanted, expr)
        if not any(d.get("name") == name for d in self.spec.dimensions):
            dimension = {"name": name, "expr": expr, "comment": text}
            at = (
                self.spec.dimensions.index(after) + 1
                if after is not None
                else len(self.spec.dimensions)
            )
            self.spec.dimensions.insert(at, dimension)
        return name

    def for_shift(self, info: Mapping[str, object]) -> tuple[str, object] | None:
        """``(dimension, offset)`` for a shift on ``info``'s calendar, None when the view
        has nothing a window can order by."""
        calendar = str(info.get("calendar"))
        if calendar not in self._found:
            self._found[calendar] = self._find(info)
        return self._found[calendar]

    def _find(self, info: Mapping[str, object]) -> tuple[str, object] | None:
        configured = self.config.get("prior_year_period_steps")
        steps = {**PERIOD_STEPS}
        if isinstance(configured, Mapping):
            steps.update({str(k): int(v) for k, v in configured.items()})
        period = _period_dimension(self.spec, steps)
        if period is not None:
            dimension, step = period
            name = self._add(
                dimension,
                f"{dimension['name']}_key",
                f"TRY_CAST({dimension['expr']} AS INT)",
                f"{dimension['name']} as a number. Prior-year measures are read by it: "
                f"group by it, and by no other time dimension.",
            )
            return name, -step
        date = _calendar_date(self.spec, info, self.config)
        if not date:
            return None
        name = self._add(
            None,
            f"{date.split('.', 1)[1]}_month",
            f"DATE_TRUNC('MONTH', TRY_CAST({date} AS DATE))",
            "Month of the calendar date. Prior-year measures are read by it: group by "
            "it, and by no other time dimension.",
        )
        return name, "-1 year"


def _demote(spec: MetricViewSpec, measure: TranslationResult, reason: str) -> None:
    measure.is_translatable = False
    measure.sql_expr = None
    measure.window_spec = None
    measure.skip_reason = reason
    spec.measures.remove(measure)
    spec.untranslatable.append(measure)


def _demote_dependents(spec: MetricViewSpec, gone: set[str]) -> None:
    """A measure that reads a measure the view no longer has goes with it."""
    while True:
        dependents = [
            m
            for m in spec.measures
            if gone & {n.lower() for n in _MEASURE_REF.findall(str(m.sql_expr or ""))}
        ]
        if not dependents:
            return
        for measure in dependents:
            gone.add(measure.measure_name.lower())
            _demote(
                spec,
                measure,
                "uses a prior-year measure that is not in this view "
                "(the view has no period a window can order by)",
            )


def _window_one_view(
    spec: MetricViewSpec,
    marked: Mapping[str, Mapping[str, object]],
    config: Mapping[str, object],
) -> list[str]:
    order = _Order(spec, config)
    windowed: list[str] = []
    gone: set[str] = set()
    for measure in list(spec.measures):
        info = marked.get(measure.original_name)
        if info is None:
            continue
        measure.dax_expression = str(info["dax"])
        found = None if measure.window_spec else order.for_shift(info)
        if found is None:
            gone.add(measure.measure_name.lower())
            _demote(
                spec,
                measure,
                (
                    "prior year of a measure that is a window already"
                    if measure.window_spec
                    else "prior year: the view has no period a window can order by "
                    "(a fiscper / calmonth column, or the measure's calendar joined)"
                ),
            )
            continue
        dimension, offset = found
        measure.window_spec = {
            "order": dimension,
            "range": "current",
            "offset": offset,
            "semiadditive": "last",
        }
        helper_of = info.get("helper_of")
        measure.helper_of = str(helper_of) if helper_of else None
        windowed.append(measure.measure_name)
    _demote_dependents(spec, gone)
    for measure in spec.untranslatable:
        info = marked.get(measure.original_name)
        if info is not None:
            measure.dax_expression = str(info["dax"])
    return windowed


def add_windows(
    specs: Mapping[str, MetricViewSpec],
    mapping: list[dict],
    config: Mapping[str, object],
) -> dict[str, list[str]]:
    """Add the window to each translated prior-year measure and give every prior-year
    measure its original DAX back (in ``specs`` and in ``mapping``). Returns the measures
    that got a window, per view."""
    marked = {
        str(entry.get(field)): entry[MARK]
        for entry in mapping
        if MARK in entry
        for field in ("original_name", "measure_name")
        if entry.get(field)
    }
    done: dict[str, list[str]] = {}
    if marked:
        for key, spec in specs.items():
            windowed = _window_one_view(spec, marked, config)
            if windowed:
                done[key] = windowed
    for entry in mapping:
        info = entry.pop(MARK, None)
        if info is not None:
            entry["dax_expression"] = info["dax"]
    return done
