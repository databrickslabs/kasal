"""Joins, dimensions and measures of a bound metric view, checked against its views' real columns.

Read mode binds each metric view to its source-layer views. When the real columns of those views
are known (``source_columns.py``), an item that reads a column its view does not have cannot
deploy: the whole view fails, and the healer removes what it can name. Such an item is removed
here, with the reason, before the writer sees it:

* a join whose ON clause reads a column its view (or the fact's view) lacks is dropped;
* a dimension or measure that reads a column its view lacks, or a join that was dropped, is
  dropped with its reason. A measure goes to the untranslated list, a dimension to the gaps.

Pure functions: no I/O.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Mapping

from .data_classes import MetricViewSpec, TranslationResult
from .source_columns import QUALIFIER, columns_read
from .source_layer_binding import normalized_name


@dataclasses.dataclass
class SourceGap:
    """One item the guard removed from a view, and why."""

    view: str
    kind: str  # "join" | "dimension" | "measure"
    name: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "view": self.view,
            "kind": self.kind,
            "name": self.name,
            "reason": self.reason,
        }


@dataclasses.dataclass
class GuardResult:
    """What is left of a bound view, the removed measures and the gaps."""

    joins: list[dict]
    dimensions: list[dict]
    measures: list[TranslationResult]
    removed_measures: list[TranslationResult]
    gaps: list[SourceGap]


def view_key_by_name(views: Mapping[str, str]) -> dict[str, str]:
    """``{normalized view name: PBI table key}`` for the bound views' names."""
    return {normalized_name(name): key for key, name in views.items()}


def _missing(text: str, alias: str, columns: frozenset[str] | None) -> list[str]:
    """Columns the expression reads through ``alias`` that the view does not have (sorted)."""
    if columns is None:
        return []
    return sorted(columns_read(text, alias) - columns)


_ONE_COLUMN = re.compile(r"\s*(\w+)\.`?(\w+)`?\s*")


def _fact_column_of_dropped_join(
    expr: str, dropped: set[str], fact_columns: frozenset[str] | None
) -> str:
    """``source.<col>`` when ``expr`` is ``<dropped join>.<col>`` and the fact has ``col``
    itself; else ``""``. Such a dimension reads the fact's column instead of vanishing.
    """
    match = _ONE_COLUMN.fullmatch(expr)
    if not match or match.group(1) not in dropped or not fact_columns:
        return ""
    column = match.group(2)
    if column.lower() not in fact_columns:
        return ""
    return f"{QUALIFIER}.{column}"


def guard_view(
    key: str,
    *,
    joins: list[dict],
    dimensions: list[dict],
    measures: list[TranslationResult],
    fact_columns: frozenset[str] | None,
    columns_by_key: Mapping[str, frozenset[str]],
    key_by_view: Mapping[str, str],
) -> GuardResult:
    """Check one bound view. ``fact_columns`` is None when the fact's columns are not known;
    ``columns_by_key`` holds the known columns (lower case) of each joined view."""
    gaps: list[SourceGap] = []
    kept_joins: list[dict] = []
    join_cols: dict[str, frozenset[str] | None] = {}
    dropped: set[str] = set()
    for join in joins:
        alias = str(join.get("name") or "")
        on = str(join.get("join_on") or join.get("on") or "")
        target = key_by_view.get(normalized_name(str(join.get("source") or "")))
        cols = columns_by_key.get(target) if target else None
        join_cols[alias] = cols
        reason = ""
        fact_gap = _missing(on, QUALIFIER, fact_columns)
        join_gap = _missing(on, alias, cols)
        if fact_gap:
            reason = f"its ON clause reads {', '.join(fact_gap)}, which the fact's source view does not have"
        elif join_gap:
            reason = f"its ON clause reads {', '.join(join_gap)}, which the source view of {target} does not have"
        if reason:
            dropped.add(alias)
            gaps.append(SourceGap(key, "join", alias, reason))
            continue
        kept_joins.append(join)

    def reason_for(text: str) -> str:
        if not text:
            return ""
        for alias in sorted(dropped):
            if re.search(rf"(?<![\w.]){re.escape(alias)}\.", text):
                return f"reads join {alias}, which is not in the view"
        missing = _missing(text, QUALIFIER, fact_columns)
        if missing:
            return f"reads column {', '.join(missing)}, which the fact's source view does not have"
        for alias, cols in join_cols.items():
            if alias in dropped:
                continue
            gap = _missing(text, alias, cols)
            if gap:
                return f"reads column {', '.join(gap)}, which the source view of join {alias} does not have"
        return ""

    kept_dims: list[dict] = []
    # A name whose dimension was dropped stays taken: a later dimension of the same name (from
    # another join, e.g. an alternative calendar) must not silently take over what the view
    # read before. The writer already kept the first of such names; this keeps that.
    taken_by_dropped: set[str] = set()
    for dim in dimensions:
        name = str(dim.get("name") or "")
        own = _fact_column_of_dropped_join(
            str(dim.get("expr") or ""), dropped, fact_columns
        )
        if own:  # a column of a dropped join that the fact has itself (e.g. a period)
            kept_dims.append({**dim, "expr": own})
            continue
        if name.lower() in taken_by_dropped:
            gaps.append(
                SourceGap(
                    key,
                    "dimension",
                    name,
                    "a dimension of the same name was dropped; this one would replace it",
                )
            )
            continue
        reason = reason_for(str(dim.get("expr") or ""))
        if reason:
            taken_by_dropped.add(name.lower())
            gaps.append(SourceGap(key, "dimension", name, reason))
            continue
        kept_dims.append(dim)

    kept_measures: list[TranslationResult] = []
    removed: list[TranslationResult] = []
    for measure in measures:
        reason = reason_for(str(measure.sql_expr or ""))
        if not reason:
            kept_measures.append(measure)
            continue
        measure.is_translatable = False
        measure.sql_expr = None
        measure.window_spec = None
        measure.skip_reason = f"not emitted: {reason}"
        removed.append(measure)
        gaps.append(SourceGap(key, "measure", measure.measure_name, reason))
    return GuardResult(kept_joins, kept_dims, kept_measures, removed, gaps)


def guard_bound_spec(
    key: str,
    spec: MetricViewSpec,
    *,
    columns_by_key: Mapping[str, frozenset[str]],
    key_by_view: Mapping[str, str],
) -> tuple[MetricViewSpec, GuardResult]:
    """The bound spec with the guarded joins, dimensions and measures, and what was removed.

    The input spec is not mutated: its lists are replaced in the returned copy."""
    result = guard_view(
        key,
        joins=list(spec.joins),
        dimensions=list(spec.dimensions),
        measures=list(spec.measures),
        fact_columns=columns_by_key.get(key),
        columns_by_key=columns_by_key,
        key_by_view=key_by_view,
    )
    guarded = dataclasses.replace(
        spec,
        joins=result.joins,
        dimensions=result.dimensions,
        measures=result.measures,
        untranslatable=[*spec.untranslatable, *result.removed_measures],
    )
    return guarded, result
