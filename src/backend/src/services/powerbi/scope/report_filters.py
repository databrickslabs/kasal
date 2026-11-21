"""Every filter of a report, the page scopes they form, and the page filters each measure is shown under.

A page-level filter on a code list (line types, material types, document types)
decides what a KPI counts, so the metric view has to carry it and the
reconciliation has to repeat it. Pages of one report often differ: grouping
pages with identical active filter sets gives the report's *scopes*, each
named by its pages. A filter on a dimension only reaches the facts related to
it (many side -> one side), so each scope says which tables it reaches.

Read from the PBIR parts: report-, page- and visual-level ``filterConfig``
entries (with the condition as text, and as ``{table, column, values}`` when it
is a plain IN list) plus slicers' saved selections, which filter data the same
way. Only definitions are read, never data.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TypedDict

from src.services.powerbi.scope import report_reachability as rr
from src.services.powerbi.scope.closure import References
from src.services.powerbi.scope.parts import JsonObj, PartTree, arr, obj, text_at
from src.services.powerbi.scope.tmdl_model import Model

_OPERATORS = {0: "=", 1: ">", 2: ">=", 3: "<", 4: "<="}


class InFilter(TypedDict):
    """A filter the reconciliation mapping can repeat (``extra_filter`` entry)."""

    table: str
    column: str
    values: list[str]


class FilterRow(TypedDict):
    level: str  # report / page / visual / slicer
    page: str
    visual_type: str
    visual_id: str
    field: str
    active: bool
    condition: str
    in_filter: InFilter | None
    hidden: bool
    locked: bool


class PageScope(TypedDict):
    scope: str  # named by its pages
    pages: list[str]
    conditions: list[str]
    filters: list[InFilter]
    reaches: list[str]  # tables the filters reach through relationships


def _literal(value: object) -> str:
    found = text_at(value, "Literal", "Value")
    return found.strip("'") if found is not None else json.dumps(value)[:60]


def field_text(expression: object, alias: Mapping[str, str]) -> str:
    """Readable ``Table[Column]`` for a PBIR field expression."""
    for kind in ("Column", "Measure"):
        node = obj(expression, kind)
        if node:
            source = obj(node, "Expression", "SourceRef")
            table = text_at(source, "Entity") or alias.get(text_at(source, "Source") or "", "?")
            return f"{table}[{text_at(node, 'Property')}]"
    aggregation = obj(expression, "Aggregation")
    if aggregation:
        return f"AGG{aggregation.get('Function')}({field_text(aggregation.get('Expression'), alias)})"
    level = obj(expression, "HierarchyLevel")
    if level:
        hierarchy = obj(level, "Expression", "Hierarchy", "Expression")
        return f"{field_text(hierarchy, alias)}.{text_at(level, 'Level')}"
    return json.dumps(expression)[:80]


def _column_ref(expression: object, alias: Mapping[str, str]) -> tuple[str, str] | None:
    node = obj(expression, "Column")
    if not node:
        return None
    source = obj(node, "Expression", "SourceRef")
    table = text_at(source, "Entity") or alias.get(text_at(source, "Source") or "", "")
    column = text_at(node, "Property")
    return (table, column) if table and column else None


def condition_text(condition: object, alias: Mapping[str, str]) -> str:
    """A PBIR filter condition as readable text."""
    negated = obj(condition, "Not")
    if negated:
        return f"NOT ({condition_text(negated.get('Expression'), alias)})"
    for joiner in ("And", "Or"):
        both = obj(condition, joiner)
        if both:
            left = condition_text(both.get("Left"), alias)
            right = condition_text(both.get("Right"), alias)
            return f"({left} {joiner.upper()} {right})"
    within = obj(condition, "In")
    if within:
        fields = ", ".join(field_text(e, alias) for e in arr(within, "Expressions"))
        if "Table" in within:
            return f"{fields} IN <table>"
        rows = [", ".join(_literal(v) for v in arr(r)) for r in arr(within, "Values")]
        return f"{fields} IN ({'; '.join(rows)})"
    comparison = obj(condition, "Comparison")
    if comparison:
        kind = comparison.get("ComparisonKind")
        operator = _OPERATORS.get(kind, str(kind)) if isinstance(kind, int) else str(kind)
        return f"{field_text(comparison.get('Left'), alias)} {operator} {_literal(comparison.get('Right'))}"
    for text_match in ("Contains", "StartsWith"):
        operands = obj(condition, text_match)
        if operands:
            return f"{field_text(operands.get('Left'), alias)} {text_match} {_literal(operands.get('Right'))}"
    return json.dumps(condition)[:200]


def _in_filter(conditions: list[object], alias: Mapping[str, str]) -> InFilter | None:
    """``{table, column, values}`` when the filter is exactly one plain IN list on one column."""
    if len(conditions) != 1:
        return None
    within = obj(conditions[0], "In")
    expressions = arr(within, "Expressions")
    if not within or len(expressions) != 1 or "Table" in within:
        return None
    ref = _column_ref(expressions[0], alias)
    if ref is None:
        return None
    values = [_literal(row[0]) for row in arr(within, "Values") if isinstance(row, list) and len(row) == 1]
    return {"table": ref[0], "column": ref[1], "values": values} if values else None


def _row(entry: object, level: str, page: str, visual_type: str, visual_id: str) -> FilterRow:
    definition = obj(entry, "filter")
    alias = {
        name: entity
        for source in arr(definition, "From")
        if (name := text_at(source, "Name")) and (entity := text_at(source, "Entity"))
    }
    conditions = [obj(where, "Condition") for where in arr(definition, "Where")]
    where = " AND ".join(condition_text(c, alias) for c in conditions)
    flags = obj(entry)
    return {
        "level": level,
        "page": page,
        "visual_type": visual_type,
        "visual_id": visual_id,
        "field": field_text(flags.get("field"), alias),
        "active": bool(where),
        "condition": where,
        "in_filter": _in_filter(list(conditions), alias),
        "hidden": bool(flags.get("isHiddenInViewMode")),
        "locked": bool(flags.get("isLockedInViewMode")),
    }


def _slicer_selection(visual: rr.Visual, page: str) -> FilterRow | None:
    """A slicer's saved selection (``objects.general[].properties.filter``) filters data like a page filter."""
    if "slicer" not in visual.type.lower():
        return None
    for general in arr(visual.raw, "visual", "objects", "general"):
        saved = obj(general, "properties", "filter", "filter")
        if saved and arr(saved, "Where"):
            projections = arr(obj(visual.raw, "visual", "query", "queryState"), "Values", "projections")
            first = projections[0] if projections else {}
            entry: JsonObj = {"filter": saved, "field": obj(first, "field")}
            return _row(entry, "slicer", page, visual.type, visual.name)
    return None


def collect_filters(report: PartTree) -> list[FilterRow]:
    """Filter rows of every level, report first, then pages, visuals and slicer selections."""
    pages = rr.load_pages(report)
    rows = [_row(e, "report", "", "", "") for e in arr(report.json("report.json"), "filterConfig", "filters")]
    for pid, page in pages.items():
        page_json = report.json(f"pages/{pid}/page.json")
        rows += [_row(e, "page", page.name, "", "") for e in arr(page_json, "filterConfig", "filters")]
    for visual in rr.load_visuals(report):
        page_name = pages[visual.page].name if visual.page in pages else visual.page
        filters = arr(visual.raw, "filterConfig", "filters")
        rows += [_row(e, "visual", page_name, visual.type, visual.name) for e in filters]
        selection = _slicer_selection(visual, page_name)
        if selection:
            rows.append(selection)
    return rows


def reached_tables(model: Model, table: str) -> set[str]:
    """Tables a filter on ``table`` reaches: itself and every table whose many-to-one path leads to it."""
    towards: dict[str, set[str]] = defaultdict(set)
    for from_table, _fc, to_table, _tc, active in model.relationships:
        if active:
            towards[to_table].add(from_table)
    found, todo = {table}, [table]
    while todo:
        for other in towards.get(todo.pop(), set()) - found:
            found.add(other)
            todo.append(other)
    return found


def page_scopes(rows: Iterable[FilterRow], model: Model | None = None) -> list[PageScope]:
    """Pages grouped by identical active page + slicer filter sets (report filters apply to all)."""
    by_page: dict[str, list[FilterRow]] = defaultdict(list)
    for row in rows:
        if row["active"] and row["level"] in ("page", "slicer"):
            by_page[row["page"]].append(row)
    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for page, page_rows in by_page.items():
        groups[tuple(sorted({r["condition"] for r in page_rows}))].append(page)
    scopes: list[PageScope] = []
    for conditions, pages in sorted(groups.items(), key=lambda kv: sorted(kv[1])):
        members = [r for p in pages for r in by_page[p] if r["condition"] in conditions]
        filters: list[InFilter] = []
        for f in (r["in_filter"] for r in members):
            if f is not None and f not in filters:
                filters.append(f)
        reaches = set().union(*(reached_tables(model, f["table"]) for f in filters)) if model and filters else set()
        scopes.append(
            {
                "scope": " / ".join(sorted(pages)),
                "pages": sorted(pages),
                "conditions": list(conditions),
                "filters": filters,
                "reaches": sorted(reaches),
            }
        )
    return scopes


def measure_page_filters(
    drawn: Mapping[tuple, set[str]], scopes: Iterable[PageScope], model: Model
) -> dict[str, list[InFilter]]:
    """PBI measure -> the page filters every page that draws it shares (the mapping's ``extra_filter``).

    Only when ALL pages drawing the measure carry the same filters, and only the
    filters that reach the measure's home table: a measure shown under two
    scopes is two KPI variants, which a person splits (scoped measure variants).
    """
    by_page = {p: s for s in scopes for p in s["pages"]}
    refs = References(model)
    out: dict[str, list[InFilter]] = {}
    for key, pages in drawn.items():
        if key[0] != "m" or key[1] not in model.home or not pages:
            continue
        scoped = [by_page.get(p) for p in pages]
        first = scoped[0]
        if first is None or any(s is not first for s in scoped):
            continue
        reads = {model.home[key[1]]} | refs.of(model.measure(key[1]))[2]
        filters = [f for f in first["filters"] if reads & reached_tables(model, f["table"])]
        if filters:
            out[key[1]] = filters
    return out
