"""The reachability closure: everything a viewer-reachable page needs, transitively.

1. Seeds: every measure / column drawn on, or filtering, a page a viewer can reach.
   Visual filters count whatever their condition; page and report filters only
   with an active condition. A field drawn on a page is no filter there.
2. Closure: everything their DAX references — measures, columns, tables — plus
   the DAX of reached calculated columns and tables, every item of a reached
   calculation group, the bodies of called user-defined functions, the tables a
   reached table's M query merges / appends, and calculated columns that are
   relationship keys between two reached tables.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from src.services.powerbi.scope import report_reachability as rr
from src.services.powerbi.scope.dax_refs import (
    called_functions,
    function_call_pattern,
    iter_references,
    name_lookup,
    qualified_references,
    referenced_tables,
    table_reference_pattern,
)
from src.services.powerbi.scope.parts import PartTree, arr, obj, text_at
from src.services.powerbi.scope.tmdl_model import Model, power_query_tables

CALC_COLUMN = "calc column "
CALC_TABLE = "calc table "
CALC_GROUP = "calc group "
FUNCTION_LABEL = "function "
POWER_QUERY = "power query "  # the M query of this table reads the table
RELATIONSHIP = "relationship "  # a calculated column that is a relationship key

Node = tuple  # ("m", measure) / ("c", table, column) / ("t", table) / ("f", function)


class References:
    """Direct references of a DAX expression: measures, (table, column) pairs, tables, functions."""

    def __init__(self, model: Model):
        self.model = model
        self.measure_names = name_lookup(model.home)
        self.columns_by_key = {
            (t.lower(), c.lower()): (t, c)
            for t, table in model.tables.items()
            for c in table.columns
        }
        self.table_pattern = table_reference_pattern(list(model.tables))
        self.function_names = name_lookup(model.functions)
        self.function_pattern = function_call_pattern(list(model.functions))

    def functions(self, expression: str | None) -> set[str]:
        return called_functions(expression, self.function_pattern, self.function_names)

    def nodes(self, expression: str | None, own_table: str | None = None) -> list[Node]:
        measures, columns, tables = self.of(expression, own_table)
        return (
            [("m", m) for m in measures]
            + [("c", t, c) for t, c in columns]
            + [("t", t) for t in tables]
            + [("f", f) for f in self.functions(expression)]
        )

    def of(
        self, expression: str | None, own_table: str | None = None
    ) -> tuple[set, set, set]:
        measures, columns = qualified_references(
            expression, self.measure_names, self.columns_by_key
        )
        if (
            own_table
        ):  # in a calculated column, an unqualified [x] can be its own table's column
            own = {
                column.lower(): column
                for column in self.model.tables[own_table].columns
            }
            for table, name in iter_references(expression):
                if table is None and name.lower() in own:
                    columns.add((own_table, own[name.lower()]))
                    measures.discard(self.measure_names.get(name.lower()))
        tables = referenced_tables(expression, self.table_pattern) | {
            t for t, _ in columns
        }
        return measures, columns, tables


@dataclass
class Seeds:
    pages: set[str]  # page ids included
    drawn: dict[Node, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )  # -> page names
    filters: dict[Node, set[str]] = field(default_factory=lambda: defaultdict(set))
    on_canvas: set[Node] = field(
        default_factory=set
    )  # drawn in a visual other than a slicer
    page_rows: list[dict[str, str]] = field(default_factory=list)


def _field_key(expr: object, model: Model) -> Node | None:
    expr = (
        obj(expr, "Aggregation", "Expression") or expr
    )  # implicit aggregation of a column
    for kind in ("Measure", "Column"):
        node = obj(expr, kind)
        if node:
            entity = text_at(node, "Expression", "SourceRef", "Entity") or ""
            prop = text_at(node, "Property") or ""
            if (
                kind == "Measure"
            ):  # resolved by name; the entity can name its old home table
                return ("m", prop) if prop in model.home else None
            table = model.tables.get(entity)
            if table is not None and prop in table.columns:
                return ("c", entity, prop)
            return None
    return None


def navigation_texts(model: Model, measures: set[str]) -> dict[str, str]:
    """measure -> its DAX plus the source of every table it reads (where a button's page names are)."""
    pattern = table_reference_pattern(list(model.tables))
    texts = {}
    for name in sorted(measures & model.home.keys()):
        dax = model.measure(name)
        tables = sorted(referenced_tables(dax, pattern))
        texts[name] = "\n".join([dax] + [model.tables[t].source for t in tables])
    return texts


def collect_seeds(
    report: PartTree, model: Model, include_measure_dt: bool = False
) -> Seeds:
    visuals = rr.load_visuals(report)
    texts = navigation_texts(model, rr.navigation_measures(visuals))
    reach = rr.report_reachability(report, texts)
    allowed = {rr.REACHABLE, rr.MEASURE_DT} if include_measure_dt else {rr.REACHABLE}
    seeds = Seeds(
        {pid for pid, e in reach.reached.items() if rr.page_status(e) in allowed}
    )
    seeds.page_rows = rr.page_rows(reach)
    pages = reach.pages
    shown = [
        v
        for v in rr.visible_visuals(visuals, rr.revealed_visuals(report))
        if v.page in seeds.pages
    ]
    for visual in (
        shown
    ):  # every drawn field first: a filter counts only where its field isn't drawn
        _visual_seeds(visual, pages[visual.page].name, model, seeds)
    for visual in shown:
        _filter_seeds(visual.raw, pages[visual.page].name, model, seeds, False)
    for pid in sorted(seeds.pages):
        _filter_seeds(
            report.json(f"pages/{pid}/page.json"), pages[pid].name, model, seeds, True
        )
    if seeds.pages:
        _filter_seeds(report.json("report.json"), "(report filter)", model, seeds, True)
    return seeds


def _visual_seeds(
    visual: rr.Visual, page_name: str, model: Model, seeds: Seeds
) -> None:
    query_state = obj(visual.raw, "visual", "query", "queryState")
    for role in query_state.values():
        for projection in arr(role, "projections"):
            key = _field_key(obj(projection, "field"), model)
            if key:
                seeds.drawn[key].add(page_name)
                if "slicer" not in visual.type.lower():
                    seeds.on_canvas.add(key)


def _filter_seeds(
    owner: object, page_name: str, model: Model, seeds: Seeds, only_active: bool
) -> None:
    """The filters in ``owner``'s ``filterConfig`` (a visual, page or report part)."""
    for entry in arr(owner, "filterConfig", "filters"):
        if only_active and not obj(entry, "filter").get("Where"):
            continue
        key = _field_key(obj(entry, "field"), model)
        if key and page_name not in seeds.drawn.get(key, ()):
            seeds.filters[key].add(page_name)


@dataclass
class Closure:
    measures: set[str] = field(default_factory=set)
    columns: set[tuple[str, str]] = field(default_factory=set)
    tables: set[str] = field(default_factory=set)
    functions: set[str] = field(default_factory=set)
    followed_tables: set[str] = field(
        default_factory=set
    )  # calc tables / groups followed
    visited_tables: set[str] = field(default_factory=set)
    used_by: dict[Node, set[str]] = field(default_factory=lambda: defaultdict(set))


def close(model: Model, seeds: Seeds) -> Closure:
    """Everything the seeds need, transitively."""
    refs = References(model)
    result = Closure()
    todo: list[tuple[Node, str | None]] = [
        (key, None) for key in list(seeds.drawn) + list(seeds.filters)
    ]
    while todo:
        while todo:
            node, referrer = todo.pop()
            if referrer:
                result.used_by[node].add(referrer)
            todo += _visit(node, model, refs, result)
        todo = _relationship_key_columns(model, result)
    return result


def _relationship_key_columns(
    model: Model, result: Closure
) -> list[tuple[Node, str | None]]:
    """Calculated columns that join two reached tables: the relationship needs their DAX."""
    found: list[tuple[Node, str | None]] = []
    for from_table, from_column, to_table, to_column, _active in model.relationships:
        if not {from_table, to_table} <= result.tables:
            continue
        ends = ((from_table, from_column, to_table), (to_table, to_column, from_table))
        for table, column, other in ends:
            if (table, column) not in result.columns and model.tables[
                table
            ].columns.get(column):
                found.append((("c", table, column), f"{RELATIONSHIP}{other}"))
    return found


def _visit(
    node: Node, model: Model, refs: References, result: Closure
) -> list[tuple[Node, str | None]]:
    expression: str | None
    own_table: str | None
    if node[0] == "m":
        if node[1] in result.measures:
            return []
        result.measures.add(node[1])
        expression, own_table, label = model.measure(node[1]), None, node[1]
    elif node[0] == "c":
        table, column = node[1], node[2]
        if (table, column) in result.columns:
            return []
        result.columns.add((table, column))
        result.tables.add(table)
        expression = model.tables[table].columns[column]
        own_table, label = table, f"{CALC_COLUMN}{table}[{column}]"
    elif node[0] == "f":
        if node[1] in result.functions:
            return []
        result.functions.add(node[1])
        expression, own_table, label = (
            model.functions[node[1]],
            None,
            f"{FUNCTION_LABEL}{node[1]}",
        )
    else:
        return _visit_table(node[1], model, refs, result)
    if not expression:
        return [(("t", own_table), None)] if own_table else []
    return [(n, label) for n in refs.nodes(expression, own_table)]


def _visit_table(
    name: str, model: Model, refs: References, result: Closure
) -> list[tuple[Node, str | None]]:
    """A calculated table's DAX, or every item of a calculation group, is followed once; so are the
    tables its M query reads."""
    table = model.tables[name]
    result.tables.add(name)
    if name in result.visited_tables:
        return []
    result.visited_tables.add(name)
    nodes: list[tuple[Node, str | None]] = [
        (("t", other), f"{POWER_QUERY}{name}")
        for other in sorted(power_query_tables(table, model))
    ]
    if table.kind == "calculated":
        expression, label = table.source, f"{CALC_TABLE}{name}"
    elif table.calc_items:
        expression, label = "\n".join(table.calc_items.values()), f"{CALC_GROUP}{name}"
    else:
        return nodes
    result.followed_tables.add(name)
    return nodes + [(n, label) for n in refs.nodes(expression) if n != ("t", name)]
