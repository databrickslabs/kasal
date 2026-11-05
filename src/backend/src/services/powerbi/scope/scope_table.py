"""The scope table: one row per measure, calculation item, function, RLS filter, table and column.

``build_report_scope`` is the entry point: it takes the TMDL parts of the semantic
model and the PBIR report definition (both already fetched by the Pipeline Config
Generator), runs the reachability closure and returns the draft rows, a CSV and a
counts-only summary. ``mirrored_as`` is left for the generator (or a person) to
fill: it names the Databricks object(s) a kept row became, and the semantic sync
reads it to say what a change in Power BI means for the mirror.
"""

from __future__ import annotations

import csv
import io
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping

from src.services.powerbi.scope.closure import (
    CALC_COLUMN,
    CALC_GROUP,
    CALC_TABLE,
    FUNCTION_LABEL,
    POWER_QUERY,
    RELATIONSHIP,
    Closure,
    References,
    Seeds,
    close,
    collect_seeds,
)
from src.services.powerbi.scope.decisions import (
    TableContext,
    calc_item_decision,
    category,
    column_decision,
    measure_decision,
    selector_tables,
    table_decision,
)
from src.services.powerbi.scope.parts import PartTree
from src.services.powerbi.scope.report_context import report_context
from src.services.powerbi.scope.tmdl_model import (
    Model,
    Relationship,
    add_report_measures,
    load_model,
    power_query_tables,
)

SCOPE_FIELDS = [
    "object_type",
    "pbi_table",
    "name",
    "in_kasal",
    "drawn",
    "load_bearing_for",
    "filter_use",
    "decision",
    "reason",
    "mirrored_as",
]
_CALC_READERS = (CALC_COLUMN, CALC_TABLE, CALC_GROUP)


def _row(
    object_type: str, table: str, name: str, decision: str, reason: str, **extra: str
) -> dict:
    row = dict.fromkeys(SCOPE_FIELDS, "")
    row.update(
        object_type=object_type,
        pbi_table=table,
        name=name,
        decision=decision,
        reason=reason,
        **extra,
    )
    return row


def _pages(nodes: Mapping[tuple, set[str]], key: tuple) -> str:
    return "; ".join(sorted(nodes.get(key, ())))[:300]


def _reach(key: tuple, seeds: Seeds, closure: Closure) -> str:
    if key in seeds.drawn:
        return "drawn"
    if key in seeds.filters:
        return "filter"
    return "indirect" if key[1] in closure.measures else "unreached"


def build_scope(
    model: Model,
    seeds: Seeds,
    closure: Closure,
    kasal_status: Mapping[str, str] | None = None,
) -> list[dict]:
    """Draft rows; ``kasal_status``: PBI measure name -> what Kasal did with it (``in_kasal`` column)."""
    rows = _measure_rows(model, seeds, closure, kasal_status or {}) + _calc_item_rows(
        model, seeds, closure
    )
    kept = {r["name"] for r in rows if r["decision"] != "drop"}
    function_rows = _function_rows(model, closure, kept)
    kept |= {
        f"{FUNCTION_LABEL}{r['name']}" for r in function_rows if r["decision"] == "keep"
    }
    segment = {r["name"] for r in rows if r["reason"].startswith("segmentation")}
    ctx = TableContext(
        _table_readers(model, seeds, closure),
        kept,
        segment,
        _live_columns(seeds, closure, kept),
    )
    return (
        rows
        + function_rows
        + _role_rows(model)
        + _table_rows(model, seeds, closure, ctx)
    )


def _measure_rows(
    model: Model, seeds: Seeds, closure: Closure, kasal_status: Mapping[str, str]
) -> list[dict]:
    selectors = selector_tables(model)
    rows = []
    for home, name in sorted(
        (t, m) for t, table in model.tables.items() for m in table.measures
    ):
        key, expression = ("m", name), model.tables[home].measures[name]
        reach = _reach(key, seeds, closure)
        cat = category(name, expression, selectors) if reach != "unreached" else ""
        rows.append(
            _row(
                "measure",
                home,
                name,
                *measure_decision(name, expression, reach, cat, selectors),
                in_kasal=kasal_status.get(name, "no") if kasal_status else "",
                drawn=_pages(seeds.drawn, key),
                filter_use=_pages(seeds.filters, key),
                load_bearing_for=_pages(closure.used_by, key)
                if reach == "indirect"
                else "",
            )
        )
    return rows


def _group_reach(
    table: str, seeds: Seeds, closure: Closure
) -> tuple[str, set[str], set[str]]:
    drawn = {
        p
        for k, pages in seeds.drawn.items()
        if k[0] == "c" and k[1] == table
        for p in pages
    }
    filtered = {
        p
        for k, pages in seeds.filters.items()
        if k[0] == "c" and k[1] == table
        for p in pages
    }
    if drawn:
        return "drawn", drawn, filtered
    if filtered:
        return "filter", drawn, filtered
    return (
        ("indirect" if table in closure.followed_tables else "unreached"),
        drawn,
        filtered,
    )


def _calc_item_rows(model: Model, seeds: Seeds, closure: Closure) -> list[dict]:
    selectors = selector_tables(model)
    rows = []
    for name in sorted(t for t in model.tables if model.tables[t].calc_items):
        reach, drawn, filtered = _group_reach(name, seeds, closure)
        for item, expression in sorted(model.tables[name].calc_items.items()):
            decision = calc_item_decision(item, expression, reach, selectors)
            rows.append(
                _row(
                    "calculation_item",
                    name,
                    item,
                    *decision,
                    drawn="; ".join(sorted(drawn)),
                    filter_use="; ".join(sorted(filtered)),
                )
            )
    return rows


def _function_rows(model: Model, closure: Closure, kept: set[str]) -> list[dict]:
    """Functions are kept when something kept calls them, directly or through another kept function."""
    callers = {f: closure.used_by.get(("f", f), set()) for f in closure.functions}
    alive: set[str] = set()
    changed = True
    while changed:
        live_labels = kept | {f"{FUNCTION_LABEL}{f}" for f in alive}
        new = {
            f for f, users in callers.items() if f not in alive and users & live_labels
        }
        alive |= new
        changed = bool(new)
    rows = []
    for name in sorted(model.functions):
        if name in alive:
            decision = (
                "keep",
                "user-defined function called by a kept measure: its logic goes into the SQL of those measures",
            )
        elif name in closure.functions:
            decision = (
                "drop",
                "user-defined function called only by dropped measures (display / selector helpers)",
            )
        else:
            decision = ("drop", "not reachable (no reachable measure calls it)")
        rows.append(
            _row(
                "function",
                "",
                name,
                *decision,
                load_bearing_for=_pages(closure.used_by, ("f", name)),
            )
        )
    return rows


def _role_rows(model: Model) -> list[dict]:
    """One review row per table an RLS role filters: which UC row filter replaces it is a person's call."""
    refs = References(model)
    roles: dict[str, set[str]] = defaultdict(set)
    reads: dict[str, set[str]] = defaultdict(set)
    dax_by_table: dict[str, list[str]] = defaultdict(list)
    for role, table, dax in model.roles:
        roles[table].add(role)
        dax_by_table[table].append(dax.strip())
        reads[table] |= refs.of(dax, table if table in model.tables else None)[2] - {
            table
        }
    rows = []
    for table in sorted(roles, key=str.lower):
        reason = f"row-level security in {len(roles[table])} role(s): decide the Unity Catalog row filter that replaces it"
        if reads[table]:
            reason += f"; reads {', '.join(sorted(reads[table]))}"
        row = _row("rls_filter", table, "; ".join(sorted(roles[table])), "review", reason)
        # Not a CSV column: the role filters' DAX, for the Source Layer Manager's value_access suggestions.
        row["dax"] = "\n".join(sorted(set(dax_by_table[table])))
        rows.append(row)
    return rows


def _table_rows(
    model: Model, seeds: Seeds, closure: Closure, ctx: TableContext
) -> list[dict]:
    rel_keys = model.relationship_keys()
    used = Counter(k[1] for k in list(seeds.drawn) + list(seeds.filters) if k[0] == "c")
    rows = []
    for name in sorted(model.tables, key=str.lower):
        table = model.tables[name]
        decision, reason = table_decision(table, model, closure, ctx)
        rows.append(
            _row("table", name, "", decision, reason, drawn=str(used[name] or ""))
        )
        if decision in ("keep", "snapshot"):
            for column in sorted(table.columns):
                key = ("c", name, column)
                rows.append(
                    _row(
                        "column",
                        name,
                        column,
                        *column_decision((name, column), closure, seeds, rel_keys),
                        drawn=_pages(seeds.drawn, key),
                        filter_use=_pages(seeds.filters, key),
                    )
                )
    return rows


def _table_readers(model: Model, seeds: Seeds, closure: Closure) -> dict[str, set[str]]:
    refs = References(model)
    readers: dict[str, set[str]] = defaultdict(set)
    for name in closure.measures:
        for table in refs.of(model.measure(name))[2]:
            readers[table].add(name)
    for table, column in closure.columns:
        expression = model.tables[table].columns[column]
        if expression:
            for other in refs.of(expression, table)[2] - {table}:
                readers[other].add(f"{CALC_COLUMN}{table}[{column}]")
    for table in closure.tables:
        if model.tables[table].kind == "calculated":
            for other in refs.of(model.tables[table].source)[2] - {table}:
                readers[other].add(f"{CALC_TABLE}{table}")
        for other in power_query_tables(model.tables[table], model):
            readers[other].add(f"{POWER_QUERY}{table}")
    for table in closure.followed_tables:
        for expression in model.tables[table].calc_items.values():
            for other in refs.of(expression)[2] - {table}:
                readers[other].add(f"{CALC_GROUP}{table}")
    for function in closure.functions:
        for other in refs.of(model.functions[function])[2]:
            readers[other].add(f"{FUNCTION_LABEL}{function}")
    for key in list(seeds.drawn) + list(seeds.filters):
        if key[0] == "c":
            readers[key[1]].add(
                "on canvas" if key in seeds.on_canvas else "slicer/filter"
            )
    return readers


def _live_columns(seeds: Seeds, closure: Closure, kept: set[str]) -> set[str]:
    live = set()
    for table, column in closure.columns:
        node = ("c", table, column)
        users = closure.used_by.get(node, set())
        if (
            node in seeds.on_canvas
            or users & kept
            or any(u.startswith(_CALC_READERS + (RELATIONSHIP,)) for u in users)
        ):
            live.add(f"{CALC_COLUMN}{table}[{column}]")
    return live


def scope_csv(rows: Iterable[Mapping[str, str]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=SCOPE_FIELDS, lineterminator="\n", extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def scope_summary(
    rows: list[dict], seeds: Seeds, closure: Closure
) -> dict[str, object]:
    """Counts only: pages, reached objects, decisions per object type."""
    counts: dict[str, dict[str, int]] = defaultdict(dict)
    for (object_type, decision), n in Counter(
        (r["object_type"], r["decision"]) for r in rows
    ).items():
        counts[object_type][decision] = n
    statuses = Counter(r["status"] for r in seeds.page_rows)
    return {
        "pages_total": len(seeds.page_rows),
        "pages_reachable": statuses.get("reachable", 0),
        "pages_measure_drillthrough_only": statuses.get("measure-DT", 0),
        "pages_unreached": statuses.get("UNREACHED", 0),
        "fields_drawn": len(seeds.drawn),
        "fields_filter_only": len(seeds.filters),
        "reached_measures": len(closure.measures),
        "reached_columns": len(closure.columns),
        "reached_tables": len(closure.tables),
        "decisions": dict(counts),
    }


def _trees(
    tmdl_parts: list[dict] | PartTree,
    report_def: Mapping[str, object] | PartTree | None,
) -> tuple[PartTree, PartTree]:
    model_tree = (
        tmdl_parts
        if isinstance(tmdl_parts, PartTree)
        else PartTree.from_parts(tmdl_parts)
    )
    report = (
        report_def
        if isinstance(report_def, PartTree)
        else PartTree.from_report_definition(report_def)
    )
    return model_tree, report


def analyse(
    tmdl_parts: list[dict] | PartTree,
    report_def: Mapping[str, object] | PartTree | None,
    *,
    relationships: list[Relationship] | None = None,
    include_measure_dt: bool = False,
) -> tuple[Model, Seeds, Closure]:
    model_tree, report = _trees(tmdl_parts, report_def)
    model = load_model(model_tree, relationships)
    add_report_measures(model, report)
    seeds = collect_seeds(report, model, include_measure_dt)
    return model, seeds, close(model, seeds)


def build_report_scope(
    tmdl_parts: list[dict] | PartTree,
    report_def: Mapping[str, object] | PartTree | None,
    *,
    relationships: list[Relationship] | None = None,
    kasal_status: Mapping[str, str] | None = None,
    include_measure_dt: bool = False,
    scan_result: Mapping[str, object] | None = None,
    dataset_id: str = "",
    report_id: str = "",
) -> dict[str, object]:
    """``{rows, csv, summary, pages}`` plus the ``report_context`` evidence (filters, page
    scopes, measure page filters, links, related reports, inventory), or ``{}`` without inputs.

    Without a report definition no page is reachable and every measure would read
    as unreached, so the scope is not built at all rather than built wrong.
    """
    model_tree, report = _trees(tmdl_parts, report_def)
    if not report.glob("pages/*/page.json") or not model_tree.glob("tables/*.tmdl"):
        return {}
    model, seeds, closure = analyse(
        model_tree,
        report,
        relationships=relationships,
        include_measure_dt=include_measure_dt,
    )
    rows = build_scope(model, seeds, closure, kasal_status)
    context = report_context(
        model, seeds, closure, report, rows, scan_result=scan_result, dataset_id=dataset_id, report_id=report_id
    )
    return {
        "rows": rows,
        "csv": scope_csv(rows),
        "summary": scope_summary(rows, seeds, closure),
        "pages": seeds.page_rows,
        **context,
    }
