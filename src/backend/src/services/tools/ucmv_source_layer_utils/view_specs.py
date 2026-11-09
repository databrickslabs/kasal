"""Source-layer entries -> ViewSpec -> DDL, plus the report-scope RLS hints.

One entry per PBI table: either a UC table (``source_table``) whose M transform
steps are folded in when ``m_expression`` is given, or verbatim ``native_sql``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder
from src.services.tools.metric_view_utils.source_layer_emitter import (
    CalcColumn,
    EmittedView,
    RowFilter,
    TableSource,
    ViewSpec,
    emit_views,
)
from src.services.tools.ucmv_source_layer_utils.inputs import (
    parse_json_list,
    parse_row_filters,
    parse_scope_rows,
    row_filter_for,
)
from src.services.tools.ucmv_source_layer_utils.names import (
    is_uc_fqn,
    quote_fqn,
    snake_case,
)
from src.services.tools.ucmv_source_layer_utils.rls_checks import (
    fact_rls_findings,
    parse_relationships,
    relationships_from_scope,
    rls_suggestion,
)

SOURCE_LAYER_PREFIX = "src_"


@dataclass(frozen=True)
class ViewEntry:
    """One source-layer view to emit (plain data, no pipeline objects)."""

    table: str
    source_table: str = ""
    native_sql: str = ""
    m_expression: str = ""
    group_by_columns: tuple[str, ...] = ()
    calc_columns: tuple[dict[str, str], ...] = ()
    target: str = ""


@dataclass
class SourceLayerPlan:
    """Specs ready for emit_views, and the M-fold TODOs per table."""

    specs: list[ViewSpec] = field(default_factory=list)
    fold_todos: dict[str, list[str]] = field(default_factory=dict)


def parse_view_entries(raw: object) -> list[ViewEntry]:
    entries: list[ViewEntry] = []
    seen: set[str] = set()
    for index, item in enumerate(parse_json_list(raw, "view_specs_json")):
        table = str(item.get("table") or "").strip()
        label = table or f"entry #{index + 1}"
        if not table:
            raise ValueError(f"view_specs_json: entry #{index + 1} needs a 'table'")
        if table in seen:
            raise ValueError(f"view_specs_json: table {table!r} is listed twice")
        seen.add(table)
        source_table = str(item.get("source_table") or "").strip()
        native_sql = str(item.get("native_sql") or "").strip()
        if bool(source_table) == bool(native_sql):
            raise ValueError(
                f"view_specs_json: {label} needs exactly one of source_table or native_sql"
            )
        if source_table and not is_uc_fqn(source_table):
            raise ValueError(
                f"view_specs_json: {label} source_table must be catalog.schema.table"
            )
        target = str(item.get("target") or "").strip()
        if target and not is_uc_fqn(target):
            raise ValueError(
                f"view_specs_json: {label} target must be catalog.schema.view"
            )
        calc = item.get("calc_columns") or []
        if not isinstance(calc, list) or not all(isinstance(c, dict) for c in calc):
            raise ValueError(
                f"view_specs_json: {label} calc_columns must be a list of objects"
            )
        groups = item.get("group_by_columns") or []
        if not isinstance(groups, list):
            raise ValueError(
                f"view_specs_json: {label} group_by_columns must be a list"
            )
        entries.append(
            ViewEntry(
                table=table,
                source_table=source_table,
                native_sql=native_sql,
                m_expression=str(item.get("m_expression") or ""),
                group_by_columns=tuple(str(g) for g in groups),
                calc_columns=tuple(
                    {str(k): str(v) for k, v in c.items()}
                    for c in calc
                    if isinstance(c, dict)
                ),
                target=target,
            )
        )
    return entries


def fold_dimension_read(
    source_table: str, m_expression: str, group_by_columns: tuple[str, ...]
) -> tuple[str | None, list[str]]:
    """The M transform steps folded onto ``SELECT * FROM source``.

    Returns (SQL, TODOs); SQL is None when nothing was folded (plain passthrough).
    A fold that raises is reported as a TODO and the passthrough is kept.
    """
    base = f"SELECT * FROM {quote_fqn(source_table)}"
    folder = MTransformFolder()
    try:
        folded = folder.fold_from_mquery(
            base, m_expression, list(group_by_columns) or None
        )
    except Exception as exc:  # noqa: BLE001 — fail-open: surfaced as a TODO
        return None, [f"M transform fold failed: {exc}"]
    todos = list(getattr(folder, "transform_todos", None) or [])
    if not folded or folded.strip() == base.strip():
        return None, todos
    return folded, todos


def _calc_columns(entry: ViewEntry) -> list[CalcColumn]:
    columns: list[CalcColumn] = []
    for item in entry.calc_columns:
        name = item.get("name", "")
        if not name:
            continue
        if item.get("sql"):
            columns.append(CalcColumn(name=name, sql_expr=item["sql"]))
        else:
            columns.append(
                CalcColumn(
                    name=name,
                    sql_expr=item.get("expression", ""),
                    translation_status="todo",
                    comment="DAX calculated column: verify or translate before use",
                )
            )
    return columns


def build_source_layer_plan(
    entries: list[ViewEntry],
    catalog: str,
    schema: str,
    *,
    materialized: bool = True,
) -> SourceLayerPlan:
    plan = SourceLayerPlan()
    for entry in entries:
        if entry.target:
            target = quote_fqn(entry.target)
        else:
            name = snake_case(entry.table)
            if not name:
                raise ValueError(
                    f"table {entry.table!r} has no usable name for its view"
                )
            target = f"{quote_fqn(catalog)}.{quote_fqn(schema)}.`{SOURCE_LAYER_PREFIX}{name}`"
        todos: list[str] = []
        if entry.native_sql:
            source = TableSource(native_sql=entry.native_sql)
        else:
            folded: str | None = None
            if entry.m_expression:
                folded, todos = fold_dimension_read(
                    entry.source_table, entry.m_expression, entry.group_by_columns
                )
            source = (
                TableSource(native_sql=folded)
                if folded
                else TableSource(full_table_name=quote_fqn(entry.source_table))
            )
        plan.fold_todos[entry.table] = todos
        plan.specs.append(
            ViewSpec(
                table_name=entry.table,
                target_view=target,
                source=source,
                calc_columns=_calc_columns(entry),
                comment=f"Source layer for PBI table {entry.table}; read the metric view off this.",
                materialized=materialized,
            )
        )
    return plan


def _row_filter_summary(row_filter: RowFilter | None) -> dict[str, object] | None:
    # Constant values are not echoed: only the shape of the filter.
    if row_filter is None:
        return None
    return {
        "function": row_filter.function,
        "columns": list(row_filter.columns),
        "constant_count": len(row_filter.constants),
    }


def rls_review_hints(
    scope_rows: list[dict[str, str]],
    filters: dict[str, RowFilter],
    dax_by_table: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    """The scope table's ``rls_filter`` review rows as hints, never auto-applied.

    The RLS DAX is read from the row's ``dax`` key or ``dax_by_table``; when it is a
    simple expression the hint carries a ``value_access`` suggestion.
    """
    hints: list[dict[str, object]] = []
    for row in scope_rows:
        if row.get("object_type") != "rls_filter":
            continue
        table = row.get("pbi_table", "")
        configured = row_filter_for(table, filters) is not None
        dax = row.get("dax") or (dax_by_table or {}).get(table, "")
        suggestion = rls_suggestion(dax, row.get("name", "")) if dax else None
        hints.append(
            {
                "pbi_table": table,
                "roles": row.get("name", ""),
                "reason": row.get("reason", ""),
                "row_filter_configured": configured,
                "hint": (
                    "a row filter is configured for this table"
                    if configured
                    else "PBI restricts this table by role; configure row_filters "
                    "(a Unity Catalog function) or accept that it is unfiltered"
                ),
                "suggestion": suggestion,
            }
        )
    return hints


def emit_source_layer(
    *,
    entries_json: object,
    catalog: str,
    schema: str,
    row_filters_json: object,
    materialized: bool,
    scope_json: object,
    access_schema: str | None = None,
    relationships_json: object = None,
    rls_dax_json: dict[str, str] | None = None,
) -> dict[str, object]:
    """Emit the source-layer DDL with its row filters, RLS hints and fact checks (pure)."""
    filters = parse_row_filters(row_filters_json, access_schema)
    entries = parse_view_entries(entries_json)
    plan = build_source_layer_plan(entries, catalog, schema, materialized=materialized)
    for spec in plan.specs:
        spec.row_filter = row_filter_for(spec.table_name, filters)
    emitted: list[EmittedView] = emit_views(plan.specs)
    scope_rows = parse_scope_rows(scope_json)
    relationships = list(
        dict.fromkeys(
            parse_relationships(relationships_json)
            + relationships_from_scope(scope_rows)
        )
    )
    rls_warnings = fact_rls_findings(
        relationships, filters, {spec.table_name for spec in plan.specs}
    )
    by_table = {spec.table_name: spec for spec in plan.specs}
    views: dict[str, dict[str, object]] = {}
    for view in emitted:
        spec = by_table[view.table_name]
        views[view.table_name] = {
            "target": view.target_view,
            "ddl": view.ddl,
            "todo_steps": list(view.todo_steps)
            + plan.fold_todos.get(view.table_name, []),
            "error": view.error,
            "row_filter": _row_filter_summary(spec.row_filter),
        }
    notes = [
        "A row filter is set when the view is created; changing it means recreating the view.",
        "Dropping a materialized view leaves its __materialization_mat_* backing tables behind.",
    ]
    return {
        "views": views,
        "rls_warnings": rls_warnings,
        "rls_hints": rls_review_hints(scope_rows, filters, rls_dax_json),
        "notes": notes,
    }
