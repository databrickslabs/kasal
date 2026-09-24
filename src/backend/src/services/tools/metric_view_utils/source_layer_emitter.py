"""
Source-layer emitter for the Power BI → Unity Catalog Metric View pipeline.

Rec #3 from the IDOR/DQA post-mortem: for each PBI table, generate one UC
CREATE [OR REPLACE] [MATERIALIZED] VIEW DDL that reproduces the M query
transformations and DAX calculated columns, reading *base* UC tables only.
Metric views then only aggregate; they do not encode raw-data transformations.

Design principles
-----------------
* Pure function of its inputs — no I/O, no network calls, no pipeline imports.
* Inputs are plain dataclasses; callers build them from whatever pipeline state
  they have available.
* Unknown / untranslatable steps are emitted as SQL ``-- TODO`` comments and
  recorded in ``EmittedView.todo_steps`` (fail-open policy).
* Generates one CTE per M step so the DDL is readable and the optimizer can
  inline at will.

Typical caller flow::

    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`main`.`sales`.`dim_country_v`",
        source=TableSource(full_table_name="`datalake`.`udm`.`cust_exp_dim_company`"),
        m_steps=[
            FilterStep("region", "IN", ["REGION 1", "Italy"]),
            DeduplicateStep(["country_abbreviation"]),
            FilterStep("country_abbreviation", "NOT IN", ["RU", "BY"]),
            ReplaceStep("country_id", "ROI", "IE"),
        ],
        calc_columns=[
            CalcColumn("week445_label",
                       "concat(CAST(week_445 AS STRING), ' Y', CAST(year AS STRING))"),
        ],
        comment="Reproduces PBI Dim_Country.",
    )
    result = emit_view(spec)
    print(result.ddl)
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


# ---------------------------------------------------------------------------
# Input dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TableSource:
    """Resolved data source for one PBI table.

    Exactly one of ``full_table_name`` or ``native_sql`` must be set.

    ``full_table_name`` is a fully-qualified UC table name (backtick-quoted
    if needed), e.g. ``"`catalog`.`schema`.`table`"``.

    ``native_sql`` is the verbatim SQL string extracted from an M
    ``Value.NativeQuery(…, "<sql>")`` expression.
    """

    full_table_name: Optional[str] = None
    native_sql: Optional[str] = None

    def __post_init__(self) -> None:
        has_table = bool(self.full_table_name and self.full_table_name.strip())
        has_sql = bool(self.native_sql and self.native_sql.strip())
        if has_table == has_sql:
            raise ValueError(
                "TableSource: set exactly one of full_table_name or native_sql"
            )


@dataclass
class FilterStep:
    """Row filter: ``WHERE column operator value``.

    ``operator`` is one of ``=``, ``!=``, ``<``, ``<=``, ``>``, ``>=``,
    ``IN``, ``NOT IN``, ``IS NULL``, ``IS NOT NULL``, ``LIKE``.
    ``value`` is a scalar or (for IN / NOT IN) a list; ignored for IS NULL /
    IS NOT NULL.
    """

    column: str
    operator: str
    value: Any = None


@dataclass
class ReplaceStep:
    """Text replace on a single column: ``replace(column, old, new)``."""

    column: str
    old_value: str
    new_value: str


@dataclass
class DeduplicateStep:
    """Keep the first row per ``key_columns`` (M's ``Table.Distinct``).

    ``order_expr`` is a SQL ORDER BY fragment that makes the choice
    deterministic, e.g. ``"CASE WHEN id = 'EE' THEN 0 ELSE 1 END, id"``.
    Defaults to ``"1"`` (arbitrary but stable within the engine).
    """

    key_columns: List[str]
    order_expr: str = "1"


@dataclass
class AppendRowsStep:
    """Append hard-coded rows via UNION ALL (M's Table.Combine / literal rows).

    ``columns`` is a list of ``(column_name, sql_type)`` tuples covering
    **all** columns that the upstream CTE exposes — the VALUES clause must
    provide every column in the same order.
    ``rows`` is a list of ``{column_name: value}`` dicts.
    """

    columns: List[tuple]  # [(name, sql_type), ...]
    rows: List[Dict[str, Any]]


@dataclass
class GroupByStep:
    """GROUP BY with aggregations (M's ``Table.Group``).

    ``group_cols`` are the grouping column names.
    ``aggregations`` maps output column name → SQL aggregate expression,
    e.g. ``{"total": "SUM(amount)", "cnt": "COUNT(*)"}``.
    """

    group_cols: List[str]
    aggregations: Dict[str, str]


@dataclass
class CalcColumn:
    """A DAX calculated column, pre-translated to a SQL expression.

    Set ``translation_status = "todo"`` for columns that could not be
    translated; the emitter emits a ``-- TODO`` comment in their place and
    records them in ``EmittedView.todo_steps``.
    """

    name: str
    sql_expr: str
    translation_status: str = "translated"  # "translated" | "todo"
    comment: Optional[str] = None


#: Union type covering all M-step dataclasses.
MStep = Union[FilterStep, ReplaceStep, DeduplicateStep, AppendRowsStep, GroupByStep]


@dataclass
class ViewSpec:
    """Complete specification for one PBI-table → UC-view mapping."""

    table_name: str          # PBI table name (informational label only)
    target_view: str         # DDL target, e.g. "`cat`.`sch`.`view_name`"
    source: TableSource
    m_steps: List[MStep] = field(default_factory=list)
    calc_columns: List[CalcColumn] = field(default_factory=list)
    comment: Optional[str] = None
    or_replace: bool = True
    materialized: bool = False
    partition_by: Optional[List[str]] = None


# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------


@dataclass
class EmittedView:
    """Result of emitting a single source-layer view."""

    table_name: str
    target_view: str
    ddl: str         # complete, executable CREATE [MATERIALIZED] VIEW DDL
    todo_steps: List[str]  # descriptions of steps emitted as TODO comments
    error: Optional[str] = None  # non-None when fail-open triggered on exception


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sql_lit(value: Any) -> str:
    """Convert a Python value to a Databricks SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _filter_condition(step: FilterStep) -> str:
    """Build the SQL condition for a FilterStep."""
    op = step.operator.strip().upper()
    col = step.column
    if op in ("IS NULL", "IS NOT NULL"):
        return f"{col} {op}"
    if op == "IN":
        vals = ", ".join(_sql_lit(v) for v in (step.value or []))
        return f"{col} IN ({vals})"
    if op == "NOT IN":
        vals = ", ".join(_sql_lit(v) for v in (step.value or []))
        return f"{col} NOT IN ({vals})"
    return f"{col} {op} {_sql_lit(step.value)}"


def _values_clause(columns: List[tuple], rows: List[Dict[str, Any]]) -> str:
    """Build the VALUES ... AS t(...) fragment for AppendRowsStep."""
    if not rows:
        return ""
    col_casts = ",\n    ".join(f"CAST({c} AS {t}) AS {c}" for c, t in columns)
    col_names = ", ".join(c for c, _ in columns)

    def _row(r: Dict[str, Any]) -> str:
        return "  (" + ", ".join(_sql_lit(r.get(c)) for c, _ in columns) + ")"

    rows_sql = ",\n".join(_row(r) for r in rows)
    return (
        f"SELECT\n    {col_casts}\n"
        f"FROM VALUES\n"
        f"{rows_sql}\n"
        f"  AS _appended({col_names})"
    )


# ---------------------------------------------------------------------------
# CTE builder (stateful, internal)
# ---------------------------------------------------------------------------


class _Builder:
    """Builds up the CTE chain for one ViewSpec."""

    def __init__(self, spec: ViewSpec) -> None:
        self._spec = spec
        self._ctes: List[tuple] = []   # [(alias, sql_body), ...]
        self._ctr = 0
        self.todo_steps: List[str] = []

    def _current(self) -> str:
        return self._ctes[-1][0] if self._ctes else "src"

    def _next(self, hint: str = "cte") -> str:
        self._ctr += 1
        return f"{hint}_{self._ctr}"

    # ------------------------------------------------------------------
    # Base
    # ------------------------------------------------------------------

    def add_base(self) -> None:
        src = self._spec.source
        if src.native_sql:
            body = textwrap.dedent(src.native_sql).strip()
        elif src.full_table_name:
            body = f"SELECT *\nFROM {src.full_table_name}"
        else:
            raise ValueError(
                "TableSource has neither full_table_name nor native_sql; "
                "cannot generate base CTE."
            )
        self._ctes.append(("src", body))

    # ------------------------------------------------------------------
    # M step handlers
    # ------------------------------------------------------------------

    def apply_step(self, step: MStep) -> None:
        if isinstance(step, FilterStep):
            self._filter(step)
        elif isinstance(step, ReplaceStep):
            self._replace(step)
        elif isinstance(step, DeduplicateStep):
            self._dedup(step)
        elif isinstance(step, AppendRowsStep):
            self._append(step)
        elif isinstance(step, GroupByStep):
            self._groupby(step)
        else:
            hint = f"Unsupported step type: {type(step).__name__}"
            self.todo_steps.append(hint)
            alias = self._next("todo")
            body = f"SELECT *\nFROM {self._current()}\n-- TODO: {hint}"
            self._ctes.append((alias, body))

    def _filter(self, step: FilterStep) -> None:
        alias = self._next("filtered")
        cond = _filter_condition(step)
        body = f"SELECT *\nFROM {self._current()}\nWHERE {cond}"
        self._ctes.append((alias, body))

    def _replace(self, step: ReplaceStep) -> None:
        alias = self._next("replaced")
        col = step.column
        expr = f"replace({col}, {_sql_lit(step.old_value)}, {_sql_lit(step.new_value)})"
        # EXCEPT + re-add uses Databricks SELECT * EXCEPT (...) syntax
        body = (
            f"SELECT * EXCEPT ({col}),\n"
            f"       {expr} AS {col}\n"
            f"FROM {self._current()}"
        )
        self._ctes.append((alias, body))

    def _dedup(self, step: DeduplicateStep) -> None:
        ranked = self._next("ranked")
        deduped = self._next("dedup")
        keys = ", ".join(step.key_columns)
        body_ranked = (
            f"SELECT *,\n"
            f"       ROW_NUMBER() OVER (\n"
            f"         PARTITION BY {keys}\n"
            f"         ORDER BY {step.order_expr}) AS _rn\n"
            f"FROM {self._current()}"
        )
        body_dedup = (
            f"SELECT * EXCEPT (_rn)\n"
            f"FROM {ranked}\n"
            f"WHERE _rn = 1"
        )
        self._ctes.append((ranked, body_ranked))
        self._ctes.append((deduped, body_dedup))

    def _append(self, step: AppendRowsStep) -> None:
        alias = self._next("appended")
        if not step.rows:
            # No-op: just alias the current
            body = f"SELECT *\nFROM {self._current()}"
        else:
            values_sql = _values_clause(step.columns, step.rows)
            body = (
                f"SELECT *\nFROM {self._current()}\n"
                f"UNION ALL\n"
                f"{values_sql}"
            )
        self._ctes.append((alias, body))

    def _groupby(self, step: GroupByStep) -> None:
        alias = self._next("grouped")
        group_list = ", ".join(step.group_cols)
        agg_items = ", ".join(
            f"{expr} AS {col}" for col, expr in step.aggregations.items()
        )
        select_list = group_list + (f", {agg_items}" if agg_items else "")
        body = (
            f"SELECT {select_list}\n"
            f"FROM {self._current()}\n"
            f"GROUP BY {group_list}"
        )
        self._ctes.append((alias, body))

    # ------------------------------------------------------------------
    # Final SELECT with calc columns
    # ------------------------------------------------------------------

    def final_select(self) -> str:
        current = self._current()
        translated = [c for c in self._spec.calc_columns if c.translation_status == "translated"]
        todo = [c for c in self._spec.calc_columns if c.translation_status != "translated"]

        for c in todo:
            desc = c.comment or c.sql_expr
            self.todo_steps.append(f"CalcColumn '{c.name}': {desc}")

        if not translated and not todo:
            return f"SELECT *\nFROM {current}"

        parts = [f"       {c.sql_expr} AS {c.name}" for c in translated]
        todo_parts = [
            f"       -- TODO calc column '{c.name}': {c.comment or c.sql_expr}"
            for c in todo
        ]
        calc_block = ",\n".join(parts + todo_parts)
        return f"SELECT *,\n{calc_block}\nFROM {current}"

    # ------------------------------------------------------------------
    # DDL assembly
    # ------------------------------------------------------------------

    def build_ddl(self) -> str:
        final = self.final_select()

        # Wrap in WITH … only when there are actual CTEs
        if not self._ctes:
            body = final
        elif len(self._ctes) == 1 and not self._spec.calc_columns:
            # Single base CTE, no calc columns → inline directly
            body = self._ctes[0][1]
        else:
            cte_clauses = ",\n".join(
                f"{alias} AS (\n{textwrap.indent(sql, '  ')}\n)"
                for alias, sql in self._ctes
            )
            body = f"WITH {cte_clauses}\n{final}"

        # CREATE header
        view_kind = "MATERIALIZED VIEW" if self._spec.materialized else "VIEW"
        or_replace = "OR REPLACE " if self._spec.or_replace else ""
        header = f"CREATE {or_replace}{view_kind} {self._spec.target_view}"
        if self._spec.partition_by:
            header += f"\n  PARTITIONED BY ({', '.join(self._spec.partition_by)})"
        if self._spec.comment:
            header += f"\n  COMMENT {_sql_lit(self._spec.comment)}"
        header += "\nAS"

        return f"{header}\n{body}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def emit_view(spec: ViewSpec) -> EmittedView:
    """Emit one ``CREATE VIEW`` DDL from a :class:`ViewSpec`.

    Fail-open: any exception during DDL generation is caught; the returned
    :class:`EmittedView` will have ``error`` set and ``ddl`` will contain a
    placeholder DDL with the error as a SQL comment.

    Parameters
    ----------
    spec:
        Full specification of the PBI table → UC view mapping.

    Returns
    -------
    EmittedView
        ``ddl`` is always a non-empty string.  Check ``todo_steps`` for steps
        that could not be translated automatically.
    """
    try:
        builder = _Builder(spec)
        builder.add_base()
        for step in spec.m_steps:
            builder.apply_step(step)
        ddl = builder.build_ddl()
        return EmittedView(
            table_name=spec.table_name,
            target_view=spec.target_view,
            ddl=ddl,
            todo_steps=builder.todo_steps,
        )
    except Exception as exc:  # pylint: disable=broad-except
        error_msg = str(exc)
        placeholder = (
            f"-- ERROR emitting view for '{spec.table_name}': {error_msg}\n"
            f"-- Fix the ViewSpec and re-run emit_view().\n"
            f"CREATE OR REPLACE VIEW {spec.target_view}\nAS SELECT 1 AS _placeholder"
        )
        return EmittedView(
            table_name=spec.table_name,
            target_view=spec.target_view,
            ddl=placeholder,
            todo_steps=[],
            error=error_msg,
        )


def emit_views(specs: List[ViewSpec]) -> List[EmittedView]:
    """Emit DDL for a list of :class:`ViewSpec` objects.

    Each spec is processed independently; an error in one does not abort
    the others (fail-open).
    """
    return [emit_view(s) for s in specs]
