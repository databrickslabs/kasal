"""Snapshots: Power BI tables that exist only inside the model -> UC tables.

Typed-in ("Enter data") tables, Excel / SharePoint tables and other
PBI-only data are read from the PUBLISHED model with ``EVALUATE`` (paged by a
column when the result would hit Power BI's row / value limits), or decoded
from the TMDL ``Binary.FromText`` payload of a helper expression when DAX
cannot return them. Each table is written as ``pbi_<table>`` with
``CREATE OR REPLACE TABLE`` (typed columns) and batched ``INSERT`` statements,
plus ``loaded_at_utc`` and ``model_refreshed_at_utc``. A configured row filter
is set again after every load, because the replace drops it.

Row counts are reported; table contents are never returned or logged.
"""

from __future__ import annotations

import base64
import json
import re
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.services.tools.metric_view_utils.source_layer_emitter import (
    RowFilter,
    alter_row_filter_sql,
)
from src.services.tools.ucmv_source_layer_utils.inputs import (
    parse_json_list,
    row_filter_for,
)
from src.services.tools.ucmv_source_layer_utils.names import (
    is_identifier,
    quote_fqn,
    quote_part,
    snake_case,
    sql_literal,
)
from src.services.tools.ucmv_source_layer_utils.pbi_client import (
    DAX_ROW_LIMIT,
    DAX_VALUE_LIMIT,
    PbiError,
    clean_rows,
    dax_column,
    dax_table,
    dax_value,
)
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlError, SqlFn

DaxFn = Callable[[str], list[dict[str, object]]]
CHUNK_ROWS = 1000
SNAPSHOT_TYPES = frozenset(
    {"STRING", "BIGINT", "INT", "DOUBLE", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP"}
)
_ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$")
_TMDL_NAME = "'{name}'"


class SnapshotError(RuntimeError):
    """A snapshot cannot be loaded as configured; the message says why."""


@dataclass(frozen=True)
class SnapshotSpec:
    pbi_table: str
    uc_table: str
    page_by: str = ""
    types: dict[str, str] = field(default_factory=dict)
    column_map: dict[str, str] = field(default_factory=dict)
    tmdl_expression: str = ""


@dataclass(frozen=True)
class SnapshotColumn:
    source: str
    name: str
    sql_type: str


def parse_snapshot_specs(raw: object) -> list[SnapshotSpec]:
    specs: list[SnapshotSpec] = []
    seen: set[str] = set()
    for index, item in enumerate(parse_json_list(raw, "snapshot_tables_json")):
        label = f"snapshot #{index + 1}"
        pbi_table = str(item.get("pbi_table") or "").strip()
        if not pbi_table:
            raise ValueError(f"snapshot_tables_json: {label} needs 'pbi_table'")
        uc_table = str(item.get("uc_table") or f"pbi_{snake_case(pbi_table)}")
        if not is_identifier(uc_table):
            raise ValueError(
                f"snapshot_tables_json: {pbi_table!r} uc_table must be an identifier"
            )
        if uc_table in seen:
            raise ValueError(
                f"snapshot_tables_json: uc_table {uc_table!r} is listed twice"
            )
        seen.add(uc_table)
        types = item.get("types") or {}
        if not isinstance(types, dict):
            raise ValueError(
                f"snapshot_tables_json: {pbi_table!r} types must be an object"
            )
        bad = [t for t in types.values() if str(t).upper() not in SNAPSHOT_TYPES]
        if bad:
            raise ValueError(
                f"snapshot_tables_json: {pbi_table!r} types must be one of {sorted(SNAPSHOT_TYPES)}"
            )
        column_map = item.get("column_map") or {}
        if not isinstance(column_map, dict) or not all(
            is_identifier(str(v)) for v in column_map.values()
        ):
            raise ValueError(
                f"snapshot_tables_json: {pbi_table!r} column_map values must be identifiers"
            )
        specs.append(
            SnapshotSpec(
                pbi_table=pbi_table,
                uc_table=uc_table,
                page_by=str(item.get("page_by") or ""),
                types={str(k): str(v).upper() for k, v in types.items()},
                column_map={str(k): str(v) for k, v in column_map.items()},
                tmdl_expression=str(item.get("tmdl_expression") or ""),
            )
        )
    return specs


# ---------------------------------------------------------------------------
# Reading the rows
# ---------------------------------------------------------------------------


def _suspect(rows: list[dict[str, object]]) -> bool:
    """True when a result is at or past a Power BI limit (it may be cut)."""
    width = len(rows[0]) if rows else 0
    return len(rows) >= DAX_ROW_LIMIT or len(rows) * width >= DAX_VALUE_LIMIT


def _count(dax: DaxFn, query: str) -> int:
    rows = clean_rows(dax(query))
    value = rows[0].get("n") if rows else 0
    return int(value) if isinstance(value, (int, float)) else 0


def fetch_rows(spec: SnapshotSpec, dax: DaxFn) -> list[dict[str, object]]:
    """Every row of the PBI table, paged by ``page_by`` when configured.

    A result that reaches a Power BI limit is never accepted as complete: the
    row count is checked against COUNTROWS, and a page at the limit fails.
    """
    table = dax_table(spec.pbi_table)
    expected = _count(dax, f'EVALUATE ROW("n", COUNTROWS({table}))')
    if not spec.page_by:
        rows = clean_rows(dax(f"EVALUATE {table}"))
        if len(rows) != expected or _suspect(rows):
            raise SnapshotError(
                f"{spec.pbi_table}: Power BI returned {len(rows)} of {expected} rows "
                "(query limit); set page_by to a column that splits the table"
            )
        return rows
    column = dax_column(spec.pbi_table, spec.page_by)
    pages = clean_rows(dax(f"EVALUATE VALUES({column})"))
    if len(pages) >= DAX_ROW_LIMIT:
        raise SnapshotError(
            f"{spec.pbi_table}: page_by has too many values; choose a coarser column"
        )
    collected: list[dict[str, object]] = []
    for number, page in enumerate(pages, start=1):
        value = next(iter(page.values()), None)
        predicate = (
            f"ISBLANK({column})" if value is None else f"{column} = {dax_value(value)}"
        )
        part = clean_rows(dax(f"EVALUATE CALCULATETABLE({table}, {predicate})"))
        if _suspect(part):
            raise SnapshotError(
                f"{spec.pbi_table}: page {number} is at the Power BI limit; choose a finer page_by"
            )
        collected.extend(part)
    if len(collected) != expected:
        raise SnapshotError(
            f"{spec.pbi_table}: paged read returned {len(collected)} of {expected} rows"
        )
    return collected


def decode_tmdl_rows(tmdl: str, expression: str) -> list[dict[str, object]]:
    """Rows of a helper query stored as ``Binary.FromText`` in ``expressions.tmdl``.

    The payload is base64 of raw-deflate JSON: a list of objects.
    """
    quoted = re.escape(_TMDL_NAME.format(name=expression.replace("'", "''")))
    bare = re.escape(expression)
    pattern = rf"expression\s+(?:{quoted}|{bare})\s*=.*?Binary\.FromText\(\"([^\"]+)\""
    match = re.search(pattern, tmdl, re.DOTALL)
    if not match:
        raise SnapshotError(
            f"{expression}: no Binary.FromText payload in the model definition"
        )
    try:
        data = json.loads(zlib.decompress(base64.b64decode(match.group(1)), -15))
    except (ValueError, zlib.error) as exc:
        raise SnapshotError(
            f"{expression}: payload is not base64 deflate JSON"
        ) from exc
    if not isinstance(data, list):
        raise SnapshotError(f"{expression}: payload is not a list of rows")
    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            raise SnapshotError(f"{expression}: payload rows must be objects")
        rows.append({str(k): v for k, v in item.items()})
    return rows


# ---------------------------------------------------------------------------
# Columns and statements
# ---------------------------------------------------------------------------


def infer_sql_type(values: list[object]) -> str:
    present = [v for v in values if v is not None]
    if not present:
        return "STRING"
    if all(isinstance(v, bool) for v in present):
        return "BOOLEAN"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in present):
        return "BIGINT"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in present):
        return "DOUBLE"
    if all(isinstance(v, str) and _ISO_DATETIME.match(v) for v in present):
        return "TIMESTAMP"
    return "STRING"


def build_columns(
    spec: SnapshotSpec, rows: list[dict[str, object]]
) -> list[SnapshotColumn]:
    sources: list[str] = []
    for row in rows:
        for key in row:
            if key not in sources:
                sources.append(key)
    columns: list[SnapshotColumn] = []
    names: set[str] = set()
    for index, source in enumerate(sources):
        name = spec.column_map.get(source) or snake_case(source) or f"col_{index + 1}"
        if not is_identifier(name):
            name = f"c_{name}"
        if name in names:
            raise SnapshotError(
                f"{spec.pbi_table}: two columns map to {name!r}; use column_map"
            )
        names.add(name)
        sql_type = spec.types.get(source) or infer_sql_type(
            [r.get(source) for r in rows]
        )
        columns.append(SnapshotColumn(source=source, name=name, sql_type=sql_type))
    return columns


def _target(catalog: str, schema: str, table: str) -> str:
    return f"{quote_fqn(catalog)}.{quote_fqn(schema)}.{quote_part(table)}"


def build_statements(
    target: str,
    columns: list[SnapshotColumn],
    rows: list[dict[str, object]],
    *,
    comment: str,
    loaded_at: str,
    refreshed_at: str | None,
) -> list[str]:
    """CREATE OR REPLACE TABLE with the first chunk, INSERT for the rest."""
    if not rows:
        raise SnapshotError(
            "the Power BI table returned no rows; the existing snapshot is kept"
        )
    names = ", ".join(quote_part(c.name) for c in columns)
    select = ", ".join(
        f"CAST({quote_part(c.name)} AS {c.sql_type}) AS {quote_part(c.name)}"
        for c in columns
    )
    refreshed = (
        f"CAST({sql_literal(refreshed_at)} AS TIMESTAMP)"
        if refreshed_at
        else "CAST(NULL AS TIMESTAMP)"
    )
    select += (
        f", CAST({sql_literal(loaded_at)} AS TIMESTAMP) AS loaded_at_utc, "
        f"{refreshed} AS model_refreshed_at_utc"
    )
    statements: list[str] = []
    for start in range(0, len(rows), CHUNK_ROWS):
        values = ",\n  ".join(
            "(" + ", ".join(sql_literal(r.get(c.source)) for c in columns) + ")"
            for r in rows[start : start + CHUNK_ROWS]
        )
        body = f"SELECT {select}\nFROM VALUES\n  {values}\n  AS t({names})"
        if start == 0:
            statements.append(
                f"CREATE OR REPLACE TABLE {target}\nCOMMENT {sql_literal(comment)}\nAS {body}"
            )
        else:
            statements.append(f"INSERT INTO {target}\n{body}")
    return statements


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotContext:
    catalog: str
    schema: str
    dax: DaxFn
    sql: SqlFn
    filters: dict[str, RowFilter]
    loaded_at: str
    refreshed_at: str | None
    tmdl: Callable[[], str] | None = None


def load_one(spec: SnapshotSpec, ctx: SnapshotContext) -> dict[str, object]:
    """Load one snapshot; returns its row count and status. Never the rows."""
    if spec.tmdl_expression:
        if ctx.tmdl is None:
            raise SnapshotError(
                f"{spec.pbi_table}: no model definition available for TMDL decoding"
            )
        rows = decode_tmdl_rows(ctx.tmdl(), spec.tmdl_expression)
    else:
        rows = fetch_rows(spec, ctx.dax)
    if not rows:
        raise SnapshotError(
            f"{spec.pbi_table}: the source returned no rows; the existing snapshot is kept"
        )
    columns = build_columns(spec, rows)
    target = _target(ctx.catalog, ctx.schema, spec.uc_table)
    statements = build_statements(
        target,
        columns,
        rows,
        comment=f"Snapshot of Power BI table {spec.pbi_table}, loaded {ctx.loaded_at} UTC.",
        loaded_at=ctx.loaded_at,
        refreshed_at=ctx.refreshed_at,
    )
    row_filter = row_filter_for(spec.pbi_table, ctx.filters)
    try:
        for statement in statements:
            ctx.sql(statement)
    except SqlError as exc:
        raise SnapshotError(
            f"{spec.pbi_table}: load stopped part-way ({exc}); the table may hold a partial "
            "load, re-run to replace it"
        ) from exc
    if row_filter is not None:
        ctx.sql(alter_row_filter_sql(target, row_filter))
    return {
        "pbi_table": spec.pbi_table,
        "uc_table": f"{ctx.catalog}.{ctx.schema}.{spec.uc_table}",
        "rows": len(rows),
        "columns": len(columns),
        "row_filter": row_filter is not None,
        "status": "loaded",
    }


def load_snapshots(
    specs: list[SnapshotSpec],
    ctx: SnapshotContext,
) -> dict[str, object]:
    """Load every snapshot; one failure is reported and the others still load."""
    results: list[dict[str, object]] = []
    for spec in specs:
        try:
            results.append(load_one(spec, ctx))
        except (SnapshotError, PbiError, SqlError, ValueError) as exc:
            results.append(
                {
                    "pbi_table": spec.pbi_table,
                    "uc_table": spec.uc_table,
                    "status": "failed",
                    "error": str(exc),
                }
            )
    failed = [r for r in results if r.get("status") != "loaded"]
    return {
        "loaded_at_utc": ctx.loaded_at,
        "model_refreshed_at_utc": ctx.refreshed_at,
        "snapshots": results,
        "all_loaded": not failed,
    }


def expressions_tmdl_text(definition: object) -> str:
    """The ``expressions.tmdl`` text from a Fabric getDefinition(TMDL) result."""
    if not isinstance(definition, dict):
        raise SnapshotError("model definition is not a JSON object")
    body = definition.get("definition")
    parts = body.get("parts") if isinstance(body, dict) else None
    for part in parts or []:
        if not isinstance(part, dict) or not str(part.get("path", "")).endswith(
            "expressions.tmdl"
        ):
            continue
        try:
            return base64.b64decode(str(part.get("payload", ""))).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise SnapshotError("expressions.tmdl payload is not base64 UTF-8") from exc
    raise SnapshotError("model definition has no expressions.tmdl part")


def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
