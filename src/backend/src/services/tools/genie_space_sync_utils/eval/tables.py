"""Result sets of the SQL Statement API, as plain values (pure).

Genie's ``query-result`` and the warehouse both return the Statement API payload:
``manifest.schema.columns`` (name and type) and ``result.data_array`` (string cells).
Rows are capped so a large result cannot grow the report without bound.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.services.tools.genie_space_sync_utils.eval.numeric import Number, parse_number

MAX_ROWS = 500
NUMERIC_TYPES = frozenset(
    {
        "BYTE",
        "SHORT",
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "TINYINT",
        "SMALLINT",
        "BIGINT",
    }
)


@dataclass(frozen=True)
class ResultTable:
    columns: tuple[str, ...]
    types: tuple[str, ...]
    rows: tuple[tuple[str | None, ...], ...]
    truncated: bool = False


def _columns(payload: dict[str, object]) -> tuple[list[str], list[str]]:
    manifest = payload.get("manifest")
    schema = manifest.get("schema") if isinstance(manifest, dict) else None
    columns = schema.get("columns") if isinstance(schema, dict) else None
    if not isinstance(columns, list):
        return [], []
    names, types = [], []
    for column in columns:
        if isinstance(column, dict):
            names.append(str(column.get("name", "")))
            types.append(str(column.get("type_name", "")).upper())
    return names, types


def statement_table(payload: dict[str, object]) -> ResultTable:
    """The result table of a Statement API payload (``{"manifest": ..., "result": ...}``)."""
    names, types = _columns(payload)
    body = payload.get("result")
    data = body.get("data_array") if isinstance(body, dict) else None
    raw_rows = data if isinstance(data, list) else []
    rows: list[tuple[str | None, ...]] = []
    for raw in raw_rows[:MAX_ROWS]:
        if isinstance(raw, list):
            rows.append(tuple(None if cell is None else str(cell) for cell in raw))
    return ResultTable(
        columns=tuple(names),
        types=tuple(types),
        rows=tuple(rows),
        truncated=len(raw_rows) > MAX_ROWS,
    )


def numeric_cells(table: ResultTable) -> list[Number]:
    """The numbers in the table's numeric columns (every parseable cell when types are unknown)."""
    numeric_index = {
        index
        for index, type_name in enumerate(table.types)
        if type_name in NUMERIC_TYPES
    }
    found: list[Number] = []
    for row in table.rows:
        for index, cell in enumerate(row):
            if cell is None or (table.types and index not in numeric_index):
                continue
            number = parse_number(cell)
            if number is not None:
                found.append(number)
    return found


def value_text(table: ResultTable) -> str:
    """The cell values, one row per line, without headers: what a number check reads."""
    return "\n".join(
        " | ".join("" if cell is None else cell for cell in row) for row in table.rows
    )


def markdown(table: ResultTable) -> str:
    """The table as Markdown, for a human reader of the report."""
    if not table.columns:
        return value_text(table)
    lines = [
        "| " + " | ".join(table.columns) + " |",
        "|" + " --- |" * len(table.columns),
    ]
    for row in table.rows:
        lines.append("| " + " | ".join("" if c is None else c for c in row) + " |")
    if table.truncated:
        lines.append(f"(more than {MAX_ROWS} rows; the rest not shown)")
    return "\n".join(lines)
