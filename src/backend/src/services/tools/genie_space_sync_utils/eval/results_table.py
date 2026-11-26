"""Optional write of evaluation rows to a Unity Catalog table, through the SQL Statement API.

The table name must be a plain ``catalog.schema.table`` (checked by ``quote_name``).
Values are written as literals: strings are escaped for backslashes and quotes, control
characters are dropped, and nothing from the answers is ever spliced in as SQL. A
failed write is reported in the output; it does not fail the run.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence

from src.services.tools.genie_space_sync_utils.sql_checks import quote_name
from src.services.tools.genie_space_sync_utils.sql_results import SqlFn
from src.services.tools.genie_space_sync_utils.verdicts import short

RESULT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"),
    ("run_ts", "STRING"),
    ("space_id", "STRING"),
    ("question_id", "STRING"),
    ("source", "STRING"),
    ("agent", "STRING"),
    ("verdict", "STRING"),
    ("status", "STRING"),
    ("expected_figures", "INT"),
    ("matched_figures", "INT"),
    ("max_relative_difference", "DOUBLE"),
    ("judge_correct", "BOOLEAN"),
    ("latency_s", "DOUBLE"),
    ("total_tokens", "BIGINT"),
    ("message_id", "STRING"),
    ("statement_ids", "STRING"),
    ("error", "STRING"),
    ("answer", "STRING"),
)
ROWS_PER_INSERT = 100
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def sql_literal(value: object) -> str:
    """A value as a SQL literal: NULL, TRUE/FALSE, a finite number, or an escaped string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return "NULL"
        return repr(value)
    text = _CONTROL.sub("", str(value)).replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def create_statement(table: str) -> str:
    columns = ", ".join(f"{name} {kind}" for name, kind in RESULT_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {quote_name(table)} ({columns})"


def insert_statements(table: str, rows: Sequence[dict[str, object]]) -> list[str]:
    """INSERT statements for *rows*, ``ROWS_PER_INSERT`` rows each. Missing keys become NULL."""
    target = quote_name(table)
    names = ", ".join(name for name, _ in RESULT_COLUMNS)
    statements: list[str] = []
    for start in range(0, len(rows), ROWS_PER_INSERT):
        chunk = rows[start : start + ROWS_PER_INSERT]
        values = ",\n".join(
            "("
            + ", ".join(sql_literal(row.get(name)) for name, _ in RESULT_COLUMNS)
            + ")"
            for row in chunk
        )
        statements.append(f"INSERT INTO {target} ({names})\nVALUES\n{values}")
    return statements


def write_rows(
    run_sql: SqlFn, table: str, rows: Sequence[dict[str, object]]
) -> dict[str, object]:
    """Create the table if needed and insert *rows*. Returns ``{"table", "written", "error"}``."""
    try:
        statements = [create_statement(table), *insert_statements(table, rows)]
    except ValueError as exc:
        return {"table": table, "written": 0, "error": short(exc)}
    written = 0
    for index, statement in enumerate(statements):
        result = run_sql(statement)
        if not result.get("success"):
            return {
                "table": table,
                "written": written,
                "error": short(result.get("error") or "statement failed"),
            }
        if index > 0:
            written = min(len(rows), written + ROWS_PER_INSERT)
    return {"table": table, "written": len(rows), "error": ""}
