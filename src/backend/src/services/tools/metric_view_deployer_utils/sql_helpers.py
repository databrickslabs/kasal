"""SQL quoting and result-reading helpers shared by the deployer's verify, grant and check steps.

A ``SqlRunner`` is any callable ``sql -> SQL Statement API result``; the deployer passes its
own ``_execute_sql_sync`` bound to the workspace, tests pass a fake. Nothing here talks to
the network.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping

SqlRunner = Callable[[str], Mapping[str, object]]

_UNSAFE_IDENTIFIER_CHARS = re.compile(r"[^a-zA-Z0-9_]")
_MAX_ERROR_CHARS = 300


def sanitize_identifier(value: str) -> str:
    """Reduce a catalog / schema / table key to ``[A-Za-z0-9_]`` (the deployer's naming rule)."""
    return _UNSAFE_IDENTIFIER_CHARS.sub("_", value)


def quote_name(name: str) -> str:
    """Backtick-quote an identifier, doubling embedded backticks."""
    return "`" + name.replace("`", "``") + "`"


def string_literal(value: str) -> str:
    """Single-quote a SQL string literal, doubling embedded quotes."""
    return "'" + value.replace("'", "''") + "'"


def error_text(result: Mapping[str, object]) -> str:
    """Error message of a failed statement, truncated (never includes result rows)."""
    return str(result.get("error") or "unknown error")[:_MAX_ERROR_CHARS]


def result_rows(data: object) -> list[list[object]]:
    """Rows of a SQL Statement API payload. An empty result may omit ``data_array`` entirely."""
    if not isinstance(data, Mapping):
        return []
    result = data.get("result")
    if not isinstance(result, Mapping):
        return []
    rows = result.get("data_array")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, list)]


def first_cell(data: object) -> str | None:
    """First value of the first row of a SQL Statement API payload, or ``None``."""
    rows = result_rows(data)
    if not rows or not rows[0]:
        return None
    cell = rows[0][0]
    return None if cell is None else str(cell)
