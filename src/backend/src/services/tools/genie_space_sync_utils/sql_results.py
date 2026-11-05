"""Reading one cell out of a SQL Statement API result (pure).

The executor returns ``{"success": bool, "data": <statement payload>, "error": str}``.
Only the first cell of the first row is ever read, and only as a count or as
the DESCRIBE JSON text, so result rows never leave this layer.
"""

from __future__ import annotations

from collections.abc import Callable

from src.services.tools.genie_space_sync_utils.verdicts import short

#: Runs one SQL statement and returns the executor's result shape.
SqlFn = Callable[[str], dict[str, object]]


def first_cell(result: dict[str, object]) -> str | None:
    data = result.get("data")
    if not isinstance(data, dict):
        return None
    body = data.get("result")
    rows = body.get("data_array") if isinstance(body, dict) else None
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], list):
        return None
    if not rows[0]:
        return None
    value = rows[0][0]
    return None if value is None else str(value)


def count_of(result: dict[str, object]) -> int | None:
    """The integer in the first cell, or None when the statement failed."""
    if not result.get("success"):
        return None
    text = first_cell(result)
    try:
        return int(text) if text is not None else None
    except ValueError:
        return None


def error_of(result: dict[str, object]) -> str:
    return short(result.get("error") or "statement failed")
