"""Joins whose fact-side key column the fact does not have.

A Power BI relationship can join on a column the model computes (a DAX calculated column, a
Power Query column of another step), e.g. ``Fact[date_key] -> Calendar[date_id]``. The
relationship is real in Power BI, but the Databricks source has no ``date_key``, so the
generated join (``source.date_key = dim_calendar.date_id``) makes the whole metric view fail on
deploy. When the fact's columns are known, such a join is dropped (with its dimensions) and the
reason recorded; when they are not known, nothing is dropped.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from .data_classes import TableInfo

_SOURCE_COLUMN = re.compile(r"\bsource\.`?([A-Za-z_]\w*)`?")
_SELECT = re.compile(
    r"^\s*SELECT\s+(?:DISTINCT\s+)?(.*?)\s+FROM\s", re.IGNORECASE | re.DOTALL
)
_ALIAS = re.compile(r"\bAS\s+`?([A-Za-z_]\w*)`?\s*$", re.IGNORECASE)
_PLAIN = re.compile(r"^(?:`?[A-Za-z_]\w*`?\.)?`?([A-Za-z_]\w*)`?$")


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


def select_list_columns(sql: str) -> set[str] | None:
    """Output column names of a SELECT, or None when they cannot be known (``*``)."""
    without_comments = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql or "", flags=re.DOTALL)
    match = _SELECT.match(without_comments)
    if not match:
        return None
    names: set[str] = set()
    for part in _split_top_level(match.group(1)):
        item = part.strip()
        if not item:
            continue
        if item == "*" or item.endswith(".*") or re.search(r"\*\s+EXCEPT", item, re.I):
            return None
        alias = _ALIAS.search(item)
        plain = _PLAIN.match(item)
        if alias:
            names.add(alias.group(1).lower())
        elif plain:
            names.add(plain.group(1).lower())
        else:
            return None  # an unnamed expression: the list is not fully known
    return names


def known_fact_columns(info: TableInfo | None) -> list[str]:
    """The fact's columns when they are fully known (its native SQL's select list), else []."""
    if info is None:
        return []
    columns = select_list_columns(info.native_query_sql or "")
    return sorted(columns) if columns else []


def missing_join_columns(join_on: str, fact_columns: Iterable[str]) -> list[str]:
    """Fact-side columns a join's ON clause uses that the fact does not have."""
    known = {c.lower() for c in fact_columns}
    if not known:
        return []
    used = {m.group(1) for m in _SOURCE_COLUMN.finditer(join_on or "")}
    return sorted(c for c in used if c.lower() not in known)
