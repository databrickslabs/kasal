"""SQL text helpers for the Genie space checks (pure, no execution).

Everything here only builds or reads statement text. The checker runs the
statements built here through an injected executor and never returns rows.
"""

from __future__ import annotations

import re

#: One part of a three-part name: plain identifier or back-quoted.
_PART = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_\-]*")
_NOT_NAME = re.compile(r"[^\w.`\-]+")
_MEASURE_CALL = re.compile(r"\bMEASURE\s*\(\s*`?([A-Za-z_]\w*)`?\s*\)", re.IGNORECASE)
_SCALED_AFTER = re.compile(r"^\s*\*\s*100(?![\d.])")
_SCALED_BEFORE = re.compile(r"(?<![\d.])100\s*\*\s*$")
_SCALE_ANYWHERE = re.compile(r"\*\s*100(?![\d.])|(?<![\d.])100\s*\*")
_SNIPPET_KINDS = frozenset({"filters", "expressions", "measures"})


def quote_name(table: str) -> str:
    """``catalog.schema.table`` as a back-quoted name; anything else is refused.

    Identifiers come from a space definition, so only plain three-part names are
    accepted. That keeps them out of the statement text as SQL.
    """
    parts = table.split(".")
    if len(parts) != 3 or not all(_PART.fullmatch(part) for part in parts):
        raise ValueError(f"not a catalog.schema.name identifier: {table!r}")
    return ".".join(f"`{part}`" for part in parts)


def strip_statement(sql: str) -> str:
    text = sql.strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return text


def count_statement(sql: str) -> str:
    """Wraps a statement so the warehouse only returns its row count.

    The closing parenthesis sits on its own line, so a trailing ``--`` comment
    cannot swallow it.
    """
    return f"SELECT COUNT(*) AS n FROM (\n{strip_statement(sql)}\n)"


def referenced_tables(sql: str) -> set[str]:
    """Three-part names a statement mentions (back-quotes removed, lower-cased).

    A fourth part is a column of the table, so it is dropped.
    """
    found: set[str] = set()
    for token in _NOT_NAME.split(sql):
        parts = token.replace("`", "").split(".")
        if len(parts) >= 3 and all(parts[:3]) and not parts[0].isdigit():
            found.add(".".join(parts[:3]).lower())
    return found


def _snippet_source(sql: str, tables: list[str]) -> str:
    """The FROM clause for a snippet: the view it qualifies its columns with."""
    for table in tables:
        alias = table.split(".")[-1]
        if re.search(rf"(?<![\w`]){re.escape(alias)}\.", sql):
            return f"{quote_name(table)} AS {alias}"
    if not tables:
        raise ValueError("the space has no data source to run a snippet against")
    return quote_name(tables[0])


def snippet_statement(kind: str, sql: str, tables: list[str]) -> str | None:
    """A statement that exercises one snippet against its view, or None for an unknown kind."""
    if kind not in _SNIPPET_KINDS:
        return None
    source = _snippet_source(sql, tables)
    if kind == "filters":
        return count_statement(f"SELECT 1 FROM {source} WHERE {sql} LIMIT 1")
    if kind == "measures":
        return count_statement(f"SELECT {sql} FROM {source}")
    return count_statement(f"SELECT {sql} FROM {source} LIMIT 1")


def _line_at(sql: str, position: int) -> str:
    start = sql.rfind("\n", 0, position) + 1
    end = sql.find("\n", position)
    return sql[start:] if end == -1 else sql[start:end]


def percent_scaling(
    sql: str, percent_measures: frozenset[str]
) -> tuple[list[str], list[str]]:
    """Percentage measures that the SQL multiplies by 100, as ``(direct, computed)``.

    ``direct``: the measure call itself is scaled (``MEASURE(x) * 100``). The
    percentage format already scales it, so this is a double scaling.
    ``computed``: the measure sits in an expression on the same line that scales
    by 100. Such values (old/new fallbacks, differences) are legitimate when they
    are named ``*_pct``, so they are reported for review, not as a failure.
    """
    direct: set[str] = set()
    computed: set[str] = set()
    for match in _MEASURE_CALL.finditer(sql):
        name = match.group(1).lower()
        if name not in percent_measures:
            continue
        after = sql[match.end() : match.end() + 24]
        before = sql[max(0, match.start() - 24) : match.start()]
        if _SCALED_AFTER.match(after) or _SCALED_BEFORE.search(before):
            direct.add(name)
        elif _SCALE_ANYWHERE.search(_line_at(sql, match.start())):
            computed.add(name)
    return sorted(direct), sorted(computed - direct)
