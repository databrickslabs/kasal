"""The columns of the source-layer views, as the warehouse reports them.

In read mode a metric view reads one source-layer view per Power BI table. The generator
translates the measures before those views exist, so it guesses their columns from the
Power Query text; the guess is often empty. When the loop has deployed the views it reads
their real columns and passes them in as ``{table key: [{name, type}]}`` (``source_columns_json``).

Two things follow from a known column list and are decided here, once:

* the fact's column set (``spec.fact_columns``) and the joined views' column sets, so a
  reference to a column a view does not have is not emitted;
* a column named like the fact's alias (``source``) is exposed under another name. In a metric
  view ``source.<name>`` is the fact's column reference; when the fact has a column called
  ``source`` and a reference to a column it lacks is written, Databricks reports
  ``INVALID_EXTRACT_BASE_FIELD_TYPE`` on ``source`` instead of the missing column, and the
  healer cannot tell which reference to remove.

Pure functions: no I/O.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

#: The alias a metric view gives its fact; a fact column with this name collides with it.
QUALIFIER = "source"
_RENAME_SUFFIX = "_"


def source_columns_enabled(config: Mapping[str, object] | None) -> bool:
    """The pipeline config switch ``use_source_columns`` (default true). Off: the loop does not read
    the source views' columns and the generator ignores any it is given, so a run can be compared
    with and without them."""
    raw = (config or {}).get("use_source_columns", True)
    if isinstance(raw, str):
        return raw.strip().lower() not in ("0", "false", "no", "off")
    return bool(raw)


def parse_source_columns(raw: object) -> dict[str, list[str]]:
    """``{table key: [column names]}`` from the JSON input (or an already parsed mapping).

    Accepts ``{key: [{"name": ..., "type": ...}]}`` and ``{key: ["name", ...]}``. A key with an
    empty list means its view exists but its columns are unknown (the parsed ones are used); a
    key that is not there has no source view at all. A malformed value is left out.
    """
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            return {}
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, list[str]] = {}
    for key, columns in raw.items():
        if not isinstance(columns, list):
            continue
        names: list[str] = []
        for column in columns:
            name = column.get("name") if isinstance(column, Mapping) else column
            if isinstance(name, str) and name.strip() and name.strip() not in names:
                names.append(name.strip())
        out[str(key)] = names
    return out


def column_renames(columns: Mapping[str, list[str]]) -> dict[str, dict[str, str]]:
    """``{table key: {column (lower case): new name}}`` for each view with a column named
    like the fact alias. The new name is ``source_`` (or more underscores when that is taken).
    """
    out: dict[str, dict[str, str]] = {}
    for key, names in columns.items():
        taken = {name.lower() for name in names}
        if QUALIFIER not in taken:
            continue
        new = QUALIFIER + _RENAME_SUFFIX
        while new in taken:
            new += _RENAME_SUFFIX
        out[key] = {QUALIFIER: new}
    return out


def renamed_columns(
    columns: Mapping[str, list[str]], renames: Mapping[str, Mapping[str, str]]
) -> dict[str, list[str]]:
    """The columns each view exposes once the renames are applied."""
    return {
        key: [renames.get(key, {}).get(name.lower(), name) for name in names]
        for key, names in columns.items()
    }


def rename_references(text: str, alias: str, renames: Mapping[str, str]) -> str:
    """``alias.col`` (back-quoted or not) rewritten to ``alias.<new name>`` for each renamed
    column, matched case-insensitively. Text without such a reference is returned unchanged.
    """
    if not text or not renames:
        return text
    pattern = re.compile(
        rf"(?<![\w.])({re.escape(alias)})\.(`?)(\w+)\2(?!\w)", re.IGNORECASE
    )

    def _sub(match: re.Match[str]) -> str:
        new = renames.get(match.group(3).lower())
        if new is None:
            return match.group(0)
        quote = match.group(2)
        return f"{match.group(1)}.{quote}{new}{quote}"

    return pattern.sub(_sub, text)


def columns_read(text: str, alias: str) -> set[str]:
    """Lower-cased column names read as ``alias.col`` in a measure or dimension expression."""
    pattern = re.compile(
        rf"(?<![\w.]){re.escape(alias)}\.`?(\w+)`?(?!\w)", re.IGNORECASE
    )
    return {name.lower() for name in pattern.findall(text or "")}


_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.)*\"")
_MEASURE_CALL = re.compile(r"\bMEASURE\s*\(\s*`?\w+`?\s*\)", re.IGNORECASE)
_TYPE_AFTER_AS = re.compile(r"\bAS\s+[A-Za-z_]\w*", re.IGNORECASE)
_QUALIFIED = re.compile(
    r"(?<![\w.`])(?:`[^`]+`|[A-Za-z_]\w*)\s*\.\s*(?:`[^`]+`|[A-Za-z_]\w*)"
)
_BACKTICKED = re.compile(r"`([^`]+)`")
_BARE_WORD = re.compile(r"(?<![\w.])([A-Za-z_]\w*)")
# Words that are SQL syntax, not column names, when they stand alone in an expression.
_SQL_WORDS = frozenset(
    {
        "and", "or", "not", "in", "is", "null", "case", "when", "then", "else", "end",
        "between", "like", "ilike", "exists", "any", "all", "some", "escape", "as",
        "filter", "where", "distinct", "over", "partition", "by", "order", "rows",
        "range", "unbounded", "preceding", "following", "current", "row", "asc", "desc",
        "nulls", "first", "last", "true", "false", "date", "timestamp", "interval",
        "day", "week", "month", "quarter", "year", "hour", "minute", "second",
        "int", "integer", "bigint", "smallint", "tinyint", "double", "float", "decimal",
        "numeric", "string", "varchar", "char", "boolean", "binary", "from", "div", "mod",
    }
)


def bare_columns_read(text: str) -> set[str]:
    """Lower-cased column names written WITHOUT a qualifier in an expression: ``filter_code`` in
    ``sum(case when filter_code in ('A1') then amount end)``.

    Databricks resolves such a name against the fact's own columns, so it fails the deploy when
    the fact has no column of that name, as ``source.<col>`` does. Not counted: string literals,
    qualified references (``alias.col``), function names (followed by ``(``), SQL keywords and
    type names, the argument of ``MEASURE(...)``, and the fact alias itself.
    """
    if not text:
        return set()
    body = _STRING_LITERAL.sub(" ", text)
    body = _MEASURE_CALL.sub(" ", body)
    body = _TYPE_AFTER_AS.sub(" ", body)
    body = _QUALIFIED.sub(" ", body)
    names: set[str] = set()
    for match in _BACKTICKED.finditer(body):
        if not re.match(r"\s*\(", body[match.end():]):
            names.add(match.group(1).lower())
    body = _BACKTICKED.sub(" ", body)
    for match in _BARE_WORD.finditer(body):
        word = match.group(1)
        if re.match(r"\s*\(", body[match.end():]) or word.lower() in _SQL_WORDS:
            continue
        names.add(word.lower())
    names.discard(QUALIFIER)
    return names
