"""Best-effort DAX references: measures, (table, column) pairs, tables and user-defined function calls.

Regex-based, not a DAX parser — the same trade-off as the rest of Kasal's PBI
tooling. Handles the shapes that occur in measure DAX:

    'Table Name'[Column] / TableName[Column]  -> a column
    [Name]                                     -> an unqualified bracket: a measure
                                                  (or, in a calculated column, a
                                                  column of its own table)

Comments and string literals are stripped first, so ``// [Old]`` or
``"[Not Applicable]"`` is never a dependency. DAX names are case-insensitive:
``[Orders In KG]`` is the measure ``Orders in kg``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping

# One alternation scanned left to right: a `//` inside a string (a URL) stays part of the string.
_NOISE_RE = re.compile(r'"(?:[^"]|"")*"|//[^\n]*|--[^\n]*|/\*.*?\*/', re.DOTALL)
_IDENTIFIER_RE = re.compile(r"[A-Za-z_]\w*", re.ASCII)
_BRACKET_RE = re.compile(r"\[[^\[\]]*\]")
_DAX_REF_RE = re.compile(
    r"(?:'(?P<qtable>[^']+)'|(?P<btable>[A-Za-z_](?a:\w)*))?\s*\[(?P<name>[^\[\]]+)\]"
)


def strip_noise(expression: str) -> str:
    """Comments -> a space; string literals -> an empty string literal."""
    return _NOISE_RE.sub(
        lambda m: '""' if m.group(0).startswith('"') else " ", expression
    )


def iter_references(expression: str | None) -> Iterator[tuple[str | None, str]]:
    """(table or None, name) for every bracket outside comments and strings."""
    if not expression or not isinstance(expression, str):
        return
    for m in _DAX_REF_RE.finditer(strip_noise(expression)):
        name = m.group("name").strip()
        if name:
            yield m.group("qtable") or m.group("btable"), name


def name_lookup(names: Iterable[str]) -> dict[str, str]:
    """``{name.lower(): name}``."""
    return {name.lower(): name for name in names}


def qualified_references(
    expression: str | None,
    measures: Mapping[str, str],
    columns_by_key: Mapping[tuple[str, str], tuple[str, str]],
) -> tuple[set[str], set[tuple[str, str]]]:
    """(measures, columns) ``expression`` references; a column keeps its table.

    ``measures``: a ``name_lookup``; ``columns_by_key``:
    ``{(table.lower(), column.lower()): (table, column)}``. A qualified bracket
    that is no known column falls back to the measure check (``RETURN [X]`` where
    the regex grabbed ``RETURN`` as a table name).
    """
    found_measures: set[str] = set()
    found_columns: set[tuple[str, str]] = set()
    for table, name in iter_references(expression):
        key = (table.lower(), name.lower()) if table else None
        if key is not None and key in columns_by_key:
            found_columns.add(columns_by_key[key])
        elif name.lower() in measures:
            found_measures.add(measures[name.lower()])
    return found_measures, found_columns


def table_reference_pattern(table_names: Iterable[str]) -> re.Pattern | None:
    """One regex for table names used as tables (quoted, or a bare identifier); longest first."""
    names = sorted(table_names, key=len, reverse=True)
    if not names:
        return None
    pattern = "'(" + "|".join(re.escape(t.replace("'", "''")) for t in names) + ")'"
    bare = [re.escape(t) for t in names if _IDENTIFIER_RE.fullmatch(t)]
    if bare:
        pattern += r"|(?<![\w'])(" + "|".join(bare) + r")(?![\w'])"
    return re.compile(pattern)


def referenced_tables(expression: str | None, pattern: re.Pattern | None) -> set[str]:
    """Tables ``expression`` names, outside comments, strings and brackets."""
    if not expression or not isinstance(expression, str) or pattern is None:
        return set()
    text = _BRACKET_RE.sub("[]", strip_noise(expression))
    found = set()
    for match in pattern.finditer(text):
        quoted = match.group(1)
        found.add(quoted.replace("''", "'") if quoted else match.group(2))
    return found


def function_call_pattern(function_names: Iterable[str]) -> re.Pattern | None:
    """One regex for calls of user-defined DAX functions (case-insensitive, longest first)."""
    names = sorted(function_names, key=len, reverse=True)
    if not names:
        return None
    return re.compile(
        r"(?<![\w.'\]])(" + "|".join(re.escape(n) for n in names) + r")[ \t\r\n]*\(",
        re.IGNORECASE,
    )


def called_functions(
    expression: str | None, pattern: re.Pattern | None, names: Mapping[str, str]
) -> set[str]:
    """User-defined functions ``expression`` calls, by their defined name."""
    if not expression or not isinstance(expression, str) or pattern is None:
        return set()
    text = _BRACKET_RE.sub("[]", strip_noise(expression))
    return {names[match.group(1).lower()] for match in pattern.finditer(text)}
