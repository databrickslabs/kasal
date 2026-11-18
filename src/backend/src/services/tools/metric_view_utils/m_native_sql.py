"""The SQL a ``Value.NativeQuery`` runs, including SQL that Power Query assembles from parameters.

``Value.NativeQuery(conn, "SELECT ... WHERE CASE WHEN '" & FiscperFilter & "' = 'Sample' ...")``
splices a parameter into the SQL text. Reading the literal alone cuts the SQL off at the first
splice. Here the whole argument is assembled: string literals as they are (``""`` unescaped),
each parameter by its value in the model. A parameter whose value is not known is replaced by an
empty string and returned as an assumption, so the caller reports it: the comparison with Power
BI then shows whether the assumption holds.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

_NATIVE = re.compile(r"Value\.NativeQuery\s*\(")
_IDENT = re.compile(r'#"((?:[^"]|"")+)"|([A-Za-z_][\w.]*)')


def _skip_first_argument(text: str, i: int) -> int:
    """Index just after the first top-level comma of a call's argument list (from ``i``)."""
    depth = 0
    while i < len(text):
        ch = text[i]
        if ch == '"':
            i += 1
            while i < len(text) and not (text[i] == '"' and text[i + 1 : i + 2] != '"'):
                i += 2 if text[i] == '"' else 1
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            if depth == 0:
                return -1
            depth -= 1
        elif ch == "," and depth == 0:
            return i + 1
        i += 1
    return -1


def native_sql_with_parameters(
    m_expression: str, parameters: Mapping[str, str]
) -> tuple[str, list[str]]:
    """``(sql, assumed parameters)``; ``("", [])`` when the query is not a native query or the
    argument is not a plain concatenation of literals and parameter names."""
    call = _NATIVE.search(m_expression or "")
    if not call:
        return "", []
    i = _skip_first_argument(m_expression, call.end())
    if i < 0:
        return "", []
    parts: list[str] = []
    assumed: list[str] = []
    expect_operand = True
    text = m_expression
    while i < len(text):
        if text[i].isspace():
            i += 1
            continue
        if expect_operand and text[i] == '"':
            j, buf = i + 1, []
            while j < len(text):
                if text[j] == '"':
                    if text[j + 1 : j + 2] == '"':
                        buf.append('"')
                        j += 2
                        continue
                    break
                buf.append(text[j])
                j += 1
            parts.append("".join(buf))
            i, expect_operand = j + 1, False
        elif expect_operand and (m := _IDENT.match(text, i)):
            name = (m.group(1) or m.group(2)).replace('""', '"')
            value = parameters.get(name)
            if value is None or str(value).startswith("TODO"):
                assumed.append(name)
                value = ""
            parts.append(str(value))
            i, expect_operand = m.end(), False
        elif not expect_operand and text[i] == "&":
            i, expect_operand = i + 1, True
        elif not expect_operand and text[i] in ",)":
            break
        else:
            return "", []
    sql = "".join(parts).strip()
    return (sql, sorted(set(assumed))) if sql else ("", [])
