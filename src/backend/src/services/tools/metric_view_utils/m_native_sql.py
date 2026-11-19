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


# A parameter value outside a string literal: a number, one bare name, or quoted literals.
_BARE_VALUE = re.compile(
    r"\s*(?:-?\d+(?:\.\d+)?|[A-Za-z_][\w.]*|'[^'\\]*'(?:\s*,\s*'[^'\\]*')*)?\s*"
)


def _splice_is_safe(value: str, in_literal: bool) -> bool:
    """Whether a parameter value can be spliced into the SQL without changing its structure.

    Power Query splices the raw text. A value that ends the literal it sits in, or that brings
    SQL of its own where a number or a name is expected, would be run as SQL: such a value is
    not used (the parameter is then assumed '' and reported, like an unknown one).
    """
    if in_literal:
        return not re.search(r"['\\]", value)
    return bool(_BARE_VALUE.fullmatch(value))


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
            in_literal = "".join(parts).count("'") % 2 == 1
            if (
                value is None
                or str(value).startswith("TODO")
                or not _splice_is_safe(str(value), in_literal)
            ):
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


def without_outer_parentheses(sql: str) -> str:
    """``sql`` without the parentheses that wrap the whole statement: ``(WITH ... )``.

    Power Query accepts a native query written as one parenthesised expression; as the body
    of a view it must start with SELECT or WITH. Only a pair that encloses everything is
    removed (``(a) UNION (b)`` stays); parentheses inside string literals are not counted.
    """
    text = (sql or "").strip().rstrip(";").strip()
    while text.startswith("("):
        depth, in_str, close = 0, False, -1
        for i, ch in enumerate(text):
            if ch == "'":
                in_str = not in_str
            elif not in_str and ch == "(":
                depth += 1
            elif not in_str and ch == ")":
                depth -= 1
                if depth == 0:
                    close = i
                    break
        if close != len(text) - 1:
            break
        text = text[1:-1].strip()
    return text
