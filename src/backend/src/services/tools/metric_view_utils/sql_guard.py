"""Guards for SQL that Kasal EXECUTES from model-derived or generated text.

Power BI content an author controls — M queries, EVALUATE row data, visual
metadata — and LLM rewrites of it become statements run on the user's
warehouse. Two guards cover those paths:

* ``is_read_only_select`` — one read-only SELECT/WITH and nothing else, for SQL
  that is run or stored as a dashboard query.
* ``sql_value_literal`` — one typed ``VALUES`` item with every string escaped,
  for the rows the M-Query pipeline loads into a target table.
"""

from __future__ import annotations

import re

from .dax_condition_sql import _sql_str_literal


def is_read_only_select(sql: str) -> bool:
    """Whether ``sql`` is a single read-only SELECT/WITH, safe to run or store.

    Rejects dangerous verbs (DROP/DELETE/…/GRANT/EXEC), stacked statements (a
    ``;`` before the end), dollar-quote breakout, and catalog probing via UNION.
    Do NOT swap in ``yaml_emitter._check_dangerous_sql``: that deny-list is
    tuned for measure expressions and rejects a leading SELECT.
    """
    if not sql or not isinstance(sql, str):
        return False
    s = sql.strip().rstrip(";").strip()
    if not s:
        return False
    # A subquery body arrives as "(SELECT …)" — the leading parens are allowed.
    if not re.match(r"^[\s(]*(SELECT|WITH)\b", s, re.IGNORECASE):
        return False
    # Any ';' left after the trailing strip starts a second statement.
    if ";" in s:
        return False
    if "$$" in s:
        return False
    up = s.upper()
    if re.search(
        r"\b(DROP|DELETE|TRUNCATE|ALTER|INSERT|UPDATE|MERGE|GRANT|"
        r"REVOKE|CREATE|EXEC|EXECUTE|CALL|COPY|MSCK|SET|USE)\b",
        up,
    ):
        return False
    if re.search(r"\bUNION\b", up) and re.search(r"INFORMATION_SCHEMA|SYSTEM\.", up):
        return False
    return True


# Functions that reach outside the query: a model, the network, files, the JVM.
SIDE_EFFECT_FUNCTIONS = (
    r"\b(?:ai_query|ai_generate_text|http_request|read_files|java_method|reflect|"
    r"try_reflect|remote_query)\s*\("
)
_WRITING_VERBS = re.compile(
    r"\b(DROP|DELETE|TRUNCATE|ALTER|INSERT|UPDATE|MERGE|GRANT|REVOKE|CREATE|EXEC|"
    r"EXECUTE|CALL|COPY|MSCK)\b|" + SIDE_EFFECT_FUNCTIONS,
    re.IGNORECASE,
)


def _without_literals(sql: str) -> str:
    """``sql`` with string literals, quoted identifiers and comments blanked, so a keyword
    check sees only what the parser reads as SQL (``WHERE status = 'Update'`` is a value).
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in "'\"`":
            i += 1
            while i < n and sql[i] != ch:
                i += 2 if sql[i] == "\\" and ch != "`" else 1
            i += 1
            out.append(" ")
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            i = n if end < 0 else end + 2
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def has_no_side_effects(sql: str) -> bool:
    """Whether model- or LLM-derived SQL is free of what a view must never carry: a second
    statement, a dollar quote, a writing verb, or a function that reaches outside the query.

    ``;`` and ``$$`` are refused anywhere in the text, literals included: how a literal ends
    depends on the warehouse's escape setting, and neither has a use in a view body.
    """
    if not sql or not isinstance(sql, str):
        return False
    text = sql.strip().rstrip(";")
    if ";" in text or "$$" in text:
        return False
    return not _WRITING_VERBS.search(_without_literals(text))


def is_read_only_view_body(sql: str) -> bool:
    """Whether ``sql`` can be the body of a view: one SELECT / WITH query (its terms may be
    parenthesised), with no side effects. For a table's native query and for SQL an LLM
    recovered from M; unlike ``is_read_only_select`` a writing verb inside a string literal
    (``reason = 'Update'``) is a value, not a statement."""
    if not has_no_side_effects(sql):
        return False
    return bool(re.match(r"^[\s(]*(SELECT|WITH)\b", sql, re.IGNORECASE))


def sql_value_literal(value: object, col_type: str) -> str:
    """One ``VALUES`` item for a column of ``col_type``.

    Numbers and booleans are re-rendered from the parsed value. Dates,
    timestamps and strings go through the escaped literal, so a row value can
    never end its literal: DATE and TIMESTAMP used to be interpolated raw, and
    strings escaped quotes but not backslashes.
    """
    col_type = col_type.upper()
    if value is None:
        return "NULL"
    if "INT" in col_type:
        try:
            return str(int(float(str(value))))
        except (ValueError, TypeError):
            return "NULL"
    if "DOUBLE" in col_type or "FLOAT" in col_type or "DECIMAL" in col_type:
        try:
            return str(float(str(value)))
        except (ValueError, TypeError):
            return "NULL"
    if "BOOLEAN" in col_type:
        return "TRUE" if str(value).lower() in ("true", "1", "yes") else "FALSE"
    if "DATE" in col_type and "TIME" not in col_type:
        return f"DATE {_sql_str_literal(str(value)[:10])}"
    if "TIMESTAMP" in col_type:
        return f"TIMESTAMP {_sql_str_literal(value)}"
    return _sql_str_literal(value)
