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
