"""Row-count check between a translated SQL table and its Power BI source.

Used by the M-Query conversion pipeline to verify a translation: the same row
count from Databricks SQL and from DAX. Only an actual comparison can verify:
a count that cannot be read (no DAX rows, an unreadable value, a malformed
result) is reported as ``unverified``, never as a match.
"""

from __future__ import annotations

import re

UNVERIFIED = "unverified"


def build_count_sql(select_sql: str) -> str:
    """A ``COUNT(*)`` wrapper that works for plain SELECTs and CTE queries.

    Spark SQL does not support nested CTEs inside subqueries:
      BAD:  SELECT COUNT(*) FROM (WITH cte AS (...) SELECT ...) _t
      GOOD: WITH cte AS (...) SELECT COUNT(*) FROM (SELECT ...) _t

    For CTE queries, keep the WITH block at the top level and wrap only the
    final SELECT in the count subquery.
    """
    stripped = select_sql.strip()
    if not stripped.upper().startswith("WITH"):
        return f"SELECT COUNT(*) AS _cnt FROM ({stripped}) _t"  # wraps SQL its one caller gated read-only — nosemgrep: tmp.databricks.sql-injection.fstring-select

    # Find the last top-level SELECT (depth 0 — not inside any parentheses)
    depth = 0
    last_top_select = -1
    upper = stripped.upper()
    for i, ch in enumerate(stripped):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and upper[i : i + 7] == "SELECT ":
            last_top_select = i

    if last_top_select == -1:
        # Fallback: can't parse, try wrapping as-is
        return f"SELECT COUNT(*) AS _cnt FROM ({stripped}) _t"  # wraps SQL its one caller gated read-only — nosemgrep: tmp.databricks.sql-injection.fstring-select

    cte_part = stripped[:last_top_select]
    final_select = stripped[last_top_select:].strip()
    # Remove trailing ORDER BY / LIMIT — not needed for COUNT
    final_select = re.sub(
        r"\s+ORDER\s+BY\s+.*$", "", final_select, flags=re.IGNORECASE | re.DOTALL
    )
    final_select = re.sub(r"\s+LIMIT\s+\S+.*$", "", final_select, flags=re.IGNORECASE)
    return f"{cte_part}SELECT COUNT(*) AS _cnt FROM ({final_select.strip()}) _cnt_agg"  # wraps SQL its one caller gated read-only — nosemgrep: tmp.databricks.sql-injection.fstring-select


def diff_counts(sql_res: dict, dax_res: dict) -> str | None:
    """``None`` when the counts match within 0.1%; otherwise a diff string.

    A diff starting with ``unverified`` means the counts could not be
    compared at all; the caller reports the table as not verified.
    """
    try:
        sql_cnt = int(sql_res["rows"][0][0])
    except (KeyError, IndexError, TypeError, ValueError):
        return f"{UNVERIFIED}: the SQL row count could not be read"
    dax_rows = dax_res.get("data") or []
    row = dax_rows[0] if dax_rows else None
    values = row.values() if isinstance(row, dict) else (row or [])
    dax_cnt = None
    for v in values:
        try:
            dax_cnt = int(float(str(v)))
            break
        except (ValueError, TypeError):
            continue
    if dax_cnt is None:
        return f"{UNVERIFIED}: DAX returned no readable row count"
    if sql_cnt == dax_cnt:
        return None
    rel = abs(sql_cnt - dax_cnt) / max(dax_cnt, 1)
    return None if rel < 0.001 else f"SQL={sql_cnt}, DAX={dax_cnt} (delta={rel:.1%})"
