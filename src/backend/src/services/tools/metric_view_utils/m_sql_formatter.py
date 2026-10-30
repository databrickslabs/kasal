"""SQL text formatting for folded source SQL (pure string transforms).

Re-lays out a ``SELECT ... FROM ... WHERE ... GROUP BY ALL`` arm (and UNION of
arms) into the compact multi-line form the metric-view source uses. Used by
``MTransformFolder``, which keeps its staticmethod names as delegates.
"""

from __future__ import annotations

import re


def reformat_source_sql(sql: str) -> str:
    """Reformat the full source SQL for clean, compact output."""
    arms = re.split(r"\s*\bUNION\s+ALL\b\s*|\s*\bUNION\b\s*", sql, flags=re.IGNORECASE)
    formatted = [_reformat_arm(a.strip()) for a in arms]
    if len(formatted) <= 1:
        return formatted[0] if formatted else sql
    parts: list[str] = []
    for i, arm in enumerate(formatted):
        arm = re.sub(r"\n\s*GROUP\s+BY\s+ALL\s*$", "", arm)
        if i == 0:
            parts.append(arm)
        else:
            arm_body = re.sub(r"^SELECT\n", "", arm, flags=re.IGNORECASE)
            parts.append(arm_body)
    return "\nGROUP BY ALL UNION ALL SELECT\n".join(parts) + "\nGROUP BY ALL"


def _reformat_arm(arm_sql: str) -> str:
    """Reformat a single SELECT…FROM…WHERE…GROUP BY arm."""
    arm_sql = arm_sql.strip()
    select_m = re.match(r"SELECT\s+(.*?)\s+FROM\s+", arm_sql, re.DOTALL | re.IGNORECASE)
    if not select_m:
        return arm_sql
    cols_text = select_m.group(1)
    rest = arm_sql[select_m.end() :]

    table_m = re.match(r"(\S+)(.*)", rest, re.DOTALL)
    if not table_m:
        return arm_sql
    table_name = table_m.group(1)
    after_table = table_m.group(2).strip()
    has_group_by = bool(re.search(r"\bGROUP\s+BY\s+ALL\b", after_table, re.IGNORECASE))
    no_gb = re.sub(
        r"\s*GROUP\s+BY\s+ALL\s*$", "", after_table, flags=re.IGNORECASE
    ).strip()
    where_text = ""
    if re.match(r"WHERE\b", no_gb, re.IGNORECASE):
        where_text = no_gb[5:].strip()

    cols = _split_select_columns(cols_text)
    normalized: list[str] = []
    for c in cols:
        c = c.strip()
        if not c:
            continue
        if "CASE" in c.upper() and "END" in c.upper() and "\n" in c:
            case_lines = c.split("\n")
            fmt_lines: list[str] = []
            for cl in case_lines:
                cl = cl.strip()
                if not cl:
                    continue
                if cl.upper().startswith(("WHEN ", "ELSE ")):
                    fmt_lines.append(f"  {cl}")
                else:
                    fmt_lines.append(cl)
            normalized.append("\n".join(fmt_lines))
        else:
            normalized.append(" ".join(c.split()))
    cols = normalized
    col_lines: list[str] = []
    buf: list[str] = []
    for col in cols:
        is_simple = (
            "\n" not in col
            and "(" not in col
            and "CASE" not in col.upper()
            and "'" not in col
            and len(col) < 30
        )
        if is_simple:
            buf.append(col)
        else:
            if buf:
                col_lines.append(", ".join(buf))
                buf = []
            col_lines.append(col)
    if buf:
        col_lines.append(", ".join(buf))

    where_fmt = ""
    if where_text:
        where_text = _reorder_re_branches(where_text)
        where_fmt = _compact_where(where_text)

    result = "SELECT\n"
    formatted_cols: list[str] = []
    for line in col_lines:
        if "\n" in line:
            sub_lines = line.split("\n")
            formatted_cols.append("\n".join(f"  {sl}" for sl in sub_lines))
        else:
            formatted_cols.append(f"  {line}")
    result += ",\n".join(formatted_cols)
    if where_fmt:
        first_line, *rest_lines = where_fmt.split("\n")
        result += f"\nFROM {table_name} WHERE {first_line}"  # re-lays out SQL it was given; deployed only via the metric-view deployer's SQL check — nosemgrep: tmp.databricks.sql-injection.fstring-where
        for rl in rest_lines:
            result += f"\n  {rl}"
    else:
        result += f"\nFROM {table_name}"
    if has_group_by:
        result += "\nGROUP BY ALL"
    return result


def _compact_where(where_text: str) -> str:
    """Compact WHERE clause into clean lines split at top-level AND."""
    where_text = " ".join(where_text.split())
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    i = 0
    while i < len(where_text):
        c = where_text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        if depth == 0 and where_text[i : i + 5].upper() == " AND " and buf:
            parts.append("".join(buf).strip())
            buf = []
            i += 5
            continue
        buf.append(c)
        i += 1
    if buf:
        parts.append("".join(buf).strip())

    if len(parts) <= 1:
        return where_text

    out_lines = [parts[0]]
    for p in parts[1:]:
        if p.startswith("(") and p.endswith(")") and " OR " in p:
            inner = p[1:-1].strip()
            or_parts = _split_top_level_or(inner)
            if len(or_parts) > 1:
                inline = f"AND {p}"
                if len(inline) <= 120:
                    out_lines.append(inline)
                    continue
                out_lines.append("AND (")
                for j, op in enumerate(or_parts):
                    prefix = "  OR " if j > 0 else "  "
                    out_lines.append(f"{prefix}{op.strip()}")
                out_lines.append(")")
                continue
        out_lines.append(f"AND {p}")
    return "\n".join(out_lines)


def _split_top_level_or(text: str) -> list[str]:
    """Split text at top-level OR (paren-aware)."""
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    i = 0
    while i < len(text):
        c = text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        if depth == 0 and text[i : i + 4].upper() == " OR " and buf:
            parts.append("".join(buf).strip())
            buf = []
            i += 4
            continue
        buf.append(c)
        i += 1
    if buf:
        parts.append("".join(buf).strip())
    return parts


def _reorder_re_branches(where_text: str) -> str:
    """Reorder month-range OR branches descending (R100→R070→R040)."""
    i = 0
    while i < len(where_text):
        if where_text[i] == "(":
            depth = 1
            j = i + 1
            while j < len(where_text) and depth > 0:
                if where_text[j] == "(":
                    depth += 1
                elif where_text[j] == ")":
                    depth -= 1
                j += 1
            inner = where_text[i + 1 : j - 1]
            or_parts = _split_top_level_or(inner.strip())
            if len(or_parts) >= 2 and all(
                re.search(r"MONTH\(CURRENT_DATE\(\)\)", p, re.IGNORECASE)
                for p in or_parts
            ):
                first_num = re.search(r">=\s*(\d+)", or_parts[0])
                last_num = re.search(r">=\s*(\d+)", or_parts[-1])
                if (
                    first_num
                    and last_num
                    and int(first_num.group(1)) < int(last_num.group(1))
                ):
                    or_parts = list(reversed(or_parts))
                    new_inner = " OR ".join(p.strip() for p in or_parts)
                    where_text = where_text[: i + 1] + new_inner + where_text[j - 1 :]
                    return where_text
            i = j
        else:
            i += 1
    return where_text


def _split_select_columns(select_body: str) -> list[str]:
    """Split SELECT column list on commas, respecting parentheses."""
    cols: list[str] = []
    current: list[str] = []
    depth = 0
    for char in select_body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            cols.append("".join(current))
            current = []
            continue
        current.append(char)
    if current:
        cols.append("".join(current))
    return cols
