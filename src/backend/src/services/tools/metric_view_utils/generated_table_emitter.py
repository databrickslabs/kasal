"""Generated-table SQL emitter (KASAL_FIXES Gap 3, materialization step).

Some Power Query tables are computed entirely in M with no physical source — the
classic case is a **calendar** built with ``List.Dates`` + ``Table.AddColumn``
steps. A UC metric view can't reference a table that doesn't exist, so this module
turns that M into a runnable ``CREATE OR REPLACE VIEW`` (Databricks SQL) that
reproduces the same rows/columns. `physical_name_resolver` detects these tables;
this emits the SQL to materialize them.

Scope: the ``List.Dates`` calendar pattern (covers virtually every PBI date dim).
For AddColumn expressions it can't translate it emits the column as a
``/* TODO */`` placeholder rather than dropping it, so the view is never silently
wrong — the reviewer sees exactly what needs a hand.
"""

from __future__ import annotations

import re

_LIST_DATES = re.compile(r"\bList\.Dates\b")
# #date(Y, M, D)
_DATE_LIT = re.compile(r"#date\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)")
_ADDCOL_START = re.compile(r"Table\.AddColumn\(")
_TRAILING_TYPE = re.compile(r",\s*(?:Int64\.Type|type\s+\w+|[A-Za-z0-9_.]+\.Type)\s*$")


def _find_addcols(m_expr: str) -> list[tuple[str, str]]:
    """Extract (column_name, M expression body) for each Table.AddColumn(...),
    using a balanced-paren scan so nested calls (Text.From(Date.X([Date]))) and
    quoted prev-step names (#"Renamed Columns") are handled correctly."""
    out: list[tuple[str, str]] = []
    for start in _ADDCOL_START.finditer(m_expr):
        i = start.end() - 1  # index of the opening '('
        depth, j = 0, i
        while j < len(m_expr):
            c = m_expr[j]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        args = m_expr[i + 1 : j]
        em = re.search(r"\beach\b", args)
        if not em:
            continue
        names = re.findall(
            r'"([^"]+)"', args[: em.start()]
        )  # last quoted before `each`
        if not names:
            continue
        body = _TRAILING_TYPE.sub("", args[em.end() :].strip()).strip()
        out.append((names[-1], body))
    return out


# M Date.ToText format token → Spark date_format token (mostly identical; the day
# name differs: M 'dddd'/'ddd' → Spark 'EEEE'/'EEE').
def _map_fmt(fmt: str) -> str:
    fmt = fmt.replace("dddd", "EEEE").replace("ddd", "EEE")
    return fmt


def _m_scalar_to_sql(expr: str) -> str | None:
    """Translate one M column expression (body of ``each …``) to Spark SQL over the
    date column ``d``. Returns None if it contains constructs we don't handle."""
    s = expr.strip()
    s = s.replace("[Date]", "d")

    # Date.ToText(d, "fmt")  → date_format(d, 'fmt')
    s = re.sub(
        r'Date\.ToText\(\s*d\s*,\s*"([^"]*)"\s*\)',
        lambda m: f"date_format(d, '{_map_fmt(m.group(1))}')",
        s,
    )
    # Date.Year / Month / Day / QuarterOfYear / WeekOfYear
    s = re.sub(r"Date\.Year\(\s*d\s*\)", "year(d)", s)
    s = re.sub(r"Date\.Month\(\s*d\s*\)", "month(d)", s)
    s = re.sub(r"Date\.Day\(\s*d\s*\)", "day(d)", s)
    s = re.sub(r"Date\.QuarterOfYear\(\s*d\s*\)", "quarter(d)", s)
    s = re.sub(r"Date\.WeekOfYear\(\s*d\s*\)", "weekofyear(d)", s)
    # Date.DayOfWeek(d, Day.Monday)  → weekday(d)  (Spark weekday: Mon=0, matches M Day.Monday base)
    s = re.sub(r"Date\.DayOfWeek\(\s*d\s*,\s*Day\.\w+\s*\)", "weekday(d)", s)
    # Text.From(x) → the inner expression (Spark concat/|| coerce implicitly; wrap in string())
    s = re.sub(r"Text\.From\(\s*(.*?)\s*\)", r"string(\1)", s)
    # M string concat '&' → SQL '||'
    s = s.replace("&", "||")
    # Text.PadStart(x, n, "c") → lpad(x, n, 'c')
    s = re.sub(
        r'Text\.PadStart\(\s*(.*?)\s*,\s*(\d+)\s*,\s*"([^"]*)"\s*\)',
        r"lpad(\1, \2, '\3')",
        s,
    )
    # bare double-quoted string literals → single-quoted
    s = re.sub(r'"([^"]*)"', r"'\1'", s)

    # If anything M-specific survived, we can't safely translate it.
    if re.search(r"\b(Date|Time|Text|Number|List|Table)\.", s) or "#" in s:
        return None
    return s


def emit_view_sql(m_expr: str, fqn: str) -> str | None:
    """Emit a CREATE OR REPLACE VIEW that reproduces a generated calendar table.

    Args:
        m_expr: the table's Power Query M.
        fqn: fully-qualified view name to create (e.g. `cat`.`sch`.`date`).

    Returns the SQL string, or None if it isn't a List.Dates calendar we can emit.
    """
    if not m_expr or not _LIST_DATES.search(m_expr):
        return None
    lits = _DATE_LIT.findall(m_expr)
    if len(lits) < 2:
        return None  # need a start and an end #date literal
    (sy, sm, sd), (ey, em, ed) = lits[0], lits[1]
    start = f"{int(sy):04d}-{int(sm):02d}-{int(sd):02d}"
    end = f"{int(ey):04d}-{int(em):02d}-{int(ed):02d}"

    cols = ["d AS `date`"]
    todo = 0
    for name, body in _find_addcols(m_expr):
        sql = _m_scalar_to_sql(body)
        col = f"`{name}`"
        if sql is None:
            cols.append(f"/* TODO translate M: {body.strip()[:80]} */ NULL AS {col}")
            todo += 1
        else:
            cols.append(f"{sql} AS {col}")

    select = ",\n    ".join(cols)
    header = (
        f"-- Auto-generated from a Power Query List.Dates calendar (no physical source).\n"
        f"-- Reproduces the M-computed date dimension so metric views can join it.\n"
    )
    if todo:
        header += f"-- NOTE: {todo} column(s) need manual translation (see /* TODO */ below).\n"
    return (
        f"{header}CREATE OR REPLACE VIEW {fqn} AS\nSELECT\n    {select}\n"
        f"FROM (SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS d);"
    )
