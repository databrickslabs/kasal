"""Measures that read a model table the generator has no source for.

Some Power BI tables have no SQL the generator can read: their Power Query combines other
queries that are not loaded, or reads a source the extract does not carry. Such a table gets no
source view and no metric view. A measure that aggregates it was still translated: its home
fell back to another fact with columns of the same name, and ``SUMX(Planning, Planning[value])``
became ``SUM(source.value)`` on that fact. The view deployed and the number was wrong.

A measure is "rooted" in such a table when its DAX aggregates the table, or references (through
code that is live, not an unused VAR) a measure that is. A rooted measure is not translated on
any view; it is reported with the table's name.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from .mquery_parser import _is_pbi_only, looks_like_raw_mquery

_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_LINE_COMMENT = re.compile(r"(?://|--)[^\n]*")
_VAR = re.compile(r"\bVAR\s+(\w+)\s*=", re.I)
_RETURN = re.compile(r"\bRETURN\b", re.I)
_MEASURE_REF = re.compile(r"(?<![\w'\]])\[([^\]]+)\]")
_AGGREGATED_TABLE = re.compile(
    r"\b(?:SUM|SUMX|AVERAGE|AVERAGEX|MIN|MAX|MINX|MAXX|COUNT|COUNTA|COUNTROWS|COUNTX|"
    r"DISTINCTCOUNT|DISTINCTCOUNTNOBLANK)\s*\(\s*(?:FILTER\s*\(\s*)?"
    r"(?:'([^']+)'|([A-Za-z_]\w*))",
    re.I,
)


def sourceless_tables(mquery_tables: Mapping[str, object]) -> set[str]:
    """Model tables whose Power Query gives the generator no source: raw M with no SQL in it
    that is not a table of rows kept in the model either."""
    out: set[str] = set()
    for name, info in mquery_tables.items():
        raw = str(getattr(info, "raw_transpiled_sql", "") or "")
        if getattr(info, "is_fact", False) or getattr(info, "source_table", ""):
            continue
        if looks_like_raw_mquery(raw) and not _is_pbi_only(raw):
            out.add(name)
    return out


def live_dax(dax: str) -> str:
    """The DAX that decides the result: comments removed, and of a ``VAR ... RETURN`` block
    only the RETURN expression and the variables it uses, directly or through other ones."""
    text = _LINE_COMMENT.sub(" ", _BLOCK_COMMENT.sub(" ", dax or ""))
    variables = list(_VAR.finditer(text))
    ret = None
    for match in _RETURN.finditer(text):
        ret = match  # the last RETURN closes the outer block
    if not variables or ret is None or ret.start() < variables[0].start():
        return text
    bodies: dict[str, str] = {}
    for index, var in enumerate(variables):
        if var.start() > ret.start():
            break
        following = variables[index + 1].start() if index + 1 < len(variables) else ret.start()
        bodies[var.group(1).lower()] = text[var.end() : min(following, ret.start())]
    live = text[ret.end() :]
    used: set[str] = set()
    pending = [live]
    while pending:
        part = pending.pop()
        for name, body in bodies.items():
            if name in used:
                continue
            if re.search(rf"(?<![\w\['])\b{re.escape(name)}\b(?![\]\[(])", part, re.I):
                used.add(name)
                live += " " + body
                pending.append(body)
    return live


def rooted_measures(
    measures: Iterable[Mapping[str, object]], tables: set[str]
) -> dict[str, str]:
    """``{Power BI measure name: sourceless table}`` for every measure rooted in one of
    ``tables``: it aggregates the table, or its live DAX references a measure that is rooted."""
    if not tables:
        return {}
    lowered = {t.lower(): t for t in tables}
    live: dict[str, str] = {}
    for row in measures:
        name = str(row.get("original_name") or row.get("measure_name") or "")
        if name:
            live[name] = live_dax(str(row.get("dax_expression") or ""))
    rooted: dict[str, str] = {}
    for name, text in live.items():
        for quoted, bare in _AGGREGATED_TABLE.findall(text):
            table = lowered.get((quoted or bare).lower())
            if table:
                rooted[name] = table
                break
    by_lower = {name.lower(): name for name in live}
    changed = bool(rooted)
    while changed:
        changed = False
        for name, text in live.items():
            if name in rooted:
                continue
            for ref in _MEASURE_REF.findall(text):
                target = by_lower.get(ref.strip().lower())
                if target in rooted:
                    rooted[name] = rooted[target]
                    changed = True
                    break
    return rooted


def reason(table: str) -> str:
    """Why a rooted measure is not translated."""
    return (
        f"reads {table}, a table the generator has no source for (its Power Query is not "
        "readable SQL); not translated against another table's columns"
    )


def demote_rooted(spec: object, rooted: Mapping[str, str]) -> list[str]:
    """Move a view's translated measures that are rooted in a sourceless table to its
    untranslated list, with the reason. Returns the Power BI names moved. The options of a
    decomposed SWITCH are left alone: each is one branch, judged by its own translation."""
    if not rooted:
        return []
    kept: list = []
    moved: list[str] = []
    for measure in getattr(spec, "measures", []):
        table = rooted.get(str(getattr(measure, "original_name", "") or ""))
        if (
            not table
            or not measure.sql_expr
            or getattr(measure, "category", "") == "switch_decomposition"
        ):
            kept.append(measure)
            continue
        measure.is_translatable = False
        measure.sql_expr = None
        measure.window_spec = None
        measure.skip_reason = reason(table)
        spec.untranslatable.append(measure)
        moved.append(measure.original_name)
    spec.measures[:] = kept
    return moved
