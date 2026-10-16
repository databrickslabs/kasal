"""Table sources handed from config generation to the UCMV generator.

Two things the Pipeline Config Generator (Tool 90) derives from each table's
Power Query M:

* ``build_ucmv_mquery``: the ``mquery_json`` entries UCMV builds views from,
  with the M compiled to SQL when every step is reproducible
  (``resolve_mquery_with_context``), and a report of the type conversions that
  SQL does not apply;
* ``enrich_join_source_tables``: the physical table a dimension join reads,
  only when the dimension's M keeps every row of that table.
"""

from __future__ import annotations

import re

from .mquery_let_evaluator import _extract_call_args
from .mquery_parser import (
    _FQN_RE,
    _extract_bare_reference,
    _extract_first_binding_reference,
    _unquote,
    classify_mquery_source,
    resolve_mquery_with_context,
)
from .mquery_steps import classify_m_steps, steps_after_native_query, type_change_notes

# Cap on the raw M shipped downstream for a table that could not be compiled.
# Matches the truncation ``pipeline.py`` applies to its own "tables not
# emitted" report rather than inventing a second cutoff.
UNRESOLVED_MQUERY_PREVIEW_CHARS = 2000

# SQL that keeps only some rows of its table, or reshapes them.
_ROW_SHAPING_SQL = re.compile(
    r"\b(WHERE|JOIN|GROUP\s+BY|DISTINCT|UNION|HAVING|LIMIT|QUALIFY|TOP)\b",
    re.IGNORECASE,
)
_TRAILING_FQN = re.compile(
    r"\bFROM\s+((?:`[^`]+`|[A-Za-z_]\w*)(?:\.(?:`[^`]+`|[A-Za-z_]\w*)){2})\s*$",
    re.IGNORECASE,
)


def resolve_mquery_with_notes(
    mquery: str, expressions: dict[str, str] | None = None
) -> tuple[str | None, list[str]]:
    """``resolve_mquery_with_context`` plus the value-changing type conversions
    (``Int64.Type``, ``Currency.Type``, ``type date`` ...) in the steps it
    compiled past. The SQL source does not apply them, so they are reported."""
    sql = resolve_mquery_with_context(mquery, expressions)
    if not sql:
        return None, []
    texts = [mquery]
    for ref in (
        _extract_bare_reference(mquery),
        _extract_first_binding_reference(mquery),
    ):
        if ref and expressions and ref in expressions:
            texts.append(expressions[ref])
    notes: list[str] = []
    for text in texts:
        steps, _ = classify_m_steps(text)
        notes += [n for n in type_change_notes(steps) if n not in notes]
    return sql, notes


def build_ucmv_mquery(
    admin_tables: dict,
    expressions: dict | None = None,
    warnings: list[str] | None = None,
) -> list[dict]:
    """Convert admin_tables into the UCMV ``mquery_json`` shape.

    ``admin_tables[table]["mquery_expression"]`` holds the partition source
    (embedded SQL or raw M). UCMV's ``MQueryParser.parse_json`` expects
    ``{table_name, transpiled_sql, validation_passed}`` and skips any entry
    without both a table name and SQL, so tables with no source are omitted.

    A deterministic compile to compact SQL is preferred over shipping the raw
    M: catalog-browse M carries no SQL for the parser to read; staging-query
    references and parameter-driven sources need the model's ``expressions``;
    and raw M is the largest text in the payload, which the receiving agent has
    to reproduce verbatim. Unresolved M is shipped raw when small, capped when
    large. An entry whose compiled SQL skips value-changing type conversions
    carries them under ``type_changes``, and one line in ``warnings`` lists
    them for the run report.
    """
    out: list[dict] = []
    type_changes: list[str] = []
    for tbl_name, tbl_info in (admin_tables or {}).items():
        if not isinstance(tbl_info, dict):
            continue
        raw = (
            tbl_info.get("mquery_expression") or tbl_info.get("mquery") or ""
        ).strip()
        if not tbl_name or not raw:
            continue
        resolved, notes = resolve_mquery_with_notes(raw, expressions)
        sql = resolved or raw[:UNRESOLVED_MQUERY_PREVIEW_CHARS]
        entry = {
            "table_name": tbl_name,
            "transpiled_sql": sql,
            # MUST be "Yes": parse_json drops any other entry (unless the SQL
            # has both SUM( and GROUP BY). The source is the authoritative
            # partition expression, so it is accepted as given.
            "validation_passed": "Yes",
        }
        if notes:
            entry["type_changes"] = notes
            type_changes.append(f"{tbl_name}: {', '.join(notes)}")
        out.append(entry)
    if type_changes and warnings is not None:
        warnings.append(
            "Power Query type conversions are not applied: the metric view reads "
            "the physical table, and these conversions can change values "
            "(whole-number types round, Currency rounds to 4 decimals, date drops "
            "the time, which breaks date-key joins). The SQL that reproduces each "
            "one is shown; apply it in a source view: " + "; ".join(type_changes[:10])
        )
    return out


def _literal_native_query(mquery: str) -> str | None:
    """The SQL of a ``Value.NativeQuery`` whose query is a plain string literal."""
    m = re.search(r"\bValue\.NativeQuery\s*\(", mquery or "")
    args = _extract_call_args(mquery, m.end()) if m else None
    if not args or len(args) < 2:
        return None
    lit = args[1].strip()
    if not re.fullmatch(r'"(?:[^"]|"")*"', lit):
        return None
    sql = lit[1:-1].replace('""', '"')
    return re.sub(r"#\((?:lf|cr|tab)\)", " ", sql, flags=re.IGNORECASE)


def plain_join_source(
    mquery: str, expressions: dict | None = None
) -> tuple[str | None, str]:
    """The physical table a dimension join can read directly, or ``(None, why)``.

    Joining the raw table is only safe when the dimension's M keeps every row
    of it. A row filter or dedup in the M (or a WHERE / DISTINCT in its native
    query) means the raw table holds rows Power BI does not, so a join on it
    could duplicate fact rows and inflate totals.
    """
    if not mquery:
        return None, "no M source expression available"
    sql = resolve_mquery_with_context(mquery, expressions)
    if sql is None and not steps_after_native_query(mquery):
        sql = _literal_native_query(mquery)
    fqn_match = _TRAILING_FQN.search(sql.strip()) if sql else None
    if sql and fqn_match and not _ROW_SHAPING_SQL.search(sql):
        parts = _FQN_RE.match(fqn_match.group(1))
        if parts:
            return ".".join(_unquote(p) for p in parts.groups()), ""
    category, reason = classify_mquery_source(mquery)
    if category != "extractable" and not (sql and _ROW_SHAPING_SQL.search(sql)):
        return None, reason
    return None, (
        "not joined on the raw table: the dimension's source keeps only some "
        "of its rows (or could not be read), so a join could duplicate or "
        f"include rows Power BI excludes; supply a source view. {reason}"
    )


def enrich_join_source_tables(
    config: dict, admin_tables: dict, expressions: dict | None = None
) -> list[dict]:
    """Fill ``join_key_map[dim].source_table`` from each dimension's M.

    Deterministic (no warehouse, no LLM). Only entries without a value (or
    with a ``TODO:`` placeholder) are filled, and only from a source that
    keeps every row of its table (``plain_join_source``); anything else is
    logged as skipped with the reason, never joined on the raw table. Returns
    the audit log for the Config Editor.
    """
    log: list[dict] = []
    for dim, entry in (config.get("join_key_map", {}) or {}).items():
        if not isinstance(entry, dict):
            continue
        existing = entry.get("source_table")
        if existing and not str(existing).startswith("TODO"):
            continue  # human/derived value — never overwrite
        tinfo = admin_tables.get(dim) or {}
        mquery = tinfo.get("mquery_expression") or tinfo.get("mquery") or ""
        resolved, reason = plain_join_source(mquery, expressions)
        item = {"tier": "P1", "key": "join_key_map", "target": dim}
        if resolved:
            entry["source_table"] = resolved
            item.update(
                status="filled", value=resolved, detail="parsed from connector M-query"
            )
        else:
            item.update(status="skipped", detail=reason)
        log.append(item)
    return log
