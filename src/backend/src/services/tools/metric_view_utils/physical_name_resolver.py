"""Physical-name resolution pass (KASAL_FIXES Gaps 1–3).

Power BI friendly table names and model (display) column names usually differ
from the real physical table/column names in the warehouse — the mapping lives
only in each table's Power Query M (``Item=``/``FROM`` for the table,
``Table.RenameColumns`` for the columns). The rest of the pipeline emits
identifiers from the *model*, so a generated UC metric view can reference tables
and columns that do not exist.

This module rewrites a built ``MetricViewSpec`` so every ``source``/join table and
every column reference uses the real physical name recovered from the M. It runs
just before YAML emission and is **fail-open**: any table/column it cannot resolve
from the M is left exactly as-is, so it can never break an already-correct spec.

Gap 1  physical TABLE names   — ``Item=`` (navigation) / ``FROM`` (NativeQuery)
Gap 2  physical COLUMN names  — inverse of ``Table.RenameColumns`` (+ verbatim for
                                un-renamed columns; matched case/underscore-insensitively
                                so camelCase physicals like ``KeyQuestion`` resolve)
Gap 3  GENERATED tables       — ``List.Dates``/``#table``/… have no physical source;
                                detected and reported so they can be materialised.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

_GENERATED = re.compile(
    r"\b(List\.Dates|List\.Numbers|List\.Generate|Table\.FromList)\b|#table\b"
)
_RENAME_PAIR = re.compile(r'\{"([^"]+)",\s*"([^"]+)"\}')
_NAV_ITEM = re.compile(r'Item\s*=\s*"([^"]+)"')


def _norm(s: str) -> str:
    """Match key: lowercase, drop every non-alphanumeric (so ``Key Question``,
    ``KeyQuestion`` and ``key_question`` all collapse to ``keyquestion``)."""
    return re.sub(r"[^0-9a-z]", "", s.lower())


def _norm_table(name: str) -> str:
    """Table match key — like ``_norm`` but also strips a leading ``dim``/``cdim``
    so a join alias (``dim_supervisor``) matches its friendly table."""
    n = _norm(name)
    for p in ("cdim", "dim"):
        if n.startswith(p) and len(n) > len(p):
            return n[len(p) :]
    return n


def physical_table(m_expr: str) -> str | None:
    """Real physical table name from an M expression, or None (generated / unknown)."""
    if not m_expr or _GENERATED.search(m_expr):
        return None
    nav = _NAV_ITEM.search(m_expr)
    if nav:
        return nav.group(1)
    if re.search(r"\bValue\.NativeQuery\b", m_expr):
        mm = re.search(r'FROM\b[^\n]*?\.([A-Za-z0-9_]+)\s*"', m_expr) or re.search(
            r'FROM\s+[`"]?[\w-]+[`"]?\.[`"]?\w+[`"]?\.([A-Za-z0-9_]+)', m_expr
        )
        if mm:
            return mm.group(1)
    return None


def is_generated(m_expr: str) -> bool:
    """True if the table is computed entirely in M (no physical source)."""
    return bool(m_expr and _GENERATED.search(m_expr))


def column_map(m_expr: str, display_columns: list[str]) -> dict[str, str]:
    """Map ``_norm(token) -> physical column`` for a table.

    Physical name = the *raw* (pre-rename) side of ``Table.RenameColumns``; for a
    column that was never renamed, the model (display) name IS the physical name.
    Indexed by the normalized display name AND the normalized physical name, so a
    token that is already physical still resolves (to itself)."""
    inv = {disp: raw for raw, disp in _RENAME_PAIR.findall(m_expr or "")}
    cmap: dict[str, str] = {}
    for disp in display_columns:
        raw = inv.get(disp, disp)
        cmap.setdefault(_norm(disp), raw)
        cmap.setdefault(_norm(raw), raw)
    return cmap


def _qcol(raw: str) -> str:
    return raw if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw or "") else f"`{raw}`"


def _rewrite_columns(text: str, prefix_maps: dict[str, dict[str, str]]) -> str:
    """Rewrite ``<prefix>.<col>`` tokens using the per-prefix column map. Only
    changes a token when a physical name is found AND it differs; unknown columns
    are left untouched (fail-open)."""
    if not text:
        return text
    prefixes = "|".join(re.escape(p) for p in prefix_maps)
    pat = re.compile(rf"\b({prefixes})\.([A-Za-z_][A-Za-z0-9_]*)")

    def _sub(mm: re.Match) -> str:
        pre, col = mm.group(1), mm.group(2)
        raw = prefix_maps.get(pre, {}).get(_norm(col))
        if raw and raw != col:
            return f"{pre}.{_qcol(raw)}"
        return mm.group(0)

    return pat.sub(_sub, text)


def _replace_leaf(source: str, physical: str) -> str:
    """Replace the table leaf of a dotted ``cat.sch.table`` source with ``physical``.
    Leaves inline-SQL sources (containing whitespace/SELECT) untouched."""
    if not source or re.search(r"\s", source) or physical is None:
        return source
    parts = source.rsplit(".", 1)
    return f"{parts[0]}.{physical}" if len(parts) == 2 else physical


def resolve_physical_names(specs, mquery_tables, mquery_expressions) -> dict:
    """Rewrite every spec's table/column identifiers to physical names from the M.

    Args:
        specs: dict[table_key -> MetricViewSpec] (mutated in place).
        mquery_tables: dict[table_key -> TableInfo] (for the display-column lists).
        mquery_expressions: dict[table_key -> raw Power Query M string].

    Returns a report dict: {'generated_tables': [...], 'tables_resolved': n,
    'columns_rewritten': n} for logging / limitations.
    """
    logger.info(
        "[physical_name_resolver] invoked: %d spec(s), %d M expression(s)",
        len(specs or {}), len(mquery_expressions or {}))
    if not mquery_expressions:
        logger.info(
            "[physical_name_resolver] skipped: no Power Query M available "
            "(pass scan_data_json / mquery_expressions) — identifiers left as model names")
        return {"generated_tables": [], "tables_resolved": 0, "columns_rewritten": 0}

    # Per friendly table: physical name, column map, generated flag — indexed by
    # several normalized keys so facts, dims and join aliases all resolve.
    by_norm: dict[str, dict] = {}
    generated: set[str] = set()
    for tkey, m in mquery_expressions.items():
        tinfo = mquery_tables.get(tkey)
        cols = []
        if tinfo is not None:
            cols = [
                c.get("source_col") or c.get("name")
                for c in (getattr(tinfo, "aggregate_columns", None) or [])
            ]
            cols += list(getattr(tinfo, "group_by_columns", None) or [])
            cols += [
                c.get("name")
                for c in (getattr(tinfo, "calculated_columns", None) or [])
            ]
        cols = [c for c in cols if c]
        phys = physical_table(m)
        entry = {
            "physical": phys,
            "colmap": column_map(m, cols),
            "generated": is_generated(m),
        }
        if entry["generated"]:
            generated.add(tkey)
        for k in {_norm_table(tkey), _norm(tkey)}:
            by_norm.setdefault(k, entry)
        if phys:
            by_norm.setdefault(_norm_table(phys), entry)

    def lookup(*names) -> dict | None:
        for n in names:
            if not n:
                continue
            for k in {_norm_table(n), _norm(n)}:
                if k in by_norm:
                    return by_norm[k]
        return None

    report = {
        "generated_tables": sorted(generated),
        "tables_resolved": 0,
        "columns_rewritten": 0,
    }

    for spec in specs.values():
        fact = lookup(
            getattr(spec, "fact_table_key", None),
            getattr(spec, "source_table", "").rsplit(".", 1)[-1],
        )
        fact_map = fact["colmap"] if fact else {}
        if fact and fact.get("physical"):
            spec.source_table = _replace_leaf(spec.source_table, fact["physical"])
            report["tables_resolved"] += 1

        # per-join: resolve the dim table + build the alias->colmap for column rewrites
        prefix_maps: dict[str, dict[str, str]] = {"source": fact_map}
        for j in getattr(spec, "joins", None) or []:
            alias = j.get("name")
            leaf = (j.get("source") or "").rsplit(".", 1)[-1]
            dim = lookup(alias, leaf)
            if dim:
                if dim.get("physical"):
                    j["source"] = _replace_leaf(j.get("source", ""), dim["physical"])
                    report["tables_resolved"] += 1
                if alias:
                    prefix_maps[alias] = dim["colmap"]

        # rewrite join ON clauses, dimension exprs, measure SQL
        for j in getattr(spec, "joins", None) or []:
            key = (
                "join_on"
                if j.get("join_on") is not None
                else ("on" if j.get("on") is not None else None)
            )
            if key:
                j[key] = _rewrite_columns(j[key], prefix_maps)
        for d in getattr(spec, "dimensions", None) or []:
            if d.get("expr"):
                d["expr"] = _rewrite_columns(d["expr"], prefix_maps)
        for m in getattr(spec, "measures", None) or []:
            if getattr(m, "sql_expr", None):
                m.sql_expr = _rewrite_columns(m.sql_expr, prefix_maps)

    if report["tables_resolved"] or generated:
        logger.info(
            "[physical_name_resolver] resolved %d table ref(s) to physical names; "
            "%d generated table(s) need materialisation: %s",
            report["tables_resolved"],
            len(generated),
            ", ".join(sorted(generated)) or "(none)",
        )
    else:
        logger.info(
            "[physical_name_resolver] no-op: %d M expression(s) but nothing matched "
            "%d spec(s) (check that fact/dim keys align with the M table names)",
            len(mquery_expressions), len(specs or {}))
    return report
