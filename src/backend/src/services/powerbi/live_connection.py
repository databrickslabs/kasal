"""Detect Power BI tables backed by a LIVE connection to a semantic model.

A table whose Power Query M uses ``AnalysisServices.Database("<server>",
"<database>")`` is not an imported or SQL-DirectQuery table — it's a *live
connection* to an Analysis Services / Power BI semantic model. The transpiler
cannot resolve a lakehouse-native SQL source for it (its measures and columns
live in that upstream model, not in a warehouse table Fabric can scan), so the
migration is only completable by going to that model.

This surfaces the fact as a note — WHICH semantic model (server + database) and
table to parse — so the team has it on their radar rather than silently getting
a thin/empty view. Detection only; no API calls, no side effects.

Kept as its own small module (not folded into the already-oversized generator
tool) and dependency-free so it is safe to import from the crew/flow subprocess.
"""

from __future__ import annotations

import re

# AnalysisServices.Database("server", "database", ...) — the M connector for a
# live connection to an AAS / Power BI semantic model. Server and database are
# the first two string arguments; later optional args (query record) are ignored.
_AAS_RE = re.compile(
    r"""AnalysisServices\.Database\s*\(\s*["']([^"']+)["']\s*,\s*["']([^"']+)["']""",
    re.IGNORECASE,
)


def detect_live_connection(mquery: str | None) -> dict | None:
    """``{"server", "database"}`` when the M-Query is a live semantic-model
    connection, else ``None``.
    """
    if not mquery:
        return None
    m = _AAS_RE.search(mquery)
    if not m:
        return None
    return {"server": m.group(1).strip(), "database": m.group(2).strip()}


def derive_live_connections(
    specs: dict,
    mquery_expressions: dict[str, str] | None,
) -> dict[str, dict]:
    """``{view_name: {server, database, table}}`` for every fact view whose
    source table's M-Query is a live connection to a semantic model.

    ``specs`` is ``{table_key: MetricViewSpec}`` (the pipeline's ``all_specs``);
    ``mquery_expressions`` is ``{table_key: raw M}``. Matched by table key
    case-insensitively. Empty ``{}`` when nothing is a live connection — the
    common case for a warehouse-sourced model.
    """
    view_by_key: dict[str, str] = {}
    for table_key, spec in (specs or {}).items():
        view_by_key[str(table_key).lower()] = getattr(spec, "view_name", table_key)

    out: dict[str, dict] = {}
    for table_key, mquery in (mquery_expressions or {}).items():
        info = detect_live_connection(mquery)
        if info:
            view = view_by_key.get(str(table_key).lower(), table_key)
            out[view] = {**info, "table": table_key}
    return out
