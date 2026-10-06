"""Phase 1 cross-table support: same-grain UNION combined sources.

When a measure aggregates across TWO (or more) fact tables at the SAME grain,
a single-source UC Metric View can't express it directly. This builds a
``UNION ALL`` combined source and routes the cross-fact measures through the
pipeline's OWN translator against it, so the output is validated and iteratively
refined by PBI reconciliation like any other measure.

Two safety rails, both deliberate:
  * GATED ON RECONCILIATION. Only runs when the reconciliation loop is active
    (the live validator). Without that cell-by-cell PBI check a wrong cross-fact
    number could ship silently — so when reconciliation is off we emit nothing
    and leave the measures exactly as today (collected, not invented).
  * STRICT SAME-GRAIN ONLY. A bucket proceeds only when the shared grain equals
    the full group-by of EVERY contributing fact. Mismatched grains / 3-fact
    joins / name collisions are deferred to limitations, never emitted — grain
    mismatch is the double-counting trap.

Key trick — NULL-aligned UNION columns: each arm selects its own fact's
aggregate columns and ``NULL`` for the others, so ``SUM(source.col_from_fact_a)``
naturally equals fact A's total (SUM ignores the NULLs contributed by fact B's
arm). A cross-fact ratio therefore translates to
``SUM(col_a) / NULLIF(SUM(col_b), 0)`` with no manual per-arm FILTER.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from .data_classes import MetricViewSpec, TableInfo
from .measure_allocator import _norm

logger = logging.getLogger(__name__)


def _fact_grain(table_info: TableInfo) -> set:
    return {_norm(c) for c in (getattr(table_info, "group_by_columns", None) or [])}


def _is_same_grain(
    facts: List[str], shared_grain: List[str], mquery_tables: dict
) -> bool:
    """True only when the shared grain equals the full group-by of EVERY fact."""
    shared = {_norm(c) for c in (shared_grain or [])}
    if not shared:
        return False
    for f in facts:
        ti = mquery_tables.get(f)
        if ti is None:
            return False
        if _fact_grain(ti) != shared:
            return False
    return True


def _build_union_source(
    facts: List[str], grain_cols: List[str], mquery_tables: dict
) -> Tuple[str, List[Dict[str, str]]]:
    """Return (union_sql, aggregate_columns) for the combined source, or ("", []).

    aggregate_columns are the combined view's output measure columns (one per
    distinct fact aggregate column); a name collision across facts returns
    ("", []) so the caller defers the bucket rather than silently merge columns.
    """
    # Collect each fact's aggregate columns, keyed by output name; bail on collision.
    agg: List[Tuple[str, str, str]] = []  # (fact, out_name, source_col)
    seen: set = set()
    for f in facts:
        ti = mquery_tables.get(f)
        for a in getattr(ti, "aggregate_columns", None) or []:
            out_name = str(a.get("name") or a.get("source_col") or "").strip()
            src_col = str(a.get("source_col") or a.get("name") or "").strip()
            if not out_name or not src_col:
                continue
            if out_name in seen:
                return "", []  # name collision across facts → defer (safety)
            seen.add(out_name)
            agg.append((f, out_name, src_col))
    if not agg:
        return "", []

    arms: List[str] = []
    for f in facts:
        ti = mquery_tables.get(f)
        src_table = getattr(ti, "source_table", "") or ""
        if not src_table:
            return "", []
        cols = list(grain_cols)  # grain columns are shared (same-grain gate)
        for owner, out_name, src_col in agg:
            if owner == f:
                cols.append(f"{src_col} AS {out_name}")
            else:
                cols.append(f"NULL AS {out_name}")
        cols.append(f"'{f}' AS _source_fact")
        arms.append("SELECT\n  " + ",\n  ".join(cols) + f"\nFROM {src_table}")

    union_sql = "\nUNION ALL\n".join(arms)
    aggregate_columns = [{"name": n, "source_col": n} for _, n, _ in agg]
    return union_sql, aggregate_columns


def plan_cross_table_sources(pipeline) -> Tuple[Dict[str, MetricViewSpec], List[dict]]:
    """Build combined-source specs for same-grain cross-fact buckets.

    Returns ``(new_specs, limitations)``. ``new_specs`` is keyed by a synthetic
    combined fact key and already has its measures translated (via the pipeline's
    own ``_process_table``). ``limitations`` lists deferred buckets with a reason.
    Fail-open: any bucket that raises is deferred, never emitted.
    """
    report = getattr(pipeline, "_allocation_report", None) or {}
    cross = report.get("cross_fact") or []
    if not cross:
        return {}, []

    mquery_tables = pipeline.mquery_tables

    # Group cross-fact measures by the SET of facts they span.
    buckets: Dict[frozenset, Dict[str, Any]] = {}
    for entry in cross:
        facts = tuple(sorted(entry.get("facts") or []))
        if len(facts) < 2:
            continue
        key = frozenset(facts)
        b = buckets.setdefault(
            key,
            {
                "facts": list(facts),
                "shared_grain": entry.get("shared_grain") or [],
                "measures": [],
            },
        )
        b["measures"].append(entry.get("measure"))

    new_specs: Dict[str, MetricViewSpec] = {}
    limitations: List[dict] = []

    for key, b in buckets.items():
        facts = b["facts"]
        combined_key = "combined_" + "_".join(_norm(f).replace(".", "_") for f in facts)
        try:
            if len(facts) > 2:
                limitations.append(
                    {
                        "facts": facts,
                        "measures": b["measures"],
                        "reason": "3+ fact span — needs JOIN/cardinality (Phase 2)",
                    }
                )
                continue
            if not _is_same_grain(facts, b["shared_grain"], mquery_tables):
                limitations.append(
                    {
                        "facts": facts,
                        "measures": b["measures"],
                        "reason": "grain mismatch — shared grain != full group-by of every fact (deferred to avoid double-counting)",
                    }
                )
                continue

            # Grain columns: the shared group-by, using the first fact's original names.
            grain_cols = list(
                getattr(mquery_tables[facts[0]], "group_by_columns", None) or []
            )
            union_sql, agg_cols = _build_union_source(facts, grain_cols, mquery_tables)
            if not union_sql:
                limitations.append(
                    {
                        "facts": facts,
                        "measures": b["measures"],
                        "reason": "could not build a safe combined source (missing source table or column name collision)",
                    }
                )
                continue

            # Gather the cross-fact measures' mapping entries (their DAX) so the
            # translator composes the ratio from the combined-source base measures.
            wanted = {_norm(m) for m in b["measures"] if m}
            measure_dicts = [
                e
                for e in pipeline.mapping
                if _norm(str(e.get("measure_name") or "")) in wanted
            ]
            if not measure_dicts:
                continue

            combined_info = TableInfo(
                table_name=combined_key,
                source_table=combined_key,
                aggregate_columns=agg_cols,
                group_by_columns=grain_cols,
                calculated_columns=[],
                is_fact=True,
                full_sql=union_sql,
            )

            spec = pipeline._process_table(combined_key, combined_info, measure_dicts)
            # Point the spec at the inline UNION source so the deployed view reads it.
            spec.source_sql = union_sql
            spec.source_table = combined_key
            spec.view_name = combined_key
            spec.comment = (
                f"Combined cross-fact source (same-grain UNION ALL of {', '.join(facts)}). "
                "Validated by PBI reconciliation."
            )
            new_specs[combined_key] = spec
            logger.info(
                "[CrossTable] built combined source %s from %s (%d measure(s))",
                combined_key,
                facts,
                len(measure_dicts),
            )
        except Exception as e:  # noqa: BLE001 — never break generation
            logger.warning("[CrossTable] bucket %s deferred (error: %s)", facts, e)
            limitations.append(
                {"facts": facts, "measures": b.get("measures"), "reason": f"error: {e}"}
            )

    return new_specs, limitations
