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
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .data_classes import MetricViewSpec, TableInfo
from .measure_allocator import _norm

if TYPE_CHECKING:
    from .pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)


@dataclass
class _Bucket:
    """Cross-fact measures that span the same set of facts."""

    facts: list[str]
    shared_grain: object
    measures: list[object] = field(default_factory=list)


def _as_str_list(value: object) -> list[str]:
    """Items of a list-like config value as strings; anything else is empty."""
    if isinstance(value, (list, tuple, set, frozenset)):
        return [str(item) for item in value]
    return []


def _as_entries(value: object) -> list[Mapping[str, object]]:
    """Dict entries of a list-valued report section; anything else is empty."""
    if not isinstance(value, list):
        return []
    return [entry for entry in value if isinstance(entry, Mapping)]


def _fact_grain(table_info: TableInfo) -> set[str]:
    return {_norm(c) for c in (table_info.group_by_columns or [])}


def _is_same_grain(
    facts: list[str], shared_grain: list[str], mquery_tables: Mapping[str, TableInfo]
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
    facts: list[str], grain_cols: list[str], mquery_tables: Mapping[str, TableInfo]
) -> tuple[str, list[dict[str, str]]]:
    """Return (union_sql, aggregate_columns) for the combined source, or ("", []).

    aggregate_columns are the combined view's output measure columns (one per
    distinct fact aggregate column); a name collision across facts returns
    ("", []) so the caller defers the bucket rather than silently merge columns.
    """
    # Collect each fact's aggregate columns, keyed by output name; bail on collision.
    agg: list[tuple[str, str, str]] = []  # (fact, out_name, source_col)
    seen: set[str] = set()
    for f in facts:
        ti = mquery_tables.get(f)
        if ti is None:
            continue
        for a in ti.aggregate_columns or []:
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

    arms: list[str] = []
    for f in facts:
        ti = mquery_tables.get(f)
        if ti is None:
            return "", []
        src_table = ti.source_table or ""
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


def _group_cross_fact_buckets(
    cross: list[Mapping[str, object]],
) -> dict[frozenset[str], _Bucket]:
    """Group cross-fact report entries by the SET of facts they span."""
    buckets: dict[frozenset[str], _Bucket] = {}
    for entry in cross:
        facts = tuple(sorted(_as_str_list(entry.get("facts"))))
        if len(facts) < 2:
            continue
        key = frozenset(facts)
        bucket = buckets.setdefault(
            key,
            _Bucket(
                facts=list(facts),
                shared_grain=entry.get("shared_grain") or [],
            ),
        )
        bucket.measures.append(entry.get("measure"))
    return buckets


def _limitation(
    facts: list[str], measures: list[object], reason: str
) -> dict[str, object]:
    return {"facts": facts, "measures": measures, "reason": reason}


def _plan_bucket(
    pipeline: MetricViewPipeline, bucket: _Bucket, combined_key: str
) -> tuple[MetricViewSpec | None, dict[str, object] | None]:
    """Translate one same-grain bucket into a combined spec, or a limitation.

    Returns ``(spec, None)`` on success, ``(None, limitation)`` when deferred and
    ``(None, None)`` when there is nothing in the mapping to translate.
    """
    facts = bucket.facts
    mquery_tables = pipeline.mquery_tables
    if len(facts) > 2:
        return None, _limitation(
            facts,
            bucket.measures,
            "3+ fact span — needs JOIN/cardinality (Phase 2)",
        )
    if not _is_same_grain(facts, _as_str_list(bucket.shared_grain), mquery_tables):
        return None, _limitation(
            facts,
            bucket.measures,
            "grain mismatch — shared grain != full group-by of every fact (deferred to avoid double-counting)",
        )

    # Grain columns: the shared group-by, using the first fact's original names.
    grain_cols = list(mquery_tables[facts[0]].group_by_columns or [])
    union_sql, agg_cols = _build_union_source(facts, grain_cols, mquery_tables)
    if not union_sql:
        return None, _limitation(
            facts,
            bucket.measures,
            "could not build a safe combined source (missing source table or column name collision)",
        )

    # Gather the cross-fact measures' mapping entries (their DAX) so the
    # translator composes the ratio from the combined-source base measures.
    wanted = {_norm(str(m)) for m in bucket.measures if m}
    measure_dicts = [
        e for e in pipeline.mapping if _norm(str(e.get("measure_name") or "")) in wanted
    ]
    if not measure_dicts:
        return None, None

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
    logger.info(
        "[CrossTable] built combined source %s from %s (%d measure(s))",
        combined_key,
        facts,
        len(measure_dicts),
    )
    return spec, None


def plan_cross_table_sources(
    pipeline: MetricViewPipeline,
) -> tuple[dict[str, MetricViewSpec], list[dict[str, object]]]:
    """Build combined-source specs for same-grain cross-fact buckets.

    Returns ``(new_specs, limitations)``. ``new_specs`` is keyed by a synthetic
    combined fact key and already has its measures translated (via the pipeline's
    own ``_process_table``). ``limitations`` lists deferred buckets with a reason.
    Fail-open: any bucket that raises is deferred, never emitted.
    """
    report = pipeline._allocation_report or {}
    cross = _as_entries(report.get("cross_fact"))
    if not cross:
        return {}, []

    new_specs: dict[str, MetricViewSpec] = {}
    limitations: list[dict[str, object]] = []

    for bucket in _group_cross_fact_buckets(cross).values():
        facts = bucket.facts
        combined_key = "combined_" + "_".join(_norm(f).replace(".", "_") for f in facts)
        try:
            spec, limitation = _plan_bucket(pipeline, bucket, combined_key)
        except Exception as e:  # noqa: BLE001 — never break generation
            logger.warning("[CrossTable] bucket %s deferred (error: %s)", facts, e)
            limitations.append(_limitation(facts, bucket.measures, f"error: {e}"))
            continue
        if spec is not None:
            new_specs[combined_key] = spec
        elif limitation is not None:
            limitations.append(limitation)

    return new_specs, limitations
