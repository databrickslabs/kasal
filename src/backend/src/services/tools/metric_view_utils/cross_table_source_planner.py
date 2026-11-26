"""Cross-fact measures: a combined source at the shared grain (reconciliation-gated).

A measure whose DAX reads columns of two fact tables cannot be expressed on a
single-source metric view. For each pair of facts that such measures need, this
module builds ONE combined source: a ``UNION ALL`` of the two facts' source-layer
views, each projected to the shared grain (the group-by columns both facts have),
with every fact column the measures read exposed once. The measures are then
translated on that source through the pipeline's own translator, and the view is
a normal entry in the generator output (yaml, resolved measures, source-layer DDL).

Why the union is exact for the measures it accepts:
  * It ADDS rows, it never joins, so no fact row is multiplied.
  * A fact column is populated only by the arm of its own fact; the other arm
    carries ``NULL``. Every SQL aggregate ignores NULL (SUM, AVG, MIN, MAX,
    COUNT(col), COUNT(DISTINCT col)), so over the union each aggregate of a
    fact column equals the same aggregate over that fact alone. A ratio of such
    aggregates is then exact too.
  * Each arm reads the fact's SOURCE VIEW (read mode), so the Power Query filters
    and transformations of that fact are already applied.

Measures are NOT accepted when the union would change their row or filter
context: row counts (COUNTROWS, COUNT(*)), row iterators over a table (SUMX ...),
filter context (CALCULATE, FILTER, ALL, time intelligence), related lookups, and
measures that read dimension columns (their joins are not on the union).

Gates, all required:
  * the reconciliation loop is active (PBI validates the numbers cell by cell);
  * read mode is on (each fact's source view carries its filters/transforms);
  * exactly two facts cover the measure's fact columns;
  * the shared grain is non-empty and holds a period column and a second column,
    so the view can be compared with Power BI (reconciliation/mapping_assembly);
  * a column on both facts is a shared-grain column, never a measure column.

Documented assumption: wildcard tables (no parsed columns) are left out of the
column index, as in the allocator's own decisions, but the allocator's global
"no cross verdict while a wildcard table exists" guard is NOT applied here. The
combined view is validated by reconciliation like every other view.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .data_classes import MetricViewSpec, TableInfo
from .source_layer_binding import read_from_source_layer
from .measure_allocator import (
    _norm,
    allocate,
    build_fact_column_index,
    build_fact_grain_index,
    build_known_columns,
    build_measure_column_index,
    candidate_fact_keys,
    resolve_effective_columns,
)

if TYPE_CHECKING:
    from .pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)

COMBINED_PREFIX = "combined_"
# Period columns whose values are period keys (mirrors reconciliation/mapping_assembly).
_PERIOD_NAMES = frozenset(
    {"fiscper", "calmonth", "fiscal_period", "period", "yearmonth", "month_id"}
)
# Functions that change the row or filter context a measure is evaluated in.
_CONTEXT_FN = re.compile(
    r"\b(CALCULATE|CALCULATETABLE|FILTER|ALL|ALLEXCEPT|ALLSELECTED|REMOVEFILTERS"
    r"|KEEPFILTERS|USERELATIONSHIP|RELATED|RELATEDTABLE|LOOKUPVALUE|EARLIER"
    r"|EARLIEST|COUNTROWS|SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|PREVIOUSMONTH"
    r"|PREVIOUSYEAR|PREVIOUSQUARTER|DATESYTD|DATESMTD|DATESQTD|TOTALYTD|TOTALMTD"
    r"|TOTALQTD|SELECTEDVALUE|ISFILTERED|HASONEVALUE|VALUES|CALENDAR)\s*\(",
    re.IGNORECASE,
)
# Row iterators (SUMX, AVERAGEX, ...) and COUNT(*) iterate the union's rows.
_ROW_ITERATOR = re.compile(
    r"\b(SUMX|AVERAGEX|COUNTX|MINX|MAXX|PRODUCTX|RANKX|GEOMEANX|COUNT\s*\(\s*\*)",
    re.IGNORECASE,
)


@dataclass
class CombinedSource:
    """One combined source: two fact source views unioned at the shared grain."""

    facts: tuple[str, str]
    grain: list[str]  # shared grain, output names
    grain_by_fact: dict[str, list[str]]  # fact -> its own column name per grain column
    out_names: list[str]  # measure columns of the union, in order
    columns_by_fact: dict[str, dict[str, str]]  # fact -> {out name: own column}


def union_sql(cs: CombinedSource, view_of: Callable[[str], str]) -> str:
    """UNION ALL of the facts' source views, NULL-aligned on the measure columns."""
    arms: list[str] = []
    for fact in cs.facts:
        select = [
            f"{own} AS {out}" for own, out in zip(cs.grain_by_fact[fact], cs.grain)
        ]
        owned = cs.columns_by_fact[fact]
        for out in cs.out_names:
            select.append(
                f"{owned[out]} AS {out}" if out in owned else f"NULL AS {out}"
            )
        arms.append("SELECT\n  " + ",\n  ".join(select) + f"\nFROM {view_of(fact)}")
    return "\nUNION ALL\n".join(arms)


@dataclass
class _Index:
    mquery: Mapping[str, TableInfo]
    known_facts: dict[str, set[str]]  # facts whose columns are parsed
    grain: dict[str, list[str]]  # fact -> group-by names (original case)
    known_cols: set[str]
    cols_by_measure: dict[str, set[str]]
    refs_by_measure: dict[str, set[str]]


@dataclass
class _Candidate:
    name: str
    entry: Mapping[str, object]
    pair: tuple[str, str]
    owned: dict[str, list[str]]  # fact -> normalized measure columns it owns
    shared: list[str]  # shared grain, normalized, in the pair's first-fact order


@dataclass
class _Bucket:
    pair: tuple[str, str]
    shared: list[str]
    candidates: list[_Candidate] = field(default_factory=list)


def _build_index(pipeline: MetricViewPipeline) -> _Index:
    mquery = pipeline.mquery_tables
    mapping = pipeline.mapping
    fact_keys = candidate_fact_keys(mquery, mapping)
    source_columns = pipeline.source_columns
    full = build_fact_column_index(mquery, fact_keys, source_columns)
    cols_by_measure, refs_by_measure = build_measure_column_index(mapping)
    return _Index(
        mquery=mquery,
        known_facts={k: v for k, v in full.items() if v},
        grain=build_fact_grain_index(mquery, fact_keys),
        known_cols=build_known_columns(mquery, source_columns),
        cols_by_measure=cols_by_measure,
        refs_by_measure=refs_by_measure,
    )


def _shared_grain(index: _Index, a: str, b: str) -> list[str]:
    """Normalized shared grain of two facts, in the first fact's group-by order."""
    gb = {_norm(c) for c in index.grain.get(b, [])}
    out: list[str] = []
    for c in index.grain.get(a, []):
        k = _norm(c)
        if k in gb and k not in out:
            out.append(k)
    return out


def _grain_is_comparable(shared: list[str]) -> bool:
    """A period column plus at least one other column, so reconciliation can compare."""
    return len(shared) >= 2 and any(k in _PERIOD_NAMES for k in shared)


def _unsafe_reason(dax: str) -> str | None:
    """Why the union would change the measure's meaning, or None."""
    fn = _CONTEXT_FN.search(dax)
    if fn:
        return (
            f"uses {fn.group(1).upper()} — the union changes its row or filter "
            "context, so it is not additive over the combined source"
        )
    it = _ROW_ITERATOR.search(dax)
    if it:
        name = re.sub(r"\s", "", it.group(1)).upper()
        return (
            f"uses {name} — a row iterator or "
            "row count over the union would include the other fact's rows"
        )
    return None


def _judge(
    index: _Index, name: str, entry: Mapping[str, object]
) -> tuple[_Candidate | None, dict[str, object] | None]:
    """A candidate for one cross-fact measure, or a deferral ``{facts, reason}``."""
    dax = str(entry.get("dax_expression") or entry.get("expression") or "")
    eff = resolve_effective_columns(dax, index.cols_by_measure, index.refs_by_measure)
    if not eff:
        return None, None
    decision = allocate(eff, index.known_facts, index.grain, index.known_cols)
    if decision.kind != "cross_fact":
        return None, None
    facts = list(decision.facts)

    def defer(reason: str) -> tuple[None, dict[str, object]]:
        return None, {"facts": facts, "measures": [name], "reason": reason}

    if eff - index.known_cols:
        return defer("references a column that no parsed table exposes")
    dim_only = {c for c in eff if c in index.known_cols} - decision.fact_columns
    if dim_only:
        return defer("references dimension columns — their joins are not on the union")
    fact_cols = decision.fact_columns
    covers = []
    for a in facts:
        for b in facts:
            if a < b and (index.known_facts[a] | index.known_facts[b]) >= fact_cols:
                covers.append((a, b))
    if not covers:
        return defer(
            f"columns span {len(facts)} facts — no two-fact cover (needs a 3+ fact union)"
        )
    if len(covers) > 1:
        return defer(
            f"{len(covers)} two-fact covers fit the columns — which pair to union is ambiguous"
        )
    a, b = covers[0]
    shared = _shared_grain(index, a, b)
    if not shared:
        return defer("the two facts share no grain column")
    if not _grain_is_comparable(shared):
        return defer(
            "shared grain has no period column and a second column — the view cannot be compared with Power BI"
        )
    owned: dict[str, list[str]] = {a: [], b: []}
    for col in sorted(fact_cols):
        if col in shared:
            continue
        in_a = col in index.known_facts[a]
        in_b = col in index.known_facts[b]
        if in_a and in_b:
            return defer(
                "a measure column exists on both facts but is not a shared grain column"
            )
        if in_a:
            owned[a].append(col)
        elif in_b:
            owned[b].append(col)
    reason = _unsafe_reason(dax)
    if reason:
        return defer(reason)
    return _Candidate(name, entry, (a, b), owned, shared), None


def _own_name(index: _Index, fact: str, col: str) -> str:
    """The fact's own spelling of a normalized column (group-by, then aggregate names)."""
    info = index.mquery[fact]
    for c in info.group_by_columns or []:
        if _norm(c) == col:
            return c
    for agg in info.aggregate_columns or []:
        for field_name in ("source_col", "name"):
            value = str((agg or {}).get(field_name) or "")
            if value and _norm(value) == col:
                return value
    for calc in info.calculated_columns or []:
        if isinstance(calc, dict) and _norm(str(calc.get("name") or "")) == col:
            return str(calc["name"])
    return col


def _combined_source(index: _Index, bucket: _Bucket) -> CombinedSource:
    a, b = bucket.pair
    grain = [_own_name(index, a, k) for k in bucket.shared]
    grain_b = [_own_name(index, b, k) for k in bucket.shared]
    out_names: list[str] = []
    columns: dict[str, dict[str, str]] = {a: {}, b: {}}
    for cand in bucket.candidates:
        for fact, cols in cand.owned.items():
            for col in cols:
                out = _own_name(index, fact, col)
                if out in columns[fact]:
                    continue
                # Output name = the fact's own spelling; the source column is the same.
                columns[fact][out] = out
                if out not in out_names:
                    out_names.append(out)
    return CombinedSource(
        facts=(a, b),
        grain=grain,
        grain_by_fact={a: grain, b: grain_b},
        out_names=out_names,
        columns_by_fact=columns,
    )


def _plan_bucket(
    pipeline: MetricViewPipeline, index: _Index, bucket: _Bucket, combined_key: str
) -> MetricViewSpec:
    cs = _combined_source(index, bucket)
    info = TableInfo(
        table_name=combined_key,
        source_table=combined_key,
        aggregate_columns=[{"name": n, "source_col": n} for n in cs.out_names],
        group_by_columns=list(cs.grain),
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )
    names = {_norm(c.name) for c in bucket.candidates}
    dicts = [
        e
        for e in pipeline.mapping
        if _norm(str(e.get("measure_name") or e.get("original_name") or "")) in names
    ]
    spec = pipeline._process_table(combined_key, info, dicts)
    # The shared grain is the view's dimensions: a period and a second dimension, which
    # reconciliation (mapping_assembly) needs to compare the view with Power BI.
    taken = {str(d.get("name") or "").lower() for d in spec.dimensions}
    for col in cs.grain:
        if col.lower() not in taken:
            spec.dimensions.append(
                {
                    "name": col,
                    "expr": f"source.{col}",
                    "comment": f"{col} (shared grain of {', '.join(cs.facts)})",
                }
            )
            taken.add(col.lower())
    # The union's select list is the complete column set of the combined source.
    # Both spellings: dimensions are written in lower case, the union keeps the model's
    # case (Spark identifiers are case-insensitive, so both name the same column).
    select_names = list(cs.grain) + list(cs.out_names)
    spec.fact_columns = sorted(set(select_names) | {n.lower() for n in select_names})
    spec.source_table = combined_key
    spec.view_name = combined_key
    spec.source_sql = ""
    spec.comment = (
        f"Combined source at the shared grain of {', '.join(cs.facts)} (UNION ALL of "
        "their source views, each fact's filters applied). Validated by PBI reconciliation."
    )
    pipeline.combined_sources[combined_key] = cs
    logger.info(
        "[CrossTable] combined source %s from %s (%d measure(s))",
        combined_key,
        list(cs.facts),
        len(dicts),
    )
    return spec


def plan_cross_table_sources(
    pipeline: MetricViewPipeline,
) -> tuple[dict[str, MetricViewSpec], list[dict[str, object]]]:
    """Build combined-source specs for cross-fact measures.

    Returns ``(new_specs, limitations)``. Each combined spec is keyed by its synthetic
    combined key; its source is recorded in ``pipeline.combined_sources`` and read by
    the source-layer builder. Fail-open: a bucket that raises is deferred, not emitted.
    """
    pipeline.combined_sources = getattr(pipeline, "combined_sources", {})
    if not read_from_source_layer(pipeline.config):
        # Without the source views the union would miss each fact's M filters.
        return {}, []
    index = _build_index(pipeline)
    buckets: dict[tuple[str, str], _Bucket] = {}
    limitations: list[dict[str, object]] = []
    for entry in pipeline.mapping:
        name = str(entry.get("measure_name") or entry.get("original_name") or "")
        if not name:
            continue
        cand, deferral = _judge(index, name, entry)
        if deferral is not None:
            limitations.append(deferral)
        if cand is None:
            continue
        bucket = buckets.setdefault(
            cand.pair, _Bucket(pair=cand.pair, shared=cand.shared)
        )
        bucket.candidates.append(cand)

    new_specs: dict[str, MetricViewSpec] = {}
    for pair, bucket in buckets.items():
        combined_key = COMBINED_PREFIX + "_".join(
            _norm(f).replace(".", "_") for f in pair
        )
        try:
            spec = _plan_bucket(pipeline, index, bucket, combined_key)
        except Exception as e:  # noqa: BLE001 — never break generation
            logger.warning("[CrossTable] pair %s deferred (error: %s)", pair, e)
            limitations.append(
                {
                    "facts": list(pair),
                    "measures": [c.name for c in bucket.candidates],
                    "reason": f"error: {e}",
                }
            )
            continue
        new_specs[combined_key] = spec
    return new_specs, limitations
