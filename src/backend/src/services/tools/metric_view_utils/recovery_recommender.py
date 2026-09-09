"""Recovery recommender (KASAL_FIXES Gaps 4 & 5).

Some DAX measures can't be a single single-source metric-view measure, but the
recovery is mechanical — a source-view precompute plus (for Gap 5) a separate
metric view at a coarser grain. Rather than decline with a generic message, this
module detects the two shapes and returns a **concrete recipe** (with the resolved
table/key when known) that the report + "Not transpiled" proposal surface.

It deliberately does NOT auto-emit the SQL: reproducing these correctly needs
warehouse validation (they hinge on filter literals, grain, and weekly/daily-style
encodings), so the pipeline hands the reviewer an exact recipe instead of a
best-effort-but-unverified view.

Gap 4  filter through a many-side / bidirectional relationship → EXISTS-flag precompute.
Gap 5  multi-stage aggregation (grouped virtual table + outer iterator) → precompute
        view at the coarser grain + a SEPARATE metric view (co-locating biases the aggregate).
"""

from __future__ import annotations

import re

# Grouped-virtual-table builders + an outer iterator over them = multi-stage aggregation.
_GROUPERS = re.compile(
    r"\b(SUMMARIZE|SUMMARIZECOLUMNS|GROUPBY|ADDCOLUMNS|CURRENTGROUP)\b", re.I
)
_ITERATORS = re.compile(
    r"\b(AVERAGEX|SUMX|COUNTX|MINX|MAXX|GEOMEANX|PRODUCTX|MEDIANX)\b", re.I
)
# NOT-EXISTS / zero-match shape: a correlated count compared to zero.
_COUNT_FN = re.compile(r"\b(COUNTROWS|COUNTX|DISTINCTCOUNT|COUNT)\b", re.I)
_CMP_ZERO = re.compile(r"=\s*0(?!\d)")


def _is_zero_match(dax: str) -> bool:
    return bool(_COUNT_FN.search(dax) and _CMP_ZERO.search(dax))


def _dax_filter_tables(dax: str) -> set[str]:
    """Table names referenced by a DAX filter: 'Table Name'[col] and Bare[col]."""
    quoted = set(re.findall(r"'([^']+)'\s*\[", dax or ""))
    bare = set(re.findall(r"(?:^|[^'\w\]])([A-Z][A-Za-z0-9 ]*?)\s*\[", dax or ""))
    return {t.strip() for t in (quoted | bare) if t.strip()}


def recommend(
    dax: str,
    *,
    fact_table: str | None = None,
    m2n_tables: set[str] | None = None,
    join_tables: set[str] | None = None,
) -> str | None:
    """Return a concrete recovery recipe for a declined measure, or None.

    Args:
        dax: the original DAX expression.
        fact_table: the measure's home fact (for the recipe text).
        m2n_tables: tables related to the fact by a skipped many:many/bidirectional
            relationship (from RelationshipsLoader.get_skipped_m2n()).
        join_tables: tables already available as declared joins (to avoid recommending
            a precompute when a plain join would do).
    """
    dax = dax or ""
    du = dax.upper()

    # Gap 5 — multi-stage aggregation: grouped virtual table + an outer iterator.
    if _GROUPERS.search(dax) and _ITERATORS.search(dax):
        return (
            "Multi-stage aggregation (grouped virtual table + outer iterator, e.g. "
            "SUMMARIZE/GROUPBY then AVERAGEX). A single metric-view measure can't nest "
            "aggregations. Precompute the inner GROUP BY at its grain in a source view, "
            "then AVG/SUM it in a SEPARATE metric view at that grain (co-locating it on the "
            "fact grain gives a row-weighted-average bug)."
        )

    # Gap 4 — filter through a many-side / bidirectional relationship.
    m2n = {t.lower() for t in (m2n_tables or set())}
    joins = {t.lower() for t in (join_tables or set())}
    filt = {t for t in _dax_filter_tables(dax) if t.lower() not in joins}
    hit = sorted(t for t in filt if t.lower() in m2n)
    if hit:
        tgt = hit[0]
        # zero-match variant → NOT EXISTS
        neg = " (zero-match: use NOT EXISTS)" if _is_zero_match(dax) else ""
        return (
            f"Filter references '{tgt}', related to {fact_table or 'the fact'} via a "
            f"many-side/bidirectional relationship (no safe 1:1 join — a plain join would "
            f"fan out). Precompute an EXISTS flag on the source view "
            f"(EXISTS/NOT EXISTS against '{tgt}' on the shared key){neg}, then "
            f"FILTER the measure on that flag. The inline `source:` SELECT accepts this "
            f"with no extra object."
        )

    # generic NOT-EXISTS / zero-match even without a known m2n table
    if _is_zero_match(dax) and (
        "DISTINCTCOUNT" in du or "VALUES" in du or "FILTER" in du
    ):
        return (
            "Zero-match / NOT-EXISTS pattern (counts entities with no matching rows). "
            "Precompute a per-entity has_<x> flag in the source view (window or self-join), "
            "then COUNT(DISTINCT id) FILTER (WHERE has_<x> = 0)."
        )

    return None


_FACT_REF = re.compile(r"'([^']*[Ff]act[^']*)'\s*\[|\b(\w*[Ff]act\w*)\b\s*\[")


def _facts_in(dax: str) -> set[str]:
    out = set()
    for a, b in _FACT_REF.findall(dax or ""):
        t = (a or b).strip()
        if t:
            out.add(t)
    return out


def draft_source_view(
    dax: str, *, measure_name: str = "measure", fact_table: str | None = None
) -> str | None:
    """Best-effort, UNVERIFIED `CREATE VIEW` scaffold for the cases that need a
    source-view reshape (cross-fact UNION, multi-stage precompute), or None.

    This is a *proposal artifact* — a labeled starting point a human completes and
    verifies against PBI. It is NEVER an emitted/active measure, so a wrong draft
    can't ship bad data; worst case the draft needs editing. Deterministic (no LLM):
    it scaffolds the structure + embeds the original DAX to translate by hand.
    """
    dax = dax or ""
    label = (
        "-- DRAFT · UNVERIFIED · verify against PBI before use\n"
        "-- Auto-scaffolded from the declined DAX — complete the <…> parts, then\n"
        "-- point a UC metric view's `source:` at this view.\n"
    )
    dax_c = "\n".join("--   " + ln for ln in dax.strip().splitlines()[:40])
    base = fact_table or "fact"

    facts = _facts_in(dax)
    if len(facts) > 1:  # cross-fact → UNION source view
        arms = "\n  UNION ALL\n".join(
            f"  SELECT /* aligned shared keys */ *, '{f}' AS _src FROM {f}"
            for f in sorted(facts)
        )
        return (
            f"{label}-- Cross-fact: spans {', '.join(sorted(facts))}. UC metric views are single-source,\n"
            f"-- so UNION the facts here (align columns, tag _src), then express the measure as\n"
            f"-- filtered SUMs over the tagged rows in a UCMV on this view.\n"
            f"-- Original DAX:\n{dax_c}\n"
            f"CREATE OR REPLACE VIEW <catalog>.<schema>.{base}__unioned AS\n{arms}\n;"
        )

    if _GROUPERS.search(dax) and _ITERATORS.search(
        dax
    ):  # multi-stage → precompute view
        return (
            f"{label}-- Multi-stage: precompute the inner GROUP BY at its grain here, then build a\n"
            f"-- SEPARATE UCMV on this view that AVG/SUMs across the outer level\n"
            f"-- (co-locating on the fact grain gives a row-weighted-average bug).\n"
            f"-- Original DAX:\n{dax_c}\n"
            f"CREATE OR REPLACE VIEW <catalog>.<schema>.{base}__by_<grain> AS\n"
            f"  SELECT <grain_cols>,\n"
            f"         /* inner aggregate per <grain>, e.g. */ SUM(<value>) AS {measure_name}_inner\n"
            f"  FROM {base}\n"
            f"  /* JOIN <dims> …  WHERE <filters> */\n"
            f"  GROUP BY <grain_cols>\n;"
        )

    return None
