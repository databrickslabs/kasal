"""Promote a raw column to its own UCMV measure — ONLY when it's actually
drawn/filtered in a visual with no named PBI measure behind it.

Split out as its own module (not appended to ``pipeline_config.py``, which is
over the file-size ceiling) — see that module's docstring for why sibling
modules in this package have no dependency on it.

The gap this closes: a report author can drag a raw numeric column straight
into a visual's Values well — PBI implicitly aggregates it (Sum by default,
but the column's own ``SummarizeBy`` setting can be Average/Count/Min/Max/
DistinctCount) with no DAX measure ever defined for it. Kasal's own
`visual_usage.py` already records these as ``Column`` field refs in
``visual_usage_index`` (same shape as a ``Measure`` ref — nothing to change
there), but nothing downstream acted on the ones with no matching measure —
they were invisible to UCMV generation, even though the report visibly shows
them. Confirmed as a real gap on a DIFFERENT customer report, not something
specific to any one report's shape.

Deliberately narrow, matching what was asked: a column is promoted ONLY when
it is both (a) aggregatable (``summarize_by`` isn't ``"None"``) and (b)
actually present in ``visual_usage_index`` — a column that merely EXISTS on a
fact table is never promoted just because it's numeric. The "why" (which
page/visual, and that this is an implicit column, not a named measure) is
carried on every entry so it reaches the emitted YAML comment.
"""

from __future__ import annotations

import re
from typing import Any

# DAX SummarizeBy -> the SQL aggregate it maps to. "None" (not aggregatable —
# most text/key columns) and anything unrecognized are deliberately absent:
# the caller skips those rather than guessing an aggregation PBI itself does
# not apply.
_SUMMARIZE_BY_TO_SQL_AGG = {
    "Sum": "SUM",
    "Average": "AVG",
    "Count": "COUNT",
    "Min": "MIN",
    "Max": "MAX",
    # DistinctCount has no plain prefix form — built specially below.
}


def _to_snake_case(name: str) -> str:
    """Kept identical to (and independently of) ``pipeline_config.to_snake_case``."""
    s = re.sub(r"%", "_pct", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[\s\-]+", "_", s)
    return s.lower().strip("_")


def derive_implicit_column_measures(
    visual_usage_index: dict[str, list[dict]],
    measures: list[dict],
    column_summarize_by: dict[str, dict[str, str]],
    fact_tables: set[str] | None = None,
) -> dict[str, list[dict]]:
    """``{table: [entry, ...]}`` — same bucket shape as ``switch_decompositions``
    (mergeable into ``table_processor.py``'s per-table measure list), one entry
    per raw column that's aggregatable AND appears in ``visual_usage_index``
    with no PBI measure of the same name.

    Each entry carries ``used_in_visuals`` (the SAME occurrences list
    ``visual_usage_annotator`` would stamp on a measure) so the reason this
    column became a measure travels with it into the YAML comment, the
    migration report, and the PBI<->UCMV mapping — a reviewer sees exactly
    why it's there, not just that it is.
    """
    if not visual_usage_index or not column_summarize_by:
        return {}

    known_measure_names: set[str] = set()
    for m in measures or []:
        for key in ("measure_name", "name", "original_name"):
            v = m.get(key)
            if v:
                known_measure_names.add(v)

    fact_tables = fact_tables or set()
    out: dict[str, list[dict]] = {}

    for field_name, occurrences in visual_usage_index.items():
        if not field_name or field_name in known_measure_names:
            continue  # already a named measure — nothing to promote

        # Find every (table, summarize_by) this field name resolves to as a
        # real column; prefer a fact-table match when the same column name
        # exists on more than one table (dimension tables reuse names).
        candidates = [
            (tbl, cols[field_name])
            for tbl, cols in column_summarize_by.items()
            if field_name in cols
        ]
        if not candidates:
            continue
        fact_candidates = [c for c in candidates if c[0] in fact_tables]
        table, summarize_by = (fact_candidates or candidates)[0]

        if summarize_by == "DistinctCount":
            sql = f"COUNT(DISTINCT source.{_to_snake_case(field_name)})"
        elif summarize_by in _SUMMARIZE_BY_TO_SQL_AGG:
            agg = _SUMMARIZE_BY_TO_SQL_AGG[summarize_by]
            sql = f"{agg}(source.{_to_snake_case(field_name)})"
        else:
            continue  # "None" or an unrecognized setting — not aggregatable

        entry: dict[str, Any] = {
            "name": _to_snake_case(field_name),
            "original_name": field_name,
            "raw_expr": sql,
            "comment": (
                f"Aggregated column as measure ({summarize_by}) — no named PBI "
                f"measure exists; included because this column is drawn/filtered "
                f"directly in a report visual with PBI's own default aggregation"
            ),
            "category": "implicit_visual_column",
            "used_in_visuals": occurrences,
            "pbi_kind": "raw_column",
            "pbi_sources": [{"kind": "raw_column", "table": table, "column": field_name}],
        }
        out.setdefault(table, []).append(entry)

    return out
