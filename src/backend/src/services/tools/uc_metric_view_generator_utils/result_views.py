"""Result shaping for the generator output.

Builds the UI-ready lists the tool returns: the "Not transpiled" review rows,
the raw DAX extract persisted to history, and the per-table worst-case extract
that is always present even when no view generates.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .shared_types import as_dict, as_list, as_str


def build_untranslatable_items(specs: object) -> list[dict[str, object]]:
    """Flatten every spec's untranslatable measures into one UI-ready list.

    Feeds the validation-UI "Not transpiled" review panel: reviewers see the
    non-emitted measures as first-class rows (original DAX + why skipped +
    category + dependency in-degree + proposal + DRAFT source-view SQL) instead
    of digging through the YAML `-- comment` block. Additive. Returns [] when
    nothing was skipped; high-impact (most-referenced) gaps sorted first.
    """
    ranked: list[tuple[int, dict[str, object]]] = []
    for table_key, spec_value in as_dict(specs).items():
        spec = as_dict(spec_value)
        view_name = spec.get("view_name")
        for m_value in as_list(spec.get("untranslatable")):
            m = as_dict(m_value)
            referenced_by = m.get("referenced_by", 0)
            item: dict[str, object] = {
                "table_key": table_key,
                "view_name": view_name,
                "original_name": m.get("original_name") or m.get("name"),
                "dax_expression": m.get("dax_expression", ""),
                "skip_reason": m.get("skip_reason", ""),
                "category": m.get("category", ""),
                "dax_class": m.get("dax_class"),
                "referenced_by": referenced_by,
                # Actionable next-step for the reviewer (HOW to handle it),
                # sourced from the LLM's recipe or a class-based default.
                "proposal": m.get("proposal", ""),
                "explanation": m.get("explanation"),
                # Labeled DRAFT CREATE VIEW scaffold for cross-fact / multi-stage
                # (proposal artifact, never an emitted measure).
                "source_view_sql_draft": m.get("source_view_sql_draft"),
                "used_in_visuals": m.get("used_in_visuals") or [],
                # Backtraced usage: a visual-placed KPI references this
                # non-emitted measure — surfaced in the review panel too.
                "indirect_visual_usage": m.get("indirect_visual_usage") or [],
            }
            count = referenced_by if isinstance(referenced_by, int) else 0
            ranked.append((count, item))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in ranked]


def build_raw_dax_extract(measures: object) -> list[dict[str, object]]:
    """Collect the full, untruncated original DAX expression per measure.

    Mirrors the M-Query extract produced by the MQuery Conversion Pipeline:
    the complete raw source DAX is retained verbatim so it can be persisted
    and retrieved later, rather than only living in the transient result.
    """
    extract: list[dict[str, object]] = []
    for m in as_list(measures):
        if not isinstance(m, dict):
            continue
        extract.append(
            {
                "measure_name": m.get("measure_name") or m.get("original_name") or "",
                "original_name": m.get("original_name") or m.get("measure_name") or "",
                "dax_expression": m.get("dax_expression") or "",
                "proposed_allocation": m.get("proposed_allocation") or "",
            }
        )
    return extract


@dataclass
class _TableSlot:
    """One table's M-Query source and the measures allocated to it."""

    table_name: str
    mquery: str = ""
    measures: list[dict[str, object]] = field(default_factory=list)


def build_fallback_extract(
    measures: object, mquery_entries: object
) -> list[dict[str, object]]:
    """Per-table tabular extract of M-Query source + associated measures/DAX.

    The worst-case safety net: when no UC Metric View can be generated (e.g.
    an all-raw-Power-Query-M model), the customer still gets the raw material
    organised per table — the M-Query source expression and every measure
    (with its DAX) allocated to that table. Rendered as a table in the UI.

    Groups by the table each measure is allocated to (proposed_allocation /
    table_name) and each M-Query entry's table_name. Tables that appear in
    either source are included, so a table with M-Query but no measures (and
    vice-versa) is still listed.
    """
    by_table: dict[str, _TableSlot] = {}

    def _slot(name: str) -> _TableSlot:
        key = name or "__unassigned__"
        if key not in by_table:
            by_table[key] = _TableSlot(table_name=key)
        return by_table[key]

    # M-Query source per table
    for entry in as_list(mquery_entries):
        if not isinstance(entry, dict):
            continue
        tname = as_str(entry.get("table_name"))
        if not tname:
            continue
        slot = _slot(tname)
        # Prefer a non-empty source; keep the first seen otherwise.
        src = as_str(entry.get("transpiled_sql") or entry.get("mquery_expression"))
        if src and not slot.mquery:
            slot.mquery = src

    # Measures + DAX per table
    for m in as_list(measures):
        if not isinstance(m, dict):
            continue
        alloc = (
            as_str(
                m.get("proposed_allocation") or m.get("table_name") or m.get("table")
            )
            or "__unassigned__"
        )
        _slot(alloc).measures.append(
            {
                "measure_name": m.get("measure_name") or m.get("original_name") or "",
                "dax_expression": m.get("dax_expression") or m.get("expression") or "",
            }
        )

    # Stable, readable ordering: tables with measures first, then by name.
    rows = list(by_table.values())
    rows.sort(key=lambda r: (-len(r.measures), r.table_name))
    return [
        {
            "table_name": r.table_name,
            "mquery": r.mquery,
            "measures": r.measures,
            "measure_count": len(r.measures),
            "has_mquery": bool(r.mquery),
        }
        for r in rows
    ]
