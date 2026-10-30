"""Human-readable annotations for emitted metric-view measure comments.

Builds the ``· referenced by …``, ``· Used on …``, ``· Indirectly used via …``
and ``· LLM[...]`` suffixes, plus the bucket label for untranslatable measures.
Pure functions over a ``TranslationResult``; nothing here touches the YAML text.
"""

from __future__ import annotations

import re
from typing import TypedDict

from .data_classes import TranslationResult


class _PageUsage(TypedDict):
    """Per-page tally used by ``_visual_usage_suffix``."""

    types: list[str]
    drawn: bool
    count: int


def _categorize_untranslatable(measure: TranslationResult) -> tuple[str, str]:
    """Classify an untranslatable measure into a human-readable CATEGORY + why.

    Turns the raw internal skip_reason / DAX shape into one of a few clear buckets
    so the emitted comment explains to a reviewer WHY a measure was not emitted:
      - display artifact (color/label/format — not data)
      - slicer/scalar helper (SELECTEDVALUE date pickers etc. — not aggregatable)
      - dynamic KPI selector (SWITCH mega-wrapper — picks a KPI by slicer)
      - prior-year time-intelligence (not expressible in a static metric view)
      - complex DAX needing manual translation (the residual)
    Returns (category, explanation).
    """
    name = (getattr(measure, "original_name", "") or "").lower()
    dax = getattr(measure, "dax_expression", "") or ""
    reason = getattr(measure, "skip_reason", "") or ""
    du = dax.upper()

    if (
        name.endswith("_color")
        or "color" in name
        or "format" in reason.lower()
        or "display" in reason.lower()
    ):
        return ("display artifact", "formatting/label only — not a data measure")
    if (
        re.search(r"SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|PREVIOUSYEAR", du)
        or "prior-year" in reason.lower()
    ):
        return (
            "prior-year time-intelligence",
            "period-shift not expressible in a static UC metric view "
            "(supply a calendar date_py column or compute in the source view)",
        )
    if "SWITCH" in du and "SELECTEDVALUE" in du:
        return (
            "dynamic KPI selector",
            "SWITCH(SELECTEDVALUE(...)) picks a KPI by slicer — no static "
            "metric-view equivalent; expand per-branch or handle in the report",
        )
    # Construct-specific guidance — state the ACTUAL unlock (or honest skip) so a
    # reviewer knows the next step, not just "needs manual translation". These run
    # BEFORE the generic SELECTEDVALUE/scalar catch below because these constructs
    # frequently co-occur with a SELECTEDVALUE arg (a slicer feeding the pattern),
    # and the specific construct is the actionable signal, not the SELECTEDVALUE.
    if "TREATAS" in du:
        return (
            "disconnected-slicer dispatch (TREATAS)",
            "slicer picks which KPI to show — display-layer, not a metric; "
            "define each underlying KPI as its own measure, no source-view unlock",
        )
    if "LOOKUPVALUE" in du:
        return (
            "parameter/label lookup (LOOKUPVALUE)",
            "builds a display string or reads a slicer parameter table — not a "
            "metric; a real attribute lookup is a join (RELATED), not this",
        )
    if "TOPN" in du:
        return (
            "top-N row selection (TOPN)",
            "ranks-and-slices rows — needs a source-view ROW_NUMBER()/QUALIFY "
            "precompute; do not approximate with MAX",
        )
    if "ALLEXCEPT" in du:
        return (
            "fixed-LOD (ALLEXCEPT)",
            "aggregate at the kept-column grain — 1 kept col → window range:all; "
            "2+ kept cols → source-view SUM(...) OVER (PARTITION BY ...)",
        )
    if "SUMMARIZE" in du or "CALCULATETABLE" in du or "ADDCOLUMNS" in du:
        return (
            "group-then-aggregate (SUMMARIZE/CALCULATETABLE)",
            "builds a grouped virtual table — materialize the GROUP BY in the "
            "source SELECT as an identity dimension, then SUM it",
        )
    if "SELECTEDVALUE" in du or re.search(
        r"\bF_(START|END)_DATE\b|CUR_MONTH|CUR_YR", du
    ):
        return (
            "slicer/scalar helper",
            "returns a single slicer-driven scalar (e.g. a date picker) — "
            "not an aggregatable measure",
        )
    if "DISTINCTCOUNT" in du:
        return ("distinct-count pattern", reason or "DISTINCTCOUNT not translated")
    return ("complex DAX — needs manual translation", reason or "no matching pattern")


def _usage_suffix(referenced_by: int) -> str:
    """Human-readable usage annotation for a measure comment.

    Counts measure→measure references only (not dashboard/visual usage), so the
    wording is deliberately "referenced by N measure(s)". Empty string when the
    measure is referenced by nothing (avoids noise on the many leaf measures).
    """
    if not referenced_by or referenced_by < 1:
        return ""
    noun = "measure" if referenced_by == 1 else "measures"
    return f" — referenced by {referenced_by} {noun}"


def _visual_usage_suffix(measure: TranslationResult) -> str:
    """Business-usage annotation for a measure comment: HOW OFTEN, HOW and
    WHERE this measure is actually used in the report (PROP-8).

    Distinct from `_usage_suffix` (measure→measure DAX references) — this is
    dashboard/visual usage, from `TranslationResult.used_in_visuals`
    (`visual_usage_annotator.annotate_visual_usage`), a list of
    `{page, visual_type, role}` occurrences. Empty when the field wasn't found
    in any visual (common — many measures exist only as intermediate building
    blocks for other measures) or when no visual_usage_index was supplied at
    all (report_id omitted, or the run predates this feature) — both cases are
    silent, not flagged as an error.

    Emits a single readable, machine-parseable line (the review UI's "Used on"
    column parses it back out):

        · Used on 3 visuals: OTC Scorecard (card/tableEx·drawn), OTC NPS (slicer·filter) (+1 more)

    - the leading count is HOW OFTEN — the number of visual occurrences;
    - per page: the visual type(s) (WHERE) and whether the field is drawn
      (shown) or only used as a filter (HOW), as `<types>·<role>`;
    - caps at 3 pages named explicitly, then "+N more" (remaining PAGES) so a
      measure used everywhere doesn't produce an unreadable comment line.

    Annotations never contain a comma (types are `/`-joined, role after `·`) so
    the comma stays a clean page separator for the parser. Defensive against
    occurrences missing `visual_type`/`role`.
    """
    usage = getattr(measure, "used_in_visuals", None)
    if not usage:
        return ""
    # Aggregate occurrences per page, preserving first-seen order. Each
    # occurrence is one visual the field appears in, so per-page `count` is the
    # number of visuals on that page (reconciles the total with the page list:
    # "7 visuals" all on one page shows as that page ×7).
    per_page: dict[str, _PageUsage] = {}
    order: list[str] = []
    for occ in usage:
        page = occ.get("page")
        if not page:
            continue
        if page not in per_page:
            per_page[page] = {"types": [], "drawn": False, "count": 0}
            order.append(page)
        per_page[page]["count"] += 1
        vt = occ.get("visual_type")
        if vt and vt not in per_page[page]["types"]:
            per_page[page]["types"].append(vt)
        # "drawn" (or a missing role) means the field is shown; only "filter"
        # occurrences alone keep a page marked filter-only.
        if occ.get("role") != "filter":
            per_page[page]["drawn"] = True
    if not order:
        return ""
    # HOW OFTEN — total visuals across the report (sum of the per-page counts,
    # so it always equals the sum of the "×N" tallies shown below).
    total = sum(info["count"] for info in per_page.values())
    shown = order[:3]
    parts: list[str] = []
    for page in shown:
        info = per_page[page]
        role = "drawn" if info["drawn"] else "filter"
        types = "/".join(info["types"])
        annot = f"{types}·{role}" if types else role
        # Per-page visual count, shown only when >1 (a lone visual needs no ×1).
        tally = f" ×{info['count']}" if info["count"] > 1 else ""
        parts.append(f"{page}{tally} ({annot})")
    suffix = f"Used on {total} visual{'s' if total != 1 else ''}: " + ", ".join(parts)
    remaining = len(order) - len(shown)
    if remaining > 0:
        suffix += f" (+{remaining} more)"
    return f" · {suffix}"


def _indirect_visual_usage_suffix(measure: TranslationResult) -> str:
    """Indirect (backtraced) visual usage: this measure isn't drawn/filtered in
    a visual itself, but a visual-placed measure references it (transitively) in
    its DAX. Kept SEPARATE from ``_visual_usage_suffix`` (direct usage) so the
    two never blur — a sub-KPI feeding a slicer KPI is real usage, but inherited.

    From ``TranslationResult.indirect_visual_usage`` ([{page, visual_type, role,
    via}], stamped by ``annotate_indirect_visual_usage``). Emits e.g.
        · Indirectly used via [OTC Health Score] on: OTC Scorecard, Exec Summary
    Groups by the ``via`` measure (what a reviewer traces back through), lists
    that parent's pages, caps parents at 2 then "+N more". Empty when there is
    no indirect usage (the common case).
    """
    usage = getattr(measure, "indirect_visual_usage", None)
    if not usage:
        return ""
    # Group pages per `via` measure, preserving first-seen order.
    per_via: dict[str, list[str]] = {}
    order: list[str] = []
    for occ in usage:
        via = occ.get("via")
        page = occ.get("page")
        if not via:
            continue
        if via not in per_via:
            per_via[via] = []
            order.append(via)
        if page and page not in per_via[via]:
            per_via[via].append(page)
    if not order:
        return ""
    shown = order[:2]
    parts: list[str] = []
    for via in shown:
        pages = ", ".join(per_via[via]) if per_via[via] else ""
        parts.append(f"[{via}] on: {pages}" if pages else f"[{via}]")
    suffix = "Indirectly used via " + "; ".join(parts)
    remaining = len(order) - len(shown)
    if remaining > 0:
        suffix += f" (+{remaining} more)"
    return f" · {suffix}"


_MAX_EXPLANATION = 140


def _provenance_suffix(measure: TranslationResult) -> str:
    """Provenance annotation for an LLM-translated measure comment.

    Makes best-effort output auditable: a reviewer sees the confidence, the
    7-category dax_class, and the LLM's one-line justification for every measure
    the LLM produced — so high-confidence ones can be trusted and lower-confidence
    ones triaged. Empty for non-LLM (deterministic regex) measures, which are
    exact by construction and need no justification.
    """
    if getattr(measure, "category", None) != "llm_translated":
        return ""
    conf = getattr(measure, "confidence", "") or "medium"
    dax_class = getattr(measure, "dax_class", "") or "llm"
    parts = f"LLM[{conf}/{dax_class}]"
    explanation = (getattr(measure, "explanation", "") or "").strip()
    if explanation:
        if len(explanation) > _MAX_EXPLANATION:
            explanation = explanation[: _MAX_EXPLANATION - 1].rstrip() + "…"
        parts += f": {explanation}"
    return f" · {parts}"
