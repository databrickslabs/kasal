"""Stamp ``TranslationResult.used_in_visuals`` from a supplied visual-usage index.

The index itself (``{original_measure_name: [{page, visual_type, role}, ...]}``)
comes from Pipeline Config Generator's ``visual_usage_index`` output
(``services.powerbi.visual_usage.derive_visual_usage_index``, run against the
report's PBIR definition) and arrives here via the flow's JSON-mode handoff,
the same way ``measures_json``/``mquery_json`` do. This module's only job is
matching it onto the ALREADY-BUILT ``MetricViewSpec``s by each measure's
``original_name`` — no PBI API calls, no DAX parsing.
"""

from __future__ import annotations

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)


def annotate_visual_usage(
    specs: dict[str, MetricViewSpec], visual_usage_index: dict[str, list[dict]]
) -> int:
    """Stamp ``used_in_visuals`` on every measure (translated + untranslatable)
    across every spec, matched by ``TranslationResult.original_name`` against
    the index's keys (the PBI field's ORIGINAL, non-snake-cased name).

    Mutates ``specs`` in place — call this BEFORE ``emit_all_yaml``/
    ``emit_all_sql``/``get_results`` so the tag reaches every downstream
    artifact (YAML comment, migration report, JSON output), not just the
    ones built after annotation.

    Returns the number of measures that got at least one usage entry — 0 is
    a normal, common result (an empty index, or a report where nothing in
    scope is actually drawn/filtered anywhere), not an error.
    """
    if not visual_usage_index:
        return 0
    annotated = 0
    for spec in specs.values():
        for bucket in (spec.measures, spec.untranslatable):
            for m in bucket:
                usage = visual_usage_index.get(m.original_name)
                if usage:
                    m.used_in_visuals = usage
                    annotated += 1
    return annotated


def prune_non_visual_measures(specs: dict[str, MetricViewSpec]) -> int:
    """TRIAGE: drop measures with NO direct and NO indirect visual usage from
    every spec. A report's legacy/unused measures — not drawn or filtered in any
    visual, and not reached transitively by anything that is — are noise the user
    never sees, so emitting (and documenting) them bloats the output.

    Base measures (``category == "base"`` — the implicit column aggregates, which
    are themselves derived FROM what visuals display) are ALWAYS kept.

    MUST run AFTER ``annotate_visual_usage`` + ``annotate_indirect_visual_usage``,
    and ONLY when a visual-usage index was actually supplied (the caller gates on
    the direct-annotation count being > 0) — otherwise every measure looks unused
    and the whole model would be dropped.

    Safe by construction: ``annotate_indirect_visual_usage`` stamps the full
    dependency closure of every visual-placed measure, so any measure a KEPT one
    references has itself been marked (indirect) and survives — nothing that
    remains can reference a pruned measure. Returns the number removed.
    """
    removed = 0
    for spec in specs.values():
        for attr in ("measures", "untranslatable"):
            bucket = getattr(spec, attr, None)
            if not bucket:
                continue
            kept = [
                m
                for m in bucket
                if getattr(m, "category", None) == "base"
                or getattr(m, "used_in_visuals", None)
                or getattr(m, "indirect_visual_usage", None)
            ]
            removed += len(bucket) - len(kept)
            setattr(spec, attr, kept)
    return removed


def annotate_indirect_visual_usage(specs: dict[str, MetricViewSpec]) -> int:
    """Propagate visual usage DOWN the measure-dependency graph (backtracing).

    A measure that is not drawn or filtered in any visual can still matter: a
    slicer/visual KPI may reference it (transitively) in its DAX. This stamps
    ``indirect_visual_usage`` on every such sub-measure — ``{page, visual_type,
    role, via}`` where ``via`` is the ORIGINAL name of the visual-placed
    measure whose dependency chain reaches it — so a sub-KPI feeding a
    visual-shown KPI is surfaced too, distinctly from direct usage.

    Must run AFTER ``annotate_visual_usage`` (it reads the direct
    ``used_in_visuals``) and BEFORE emission. Cycle-safe. Keyed on each
    measure's ``original_name`` (the PBI name DAX ``[refs]`` use), matching
    ``dependency_graph``'s convention. Returns the number of measures that
    gained at least one indirect entry (0 is normal — no visual-placed measure
    references anything, or nothing was drawn at all).
    """
    from src.services.tools.metric_view_utils.dependency_graph import (
        _find_measure_refs,
    )

    # Index every measure (translated + untranslatable) by its PBI name, and
    # collect the direct usage each one carries.
    by_name: dict[str, TranslationResult] = {}
    for spec in specs.values():
        for bucket in (spec.measures, spec.untranslatable):
            for m in bucket:
                if getattr(m, "original_name", None):
                    by_name[m.original_name] = m
    if not by_name:
        return 0
    all_names = set(by_name)

    # adjacency: measure → the measures its DAX references (its sub-measures).
    adjacency: dict[str, set[str]] = {}
    for name, m in by_name.items():
        dax = getattr(m, "dax_expression", "") or ""
        adjacency[name] = _find_measure_refs(dax, all_names - {name})

    # For each measure with DIRECT visual usage, walk its dependency closure and
    # attribute that usage to each reachable sub-measure (via the direct one).
    annotated: set[str] = set()
    for root_name, root in by_name.items():
        direct = getattr(root, "used_in_visuals", None)
        if not direct:
            continue
        # Iterative DFS over dependencies; `via` stays the visual-placed root.
        seen: set[str] = {root_name}
        stack = list(adjacency.get(root_name, ()))
        while stack:
            dep = stack.pop()
            if dep in seen:
                continue
            seen.add(dep)
            target = by_name.get(dep)
            if target is not None:
                existing = target.indirect_visual_usage
                have = {(e.get("page"), e.get("via")) for e in existing}
                for occ in direct:
                    key = (occ.get("page"), root_name)
                    if key in have:
                        continue
                    have.add(key)
                    existing.append(
                        {
                            "page": occ.get("page"),
                            "visual_type": occ.get("visual_type"),
                            "role": occ.get("role"),
                            "via": root_name,
                        }
                    )
                annotated.add(dep)
            stack.extend(adjacency.get(dep, ()))
    return len(annotated)
