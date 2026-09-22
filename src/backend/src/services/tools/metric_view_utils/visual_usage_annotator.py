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

from src.services.tools.metric_view_utils.data_classes import MetricViewSpec


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
