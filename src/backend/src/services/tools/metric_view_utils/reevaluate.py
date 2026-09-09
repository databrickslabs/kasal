"""Re-evaluate previously-untranslatable measures against today's transpiler.

WHY: capability improvements (new translator patterns, a better LLM-first path, an
evolving skill corpus) only ever helped NEW conversions. A model converted last month
still shows the measures that failed back then, even though several are translatable
now. This module re-tries ONLY the previously-failed measures and reports which ones
are recoverable, so a human can choose to re-transpile them.

Hard rules:
  * Never touch a measure that already succeeded — re-transpiling a validated measure
    risks silently changing it.
  * Propose only. This module returns a report; applying it goes through the normal
    UCMV route, human-triggered.

Cost / noise controls:
  * CATEGORY GATE — some skip reasons are permanent (a display artifact or a
    slicer-driven scalar will never become an aggregatable measure). Retrying those
    forever is wasted compute, so they are skipped unless ``include_impossible``.
  * DETERMINISTIC-FIRST — ``trivial_only=True`` by default: only the regex fast-path
    runs, so a sweep over many datasets burns no LLM tokens. ``use_llm=True`` opts
    into the full (LLM-capable) path.
  * SUPPRESSION — measures a human already dismissed in the review panel
    (``untranslatable_review`` status ``wont_fix`` / ``hand_written``) are skipped, so
    the sweep stops re-proposing settled items.
  * IMPACT ORDER — results are sorted by ``referenced_by`` desc so the measures other
    measures depend on surface first.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

# Categories emitted by yaml_emitter._categorize_untranslatable that represent a
# PERMANENT limitation — not a transpiler gap. No amount of capability improvement
# turns a colour string into a metric, so these are not retried by default.
IMPOSSIBLE_CATEGORIES = frozenset({
    'display artifact',
    'slicer/scalar helper',
    'dynamic KPI selector',
    'disconnected-slicer dispatch (TREATAS)',
    'parameter/label lookup (LOOKUPVALUE)',
    'prior-year time-intelligence',
})

# Categories that are hard but EXPRESSIBLE — each has a documented unlock, so a
# capability improvement can genuinely recover them. These are the retry targets.
RETRYABLE_CATEGORIES = frozenset({
    'top-N row selection (TOPN)',
    'fixed-LOD (ALLEXCEPT)',
    'group-then-aggregate (SUMMARIZE/CALCULATETABLE)',
    'distinct-count pattern',
    'complex DAX — needs manual translation',
})

# Review statuses that mean "a human settled this — stop proposing it".
_SUPPRESSING_STATUSES = frozenset({'wont_fix', 'hand_written'})

# dax_class verdicts the LLM-first translator assigns when IT has already examined a
# measure and declined. Retrying these at the SAME capability level just re-derives
# the same "no" (and costs tokens when use_llm=true), so they are skipped unless the
# caller opts in. They are NOT the same as "nobody looked at it yet".
LLM_DECLINED_CLASSES = frozenset({
    'unsupported',
    'display_layer',
    'out_of_scope',
    'architecture_change',
})


def review_key(item: dict) -> str:
    """Stable key matching the frontend's ``untranslatableKey`` (table::measure)."""
    table = item.get('table_key') or item.get('view_name') or ''
    name = item.get('original_name') or item.get('name') or ''
    return f"{table}::{name}"


def is_suppressed(item: dict, review: Optional[dict]) -> bool:
    """True when a reviewer already marked this measure as settled."""
    if not review:
        return False
    ann = review.get(review_key(item)) or {}
    return (ann.get('status') or '') in _SUPPRESSING_STATUSES


# Substrings that mark a PERMANENT limitation. Real stored rows carry the
# allocation bucket in `category` ('unassigned', 'single_table', …) and the
# human-readable classification in `skip_reason` / `previous_category`, so the gate
# must match on TEXT across both fields rather than only on an exact category name.
_IMPOSSIBLE_MARKERS = (
    'display artifact',
    'slicer/scalar helper',
    'dynamic kpi selector',
    'disconnected-slicer dispatch',
    'parameter/label lookup',
    'selectedvalue',          # slicer-driven scalar: no static metric equivalent
    'isfiltered',             # PBI-specific filter-state introspection
    'blank() placeholder',
    'display-only',
    'covered on primary table',   # already emitted elsewhere — not a gap
)


def _classification_text(item: dict) -> str:
    """All the fields that may carry the human-readable failure classification."""
    return ' '.join(str(item.get(k) or '') for k in
                    ('category', 'skip_reason', 'previous_category',
                     'previous_skip_reason', 'dax_class')).lower()


def is_retryable(
    item: dict,
    *,
    include_impossible: bool = False,
    review: Optional[dict] = None,
) -> tuple[bool, str]:
    """Decide whether a previously-failed measure is worth re-trying.

    Returns ``(retryable, reason_when_not)`` so the caller can report WHY something
    was skipped instead of silently dropping it.
    """
    if not (item.get('dax_expression') or '').strip():
        return False, 'no DAX stored to re-translate'
    if is_suppressed(item, review):
        return False, 'dismissed by reviewer'
    if not include_impossible:
        # The LLM's own verdict is the strongest signal: if the LLM-first translator
        # already examined this measure and classified it as untranslatable, retrying
        # at the SAME capability level re-derives the same "no". Checked FIRST because
        # it is more reliable than pattern-matching on free-text reasons.
        dax_class = (item.get('dax_class') or '').strip()
        if dax_class in LLM_DECLINED_CLASSES:
            return False, f'LLM already declined at this capability level ({dax_class})'

        text = _classification_text(item)
        # Exact-category match (kept for the curated buckets) …
        category = (item.get('category') or '').strip()
        if category in IMPOSSIBLE_CATEGORIES:
            return False, f'permanent limitation ({category})'
        # … plus substring markers, which is what real stored rows actually carry.
        for marker in _IMPOSSIBLE_MARKERS:
            if marker in text:
                return False, f'permanent limitation ({marker})'
    return True, ''


def _measure_payload(item: dict) -> dict:
    """Shape a stored untranslatable row into the dict DaxTranslator.translate wants."""
    name = item.get('original_name') or item.get('name') or ''
    return {
        'measure_name': name,
        'original_name': name,
        'dax_expression': item.get('dax_expression') or '',
        'table_refs': item.get('table_refs') or [],
    }


def reevaluate_measures(
    untranslatable_items: Iterable[dict],
    config: Optional[dict] = None,
    *,
    review: Optional[dict] = None,
    include_impossible: bool = False,
    use_llm: bool = False,
    limit: Optional[int] = None,
) -> dict:
    """Re-run the transpiler over previously-failed measures only.

    Args:
        untranslatable_items: stored ``untranslatable_items`` rows from a prior run.
        config: the stored ``proposed_config`` (filter_sets, column_overrides,
            fact_join_map, …) so the retry sees the same context as the original run.
        review: persisted ``untranslatable_review`` map — dismissed measures are skipped.
        include_impossible: also retry the permanent-limitation categories.
        use_llm: allow the LLM-first path (costs tokens). Default False = regex
            fast-path only, so a scheduled sweep is free.
        limit: cap how many measures are retried (cost guard).

    Returns:
        ``{newly_translatable, still_failing, skipped, counts}``. Each recovered row
        carries ``new_sql`` plus the ``previous_skip_reason`` for context.
    """
    items = [i for i in (untranslatable_items or []) if isinstance(i, dict)]
    newly_translatable: list[dict] = []
    still_failing: list[dict] = []
    skipped: list[dict] = []

    candidates: list[dict] = []
    for item in items:
        ok, why = is_retryable(item, include_impossible=include_impossible, review=review)
        if ok:
            candidates.append(item)
        else:
            skipped.append({
                'original_name': item.get('original_name') or item.get('name'),
                'table_key': item.get('table_key'),
                'category': item.get('category'),
                'skipped_because': why,
            })

    # Highest-impact first, and apply the cost cap AFTER ordering so a limited run
    # still retries the measures that matter most.
    candidates.sort(key=lambda i: i.get('referenced_by') or 0, reverse=True)
    if limit is not None and limit >= 0:
        candidates = candidates[:limit]

    if candidates:
        try:
            from .dax_translator import DaxTranslator
        except Exception as exc:  # fail-open: report nothing rather than crash a sweep
            logger.warning("[reevaluate] translator unavailable: %s", exc)
            return {
                'newly_translatable': [], 'still_failing': [], 'skipped': skipped,
                'counts': {
                    'considered': len(items), 'retried': 0, 'recovered': 0,
                    'still_failing': 0, 'skipped': len(skipped),
                },
                'error': f'translator unavailable: {exc}',
            }

        translator = DaxTranslator(config or {})
        for item in candidates:
            name = item.get('original_name') or item.get('name') or ''
            table_key = item.get('table_key') or item.get('view_name') or ''
            try:
                result = translator.translate(
                    _measure_payload(item), table_key, trivial_only=not use_llm
                )
            except Exception as exc:  # one bad measure must not kill the sweep
                logger.debug("[reevaluate] %s raised: %s", name, exc)
                still_failing.append({
                    'original_name': name, 'table_key': table_key,
                    'category': item.get('category'),
                    'skip_reason': f'retry raised: {exc}',
                    'referenced_by': item.get('referenced_by') or 0,
                })
                continue

            sql = (getattr(result, 'sql_expr', None) or '').strip()
            if sql and getattr(result, 'is_translatable', False):
                newly_translatable.append({
                    'original_name': name,
                    'table_key': table_key,
                    'view_name': item.get('view_name'),
                    'dax_expression': item.get('dax_expression') or '',
                    'previous_skip_reason': item.get('skip_reason') or '',
                    'previous_category': item.get('category') or '',
                    'new_sql': sql,
                    'confidence': getattr(result, 'confidence', '') or '',
                    'dax_class': getattr(result, 'dax_class', None),
                    'referenced_by': item.get('referenced_by') or 0,
                })
            else:
                still_failing.append({
                    'original_name': name,
                    'table_key': table_key,
                    'category': item.get('category'),
                    'skip_reason': getattr(result, 'skip_reason', '') or item.get('skip_reason') or '',
                    'referenced_by': item.get('referenced_by') or 0,
                })

    newly_translatable.sort(key=lambda r: r.get('referenced_by') or 0, reverse=True)
    still_failing.sort(key=lambda r: r.get('referenced_by') or 0, reverse=True)

    return {
        'newly_translatable': newly_translatable,
        'still_failing': still_failing,
        'skipped': skipped,
        'counts': {
            'considered': len(items),
            'retried': len(candidates),
            'recovered': len(newly_translatable),
            'still_failing': len(still_failing),
            'skipped': len(skipped),
        },
    }
