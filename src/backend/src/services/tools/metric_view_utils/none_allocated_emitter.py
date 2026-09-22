"""Catch-all emitter — guarantees no reference measure is silently dropped.

Every measure in the pipeline's ``mapping`` that never landed on a fact view's
spec (neither translated into ``spec.measures`` nor already documented in
``spec.untranslatable``) is gathered into a single synthetic
``none_allocated_measures`` view:

- **best-effort translated** where the DAX translator can (so "whatever we can
  translate" IS translated), and
- otherwise emitted as a **documented comment** (original DAX + reason) via the
  same ``emit_yaml`` path every other view uses.

Purely-visual / formatting / slicer-dispatch measures have no metric-view (and
no Genie / analytical) form, so they are still listed — under an explicit
reason — rather than translated. Net effect: nothing disappears without a
trace. The measures span multiple fact tables, so ``source:`` is a placeholder
and the file is a reconciliation worklist, not a deployable view.
"""

from __future__ import annotations

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.utils import to_snake_case
from src.services.tools.metric_view_utils.yaml_emitter import emit_yaml

_VIEW_KEY = "none_allocated_measures"
# Deliberately not a real 3-level name: these measures reference columns across
# several facts, so there is no single source. Flags it as a worklist.
_PLACEHOLDER_SOURCE = "TODO.none_allocated.measures_span_multiple_facts"


def _covered_names(all_specs: dict) -> set:
    """Every measure name already accounted for on some fact view — translated
    OR documented as untranslatable — in both PBI and snake_case form."""
    covered: set = set()
    for spec in all_specs.values():
        for m in list(spec.measures) + list(spec.untranslatable):
            covered.add(m.original_name)
            covered.add(to_snake_case(m.original_name))
    return covered


def build_none_allocated_spec(
    mapping: list[dict],
    all_specs: dict,
    translator,
    artifact_patterns,
) -> MetricViewSpec | None:
    """Build the synthetic catch-all spec, or ``None`` if nothing is orphaned.

    ``artifact_patterns`` is the pipeline's compiled ``_PBI_ARTIFACT_PATTERNS``
    regex; a DAX that matches it is labelled a visual/formatting/slicer artifact
    (documented, not translated).
    """
    covered = _covered_names(all_specs)
    measures: list[TranslationResult] = []
    untranslatable: list[TranslationResult] = []
    seen: set = set()

    for m in mapping:
        name = m.get("measure_name") or ""
        orig = m.get("original_name") or name
        if not orig:
            continue
        if orig in covered or to_snake_case(orig) in covered or orig in seen:
            continue
        seen.add(orig)

        dax = m.get("dax_expression") or ""
        is_artifact = bool(artifact_patterns.search(dax)) if dax else False

        try:
            res = translator.translate(m, _VIEW_KEY)
        except Exception as e:  # translator must never break the catch-all
            res = TranslationResult(
                measure_name=to_snake_case(orig),
                original_name=orig,
                sql_expr=None,
                is_translatable=False,
                skip_reason=f"translation error: {e}",
                dax_expression=dax,
                confidence="low",
                category="unassigned",
            )

        if res.is_translatable and res.sql_expr:
            # "whatever we can translate" — a real, best-effort measure.
            measures.append(res)
        else:
            # Documented, never dropped. Tag the visual/formatting/slicer ones so
            # it is obvious WHY they aren't measures (Genie/analytics don't use
            # them), instead of a generic "no matching pattern".
            if is_artifact and "artifact" not in (res.skip_reason or "").lower():
                reason = res.skip_reason or ""
                res.skip_reason = (reason + " · " if reason else "") + (
                    "visual/formatting/slicer artifact — no Genie/analytical form"
                )
            res.is_translatable = False
            if not res.dax_expression:
                res.dax_expression = dax
            untranslatable.append(res)

    if not measures and not untranslatable:
        return None

    comment = (
        f"NONE-ALLOCATED MEASURES ({_VIEW_KEY}) — catch-all so nothing is dropped.\n"
        "PBI measures that did not land on any fact view are gathered here: the ones "
        "we could translate are emitted as measures below (best-effort); the rest are "
        "documented as comments with their DAX + reason. Visual / formatting / slicer "
        "measures are listed with that reason — they have no metric-view or Genie form.\n"
        "These reference columns across multiple facts, so `source:` is a placeholder — "
        "treat this file as a reconciliation worklist, not a deployable view.\n"
        f"{len(measures)} best-effort translated · {len(untranslatable)} documented."
    )
    return MetricViewSpec(
        fact_table_key=_VIEW_KEY,
        source_table=_PLACEHOLDER_SOURCE,
        view_name=_VIEW_KEY,
        comment=comment,
        joins=[],
        dimensions=[],
        measures=measures,
        untranslatable=untranslatable,
        base_measure_count=0,
        dax_measure_count=len(measures),
    )


def build_none_allocated_yaml(
    mapping: list[dict],
    all_specs: dict,
    translator,
    artifact_patterns,
) -> str | None:
    """Emit the catch-all view's YAML text, or ``None`` when nothing is orphaned
    (or emission fails — the caller treats this as best-effort)."""
    spec = build_none_allocated_spec(mapping, all_specs, translator, artifact_patterns)
    if spec is None:
        return None
    try:
        return emit_yaml(spec)
    except Exception:
        return None
