"""The report's scope table, completed with what this generation run produced.

The Pipeline Config Generator drafts the scope from the live model and report
(``services/powerbi/scope``): one row per measure, calculation item, function,
RLS filter, table and column, with a reachability-based decision. Here each
measure row learns what Kasal did with it (``in_kasal``: translated / TODO expr /
not emitted / no) and where it went (``mirrored_as``: ``<view>.<measure>``), and
each fact-table row the metric view built on it. That turns Kasal's output from
"every measure of every fact" into a reviewable list: a kept measure Kasal did
not translate, or a translated one nobody can reach, is visible at a glance.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)

_TRANSLATED = "translated"
_TODO = "TODO expr"
_NOT_EMITTED = "not emitted"


def _parse(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            value = json.loads(raw)
        except ValueError:
            return {}
        return value if isinstance(value, dict) else {}
    return {}


def _measure_index(
    pipeline: MetricViewPipeline,
) -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    """(PBI measure -> (status, mirrored_as), PBI table key -> view name)."""
    measures: dict[str, tuple[str, str]] = {}
    views: dict[str, str] = {}
    for key, spec in (getattr(pipeline, "all_specs", None) or {}).items():
        views[str(key).lower()] = spec.view_name
        for measure in spec.measures:
            status = _TRANSLATED if measure.sql_expr else _TODO
            measures.setdefault(
                measure.original_name,
                (status, f"{spec.view_name}.{measure.measure_name}"),
            )
        for measure in spec.untranslatable:
            measures.setdefault(measure.original_name, (_NOT_EMITTED, ""))
    return measures, views


def finalize_report_scope(
    raw: object, pipeline: MetricViewPipeline
) -> dict[str, object] | None:
    """The scope with ``in_kasal`` / ``mirrored_as`` filled, or None without a scope input.

    Fail-open: any error returns the draft unchanged, so the views never depend on it.
    """
    scope = _parse(raw)
    given = scope.get("rows")
    rows = [r for r in given if isinstance(r, dict)] if isinstance(given, list) else []
    if not rows:
        return None
    try:
        from src.services.powerbi.scope import scope_csv

        measures, views = _measure_index(pipeline)
        for row in rows:
            if row.get("object_type") == "measure":
                status, mirrored = measures.get(row.get("name", ""), ("no", ""))
                row["in_kasal"] = status
                if mirrored and not row.get("mirrored_as"):
                    row["mirrored_as"] = mirrored
            elif row.get("object_type") == "table":
                view = views.get(str(row.get("pbi_table", "")).lower())
                if view and not row.get("mirrored_as"):
                    row["mirrored_as"] = view
        scope["csv"] = scope_csv(rows)
        scope["kasal_gaps"] = _gaps(rows)
        scope["review"] = _review(rows, pipeline)
    except (AttributeError, KeyError, TypeError, ValueError) as err:
        logger.warning(
            "[UCMVGenerator] report scope completion skipped: %s", err, exc_info=True
        )
    return scope


def _review(rows: list[dict], pipeline: MetricViewPipeline) -> list[dict[str, object]]:
    """The open rows in buckets with one proposed decision each (``services/powerbi/scope/scope_review``),
    using what this run generated as evidence."""
    from src.services.powerbi.scope.scope_review import buckets, evidence, summary

    specs: dict[str, dict[str, object]] = {}
    for spec in (getattr(pipeline, "all_specs", None) or {}).values():
        specs[spec.view_name] = {
            "measures": [
                {"name": m.measure_name, "comment": f"PBI measure: {m.original_name}"} for m in spec.measures
            ],
            "dimensions": [d for d in spec.dimensions if isinstance(d, dict)],
        }
    return summary(buckets(rows, evidence(rows, specs)))


def _gaps(rows: list[dict]) -> dict[str, int]:
    """Counts the review starts from: kept but not translated; translated but unreachable."""
    kept_missing = sum(
        1
        for r in rows
        if r.get("object_type") == "measure"
        and r.get("decision") == "keep"
        and r.get("in_kasal") != _TRANSLATED
    )
    unreachable_translated = sum(
        1
        for r in rows
        if r.get("object_type") == "measure"
        and r.get("decision") == "drop"
        and r.get("in_kasal") in (_TRANSLATED, _TODO)
    )
    return {
        "kept_but_not_translated": kept_missing,
        "translated_but_dropped": unreachable_translated,
    }
