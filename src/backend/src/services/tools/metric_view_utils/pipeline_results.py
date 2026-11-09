"""Serializable results and metadata lint for a finished pipeline run.

``build_results`` assembles the dict ``MetricViewPipeline.get_results`` returns:
per-table specs with their measures, the business-coverage summary, the lint
findings and the migration report.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .metadata_lint import LintConfig, lint_metric_view, summarize_findings
from .report_emitter import build_proposal, emit_migration_report

if TYPE_CHECKING:
    from .pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)


def build_results(pipeline: MetricViewPipeline) -> dict:
    """Return pipeline results as a serializable dict."""
    """Return pipeline results as a serializable dict."""
    from .recovery_recommender import draft_source_view as _draft_source_view
    from .recovery_recommender import recommend as _recovery_recipe

    # Tables reachable only via a skipped many:many/bidirectional relationship
    # — used to recommend an EXISTS-precompute recovery (Gap 4) instead of a
    # generic decline.
    _m2n_tables: set[str] = set()
    for _r in pipeline._limitations.get("m2n_relationships", []) or []:
        _m2n_tables.add(_r.get("from_table", ""))
        _m2n_tables.add(_r.get("to_table", ""))
    _m2n_tables.discard("")
    results: dict = {
        "specs": {},
        "stats": pipeline.stats,
        "cross_table_count": len(pipeline.cross_table_measures),
        "filter_warnings": pipeline._filter_warnings,
    }
    _all_lint_findings: list = []
    _lint_config = LintConfig.from_dict(
        (pipeline.config.get("naming_config") or {}).get("lint")
    )
    for table_key, spec in pipeline.all_specs.items():
        results["specs"][table_key] = {
            "fact_table_key": spec.fact_table_key,
            "source_table": spec.source_table,
            "view_name": spec.view_name,
            "measures_count": len(spec.measures),
            "untranslatable_count": len(spec.untranslatable),
            "base_measure_count": spec.base_measure_count,
            "dax_measure_count": spec.dax_measure_count,
            "switch_measure_count": spec.switch_measure_count,
            "joins_count": len(spec.joins),
            "dimensions_count": len(spec.dimensions),
            "measures": [
                {
                    "name": m.measure_name,
                    "original_name": m.original_name,
                    "sql_expr": m.sql_expr,
                    # dax_expression is the ORIGINAL DAX — the validator needs
                    # it to compare against the translated SQL (filter/agg check).
                    "dax_expression": m.dax_expression,
                    "confidence": m.confidence,
                    "category": m.category,
                    # Business-usage signal (PROP-8): which report page(s)/
                    # visual(s) draw or filter on this measure — see
                    # `visual_usage_annotator.annotate_visual_usage`.
                    "used_in_visuals": m.used_in_visuals,
                    # Backtraced (indirect) usage: a visual-placed KPI
                    # references this measure — see annotate_indirect_visual_usage.
                    "indirect_visual_usage": getattr(m, "indirect_visual_usage", []),
                }
                for m in spec.measures
            ],
            "untranslatable": [
                {
                    "name": m.original_name,
                    "original_name": m.original_name,
                    "skip_reason": m.skip_reason,
                    "category": m.category,
                    # Rich fields for the validation-UI "Not transpiled" review
                    # panel: the full original DAX, the LLM translation-class
                    # label, and how many other measures depend on this one (so
                    # reviewers can triage high-impact gaps first).
                    "dax_expression": m.dax_expression,
                    "dax_class": m.dax_class,
                    "referenced_by": getattr(m, "referenced_by", 0),
                    "explanation": getattr(m, "explanation", None),
                    # Prefer the LLM's own recipe; else a concrete Gap 4/5 recovery
                    # recipe (EXISTS precompute / separate-grain view) when the DAX
                    # shape matches; else the class-based default.
                    "proposal": build_proposal(
                        m.dax_class,
                        getattr(m, "explanation", None)
                        or _recovery_recipe(
                            m.dax_expression,
                            fact_table=spec.fact_table_key,
                            m2n_tables=_m2n_tables,
                            join_tables={
                                n for j in (spec.joins or []) if (n := j.get("name"))
                            },
                        ),
                        m.skip_reason,
                    ),
                    # Best-effort, UNVERIFIED source-view SQL scaffold for cross-fact /
                    # multi-stage measures — a proposal starting point, never an emitted
                    # measure. Clearly labeled DRAFT; complete + verify against PBI.
                    "source_view_sql_draft": _draft_source_view(
                        m.dax_expression,
                        measure_name=m.measure_name,
                        fact_table=spec.fact_table_key,
                    ),
                    "used_in_visuals": m.used_in_visuals,
                    "indirect_visual_usage": getattr(m, "indirect_visual_usage", []),
                }
                for m in spec.untranslatable
            ],
        }
        # Metadata / naming lint (advisory): flag snake_case, scenario/unit
        # suffixes, duplicate dimensions and synonym clashes so the migration
        # owner can fix Genie-facing metadata before deploy. Never fatal.
        try:
            _findings = lint_metric_view(spec, _lint_config)
        except Exception:  # lint is advisory, must never break generation
            logger.warning(
                "metadata lint failed for %s; continuing without findings",
                table_key,
                exc_info=True,
            )
            _findings = []
        if _findings:
            results["specs"][table_key]["metadata_lint"] = [
                {
                    "rule_id": f.rule_id,
                    "severity": f.severity,
                    "field": f.field,
                    "message": f.message,
                }
                for f in _findings
            ]
            _all_lint_findings.extend(_findings)
    total_measures = sum(
        s.get("total", 0) for k, s in pipeline.stats.items() if k != "__unassigned__"
    )
    total_translated = sum(
        s.get("translated", 0)
        for k, s in pipeline.stats.items()
        if k != "__unassigned__"
    )
    total_artifacts = sum(
        s.get("artifacts", 0)
        for k, s in pipeline.stats.items()
        if k != "__unassigned__"
    )
    business_scope = total_measures - total_artifacts
    results["business_coverage"] = {
        "total_measures": total_measures,
        "translated": total_translated,
        "artifacts_excluded": total_artifacts,
        "business_scope": business_scope,
        "overall_pct": (
            total_translated * 100 // total_measures if total_measures else 0
        ),
        "business_pct": (
            total_translated * 100 // business_scope if business_scope > 0 else 0
        ),
    }
    pipeline._record_source_gaps()
    _lint_summary = summarize_findings(_all_lint_findings)
    results["metadata_lint_summary"] = {
        "total": _lint_summary.total,
        "errors": _lint_summary.error_count,
        "warnings": _lint_summary.warning_count,
        "by_rule": _lint_summary.by_rule,
    }
    results["limitations"] = pipeline._limitations
    results["migration_report"] = emit_migration_report(
        pipeline.all_specs,
        pipeline.stats,
        pipeline.config,
        limitations=pipeline._limitations,
    )
    return results
