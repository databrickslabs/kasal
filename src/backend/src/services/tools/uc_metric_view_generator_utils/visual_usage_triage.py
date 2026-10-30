"""Visual-usage annotation, triage, and the PBI↔UCMV mapping for a finished pipeline.

Runs after the pipeline has translated the measures, so each measure can be
tagged with where it is actually shown in the report, and the mapping and live
connection flags are derived from that same state.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VisualUsageResult:
    """What the visual-usage pass tagged or removed."""

    visual_usage_obj: dict | None
    annotated: int
    indirect_annotated: int
    pruned: int


def apply_visual_usage(
    pipeline: MetricViewPipeline,
    visual_usage_raw: object,
    *,
    prune_requested: bool,
) -> VisualUsageResult:
    """Tag measures with direct and indirect visual usage, then optionally triage.

    Mutates ``pipeline.all_specs`` in place, so it must run before emission to reach
    the YAML comment, migration report, and JSON output alike.
    """
    annotated = 0
    # Kept so the none_allocated catch-all can annotate its own (freshly
    # translated) measures with the same visual reference.
    visual_usage_obj: dict | None = None
    if visual_usage_raw:
        try:
            from src.services.tools.metric_view_utils.visual_usage_annotator import (
                annotate_visual_usage,
            )

            parsed_vu: object = (
                json.loads(visual_usage_raw)
                if isinstance(visual_usage_raw, str)
                else visual_usage_raw
            )
            visual_usage_obj = parsed_vu if isinstance(parsed_vu, dict) else None
            if isinstance(visual_usage_obj, dict):
                annotated = annotate_visual_usage(pipeline.all_specs, visual_usage_obj)
        except Exception as e:
            logger.warning(
                "Failed to parse/apply visual_usage_index: %s", e, exc_info=True
            )

    # Backtrace: propagate that direct usage DOWN the measure-dependency graph so
    # sub-KPIs feeding a visual-placed KPI are surfaced too (indirect_visual_usage,
    # kept separate from the direct tag). Fail-open.
    indirect_annotated = 0
    try:
        from src.services.tools.metric_view_utils.visual_usage_annotator import (
            annotate_indirect_visual_usage,
        )

        indirect_annotated = annotate_indirect_visual_usage(pipeline.all_specs)
    except Exception as e:
        logger.warning("Failed to derive indirect visual usage: %s", e, exc_info=True)

    # TRIAGE (opt-in): drop measures with no direct AND no indirect visual usage.
    # Gated on BOTH the flag AND real visual data — without a visual-usage index
    # every measure looks unused and we must NOT prune (we'd drop the whole model).
    pruned = 0
    if prune_requested and annotated > 0:
        try:
            from src.services.tools.metric_view_utils.visual_usage_annotator import (
                prune_non_visual_measures,
            )

            pruned = prune_non_visual_measures(pipeline.all_specs)
            logger.info(
                "[UCMVGenerator] Visual-usage triage pruned %s measure(s) not "
                "reachable from any visual (directly or via dependency chain).",
                pruned,
            )
        except Exception as e:
            logger.warning("Visual-usage pruning skipped: %s", e, exc_info=True)

    return VisualUsageResult(
        visual_usage_obj=visual_usage_obj,
        annotated=annotated,
        indirect_annotated=indirect_annotated,
        pruned=pruned,
    )


def derive_mapping_and_connections(
    pipeline: MetricViewPipeline,
) -> tuple[dict[str, str], dict]:
    """The reconciliation mapping draft and the live-connection flags.

    The mapping is a deterministic PBI-measure ↔ UCMV-measure draft per fact table
    (it reads each measure's used_in_visuals, so it runs after the annotation).
    Live connections name the views backed by a semantic model (M-Query
    AnalysisServices.Database), which the transpiler cannot resolve.
    """
    pbi_ucmv_mapping: dict[str, str] = {}
    try:
        from src.services.tools.metric_view_utils.pbi_ucmv_mapping import (
            derive_pbi_ucmv_mapping,
        )

        pbi_ucmv_mapping = derive_pbi_ucmv_mapping(pipeline.all_specs)
    except Exception as e:
        logger.warning("Failed to derive pbi_ucmv_mapping: %s", e, exc_info=True)

    live_connections: dict = {}
    try:
        from src.services.powerbi.live_connection import derive_live_connections

        live_connections = derive_live_connections(
            pipeline.all_specs, getattr(pipeline, "_mquery_expressions", {})
        )
        if live_connections:
            logger.info(
                "[UCMVGenerator] %s view(s) on a live semantic-model connection "
                "(flagged for manual parse)",
                len(live_connections),
            )
    except Exception as e:
        logger.warning("Failed to derive live_connections: %s", e, exc_info=True)
    return pbi_ucmv_mapping, live_connections
