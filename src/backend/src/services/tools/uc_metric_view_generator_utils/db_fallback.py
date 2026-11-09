"""DB fallback for the generator's JSON inputs.

Crew 1 (the Power BI extraction crew) persists its extraction for the flow
execution. When flow injection left measures / mquery / config / relationships /
visual usage empty, this rebuilds them from that row, looked up by this tool's
own execution id, never by "most recent row for this dataset".

It runs BEFORE API-mode extraction, so Crew 1's higher-quality result is
preferred and API-mode extraction is only the last resort. It goes through
Kasal's DB rather than a file because Crew 1 and Crew 2 run in separate
subprocesses.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from .shared_types import RawInputs, RunAsync, UCMVDiagnostics, execution_id_for

if TYPE_CHECKING:
    from src.models.powerbi_extraction import PowerBIExtraction

logger = logging.getLogger(__name__)


async def _load_extraction(job_id: str) -> PowerBIExtraction | None:
    """The newest powerbi_extraction row recorded for one execution, if any."""
    from src.services.tools.tool_session_provider import ToolSessionProvider

    async with ToolSessionProvider.powerbi_extraction_service() as svc:
        rows = await svc.list_for_execution(job_id)
        return rows[0] if rows else None


def _relationships_missing(relationships: object) -> bool:
    return (not relationships) or (
        isinstance(relationships, str) and relationships.strip() in ("", "[]", "null")
    )


def _visual_usage_missing(visual_usage: object) -> bool:
    return (not visual_usage) or (
        isinstance(visual_usage, str)
        and visual_usage.strip() in ("", "{}", "[]", "null")
    )


def apply_db_fallback(
    *,
    tool: object,
    raw: RawInputs,
    diag: UCMVDiagnostics,
    run_async: RunAsync,
) -> None:
    """Fill empty inputs in ``raw`` from the Crew 1 extraction row (fail-open)."""
    rel_missing = _relationships_missing(raw.relationships)
    vu_missing = _visual_usage_missing(raw.visual_usage)
    if not (
        raw.measures == "[]"
        or raw.mquery == "[]"
        or raw.config == "{}"
        or rel_missing
        or vu_missing
    ):
        return

    job_id = execution_id_for(tool)
    if not job_id:
        logger.info(
            "[UCMV] DB fallback: no job_id on trace_context — cannot look up powerbi_extraction"
        )
        return
    try:
        extraction = run_async(_load_extraction(job_id))
        if extraction is None:
            logger.info(
                "[UCMV] DB fallback: no powerbi_extraction row found for job_id=%s",
                job_id,
            )
            return
        _rebuild_from_extraction(
            extraction,
            raw,
            diag,
            job_id=job_id,
            rel_missing=rel_missing,
            vu_missing=vu_missing,
        )
    except Exception as db_err:
        diag["db_fallback_error"] = str(db_err)
        logger.warning("[UCMV] DB fallback failed: %s", db_err, exc_info=True)


def _rebuild_from_extraction(
    extraction: PowerBIExtraction,
    raw: RawInputs,
    diag: UCMVDiagnostics,
    *,
    job_id: str,
    rel_missing: bool,
    vu_missing: bool,
) -> None:
    """Rebuild each empty input from the extraction row, recording what fired."""
    diag["db_fallback_extraction_id"] = extraction.id
    db_admin_tables = extraction.admin_tables or {}
    db_expressions = extraction.expressions or {}
    db_measures = extraction.measures or []
    db_config = extraction.proposed_config or {}
    logger.info(
        "[UCMV] DB fallback: found powerbi_extraction id=%s for job_id=%s "
        "(%s tables, %s expressions)",
        extraction.id,
        job_id,
        len(db_admin_tables),
        len(db_expressions),
    )
    from src.services.tools.pipeline_config_generator_tool import (
        PipelineConfigGeneratorTool,
    )

    if raw.mquery == "[]" and db_admin_tables:
        rebuilt_mquery = PipelineConfigGeneratorTool._build_ucmv_mquery(
            db_admin_tables, db_expressions
        )
        if rebuilt_mquery:
            raw.mquery = json.dumps(rebuilt_mquery)
            diag["db_fallback_fired_for"].append("mquery_json")
            logger.info(
                "[UCMV] DB fallback: rebuilt mquery_json (%s tables)",
                len(rebuilt_mquery),
            )
    if raw.measures == "[]" and db_measures:
        rebuilt_measures = PipelineConfigGeneratorTool._build_ucmv_measures(
            db_measures,
            admin_tables=db_admin_tables,
            config=db_config,
        )
        if rebuilt_measures:
            raw.measures = json.dumps(rebuilt_measures)
            diag["db_fallback_fired_for"].append("measures_json")
            logger.info(
                "[UCMV] DB fallback: rebuilt measures_json (%s measures)",
                len(rebuilt_measures),
            )
    if raw.config == "{}" and db_config:
        raw.config = json.dumps(db_config)
        diag["db_fallback_fired_for"].append("config_json")
    # Relationships drive RelationshipsLoader's fact→dim join enrichment. Without
    # rebuilding them here, every SELECT-* fact whose joins come from PBI
    # relationships silently loses ALL its joins when the flow injects an empty
    # relationships_json — the exact case this fallback exists for.
    db_relationships = getattr(extraction, "relationships", None)
    if rel_missing and db_relationships:
        raw.relationships = json.dumps(db_relationships)
        diag["db_fallback_fired_for"].append("relationships_json")
        logger.info(
            "[UCMV] DB fallback: rebuilt relationships_json (%s relationships)",
            len(db_relationships),
        )
    # Visual usage has no column of its own, but the raw report_definition IS
    # persisted — re-derive the index from it when the handoff dropped it, so
    # used_in_visuals / the "Used on" column / the visual-usage triage don't
    # silently vanish on an empty handoff.
    db_report_def = getattr(extraction, "report_definition", None)
    if vu_missing and db_report_def:
        try:
            from src.services.powerbi.visual_usage import derive_visual_usage_index

            rebuilt_vu = derive_visual_usage_index(db_report_def)
            if rebuilt_vu:
                raw.visual_usage = json.dumps(rebuilt_vu)
                diag["db_fallback_fired_for"].append("visual_usage_index")
                logger.info(
                    "[UCMV] DB fallback: re-derived visual_usage_index (%s "
                    "field(s)) from persisted report_definition",
                    len(rebuilt_vu),
                )
        except Exception as vu_err:
            logger.warning(
                "[UCMV] DB fallback: visual_usage re-derive failed: %s",
                vu_err,
                exc_info=True,
            )
