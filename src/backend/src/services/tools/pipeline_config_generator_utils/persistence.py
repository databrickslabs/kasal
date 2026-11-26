"""Persist config-gen output to ``conversion_history`` and ``powerbi_extraction``.

Both writes are fail-open: observability must never break a config-gen run.
The execution id arrives as a callable so it is resolved inside that guard.
"""

import logging
from collections.abc import Callable, Mapping

from src.services.tools.metric_view_utils.mquery_handoff import build_ucmv_mquery
from src.services.tools.pipeline_config_generator_utils.config_access import (
    first_text,
    read_section,
)

logger = logging.getLogger("src.services.tools.pipeline_config_generator_tool")


def _dax_text(measure: Mapping[str, object]) -> str:
    return first_text(measure, "expression", "dax_expression").strip()


def _is_selectedvalue_switch(measure: Mapping[str, object]) -> bool:
    text = first_text(measure, "expression", "dax_expression").upper()
    return "SWITCH" in text and "SELECTEDVALUE" in text


def _extract_arrays(
    admin_tables: dict[str, dict], relationships: list[dict]
) -> dict[str, object]:
    """M-Query + relationships for the download bundle (best-effort)."""
    extra: dict[str, object] = {}
    try:
        extra["mquery_json"] = build_ucmv_mquery(admin_tables)
    except Exception:
        logger.debug(
            "[PipelineConfigGen] mquery_json omitted from history download",
            exc_info=True,
        )
    extra["relationships_json"] = relationships or []
    return extra


async def save_conversion_history(
    *,
    resolve_execution_id: Callable[[], str | None],
    measures: list[dict],
    config: dict[str, object],
    relationships: list[dict],
    admin_tables: dict[str, dict],
    workspace_id: str,
    dataset_id: str,
) -> None:
    """Persist the config-gen result to conversion_history (fail-open).

    Records the extracted measures with DAX (``input_data.measures``), the proposed
    config (``output_data.proposed_config``) and a DAX diagnostic. Retrievable via
    GET /conversion-history (filter source_format=powerbi_config).
    """
    try:
        from src.schemas.conversion import ConversionHistoryCreate
        from src.services.tools.tool_session_provider import ToolSessionProvider
        from src.utils.user_context import UserContext

        measures = measures or []
        with_dax = sum(1 for m in measures if _dax_text(m))
        switch_cnt = sum(1 for m in measures if _is_selectedvalue_switch(m))

        switch_tables = len(read_section(config, "switch_decompositions"))
        measure_resolutions = len(read_section(config, "measure_resolutions"))

        history_data = ConversionHistoryCreate(
            execution_id=resolve_execution_id(),
            source_format="powerbi_config",
            target_format="pipeline_config",
            input_data={
                "workspace_id": workspace_id or "",
                "dataset_id": dataset_id or "",
                "measures": measures,
                "relationships_count": len(relationships or []),
                "admin_tables_count": len(admin_tables or {}),
            },
            input_summary=(
                f"{len(measures)} measures ({with_dax} with DAX, "
                f"{switch_cnt} SELECTEDVALUE+SWITCH)"
            )[:500],
            # The extracted arrays ride along so the UI can offer them as downloads
            # by execution_id (the crew's answer is a markdown summary).
            output_data={
                "proposed_config": config,
                "measures_json": measures,
                **_extract_arrays(admin_tables, relationships),
            },
            output_summary=(
                f"config: {switch_tables} switch tables, "
                f"{measure_resolutions} measure resolutions"
            )[:500],
            configuration={"workspace_id": workspace_id, "dataset_id": dataset_id},
            status="success",
            measure_count=len(measures),
        )

        group_context = None
        try:
            group_context = UserContext.get_group_context()
        except Exception:
            logger.debug(
                "[PipelineConfigGen] No group context; saving history without it",
                exc_info=True,
            )

        # Through ConverterService, which owns conversion history and stamps
        # group_id / created_by_email itself.
        async with ToolSessionProvider.converter_service(
            group_context=group_context
        ) as converter:
            record = await converter.create_history(history_data)
            await converter.session.commit()
            logger.info(
                "[PipelineConfigGen] Saved conversion_history id=%s "
                "(measures=%s, with_dax=%s, switch=%s)",
                record.id,
                len(measures),
                with_dax,
                switch_cnt,
            )
    except Exception as e:
        logger.warning(
            "[PipelineConfigGen] Failed to save conversion_history (non-fatal): %s",
            e,
            exc_info=True,
        )


async def save_powerbi_extraction(
    *,
    resolve_execution_id: Callable[[], str | None],
    relationships: list[dict],
    measures: list[dict],
    admin_tables: dict[str, dict],
    report_def: dict[str, object] | None,
    config: dict[str, object],
    warnings: list[str],
    workspace_id: str,
    dataset_id: str,
    report_id: str | None,
    expressions: dict[str, str] | None = None,
) -> None:
    """Persist the FULL raw extraction to the powerbi_extraction table (fail-open).

    Unlike conversion_history (summaries and counts), this keeps the complete
    artifacts, so they are SQL-queryable per run for BI review and debugging.
    """
    try:
        from src.schemas.powerbi_extraction import PowerBIExtractionCreate
        from src.services.tools.tool_session_provider import ToolSessionProvider
        from src.utils.user_context import UserContext

        measures = measures or []
        relationships = relationships or []
        admin_tables = admin_tables or {}

        with_dax = sum(1 for m in measures if _dax_text(m))
        summary = (
            f"{len(relationships)} relationships, {len(measures)} measures "
            f"({with_dax} with DAX), {len(admin_tables)} tables"
        )[:500]

        group_id: str | None = None
        created_by_email: str | None = None
        gc = None  # bound before the try so the service call below cannot NameError
        try:
            gc = UserContext.get_group_context()
            if gc:
                group_id = getattr(gc, "primary_group_id", None)
                created_by_email = getattr(gc, "group_email", None) or getattr(
                    gc, "email", None
                )
        except Exception:
            logger.debug(
                "[PipelineConfigGen] No group context; saving extraction without it",
                exc_info=True,
            )

        data = PowerBIExtractionCreate(
            execution_id=resolve_execution_id(),
            workspace_id=workspace_id or None,
            dataset_id=dataset_id or None,
            report_id=report_id or None,
            relationships=relationships,
            measures=measures,
            admin_tables=admin_tables,
            expressions=expressions or {},
            report_definition=report_def or None,
            proposed_config=config,
            warnings=warnings or [],
            relationships_count=len(relationships),
            measures_count=len(measures),
            measures_with_dax_count=with_dax,
            admin_tables_count=len(admin_tables),
            summary=summary,
            group_id=group_id,
            created_by_email=created_by_email,
        )

        # Extractions belong to PowerBIExtractionService, which stamps the tenant.
        async with ToolSessionProvider.powerbi_extraction_service(
            group_context=gc
        ) as extractions:
            record = await extractions.record_extraction(data)
            await extractions.session.commit()
            logger.info(
                "[PipelineConfigGen] Saved powerbi_extraction id=%s (%s)",
                record.id,
                summary,
            )
    except Exception as e:
        logger.warning(
            "[PipelineConfigGen] Failed to save powerbi_extraction (non-fatal): %s",
            e,
            exc_info=True,
        )
