"""Durable raw-DAX persistence to conversion_history (fail-open).

The untruncated source DAX lands in ``input_data.dax_raw`` and is retrievable
afterwards via ``GET /conversion-history`` (filter by ``source_format=powerbi_dax``
or ``execution_id``). Any failure here is non-fatal: it must never break the
generation itself.
"""

from __future__ import annotations

import logging

from .shared_types import execution_id_for

logger = logging.getLogger(__name__)


def _capability_fingerprint() -> str:
    """Current transpiler capability fingerprint (fail-open)."""
    try:
        from src.services.tools.metric_view_utils.capability_version import (
            capability_fingerprint,
        )

        return capability_fingerprint()
    except Exception:
        logger.debug(
            "[UCMVGenerator] capability fingerprint unavailable", exc_info=True
        )
        return "unknown"


async def save_dax_to_conversion_history(
    tool: object,
    *,
    raw_dax: list[dict[str, object]],
    yaml_output: object,
    sql_output: object,
    workspace_id: str | None,
    dataset_id: str | None,
    catalog: str | None,
    schema: str | None,
    untranslatable_items: list[dict[str, object]] | None = None,
    pbi_ucmv_mapping: dict | None = None,
    live_connections: dict | None = None,
    source_layer_ddl: dict | None = None,
    pbi_only_ingestion_tasks: list | None = None,
    pbi_validation: dict | None = None,
    reconciliation_skipped: str | None = None,
) -> None:
    """Persist the full raw DAX extract to conversion_history (fail-open).

    ``untranslatable_items`` and the transpiler ``capability_fingerprint`` are
    persisted so re-evaluation can later answer "which measures failed, and at
    what capability level?" and decide whether a retry can gain anything —
    without re-hitting the PowerBI API.

    ``pbi_ucmv_mapping`` (per-view PBI<->UCMV reconciliation mapping draft) is
    persisted in ``output_data`` alongside ``yaml``/``sql`` so the "Download
    Mapping" artifact is retrievable by ``execution_id`` after the run.
    """
    try:
        from src.schemas.conversion import ConversionHistoryCreate
        from src.services.tools.tool_session_provider import ToolSessionProvider
        from src.utils.user_context import UserContext

        raw_dax_count = len(raw_dax)
        # In JSON/flow mode the tool does not re-extract, so raw_dax is empty
        # even though views ARE generated (measures arrive via config_json /
        # task context). Count the actual views produced so the diagnostic
        # reflects success instead of a misleading "0 measures".
        view_count = len(yaml_output) if isinstance(yaml_output, dict) else 0
        # measure_count reflects extracted DAX when present, else views built.
        measure_count = raw_dax_count or view_count
        history_execution_id = execution_id_for(tool)
        history_data = ConversionHistoryCreate(
            execution_id=history_execution_id,
            source_format="powerbi_dax",
            target_format="uc_metrics",
            input_data={
                "workspace_id": workspace_id or "",
                "dataset_id": dataset_id or "",
                "dax_raw": raw_dax,
            },
            input_summary=(
                f"DAX extract: {raw_dax_count} measure(s)"
                + (f" from workspace {workspace_id}" if workspace_id else "")
            )[:500],
            output_data={
                "yaml": yaml_output,
                "sql": sql_output,
                "catalog": catalog,
                "schema": schema,
                # Re-evaluation inputs: WHICH measures failed, and at WHAT
                # capability level. A later sweep re-tries only these.
                "untranslatable_items": untranslatable_items or [],
                # Per-view PBI<->UCMV reconciliation mapping draft
                # ({view_name: mapping_candidates_yaml_text}, from
                # pbi_ucmv_mapping.py). Persisted alongside yaml/sql so the
                # "Download Mapping" artifact is retrievable by execution_id
                # (GET /conversion-history), not only from the live tool
                # result the UI happens to hold in-session.
                "pbi_ucmv_mapping": pbi_ucmv_mapping or {},
                # Live-connection flag (which semantic model/table to parse),
                # persisted so the UI note is retrievable by execution_id.
                "live_connections": live_connections or {},
                # Source-layer DDL (rec#3): {table: {ddl, todo_steps, error}}
                # — persisted so the UI can show the base views a run proposes.
                "source_layer_ddl": source_layer_ddl or {},
                # PBI-only ingestion tasks (S8): tables needing a snapshot
                # loader rather than a SQL source.
                "pbi_only_ingestion_tasks": pbi_only_ingestion_tasks or [],
                # Validation-loop status (rec#4): skipped/ran/error + detail.
                "pbi_validation": pbi_validation or {},
                # Reason reconciliation did not run when it was requested
                # (persisted so it is retrievable by execution_id, not just
                # in the live result).
                "reconciliation_skipped": reconciliation_skipped,
            },
            output_summary=(
                f"Generated {view_count} UC metric view(s)"
                + (
                    f" from {raw_dax_count} extracted measure(s)"
                    if raw_dax_count
                    else " (JSON/flow mode)"
                )
            )[:500],
            configuration={
                "workspace_id": workspace_id,
                "dataset_id": dataset_id,
                "catalog": catalog,
                "schema": schema,
                # Capability level that produced this result. Re-evaluation
                # compares it against the current fingerprint to decide
                # whether a retry can possibly gain anything.
                "capability_fingerprint": _capability_fingerprint(),
            },
            status="success",
            measure_count=measure_count,
        )

        group_context = None  # stays None if the lookup below fails
        try:
            group_context = UserContext.get_group_context()
            if group_context:
                getattr(group_context, "primary_group_id", None)
        except Exception as gc_err:
            logger.debug(
                "[UCMVGenerator] Could not resolve group_id for history: %s",
                gc_err,
                exc_info=True,
            )

        # Through ConverterService, which OWNS conversion history and stamps
        # group_id/created_by_email itself — the repository path required every
        # tool to remember that by hand.
        async with ToolSessionProvider.converter_service(
            group_context=group_context
        ) as converter:
            record = await converter.create_history(history_data)
            await converter.session.commit()
            logger.info(
                "[UCMVGenerator] Saved conversion_history record id=%s "
                "(source_format=powerbi_dax, views=%s, extracted_measures=%s)",
                record.id,
                view_count,
                raw_dax_count,
            )
    except Exception as e:
        logger.warning(
            "[UCMVGenerator] Failed to save conversion_history (non-fatal): %s",
            e,
            exc_info=True,
        )
