"""Source-layer views, PBI-only ingestion tasks and the PBI validation hook.

Three extra outputs, each its own key on the tool result: the source-layer views
the metric views sit on (rec#3), the tables that need an ingestion snapshot rather
than a SQL translation (S8), and the validation-loop status (rec#4). They share one
fail-open step so an error here never costs the views themselves.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.data_classes import TableInfo
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)


@dataclass
class SourceLayerOutputs:
    """The three source-layer style outputs, empty when the step was skipped."""

    source_layer_ddl: dict = field(default_factory=dict)
    pbi_only_ingestion_tasks: list = field(default_factory=list)
    pbi_validation: dict = field(default_factory=dict)


def build_source_layer_outputs(
    *,
    pipeline: MetricViewPipeline,
    mquery_tables: dict[str, TableInfo],
    config: dict,
    catalog: str,
    schema: str,
    yaml_output: dict,
    validate_against_pbi: bool,
    access_token: str | None,
    workspace_id: str | None,
    dataset_id: str | None,
    pbi_api_base_url: str | None,
) -> SourceLayerOutputs:
    """Build the source-layer DDL, ingestion tasks and validation report (fail-open)."""
    outputs = SourceLayerOutputs()
    try:
        from src.services.tools.metric_view_utils.source_artifacts import (
            build_source_layer,
            detect_pbi_only_tables,
            run_pbi_validation,
        )

        mq_exprs = getattr(pipeline, "_mquery_expressions", {}) or {}
        outputs.source_layer_ddl = build_source_layer(
            pipeline.all_specs,
            mquery_tables,
            config,
            catalog,
            schema,
            mquery_expressions=mq_exprs,
        )
        outputs.pbi_only_ingestion_tasks = detect_pbi_only_tables(
            mq_exprs, catalog, schema
        )
        # rec#4: only build a live PBI callback when explicitly opted in (and
        # creds are present) — otherwise run_pbi_validation reports "skipped"
        # and makes no network calls.
        evaluate_fn = None
        if validate_against_pbi:
            from src.services.tools.metric_view_utils.source_artifacts import (
                build_pbi_evaluate_fn,
            )

            evaluate_fn = build_pbi_evaluate_fn(
                access_token,
                workspace_id,
                dataset_id,
                base_url=pbi_api_base_url,
            )
        outputs.pbi_validation = run_pbi_validation(
            pipeline.all_specs, yaml_output, evaluate_fn=evaluate_fn
        )
        if outputs.source_layer_ddl or outputs.pbi_only_ingestion_tasks:
            logger.info(
                "[UCMVGenerator] source layer: %s view(s); PBI-only ingestion tasks: %s",
                len(outputs.source_layer_ddl),
                len(outputs.pbi_only_ingestion_tasks),
            )
    except Exception as e:
        logger.warning(
            "[UCMVGenerator] source-layer/ingestion/validation step skipped: %s",
            e,
            exc_info=True,
        )
    return outputs
