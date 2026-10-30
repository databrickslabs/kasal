"""Construction and execution of the metric-view pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .model_inputs import ParsedModelInputs

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline


def run_pipeline(
    *,
    measures: list[dict],
    parsed: ParsedModelInputs,
    config: dict,
    inner_dim_joins: bool,
    unflatten_tables: bool,
    llm_config: dict | None,
) -> MetricViewPipeline:
    """Build the pipeline from the parsed inputs and run it."""
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

    scan_parser = parsed.scan_parser
    pipeline = MetricViewPipeline(
        mapping=measures,
        mquery_tables=parsed.mquery_tables,
        config=config,
        inner_dim_joins=inner_dim_joins,
        scan_data=parsed.scan_data,
        unflatten_tables=unflatten_tables or bool(parsed.scan_data),
        relationships_enrichment=parsed.relationships_enrichment,
        llm_config=llm_config,
        inactive_relationships=parsed.inactive_relationships or None,
        m2n_relationships=parsed.m2n_relationships or None,
        refresh_policy_tables=scan_parser.get_refresh_policy_tables() or None,
        no_summarize_columns=scan_parser.get_no_summarize_columns() or None,
        rls_tables=scan_parser.get_rls_tables() or None,
    )
    pipeline.run()
    return pipeline
