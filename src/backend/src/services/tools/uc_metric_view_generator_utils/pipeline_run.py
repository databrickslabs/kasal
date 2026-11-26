"""Construction and execution of the metric-view pipeline."""

from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING, Optional

from .model_inputs import ParsedModelInputs
from .previous_run_pins import (
    PIN_COMMENT,
    PinPlan,
    pin_outcome,
    stale_pins,
)

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)


def _build_and_run(
    *,
    measures: list[dict],
    parsed: ParsedModelInputs,
    config: dict,
    inner_dim_joins: bool,
    unflatten_tables: bool,
    llm_config: dict | None,
    source_columns: dict[str, list[str]] | None = None,
) -> MetricViewPipeline:
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
        relationships=parsed.relationships or None,
        source_columns=source_columns,
    )
    pipeline.run()
    return pipeline


def _without_pins(config: dict, dropped: set) -> dict:
    """``config`` with the rerun-pin entries of ``dropped`` (table, measure) removed."""
    overrides = config.get("manual_overrides")
    if not isinstance(overrides, dict):
        return config
    kept = {
        table: [
            entry
            for entry in (entries or [])
            if not (
                isinstance(entry, dict)
                and entry.get("comment") == PIN_COMMENT
                and (table, str(entry.get("name"))) in dropped
            )
        ]
        for table, entries in overrides.items()
    }
    return {**config, "manual_overrides": kept}


def run_pipeline(
    *,
    measures: list[dict],
    parsed: ParsedModelInputs,
    config: dict,
    inner_dim_joins: bool,
    unflatten_tables: bool,
    llm_config: dict | None,
    pins: Optional[PinPlan] = None,
    source_columns: dict[str, list[str]] | None = None,
) -> MetricViewPipeline:
    """Build the pipeline from the parsed inputs and run it.

    With ``pins`` (rerun pins from a previous run): a pin that no longer fits the view it
    landed in is dropped, and the pipeline runs once more without it. The returned pipeline
    carries ``pin_outcome``. If the inputs cannot be copied for that second run, the pins are
    not used at all (a plain run is always correct).
    """

    def build(m: list[dict], p: ParsedModelInputs, c: dict) -> MetricViewPipeline:
        return _build_and_run(
            measures=m,
            parsed=p,
            config=c,
            inner_dim_joins=inner_dim_joins,
            unflatten_tables=unflatten_tables,
            llm_config=llm_config,
            source_columns=source_columns,
        )

    if pins is None or not pins.pins:
        return build(measures, parsed, config)
    try:
        pristine = copy.deepcopy((measures, parsed, config))
    except (copy.Error, TypeError, RecursionError) as e:  # pins are optional
        logger.warning("[UCMV] rerun pins skipped (inputs not copyable): %s", e)
        return build(measures, parsed, config)
    pipeline = build(measures, parsed, config)
    stale = stale_pins(pipeline.all_specs, pins)
    if stale:
        logger.info(
            "[UCMV] %d rerun pin(s) no longer fit their view; retranslating them",
            len(stale),
        )
        pristine_measures, pristine_parsed, pristine_config = pristine
        pipeline = build(
            pristine_measures,
            pristine_parsed,
            _without_pins(pristine_config, set(stale)),
        )
        pins = pins.without(stale)
    # Read back by previous_run_pins.pin_report (not declared on the pipeline class).
    setattr(pipeline, "pin_outcome", pin_outcome(pipeline.all_specs, pins, stale))
    return pipeline
