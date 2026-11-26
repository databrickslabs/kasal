"""Best-effort drafting: thin views from the measures that already resolved, opt-in only.

Used when the generator produced no view, the caller allowed best-effort output and supplied
``fact_source_map``. Every drafted measure is marked TODO, so the drafts are never silent.
"""

from __future__ import annotations

import json
import logging
from src.services.tools.uc_metric_view_generator_utils.zero_view_diagnosis import (
    build_best_effort_views,
)

logger = logging.getLogger(__name__)


def apply_best_effort(
    yaml_output: dict,
    *,
    allow: object,
    fact_source_map: object,
    config: dict,
    measures: list,
) -> tuple[dict, dict | None]:
    """``(yaml_output, report)``: the drafted views replace an empty ``yaml_output`` when
    best-effort applies; otherwise both are returned unchanged (report None)."""
    if yaml_output or not allow or not fact_source_map:
        return yaml_output, None
    fsm = fact_source_map
    if isinstance(fsm, str):
        try:
            fsm = json.loads(fsm)
        except Exception:
            logger.debug(
                "[UCMVGenerator] fact_source_map is not valid JSON; ignoring it",
                exc_info=True,
            )
            fsm = None
    be_views, report = build_best_effort_views(
        fact_source_map=fsm, config=config, measures=measures
    )
    if not be_views:
        return yaml_output, dict(report)
    logger.warning(
        "[UCMVGenerator] BEST-EFFORT mode: drafted %s view(s) / %s measure(s) "
        "from a thin report; %s measure(s) unresolved and skipped. "
        "Tables/measures may be MISSING — every measure marked TODO: verify.",
        report["tables_emitted"],
        report["measures_emitted"],
        report["measures_skipped_unresolved"],
    )
    return be_views, dict(report)
