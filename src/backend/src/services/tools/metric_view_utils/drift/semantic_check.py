"""LLM check for measures deployed BEFORE fingerprinting existed.

A legacy measure has no recorded source DAX, so "did it change in PBI?" can only be
judged by comparing today's DAX with the deployed SQL. That is an LLM judgement, so
its verdict is reported as ``possibly_changed`` / ``unchanged_llm`` — never as the
exact ``changed_in_pbi`` — and a possibly-changed measure is NOT applied to the
proposed YAML unless the operator opts in.

Measures are judged in batches (one call per ``batch_size`` measures) because a
per-measure call pattern is what previously blew the endpoint's tokens/min limit.
Fail-open: a failed batch leaves its measures ``legacy_unverified``.
"""

import json
import logging
from typing import Callable, Optional

from src.services.tools.metric_view_utils.drift.diff import (
    LEGACY,
    POSSIBLY_CHANGED,
    UNCHANGED_LLM,
    MeasureDrift,
)

logger = logging.getLogger(__name__)

# (system_prompt, user_prompt) -> raw model text, or None on failure
CompleteFn = Callable[[str, str], Optional[str]]

DEFAULT_BATCH_SIZE = 20

SYSTEM_PROMPT = (
    "You compare a Power BI DAX measure with the Databricks SQL expression of a Unity "
    "Catalog metric-view measure that was translated from an EARLIER version of that "
    "DAX. Decide whether today's DAX still computes the same thing as the deployed "
    "SQL. Ignore formatting, naming of aliases, and differences that are only how "
    "DAX filter context maps to SQL. Answer CHANGED only when the business logic "
    "differs: a different column, aggregation, filter value, time window, divisor, "
    "or an added/removed term. When unsure, answer CHANGED."
)


def _user_prompt(batch: list[MeasureDrift]) -> str:
    items = [
        {
            "id": i,
            "measure": d.original_name,
            "dax_today": d.current_dax,
            "deployed_sql": d.baseline_expr,
        }
        for i, d in enumerate(batch)
    ]
    return (
        "For each item return a verdict.\n"
        'Respond with ONLY a JSON object: {"results": [{"id": <int>, '
        '"verdict": "SAME" | "CHANGED", "reason": "<one short sentence>"}]}\n\n'
        + json.dumps(items, indent=1)
    )


def judge_legacy_measures(
    drifts: list[MeasureDrift],
    complete_fn: CompleteFn,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_measures: int = 300,
) -> dict:
    """Re-label LEGACY drifts in place. Returns counts for the report."""
    from src.core.llm.json_extraction import extract_json_dict

    legacy = [
        d for d in drifts if d.status == LEGACY and d.current_dax and d.baseline_expr
    ]
    todo = legacy[:max_measures]
    judged = changed = failed = 0
    for start in range(0, len(todo), max(1, batch_size)):
        batch = todo[start : start + batch_size]
        raw = None
        try:
            raw = complete_fn(SYSTEM_PROMPT, _user_prompt(batch))
        except Exception as exc:  # noqa: BLE001 — fail-open per batch
            logger.warning(f"[UCMVDrift] semantic batch failed: {exc}")
        parsed = extract_json_dict(raw) if raw else None
        results = (parsed or {}).get("results") if isinstance(parsed, dict) else None
        if not isinstance(results, list):
            failed += len(batch)
            continue
        by_id = {r.get("id"): r for r in results if isinstance(r, dict)}
        for i, d in enumerate(batch):
            r = by_id.get(i)
            if not r:
                failed += 1
                continue
            verdict = str(r.get("verdict", "")).upper()
            d.status = UNCHANGED_LLM if verdict == "SAME" else POSSIBLY_CHANGED
            d.llm_reason = str(r.get("reason") or "")[:500] or None
            judged += 1
            changed += d.status == POSSIBLY_CHANGED
    return {
        "legacy_measures": len(legacy),
        "judged": judged,
        "possibly_changed": changed,
        "unverified": len(legacy) - judged,
        "skipped_over_cap": max(0, len(legacy) - len(todo)),
        "failed": failed,
    }
