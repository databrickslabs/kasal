"""The static verdict next to the comparison with Power BI, when the comparison is there.

The validator reads the SQL and the DAX; the reconciliation (crew 2b) reads the cells. They
are different evidence, and both stay visible: this module never changes a static verdict.
For each measure the validator judged it adds ``comparison``: the Power BI outcome (share of
cells aligned, cell counts), a class, and the action a reader can take:

- ``flagged_matches_pbi``: the static check flags a difference, the measure matches Power BI
  at the bar: check the rule, not the measure;
- ``flagged_below_bar``: flagged and below the bar in the comparison: fix the translation;
- ``valid_below_bar``: valid statically but below the bar: the static check cannot see this;
- ``valid_matches_pbi``: valid statically and at the bar;
- ``not_compared``: no comparison for this measure (not deployed, removed, not compared).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Dict, Optional, Tuple

from ..metric_view_utils.reconciliation.review_list import build_review_list, review_bar

logger = logging.getLogger(__name__)

CLASSES = (
    "flagged_matches_pbi",
    "flagged_below_bar",
    "valid_below_bar",
    "valid_matches_pbi",
    "not_compared",
)
_FLAGGED = ("REVIEW", "INVALID")
_JUDGED = ("VALID", "EQUIVALENT", "REVIEW", "INVALID")


def comparison_rows(output: Mapping) -> Dict[Tuple[str, str], dict]:
    """``{(view, measure): review row}`` of a crew 2b output that carries a reconciliation;
    empty when it carries none."""
    if not isinstance(output, Mapping) or not output.get("reconciliation"):
        return {}
    try:
        rows = build_review_list(output)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        logger.warning("[Validator] comparison rows unavailable: %s", exc)
        return {}
    return {(str(r["view"]), str(r["measure"])): r for r in rows}


def judge(static_status: str, row: Optional[dict]) -> dict:
    """The comparison judgement of one measure given its static verdict and review row."""
    flagged = static_status in _FLAGGED
    if row is None or row.get("status") not in ("reconciled", "below_bar"):
        status = "no row" if row is None else str(row.get("status"))
        return {
            "class": "not_compared",
            "pbi_status": status,
            "action": f"not compared with Power BI ({status}): the static verdict stands alone",
        }
    matches = row["status"] == "reconciled"
    cells = f"{row.get('cells_aligned')} of {row.get('cells_total')} cells"
    pct = row.get("pct_aligned")
    base = {
        "pbi_status": row["status"],
        "pct_aligned": pct,
        "cells_aligned": row.get("cells_aligned"),
        "cells_total": row.get("cells_total"),
    }
    if flagged and matches:
        return {
            **base,
            "class": "flagged_matches_pbi",
            "action": f"static check flags a difference, but the measure matches Power BI "
            f"on {cells}: check the rule, not the measure",
        }
    if flagged:
        return {
            **base,
            "class": "flagged_below_bar",
            "action": f"flagged and below the bar in the comparison ({pct}% of cells): "
            "fix the translation",
        }
    if matches:
        return {
            **base,
            "class": "valid_matches_pbi",
            "action": f"valid statically and matches Power BI on {cells}",
        }
    return {
        **base,
        "class": "valid_below_bar",
        "action": f"valid statically but below the bar ({pct}% of cells): the static "
        "check cannot see this",
    }


def annotate(results: dict, rows: Dict[Tuple[str, str], dict]) -> Dict[str, int]:
    """Adds ``comparison`` to each judged measure of ``results`` (the per-view results of the
    validator, with ``details``), and returns the count of each class."""
    counts = dict.fromkeys(CLASSES, 0)
    for view, result in results.items():
        if not isinstance(result, dict):
            continue
        for item in result.get("details", []) or []:
            verdict = item.get("measure_eval_result")
            if not isinstance(verdict, dict) or verdict.get("status") not in _JUDGED:
                continue
            judged = judge(
                str(verdict["status"]),
                rows.get((str(view), str(item.get("measure_name")))),
            )
            verdict["comparison"] = judged
            counts[judged["class"]] += 1
    return counts


def comparison_summary_for(results: dict, ucmv_output: object) -> Dict[str, object]:
    """The summary keys for a run: whether a comparison was available, the bar, and the count
    of each class. Without a reconciliation in ``ucmv_output`` nothing is annotated."""
    if isinstance(ucmv_output, str):
        try:
            ucmv_output = json.loads(ucmv_output)
        except json.JSONDecodeError:
            ucmv_output = {}
    rows = comparison_rows(ucmv_output if isinstance(ucmv_output, dict) else {})
    counts = annotate(results, rows) if rows else dict.fromkeys(CLASSES, 0)
    return {
        "comparison_available": bool(rows),
        "comparison_bar_pct": review_bar(),
        "comparison_classes": counts,
    }
