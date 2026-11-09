"""Iterative-reconciliation gate for the UC Metric View generator.

When reconciliation is requested (explicitly, or implied by a warehouse id) and
the call is not already running inside the loop, delegate to the
generate → deploy → reconcile → refine orchestrator. The loop calls the
generator back with reconciliation off, so there is no recursion.

Missing prerequisites never fail silently: the returned reason names exactly
what is missing, and generation then runs single-pass.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.services.tools.ucmv_iterative_recon import ToolRunner

logger = logging.getLogger(__name__)


def run_reconciliation_gate(
    tool: ToolRunner,
    kwargs: dict[str, object],
    get: Callable[[str], object],
) -> tuple[str | None, str | None]:
    """Run the reconciliation loop when requested and possible.

    Returns ``(loop_report_json, skip_reason)``. ``loop_report_json`` is set when
    the loop ran and the caller must return it. ``skip_reason`` is set when
    reconciliation was wanted but did not run; it is None otherwise.
    """
    recon_requested = get("enable_reconciliation") or get("warehouse_id")
    if not recon_requested or kwargs.get("_in_recon_loop"):
        return None, None

    # Reconciliation needs BOTH sides: a Databricks SQL warehouse (to deploy +
    # query the view) AND a Power BI connection + credentials (to pull the
    # ground-truth numbers). A warehouse alone is NOT enough — with no PBI creds
    # the loop deploys the view then fails every one on "pbi_workspace_id is
    # required", which surfaces as a generic "deploy failed". Name exactly what
    # is missing so the UI/history says why.
    from src.services.tools.metric_view_utils.reconciliation_prereqs import (
        missing_reconciliation_inputs,
    )

    missing = missing_reconciliation_inputs(get)
    if missing:
        skipped = (
            "Reconciliation was requested but did NOT run — missing: "
            f"{'; '.join(missing)}. Fill these in the UC Metric View "
            "Generator's Reconciliation tab to enable the live cell-by-cell "
            "Power BI check; generation ran single-pass."
        )
        logger.warning("[UCMV] %s", skipped)
        return None, skipped

    try:
        from src.services.tools.ucmv_iterative_recon import run_from_generator

        loop_out = run_from_generator(tool, kwargs)
        if loop_out is not None:
            return json.dumps(loop_out), None
        skipped = (
            "Reconciliation was requested but did NOT run — the warehouse "
            "or Power BI credentials could not be resolved; generation ran "
            "single-pass."
        )
        logger.warning("[UCMV] %s", skipped)
        return None, skipped
    except Exception as e:
        # The loop must never break generation: fall back to single-pass.
        skipped = f"Reconciliation was requested but FAILED: {e}"
        logger.warning(
            "[UCMV] %s — falling back to single-pass.", skipped, exc_info=True
        )
        return None, skipped
