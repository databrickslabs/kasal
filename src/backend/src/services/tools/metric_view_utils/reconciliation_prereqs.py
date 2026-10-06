"""What reconciliation needs before it can run — and a human-readable list of
what's missing when it can't.

Reconciliation compares a deployed UC Metric View against the LIVE Power BI model,
so it needs BOTH sides:
  * a Databricks SQL warehouse (to deploy the view and query it), and
  * a Power BI connection + credentials (to pull the ground-truth numbers).

A warehouse alone is not enough: with no Power BI credentials the iterative loop
deploys the view then fails every one on ``pbi_workspace_id is required``, which
surfaces to the operator as a generic "deploy failed". This module lets the
generator gate name exactly what is missing instead, so the UI and the saved
history say WHY a run stayed single-pass.

The Power BI fields are the generator's OWN config keys
(``workspace_id`` / ``dataset_id`` / ``tenant_id`` / ``client_id`` /
``client_secret`` / ``username`` / ``password`` / ``access_token``) — the same
ones ``ucmv_iterative_recon`` forwards to the reconciliation tool — not something
carried from a prior pipeline step.
"""

from __future__ import annotations

from typing import Any, Callable, List


def _has_pbi_auth(get: Callable[[str], Any]) -> bool:
    """True when a usable Power BI credential set is present via ``get``."""
    if get("access_token"):
        return True  # user OAuth / pasted bearer token
    if get("tenant_id") and get("client_id") and get("client_secret"):
        return True  # service principal
    if get("tenant_id") and get("client_id") and get("username") and get("password"):
        return True  # service account
    return False


def missing_reconciliation_inputs(get: Callable[[str], Any]) -> List[str]:
    """Return human-readable names of the inputs reconciliation needs but lacks.

    ``get(key)`` resolves a config value (e.g. the generator's ``_get`` closure
    over the call kwargs and the tool's default config). An empty list means
    reconciliation has everything it needs to run.
    """
    missing: List[str] = []
    if not get("warehouse_id"):
        missing.append("Databricks SQL warehouse ID (warehouse_id)")
    if not get("workspace_id"):
        missing.append("Power BI workspace ID (workspace_id)")
    if not get("dataset_id"):
        missing.append("Power BI dataset / semantic-model ID (dataset_id)")
    if not _has_pbi_auth(get):
        missing.append(
            "Power BI credentials (service principal: tenant_id+client_id"
            "+client_secret; service account: tenant_id+client_id+username"
            "+password; or an access_token)"
        )
    return missing
