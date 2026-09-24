"""Find the PBI report bound to a semantic model, when the caller didn't
already know its report_id.

Split out of ``pipeline_config.py`` (over the file-size ceiling) as its own
seam, following the same pattern as ``switch_decomposition.py`` /
``calculation_groups.py`` / ``custom_function_resolution.py`` /
``mapping_only_tables.py`` — see any of those for why this module has no
dependency on ``pipeline_config`` (which is also loaded standalone, by file
path, via ``generate_config.py``'s CLI fallback).
"""

from __future__ import annotations

import requests


def _headers(token: str) -> dict[str, str]:
    """Kept identical to (and independently of) ``pipeline_config._headers``."""
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _pick_best_report_match(reports: list[dict]) -> dict | None:
    """From reports already filtered to one ``datasetId``, prefer a real report
    over an auto-generated/usage one; else the first match. Shared by both
    discovery sources below so they rank candidates identically.
    """
    if not reports:
        return None
    ranked = sorted(
        reports,
        key=lambda r: (
            "usage metrics" in str(r.get("name", "")).lower(),
            "auto" in str(r.get("name", "")).lower(),
        ),
    )
    return ranked[0]


def discover_report_id(
    token: str,
    workspace_id: str,
    dataset_id: str,
    scan_result: dict | None = None,
) -> str | None:
    """PROP-7: find the report bound to ``dataset_id`` in the workspace.

    Running the pipeline WITHOUT a report_id silently degrades measure DAX (the
    report's visual bindings carry the full measure expressions). When the caller
    did not supply one, auto-discover it.

    **Prefer the admin-scan payload when the caller already has one.** The
    classic ``GET .../groups/{ws}/reports`` endpoint below needs a delegated
    PBI-scoped token with workspace access — a Service-Account-only caller
    (the primary auth tier for automated migrations, see backend/CLAUDE.md)
    gets a bare 401 from it, confirmed live on otc_management even though
    that SAME account's Admin-Scanner-fetched ``scan_result`` (API 3, already
    fetched moments earlier for `admin_tables` — no extra call here) already
    carries a ``workspaces[0].reports`` array with ``datasetId`` per report,
    because the Admin Scanner runs on a DIFFERENT, tenant-admin-scoped token.
    Every report discovery bug traced back to an empty ``visual_usage_index``
    was this: the classic endpoint 401'd, so the loud "none auto-discovered"
    warning fired and every downstream visual-usage tag silently came back
    empty, even though the answer was sitting in data already in hand.

    Falls back to the classic REST call when no ``scan_result`` is supplied
    (e.g. a standalone CLI run) or when it has no usable ``reports`` array.
    Returns None (fail-open) if nothing resolves either way — the caller then
    proceeds report-less with a loud warning.
    """
    if scan_result:
        try:
            workspaces = scan_result.get("workspaces") or []
            for ws in workspaces:
                reports = ws.get("reports") or []
                matches = [
                    r
                    for r in reports
                    if str(r.get("datasetId", "")).lower() == str(dataset_id).lower()
                ]
                best = _pick_best_report_match(matches)
                if best:
                    return best.get("id")
        except Exception:
            pass  # fall through to the classic REST call below

    try:
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
        resp = requests.get(url, headers=_headers(token), timeout=30)
        if resp.status_code != 200:
            return None
        reports = resp.json().get("value", [])
        matches = [
            r
            for r in reports
            if str(r.get("datasetId", "")).lower() == str(dataset_id).lower()
        ]
        best = _pick_best_report_match(matches)
        return best.get("id") if best else None
    except Exception:
        return None
