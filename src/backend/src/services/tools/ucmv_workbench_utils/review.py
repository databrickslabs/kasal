"""The review the workbench export carries: the same review list the generator builds, for the
working copy as it stands.

The rows, the per-view counts (``review_overview``) and the summary come from
``metric_view_utils/reconciliation/review_list.py`` (imported, not re-implemented). The input is the
working copy: each view's YAML, its mapping and its last comparison. A view the workbench did not
change keeps crew 2b's comparison, so its rows are the ones crew 2b reported. The views the
workbench changed since the incoming review are listed in ``review_changed_views``.
"""

from __future__ import annotations

from src.services.tools.metric_view_utils.reconciliation.review_list import (
    build_review_list,
    review_csv,
    review_overview,
    review_summary,
)

from .state import Workbench

_OPEN_STATUSES = (
    "not_deployed",
    "removed_on_deploy",
    "below_bar",
    "not_compared",
    "not_translated",
)


def _deploy_failures(bench: Workbench) -> dict[str, str]:
    """The incoming deploy failures, with the workbench's own deploys replacing them."""
    last = (
        bench.cycles[-1] if bench.cycles and isinstance(bench.cycles[-1], dict) else {}
    )
    failures = {
        str(k): str(v)
        for k, v in (last.get("deploy_failures") or {}).items()
        if isinstance(last.get("deploy_failures"), dict)
    }
    for key, v in bench.views.items():
        if v.deployed == "ok":
            failures.pop(key, None)
        elif v.deployed:
            failures[key] = f"error: {v.deployed}"
    return failures


def _output(bench: Workbench) -> dict[str, object]:
    """The verify-numbers output the review functions read, built from the working copy."""
    views: dict[str, dict[str, object]] = {}
    for key, v in bench.views.items():
        # A view whose last deploy failed is not compared: its comparison is of a view that is
        # not there any more, so the review lists it as not deployed.
        failed = bool(v.deployed) and v.deployed != "ok"
        views[key] = (
            {**v.report, "measures": dict(v.reconciliation)}
            if v.reconciled and not failed
            else {}
        )
    last = (
        dict(bench.cycles[-1])
        if bench.cycles and isinstance(bench.cycles[-1], dict)
        else {}
    )
    last["deploy_failures"] = _deploy_failures(bench)
    cycles = [*bench.cycles[:-1], last] if bench.cycles else [last]
    return {
        "yaml": {k: v.yaml for k, v in bench.views.items()},
        "pbi_ucmv_mapping": {k: v.mapping for k, v in bench.views.items()},
        "reconciliation": {"views": views},
        "untranslatable_items": [
            *(item for k in bench.views for item in bench.review_untranslated(k)),
            *bench.orphans,
        ],
        "resolved_measures_by_table": bench.resolved,
        "cycles": cycles,
    }


def review_for_export(bench: Workbench) -> dict[str, object]:
    """``review_overview`` (per view and total), ``review_summary``, ``review_csv``, the open
    measures (in scope, not at the bar) and the views changed since the incoming review.
    """
    output = _output(bench)
    bar = (
        float(bench.review_bar) if isinstance(bench.review_bar, (int, float)) else 90.0
    )
    rows = build_review_list(output, bar)
    open_measures = [
        {
            "view": r["view"],
            "measure": r["measure"],
            "status": r["status"],
            "reason": r.get("reason", ""),
        }
        for r in rows
        if r.get("scope", "in_scope") == "in_scope" and r["status"] in _OPEN_STATUSES
    ]
    return {
        # The comparison the review was built from, as a verify-numbers output carries it: a
        # consumer that reads the export (the deployer's gate and flags) sees the current numbers.
        "reconciliation": output["reconciliation"],
        "review_bar_pct": bar,
        "review_overview": review_overview(rows),
        "review_summary": review_summary(output, rows, bar),
        "review_open_measures": open_measures,
        "review_csv": review_csv(rows),
        "review_changed_views": bench.changed_views(),
    }
