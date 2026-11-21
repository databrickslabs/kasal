"""The verify-numbers loop on a real-size model: one view that does not deploy must not stop the
comparison of the others, and the reason must reach the run output.

Before, any deploy error ended the loop in cycle 1 with "deploy failed", no reason and no YAML, so a
36-view model with a single failing view reconciled nothing.
"""

import json

from src.services.tools.ucmv_iterative_recon import (
    deploy_outcome,
    run_iterative_ucmv_generation,
)


class _Generator:
    def _run(self, **kwargs):
        return json.dumps(
            {
                "yaml": {"Fact_A": "yaml-a", "Fact_B": "yaml-b"},
                "pbi_ucmv_mapping": {"Fact_A": "mapping-a", "Fact_B": "mapping-b"},
            }
        )


class _Deployer:
    def __init__(self, results=None, error=None):
        self._results, self._error = results, error

    def _run(self, **kwargs):
        if self._error:
            return json.dumps({"error": self._error})
        return json.dumps({"deployment_results": self._results, "summary": {}})


class _Reconciler:
    def __init__(self):
        self.mappings_seen = []

    def _run(self, **kwargs):
        mappings = json.loads(kwargs["mappings_json"])
        self.mappings_seen.append(sorted(mappings))
        return json.dumps(
            {
                "views": {"Fact_A": {"measures": {"m": {"pct_aligned": 100.0}}}},
                "overall": {
                    "cells_total": 4,
                    "overall_cell_pct": 100.0,
                    "measures_at_100": 1,
                    "total_measures": 1,
                },
            }
        )


DENIED = {
    "status": "error",
    "message": "PERMISSION_DENIED: User does not have SELECT on table x",
}


def test_the_deployed_views_are_reconciled_and_the_failed_one_is_named():
    recon = _Reconciler()
    out = run_iterative_ucmv_generation(
        generator=_Generator(),
        deployer=_Deployer({"Fact_A": {"status": "deployed"}, "Fact_B": DENIED}),
        reconciler=recon,
        max_cycles=1,
    )
    assert recon.mappings_seen == [["Fact_A"]]  # only the live view is compared
    assert out["stop_reason"] == "target_reached"
    cycle = out["cycles"][0]
    assert cycle["deploy_ok"] is True
    assert "PERMISSION_DENIED" in cycle["deploy_failures"]["Fact_B"]
    assert cycle["note"].startswith("1 of 2 views deployed")


def test_when_nothing_deploys_the_reason_and_the_yaml_stay_in_the_output():
    recon = _Reconciler()
    out = run_iterative_ucmv_generation(
        generator=_Generator(),
        deployer=_Deployer({"Fact_A": DENIED, "Fact_B": DENIED}),
        reconciler=recon,
        max_cycles=3,
    )
    assert recon.mappings_seen == []
    assert out["stop_reason"] == "deploy_failed"
    cycle = out["cycles"][0]
    assert (
        "no view deployed; 2 failed" in cycle["note"]
        and "PERMISSION_DENIED" in cycle["note"]
    )
    assert set(cycle["deploy_failures"]) == {"Fact_A", "Fact_B"}
    assert out["yaml"] == {"Fact_A": "yaml-a", "Fact_B": "yaml-b"}


def test_a_deployer_error_before_any_view_is_reported():
    out = run_iterative_ucmv_generation(
        generator=_Generator(),
        deployer=_Deployer(error="warehouse_id is required for mode 'deploy'"),
        reconciler=_Reconciler(),
        max_cycles=1,
    )
    assert "warehouse_id is required" in out["cycles"][0]["note"]


def test_a_truncated_view_is_not_reconciled_but_grant_gaps_are():
    verdict = deploy_outcome(
        {
            "deployment_results": {
                "A": {"status": "deployed_grants_incomplete"},
                "B": {"status": "deployed_incomplete", "message": "YAML truncated"},
            }
        }
    )
    assert verdict["ok"] is True and verdict["deployed_views"] == ["A"]
    assert verdict["failures"] == {"B": "deployed_incomplete: YAML truncated"}
