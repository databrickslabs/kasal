"""Tests for the iterative generate→deploy→reconcile→refine loop.

The three side-effecting steps are injected as fakes — no network, no LLM."""

from src.services.tools.metric_view_utils.reconciliation import (
    build_refinement_feedback,
    feedback_to_prompt_map,
    run_reconciliation_loop,
)


class TestFeedbackToPromptMap:
    def test_list_form_builds_hint_strings(self):
        fb = [
            {
                "measure": "actual_value",
                "pct_aligned": 61.1,
                "sample_mismatches": [
                    {
                        "dimension": "AT",
                        "period": "2025007",
                        "ucmv_value": 5,
                        "pbi_value": 9,
                    }
                ],
            }
        ]
        out = feedback_to_prompt_map(fb)
        assert "actual_value" in out
        assert "61.1%" in out["actual_value"]
        assert "AT/2025007" in out["actual_value"]

    def test_dict_form_passthrough(self):
        assert feedback_to_prompt_map({"m": "wrong"}) == {"m": "wrong"}

    def test_junk_ignored(self):
        assert feedback_to_prompt_map(None) == {}
        assert feedback_to_prompt_map([{"no_measure": 1}]) == {}


def _report(overall_pct, *, measures):
    """Build a reconciliation-report-shaped dict.

    ``measures`` = {name: (pct_aligned, classification, samples)}."""
    total = len(measures)
    at100 = sum(1 for (p, _c, _s) in measures.values() if p >= 100)
    return {
        "views": {
            "v": {
                "measures": {
                    m: {
                        "pct_aligned": p,
                        "classification": c,
                        "sample_mismatches": s,
                    }
                    for m, (p, c, s) in measures.items()
                }
            }
        },
        "overall": {
            "measures_at_100": at100,
            "total_measures": total,
            "cells_total": 100,
            "cells_aligned": int(overall_pct),
            "overall_cell_pct": overall_pct,
        },
    }


class TestBuildRefinementFeedback:
    def test_surfaces_only_real_errors_below_target(self):
        report = _report(
            80,
            measures={
                "good": (100.0, "aligned", []),
                "drifted": (60.0, "drift", [{"delta": 1}]),
                "defect": (50.0, "known_defect", []),
                "bug": (40.0, "real_error", [{"dimension": "AT", "delta": 9}]),
            },
        )
        fb = build_refinement_feedback(report, target_pct=100.0)
        names = {f["measure"] for f in fb}
        assert names == {"bug"}  # drift/known_defect/aligned excluded
        assert fb[0]["view"] == "v"
        assert fb[0]["sample_mismatches"] == [{"dimension": "AT", "delta": 9}]

    def test_caps_samples(self):
        many = [{"i": i} for i in range(20)]
        report = _report(10, measures={"bug": (10.0, "real_error", many)})
        fb = build_refinement_feedback(report)
        assert len(fb[0]["sample_mismatches"]) == 5


class TestRunReconciliationLoop:
    def _fakes(self, pcts):
        """generate/deploy/reconcile fakes; reconcile returns pcts[cycle-1]."""
        calls = {"generate": [], "deploy": 0}
        seq = list(pcts)

        def generate(feedback):
            calls["generate"].append(feedback)
            return {"yaml": {"v": f"yaml-{len(calls['generate'])}"}, "mappings": {}}

        def deploy(yaml):
            calls["deploy"] += 1
            return {"ok": True}

        def reconcile(yaml, mappings):
            pct = seq[len(calls["generate"]) - 1]
            return _report(pct, measures={"bug": (pct, "real_error", [{"d": 1}])})

        return generate, deploy, reconcile, calls

    def test_stops_when_target_reached(self):
        gen, dep, rec, calls = self._fakes([70, 100])
        res = run_reconciliation_loop(
            generate=gen, deploy=dep, reconcile=rec, max_cycles=5, target_pct=100
        )
        assert res.stop_reason == "target_reached"
        assert len(res.cycles) == 2
        assert res.best_pct == 100
        assert res.best_cycle == 2
        assert res.final_yaml == {"v": "yaml-2"}
        # first cycle got no feedback; second got feedback from cycle 1
        assert calls["generate"][0] is None
        assert calls["generate"][1] and calls["generate"][1][0]["measure"] == "bug"

    def test_improving_runs_to_max_cycles(self):
        gen, dep, rec, _ = self._fakes([50, 60, 70])
        res = run_reconciliation_loop(
            generate=gen, deploy=dep, reconcile=rec, max_cycles=3, target_pct=100
        )
        assert res.stop_reason == "max_cycles"
        assert [c.overall_pct for c in res.cycles] == [50, 60, 70]
        assert res.best_cycle == 3

    def test_converged_stops_when_no_improvement(self):
        gen, dep, rec, _ = self._fakes([70, 70, 90])
        res = run_reconciliation_loop(
            generate=gen, deploy=dep, reconcile=rec, max_cycles=5, target_pct=100
        )
        assert res.stop_reason == "converged"
        assert len(res.cycles) == 2  # stopped at the non-improving 2nd cycle

    def test_best_cycle_retained_if_later_worse(self):
        # 80 then 60 (worse): converged stop, best stays cycle 1.
        gen, dep, rec, _ = self._fakes([80, 60])
        res = run_reconciliation_loop(
            generate=gen, deploy=dep, reconcile=rec, max_cycles=5, target_pct=100
        )
        assert res.best_cycle == 1
        assert res.best_pct == 80
        assert res.final_yaml == {"v": "yaml-1"}

    def test_deploy_failure_aborts(self):
        gen, _, rec, _ = self._fakes([100])

        def bad_deploy(yaml):
            return {"ok": False, "error": "warehouse down"}

        res = run_reconciliation_loop(
            generate=gen, deploy=bad_deploy, reconcile=rec, max_cycles=5
        )
        assert res.stop_reason == "deploy_failed"
        assert res.cycles[0].deploy_ok is False

    def test_no_recon_when_nothing_scored(self):
        def gen(feedback):
            return {"yaml": {"v": "y"}, "mappings": {}}

        def dep(yaml):
            return {"ok": True}

        def rec(yaml, mappings):
            return {"views": {}, "overall": {"cells_total": 0}}

        res = run_reconciliation_loop(generate=gen, deploy=dep, reconcile=rec)
        assert res.stop_reason == "no_recon"
        assert res.final_yaml == {"v": "y"}
