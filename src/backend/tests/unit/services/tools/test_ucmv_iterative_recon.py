"""Tests for the iterative UCMV generation orchestrator — wiring of the three
tools into the reconciliation loop. Fakes stand in for the real tools; no network."""

import json

from src.services.tools.ucmv_iterative_recon import (
    _carry_forward_overrides,
    run_iterative_ucmv_generation,
)


class _FakeGenerator:
    """Emits a fixed YAML + mapping; records feedback it received each cycle."""

    def __init__(self):
        self.feedback_seen = []

    def _run(self, **kwargs):
        self.feedback_seen.append(kwargs.get("refinement_feedback"))
        return json.dumps(
            {
                "yaml": {"v": f"yaml-{len(self.feedback_seen)}"},
                "pbi_ucmv_mapping": {"v": "mapping-text"},
            }
        )


class _FakeDeployer:
    def __init__(self, ok=True):
        self._ok = ok

    def _run(self, **kwargs):
        if self._ok:
            return json.dumps({"summary": {"deployed": 1, "errors": 0}})
        return json.dumps({"summary": {"deployed": 0, "errors": 1}})


class _FakeReconciler:
    """Returns a scripted overall %/measure per cycle."""

    def __init__(self, pcts):
        self._pcts = list(pcts)
        self._i = 0

    def _run(self, **kwargs):
        pct = self._pcts[self._i]
        self._i += 1
        return json.dumps(
            {
                "views": {
                    "v": {
                        "measures": {
                            "bug": {
                                "pct_aligned": pct,
                                "classification": "real_error",
                                "sample_mismatches": [
                                    {
                                        "dimension": "AT",
                                        "period": "2025007",
                                        "ucmv_value": 5,
                                        "pbi_value": 9,
                                    }
                                ],
                            }
                        }
                    }
                },
                "overall": {
                    "measures_at_100": 1 if pct >= 100 else 0,
                    "total_measures": 1,
                    "cells_total": 100,
                    "cells_aligned": int(pct),
                    "overall_cell_pct": pct,
                },
            }
        )


def test_loop_converges_to_target_and_feeds_back():
    gen = _FakeGenerator()
    out = run_iterative_ucmv_generation(
        generator=gen,
        deployer=_FakeDeployer(ok=True),
        reconciler=_FakeReconciler([70, 100]),
        gen_kwargs={"measures_json": "[]"},
        deploy_kwargs={"warehouse_id": "wh"},
        recon_kwargs={"warehouse_id": "wh", "reference_years": [2025]},
        max_cycles=5,
        target_pct=100,
    )
    assert out["stop_reason"] == "target_reached"
    assert out["best_pct"] == 100
    assert len(out["cycles"]) == 2
    assert out["yaml"] == {"v": "yaml-2"}
    # cycle 1 got no feedback; cycle 2 was handed the failing measure
    assert gen.feedback_seen[0] is None
    assert "bug" in gen.feedback_seen[1]


def test_deploy_failure_stops_before_reconcile():
    recon = _FakeReconciler([100])
    out = run_iterative_ucmv_generation(
        generator=_FakeGenerator(),
        deployer=_FakeDeployer(ok=False),
        reconciler=recon,
        max_cycles=3,
    )
    assert out["stop_reason"] == "deploy_failed"
    assert recon._i == 0  # never reconciled a failed deploy


def test_cycles_serialized_to_dicts():
    out = run_iterative_ucmv_generation(
        generator=_FakeGenerator(),
        deployer=_FakeDeployer(ok=True),
        reconciler=_FakeReconciler([50, 60, 70]),
        max_cycles=3,
        target_pct=100,
    )
    assert out["stop_reason"] == "max_cycles"
    assert [c["overall_pct"] for c in out["cycles"]] == [50, 60, 70]
    assert all(isinstance(c, dict) for c in out["cycles"])


# ── run_from_generator bridge + generator delegation guard ──────────────────

from unittest.mock import patch  # noqa: E402


class _StubGen:
    def __init__(self, default_config=None):
        self._default_config = default_config or {}

    def _run(self, **kwargs):
        return json.dumps({"yaml": {}, "pbi_ucmv_mapping": {}})


def test_run_from_generator_degrades_when_warehouse_missing():
    from src.services.tools.ucmv_iterative_recon import run_from_generator

    # no warehouse_id → None (caller does single-pass). reference_years is
    # optional, so its absence alone must NOT degrade.
    assert run_from_generator(_StubGen(), {"enable_reconciliation": True}) is None


def test_run_from_generator_delegates_without_reference_years():
    """warehouse_id present but no reference_years → still reconciles, with an
    empty year list (compare all years)."""
    from src.services.tools import ucmv_iterative_recon as mod

    with (
        patch.object(mod, "run_iterative_ucmv_generation", return_value={"ok": 1}) as m,
        patch("src.services.tools.ucmv_reconciliation_tool.UCMVReconciliationTool"),
        patch("src.services.tools.metric_view_deployer_tool.MetricViewDeployerTool"),
    ):
        out = mod.run_from_generator(_StubGen(), {"warehouse_id": "wh"})

    assert out == {"ok": 1}
    assert m.call_args.kwargs["recon_kwargs"]["reference_years"] == []


def test_run_from_generator_maps_creds_and_delegates():
    from src.services.tools import ucmv_iterative_recon as mod

    gen = _StubGen(
        {
            "workspace_id": "ws1",
            "client_id": "cid",
            "client_secret": "sec",
            "tenant_id": "tid",
            "catalog": "cat",
            "schema_name": "sch",
        }
    )
    kwargs = {
        "warehouse_id": "wh",
        "reference_years": "[2025, 2026]",
        "max_reconciliation_cycles": 3,
        "reconciliation_target_pct": 95,
    }
    with (
        patch.object(mod, "run_iterative_ucmv_generation", return_value={"ok": 1}) as m,
        patch(
            "src.services.tools.ucmv_reconciliation_tool.UCMVReconciliationTool"
        ) as recon_cls,
        patch("src.services.tools.metric_view_deployer_tool.MetricViewDeployerTool"),
    ):
        out = mod.run_from_generator(gen, kwargs)

    assert out == {"ok": 1}
    # PBI creds mapped from generator field names to pbi_* ctor kwargs
    ctor = recon_cls.call_args.kwargs
    assert ctor["pbi_workspace_id"] == "ws1"
    assert ctor["pbi_client_secret"] == "sec"
    # loop params threaded
    call = m.call_args.kwargs
    assert call["max_cycles"] == 3
    assert call["target_pct"] == 95.0
    assert call["recon_kwargs"]["reference_years"] == [2025, 2026]
    assert call["gen_kwargs"]["enable_reconciliation"] is False
    assert call["gen_kwargs"]["_in_recon_loop"] is True


def test_generator_delegates_when_flag_set():
    """The generator's _run short-circuits to the loop when reconciliation is
    requested, the required inputs are present, and the bridge returns a result."""
    from src.services.tools.uc_metric_view_generator_tool import (
        UCMetricViewGeneratorTool,
    )

    tool = UCMetricViewGeneratorTool()
    with patch(
        "src.services.tools.ucmv_iterative_recon.run_from_generator",
        return_value={"stop_reason": "target_reached", "best_pct": 100},
    ):
        out = json.loads(
            tool._run(
                enable_reconciliation=True,
                warehouse_id="wh123",
                reference_years="[2025, 2026]",
                # Reconciliation needs the Power BI side too: workspace + dataset
                # + credentials. Without these the gate skips (see the skip-reason
                # tests below) and never delegates.
                workspace_id="ws1",
                dataset_id="ds1",
                access_token="tok",
            )
        )
    assert out["stop_reason"] == "target_reached"


def test_generator_surfaces_skip_reason_when_required_input_missing():
    """Reconciliation requested (flag on) but every required input is missing →
    does NOT silently single-pass: the generator surfaces reconciliation_skipped
    naming each missing field, and never calls the loop. (reference_years is
    optional, so its absence does NOT trigger a skip.)"""
    from src.services.tools.uc_metric_view_generator_tool import (
        UCMetricViewGeneratorTool,
    )

    tool = UCMetricViewGeneratorTool()
    with patch(
        "src.services.tools.ucmv_iterative_recon.run_from_generator"
    ) as mock_bridge:
        out = json.loads(tool._run(enable_reconciliation=True))
    mock_bridge.assert_not_called()  # no delegation when prerequisites are absent
    assert "stop_reason" not in out
    reason = out.get("reconciliation_skipped")
    assert reason
    # Names BOTH sides so the operator knows exactly what to fill, not a generic
    # "deploy failed".
    assert "warehouse_id" in reason
    assert "workspace_id" in reason
    assert "dataset_id" in reason
    assert "credentials" in reason.lower()


def test_generator_skips_when_only_warehouse_present_pbi_creds_missing():
    """The customer's real case: warehouse_id (and reference_years) filled, but no
    Power BI workspace/dataset/credentials → reconciliation cannot reach the PBI
    ground-truth side, so it is skipped with a reason that names the PBI gaps
    (NOT delegated to the loop, which would deploy then fail every view on
    'pbi_workspace_id is required')."""
    from src.services.tools.uc_metric_view_generator_tool import (
        UCMetricViewGeneratorTool,
    )

    tool = UCMetricViewGeneratorTool()
    with patch(
        "src.services.tools.ucmv_iterative_recon.run_from_generator"
    ) as mock_bridge:
        out = json.loads(
            tool._run(
                enable_reconciliation=True,
                warehouse_id="wh123",
                reference_years="[2025, 2026]",
            )
        )
    mock_bridge.assert_not_called()
    reason = out.get("reconciliation_skipped")
    assert reason
    assert "warehouse_id" not in reason  # the one thing they DID provide
    assert "workspace_id" in reason
    assert "dataset_id" in reason
    assert "credentials" in reason.lower()


def test_generator_delegates_on_warehouse_id_without_reference_years():
    """reference_years is optional: a warehouse_id alone (reconcile ticked or
    not) is enough to request reconciliation — the loop is invoked and years
    default to all."""
    from src.services.tools.uc_metric_view_generator_tool import (
        UCMetricViewGeneratorTool,
    )

    tool = UCMetricViewGeneratorTool()
    with patch(
        "src.services.tools.ucmv_iterative_recon.run_from_generator",
        return_value={"stop_reason": "max_cycles", "best_pct": 97},
    ):
        out = json.loads(
            tool._run(
                warehouse_id="wh123",
                workspace_id="ws1",
                dataset_id="ds1",
                access_token="tok",
            )
        )
    assert out["stop_reason"] == "max_cycles"


def test_generator_single_pass_when_bridge_returns_none():
    """If the bridge returns None (prereqs missing), generation proceeds normally
    (does not raise, does not return a loop report)."""
    from src.services.tools.uc_metric_view_generator_tool import (
        UCMetricViewGeneratorTool,
    )

    tool = UCMetricViewGeneratorTool()
    with patch(
        "src.services.tools.ucmv_iterative_recon.run_from_generator", return_value=None
    ):
        # Full recon prereqs so the gate delegates to the (patched) bridge; the
        # non-empty measures/mquery keep generation in JSON mode (no live API
        # extraction, which workspace_id+dataset_id would otherwise trigger).
        out = json.loads(
            tool._run(
                enable_reconciliation=True,
                warehouse_id="wh123",
                workspace_id="ws1",
                dataset_id="ds1",
                access_token="tok",
                measures_json='[{"name": "sales", "expression": "SUM(S[a])", "table": "S"}]',
                mquery_json='[{"table": "S", "expression": "let x=1 in x"}]',
            )
        )
    assert "stop_reason" not in out


# ── Incremental refinement: carry forward reconciled measures across cycles ──


def test_carry_forward_overrides_reuses_passing_and_skips_failing():
    resolved = {
        "fact_a": [
            {"measure_name": "good", "sql_expr": "SUM(x)", "original_name": "Good"},
            {"measure_name": "bug", "sql_expr": "SUM(y)", "original_name": "Bug"},
        ]
    }
    specs = {"fact_a": {"view_name": "v"}}
    feedback = [{"view": "v", "measure": "bug", "pct_aligned": 70}]
    ov = _carry_forward_overrides(resolved, specs, feedback)
    assert set(o["name"] for o in ov["fact_a"]) == {"good"}  # passing carried
    assert ov["fact_a"][0]["expr"] == "SUM(x)"


def test_carry_forward_excludes_failing_even_when_view_unmapped():
    # No view→table map → the failing measure is still excluded by name (safe).
    resolved = {"fact_a": [{"measure_name": "bug", "sql_expr": "SUM(y)"}]}
    ov = _carry_forward_overrides(resolved, {}, [{"view": "zz", "measure": "bug"}])
    assert ov == {}


def test_carry_forward_empty_without_feedback_or_resolved():
    assert _carry_forward_overrides({}, {}, [{"measure": "x"}]) == {}
    assert (
        _carry_forward_overrides(
            {"t": [{"measure_name": "m", "sql_expr": "S"}]}, {}, []
        )
        == {}
    )


class _FakeGenWithResolved:
    """Records the manual_overrides it received each cycle; emits resolved/specs."""

    def __init__(self):
        self.manual_overrides_seen = []

    def _run(self, **kwargs):
        self.manual_overrides_seen.append(kwargs.get("manual_overrides"))
        return json.dumps(
            {
                "yaml": {"v": "y"},
                "pbi_ucmv_mapping": {"v": "m"},
                "specs_summary": {"fact_a": {"view_name": "v"}},
                "resolved_measures_by_table": {
                    "fact_a": [
                        {"measure_name": "good", "sql_expr": "SUM(x)"},
                        {"measure_name": "bug", "sql_expr": "SUM(y)"},
                    ]
                },
            }
        )


class _ReconGoodBug:
    """`good` always 100%; `bug` follows the scripted pcts."""

    def __init__(self, bug_pcts):
        self._p = list(bug_pcts)
        self._i = 0

    def _run(self, **kwargs):
        bug = self._p[self._i]
        self._i += 1
        return json.dumps(
            {
                "views": {
                    "v": {
                        "measures": {
                            "good": {"pct_aligned": 100, "classification": "aligned"},
                            "bug": {
                                "pct_aligned": bug,
                                "classification": "real_error",
                                "sample_mismatches": [],
                            },
                        }
                    }
                },
                "overall": {
                    "measures_at_100": 2 if bug >= 100 else 1,
                    "total_measures": 2,
                    "cells_total": 200,
                    "overall_cell_pct": (100 + bug) / 2,
                },
            }
        )


def test_refine_cycle_carries_forward_passing_measures_only():
    gen = _FakeGenWithResolved()
    run_iterative_ucmv_generation(
        generator=gen,
        deployer=_FakeDeployer(ok=True),
        reconciler=_ReconGoodBug([70, 100]),
        gen_kwargs={"measures_json": "[]"},
        deploy_kwargs={},
        recon_kwargs={},
        max_cycles=5,
        target_pct=100,
    )
    # Cycle 1: no overrides. Cycle 2: carry forward the passing `good`, NOT `bug`.
    assert gen.manual_overrides_seen[0] is None
    mo = gen.manual_overrides_seen[1]
    assert mo and set(o["name"] for o in mo["fact_a"]) == {"good"}
