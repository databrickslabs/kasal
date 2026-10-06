"""Iterative generate → deploy → reconcile → refine loop.

The reconciliation layer verifies a UCMV against the live Power BI model. This
module closes the feedback loop: after each generation cycle it reconciles, and
feeds the measures that did NOT match (with sample PBI-vs-UCMV mismatches) back
into the next generation cycle, so the model refines the failing measures — up to
``max_cycles`` or until an overall cell-alignment target is reached.

Design: the three side-effecting steps (generate / deploy / reconcile) are
INJECTED callables, exactly like the reconciliation runner injects its query
executors. The loop itself is pure control flow, so it is unit-tested with fakes
and never touches Databricks or Power BI here. The composing tool
(``ucmv_reconciliation_tool`` / the generator) supplies the real callables.

Note: reconciliation queries a DEPLOYED metric view, so every cycle deploys — a
real, repeated side effect. The loop only runs when the caller has creds + a
warehouse; otherwise generation stays single-pass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

# How many mismatching cells to carry back per measure as feedback — enough to
# show the pattern to the model, not the whole failing set.
_MAX_FEEDBACK_SAMPLES = 5


@dataclass
class CycleResult:
    """Outcome of one generate→deploy→reconcile cycle."""

    cycle: int
    overall_pct: float
    measures_at_100: int
    total_measures: int
    failing_measures: list = field(default_factory=list)
    deploy_ok: bool = True
    note: str = ""


@dataclass
class LoopResult:
    """The whole loop: per-cycle history plus the BEST cycle's artifacts."""

    cycles: list = field(default_factory=list)
    final_yaml: dict = field(default_factory=dict)
    final_reconciliation: dict = field(default_factory=dict)
    stop_reason: str = (
        ""  # target_reached | converged | max_cycles | deploy_failed | no_recon
    )
    best_cycle: int = 0
    best_pct: float = 0.0


def _overall_pct(recon_report: dict) -> Optional[float]:
    """Overall cell-alignment percentage from a reconciliation report, or None
    when nothing was scored (so the loop can stop rather than divide by zero)."""
    overall = (recon_report or {}).get("overall") or {}
    total = overall.get("cells_total")
    if not total:
        return None
    return float(overall.get("overall_cell_pct", 0.0))


def build_refinement_feedback(recon_report: dict, target_pct: float = 100.0) -> list:
    """Turn a reconciliation report into per-measure feedback for the measures
    below ``target_pct``. Each item is self-contained so the DAX-translation step
    can tell the model which measure was wrong and show a few real mismatches:

        {"view", "measure", "pct_aligned", "classification",
         "sample_mismatches": [{dimension, period, ucmv_value, pbi_value, delta}]}

    Only ``real_error`` / unclassified misses are surfaced — a measure classified
    as ``drift`` (PBI not refreshed) or ``known_defect`` is NOT a translation bug,
    so re-prompting on it would be noise.
    """
    feedback: list = []
    for view_name, view in (recon_report or {}).get("views", {}).items():
        for measure, info in (view.get("measures") or {}).items():
            pct = info.get("pct_aligned")
            if pct is None or pct >= target_pct:
                continue
            classification = info.get("classification", "")
            if classification in ("drift", "known_defect", "aligned"):
                continue  # not a translation problem — don't re-prompt on it
            feedback.append(
                {
                    "view": view_name,
                    "measure": measure,
                    "pct_aligned": pct,
                    "classification": classification,
                    "sample_mismatches": (info.get("sample_mismatches") or [])[
                        :_MAX_FEEDBACK_SAMPLES
                    ],
                }
            )
    return feedback


def feedback_to_prompt_map(feedback) -> dict:
    """Normalise reconciliation feedback into ``{measure_name -> hint string}`` for
    the DAX-translation prompt. Accepts the ``build_refinement_feedback`` list OR a
    pre-built ``{measure: hint}`` dict; ignores junk."""
    if isinstance(feedback, dict):
        return {str(k): str(v) for k, v in feedback.items() if v}
    out: dict = {}
    if isinstance(feedback, list):
        for item in feedback:
            if not isinstance(item, dict) or not item.get("measure"):
                continue
            parts = []
            pct = item.get("pct_aligned")
            if pct is not None:
                parts.append(f"only {float(pct):.1f}% of cells matched Power BI")
            for s in (item.get("sample_mismatches") or [])[:3]:
                parts.append(
                    f"[{s.get('dimension')}/{s.get('period')}] "
                    f"UCMV={s.get('ucmv_value')} vs PBI={s.get('pbi_value')}"
                )
            out[str(item["measure"])] = (
                "; ".join(parts) if parts else "did not match Power BI"
            )
    return out


def run_reconciliation_loop(
    *,
    generate: Callable,
    deploy: Callable,
    reconcile: Callable,
    max_cycles: int = 5,
    target_pct: float = 100.0,
) -> LoopResult:
    """Run the iterative loop.

    Injected callables:
    - ``generate(feedback) -> {"yaml": {view: yaml_str}, "mappings": {view: mapping}}``
      (``feedback`` is None on the first cycle, else the list from
      ``build_refinement_feedback``).
    - ``deploy(yaml) -> {"ok": bool, ...}`` — deploys the views; the loop needs a
      live view to reconcile. Return falsy/``ok=False`` to abort the loop.
    - ``reconcile(yaml, mappings) -> recon_report`` — the reconciliation report
      with an ``overall`` block.

    Returns the BEST cycle's YAML + reconciliation (highest overall %), the
    per-cycle history, and why it stopped.
    """
    max_cycles = max(1, int(max_cycles))
    result = LoopResult()
    feedback: Optional[list] = None
    prev_pct: Optional[float] = None

    for cycle in range(1, max_cycles + 1):
        gen = generate(feedback) or {}
        yaml_specs = gen.get("yaml") or {}
        mappings = gen.get("mappings") or {}

        deploy_out = deploy(yaml_specs) or {}
        deploy_ok = bool(deploy_out.get("ok", True))
        if not deploy_ok:
            result.cycles.append(
                CycleResult(
                    cycle=cycle,
                    overall_pct=0.0,
                    measures_at_100=0,
                    total_measures=0,
                    deploy_ok=False,
                    note=str(deploy_out.get("error", "deploy failed")),
                )
            )
            result.stop_reason = "deploy_failed"
            break

        report = reconcile(yaml_specs, mappings) or {}
        pct = _overall_pct(report)
        overall = report.get("overall") or {}
        cr = CycleResult(
            cycle=cycle,
            overall_pct=pct if pct is not None else 0.0,
            measures_at_100=overall.get("measures_at_100", 0),
            total_measures=overall.get("total_measures", 0),
            failing_measures=[
                f["measure"] for f in build_refinement_feedback(report, target_pct)
            ],
        )
        result.cycles.append(cr)

        # Nothing scored → cannot iterate on quality.
        if pct is None:
            result.stop_reason = "no_recon"
            if not result.final_yaml:
                result.final_yaml, result.final_reconciliation = yaml_specs, report
            break

        # Track the best cycle so a later, worse cycle never overwrites a good one.
        if pct > result.best_pct or not result.final_yaml:
            result.best_pct = pct
            result.best_cycle = cycle
            result.final_yaml = yaml_specs
            result.final_reconciliation = report

        if pct >= target_pct:
            result.stop_reason = "target_reached"
            break
        if prev_pct is not None and pct <= prev_pct:
            result.stop_reason = "converged"  # no improvement — stop burning cycles
            break
        prev_pct = pct
        feedback = build_refinement_feedback(report, target_pct)
    else:
        result.stop_reason = "max_cycles"

    return result
