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

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Optional

from ..raw_fields import RawMap, as_int, as_list, as_map, opt_float, str_or

# A reconciliation report / generation output: a JSON-shaped mapping.
GenerateFn = Callable[[Optional[list[RawMap]]], Mapping[str, object] | None]
DeployFn = Callable[[RawMap], Mapping[str, object] | None]
ReconcileFn = Callable[[RawMap, RawMap], Mapping[str, object] | None]

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
    failing_measures: list[str] = field(default_factory=list)
    deploy_ok: bool = True
    note: str = ""


@dataclass
class LoopResult:
    """The whole loop: per-cycle history plus the BEST cycle's artifacts."""

    cycles: list[CycleResult] = field(default_factory=list)
    final_yaml: RawMap = field(default_factory=dict)
    final_reconciliation: RawMap = field(default_factory=dict)
    stop_reason: str = (
        ""  # target_reached | converged | max_cycles | deploy_failed | no_recon
    )
    best_cycle: int = 0
    best_pct: float = 0.0


def _overall_pct(recon_report: RawMap) -> Optional[float]:
    """Overall cell-alignment percentage from a reconciliation report, or None
    when nothing was scored (so the loop can stop rather than divide by zero)."""
    overall = as_map(recon_report.get("overall"))
    total = overall.get("cells_total")
    if not total:
        return None
    return opt_float(overall.get("overall_cell_pct")) or 0.0


def build_refinement_feedback(
    recon_report: RawMap, target_pct: float = 100.0
) -> list[RawMap]:
    """Turn a reconciliation report into per-measure feedback for the measures
    below ``target_pct``. Each item is self-contained so the DAX-translation step
    can tell the model which measure was wrong and show a few real mismatches:

        {"view", "measure", "pct_aligned", "classification",
         "sample_mismatches": [{dimension, period, ucmv_value, pbi_value, delta}]}

    Only ``real_error`` / unclassified misses are surfaced — a measure classified
    as ``drift`` (PBI not refreshed) or ``known_defect`` is NOT a translation bug,
    so re-prompting on it would be noise.
    """
    feedback: list[RawMap] = []
    for view_name, view in as_map(recon_report.get("views")).items():
        for measure, info_raw in as_map(as_map(view).get("measures")).items():
            info = as_map(info_raw)
            pct = opt_float(info.get("pct_aligned"))
            if pct is None or pct >= target_pct:
                continue
            classification = str_or(info.get("classification"), "")
            if classification in ("drift", "known_defect", "aligned"):
                continue  # not a translation problem — don't re-prompt on it
            feedback.append(
                {
                    "view": view_name,
                    "measure": measure,
                    "pct_aligned": pct,
                    "classification": classification,
                    "sample_mismatches": as_list(info.get("sample_mismatches"))[
                        :_MAX_FEEDBACK_SAMPLES
                    ],
                }
            )
    return feedback


def feedback_to_prompt_map(feedback: object) -> dict[str, str]:
    """Normalise reconciliation feedback into ``{measure_name -> hint string}`` for
    the DAX-translation prompt. Accepts the ``build_refinement_feedback`` list OR a
    pre-built ``{measure: hint}`` dict; ignores junk."""
    if isinstance(feedback, Mapping):
        return {str(k): str(v) for k, v in feedback.items() if v}
    out: dict[str, str] = {}
    for item_raw in as_list(feedback):
        if not isinstance(item_raw, Mapping):
            continue
        item = as_map(item_raw)
        if not item.get("measure"):
            continue
        parts: list[str] = []
        pct = opt_float(item.get("pct_aligned"))
        if pct is not None:
            parts.append(f"only {pct:.1f}% of cells matched Power BI")
        for s_raw in as_list(item.get("sample_mismatches"))[:3]:
            s = as_map(s_raw)
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
    generate: GenerateFn,
    deploy: DeployFn,
    reconcile: ReconcileFn,
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
    feedback: Optional[list[RawMap]] = None
    prev_pct: Optional[float] = None

    for cycle in range(1, max_cycles + 1):
        gen = as_map(generate(feedback))
        yaml_specs = as_map(gen.get("yaml"))
        mappings = as_map(gen.get("mappings"))

        deploy_out = as_map(deploy(yaml_specs))
        deploy_ok = bool(deploy_out.get("ok", True))
        if not deploy_ok:
            result.cycles.append(
                CycleResult(
                    cycle=cycle,
                    overall_pct=0.0,
                    measures_at_100=0,
                    total_measures=0,
                    deploy_ok=False,
                    note=str_or(deploy_out.get("error"), "deploy failed"),
                )
            )
            result.stop_reason = "deploy_failed"
            break

        report = as_map(reconcile(yaml_specs, mappings))
        pct = _overall_pct(report)
        overall = as_map(report.get("overall"))
        cr = CycleResult(
            cycle=cycle,
            overall_pct=pct if pct is not None else 0.0,
            measures_at_100=as_int(overall.get("measures_at_100")),
            total_measures=as_int(overall.get("total_measures")),
            failing_measures=[
                str(f["measure"]) for f in build_refinement_feedback(report, target_pct)
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
