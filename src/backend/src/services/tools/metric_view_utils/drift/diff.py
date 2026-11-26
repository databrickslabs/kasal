"""Deterministic drift classification — no LLM, no network.

Every measure on either side ends up in exactly one bucket:

  unchanged          deployed fingerprint == fingerprint of today's DAX
  changed_in_pbi     deployed fingerprint != today's fingerprint (exact)
  legacy_unverified  matched, but the deployed measure predates fingerprinting —
                     handed to the (optional) semantic check, which re-labels it
                     ``possibly_changed`` or ``unchanged_llm``
  new_in_pbi         in PBI, not in any deployed view, and its fact table maps to
                     one of the views being monitored
  unassigned         in PBI, not deployed, and no monitored view hosts its fact
  removed_from_pbi   deployed, tagged as coming from PBI, but PBI no longer has it
                     (flagged only — the baseline keeps it)
  baseline_only      deployed, carries no PBI tag, and matched nothing (a base
                     column measure or a hand-written one) — not tracked
"""

from dataclasses import dataclass, field
from typing import Optional

from src.services.tools.metric_view_utils.drift.fingerprint import dax_fingerprint
from src.services.tools.metric_view_utils.drift.matcher import (
    BaselineMeasure,
    MatchResult,
    PbiMeasure,
)

UNCHANGED = "unchanged"
CHANGED = "changed_in_pbi"
LEGACY = "legacy_unverified"
POSSIBLY_CHANGED = "possibly_changed"
UNCHANGED_LLM = "unchanged_llm"
NEW = "new_in_pbi"
UNASSIGNED = "unassigned"
REMOVED = "removed_from_pbi"
BASELINE_ONLY = "baseline_only"


@dataclass
class MeasureDrift:
    status: str
    original_name: str
    view: Optional[str] = None
    measure_name: Optional[str] = None  # YAML name (baseline, or proposed for new)
    baseline_expr: Optional[str] = None
    current_dax: Optional[str] = None
    baseline_fingerprint: Optional[str] = None
    current_fingerprint: Optional[str] = None
    allocations: list[str] = field(default_factory=list)
    note: Optional[str] = None
    # Filled by later stages
    proposed_expr: Optional[str] = None
    translation: Optional[str] = (
        None  # "llm" | "deterministic" — who wrote proposed_expr
    )
    applied: bool = False
    llm_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v not in (None, [], "")}


def classify(match: MatchResult, pbi: list[PbiMeasure]) -> list[MeasureDrift]:
    by_name = {p.original_name: p for p in pbi}
    out: list[MeasureDrift] = []

    for original, hits in match.matched.items():
        p = by_name[original]
        current_fp = dax_fingerprint(p.dax)
        for b in hits:
            out.append(_matched_drift(p, b, current_fp))

    for p in match.unmatched_pbi:
        view = next(
            (match.fact_to_view[f] for f in p.allocations if f in match.fact_to_view),
            None,
        )
        if not p.dax.strip():
            note = "PBI returned no DAX for this measure — cannot translate"
        elif view is None:
            note = (
                "its fact table ("
                + (", ".join(p.allocations) or "unknown")
                + ") is not one of the monitored metric views"
            )
        else:
            note = None
        out.append(
            MeasureDrift(
                status=NEW if view else UNASSIGNED,
                original_name=p.original_name,
                view=view,
                current_dax=p.dax,
                current_fingerprint=dax_fingerprint(p.dax) or None,
                allocations=p.allocations,
                note=note,
            )
        )

    for b in match.unclaimed_baseline:
        tracked = bool(b.pbi_tag or b.fingerprint)
        out.append(
            MeasureDrift(
                status=REMOVED if tracked else BASELINE_ONLY,
                original_name=b.pbi_tag or b.name,
                view=b.view,
                measure_name=b.name,
                baseline_expr=b.expr,
                baseline_fingerprint=b.fingerprint,
                note=(
                    "no longer in the Power BI model — kept in the metric view; "
                    "remove it manually if intended"
                    if tracked
                    else "no PBI tag and no PBI measure of this name — not tracked"
                ),
            )
        )
    return out


def _matched_drift(p: PbiMeasure, b: BaselineMeasure, current_fp: str) -> MeasureDrift:
    if b.fingerprint:
        status = UNCHANGED if b.fingerprint == current_fp else CHANGED
    else:
        status = LEGACY
    return MeasureDrift(
        status=status,
        original_name=p.original_name,
        view=b.view,
        measure_name=b.name,
        baseline_expr=b.expr,
        current_dax=p.dax,
        baseline_fingerprint=b.fingerprint,
        current_fingerprint=current_fp or None,
        allocations=p.allocations,
    )
