"""Best-effort classification of a measure's mismatch pattern (pure).

Given a per-measure mismatch summary plus (optional) PBI and UCMV refresh
boundaries, label WHY a measure is sub-100%. This is a heuristic triage aid, not
a verdict — the deterministic value comparison is authoritative; this only helps
a human decide which mismatches to look at first.

Labels:

- ``aligned``: 100% of cells within tolerance — nothing to explain.
- ``known_defect``: value_format is ``percent_string_unscaled``, i.e. a
  confirmed PBI-model defect (a SWITCH branch missing its ``*100``) — the
  reconciler already compensates, residual misalignment here is expected.
- ``drift``: the ONLY failing periods are trailing-edge ones after a refresh
  boundary (PBI or the UCMV materialisation hasn't fully refreshed the most
  recent period(s)) — a timing artefact, not a formula error.
- ``structural``: failure is broad on BOTH axes (most periods AND most
  dimensions) — a filter/formula/scope mismatch in the mapping itself.
- ``real_error``: everything else — a genuine, localised discrepancy worth
  investigating.

Refresh boundaries are period strings ("YYYYNNN") supplied by the caller (this
module never fetches them). When neither boundary is given, the trailing-edge
test is skipped and a period-concentrated failure falls through to real_error.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# A failure counts as "broad" on an axis when at least this fraction of that
# axis's values fail — the threshold separating structural from localised.
_BROAD_FRACTION = 0.6


@dataclass
class MismatchStats:
    """The per-measure inputs classification needs — a subset of a summary row,
    plus the concrete list of failing period strings."""

    pct_aligned: float
    n_periods: int
    periods_failing: int
    n_dims: int
    dims_failing: int
    failing_periods: list = field(default_factory=list)
    value_format: str = "raw_number"


def _all_trailing_edge(failing_periods: list, boundary: Optional[str]) -> bool:
    """True if every failing period is strictly after the refresh boundary — the
    signature of one-sided refresh lag rather than a systematic error."""
    if not failing_periods or boundary is None:
        return False
    return all(str(p) > str(boundary) for p in failing_periods)


def classify(
    stats: MismatchStats,
    *,
    pbi_refresh_period: Optional[str] = None,
    mv_refresh_period: Optional[str] = None,
) -> str:
    """Classify one measure's mismatch pattern. See the module docstring."""
    if stats.pct_aligned >= 100:
        return "aligned"

    if stats.value_format == "percent_string_unscaled":
        return "known_defect"

    # The earlier of the two boundaries is the one that can leave a trailing
    # period unrefreshed on either side.
    boundaries = [b for b in (pbi_refresh_period, mv_refresh_period) if b is not None]
    boundary = min(boundaries) if boundaries else None
    if _all_trailing_edge(stats.failing_periods, boundary):
        return "drift"

    period_broad = stats.n_periods > 0 and (
        stats.periods_failing / stats.n_periods >= _BROAD_FRACTION
    )
    dim_broad = stats.n_dims > 0 and (
        stats.dims_failing / stats.n_dims >= _BROAD_FRACTION
    )
    if period_broad and dim_broad:
        return "structural"

    return "real_error"
