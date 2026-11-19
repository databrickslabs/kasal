"""Optional gate: deploy only the metric views that were compared with Power BI and reached a bar.

Off unless ``min_reconciled_pct`` is set in the deployer's configuration (it is not an
argument an agent can pass). With it, a view is held back, and reported as skipped with the
reason, when the verify-numbers output the deployer received shows it below the bar or not
compared at all. An input without a reconciliation holds every view back: a bar that cannot
be checked must not pass silently.

Pure functions: no I/O.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping

from src.services.tools.metric_view_utils.raw_fields import as_map

Held = dict[str, dict[str, object]]


def _as_output(ucmv_output: object) -> Mapping[str, object]:
    if isinstance(ucmv_output, str):
        try:
            return as_map(json.loads(ucmv_output))
        except ValueError:
            return {}
    return as_map(ucmv_output)


def _bar(value: object) -> float | None:
    try:
        bar = float(str(value))
    except (TypeError, ValueError):
        return None
    return bar if bar > 0 else None


def held_back(
    yaml_specs: Mapping[str, object], ucmv_output: object, min_reconciled_pct: object
) -> Held:
    """``{view: skipped result}`` for the views the bar holds back; empty when no bar is set."""
    bar = _bar(min_reconciled_pct)
    if bar is None:
        return {}
    output = _as_output(ucmv_output)
    if "reconciliation" not in output:
        reason = (
            f"held back: min_reconciled_pct is {bar:g} but the input carries no comparison "
            "with Power BI (it needs a verify-numbers output)"
        )
        return {view: {"status": "skipped", "message": reason} for view in yaml_specs}
    compared = as_map(as_map(output.get("reconciliation")).get("views"))
    held: Held = {}
    for view in yaml_specs:
        if not str(yaml_specs.get(view) or "").strip():
            continue  # no metric view at all: reported by the deployer as before
        pct = as_map(as_map(compared.get(view)).get("summary")).get("overall_cell_pct")
        if isinstance(pct, (int, float)) and not isinstance(pct, bool):
            if pct >= bar:
                continue
            reason = f"held back: {pct:g} % of cells match Power BI, below the bar of {bar:g} %"
        else:
            reason = f"held back: not compared with Power BI (bar {bar:g} %)"
        held[view] = {"status": "skipped", "message": reason}
    return held


def gate(
    yaml_specs: Mapping[str, object], setting: Callable[[str], object]
) -> tuple[dict[str, object], Held]:
    """Apply both opt-in settings of the deployer: ``(specs, held back)``. The specs carry
    the notes on unreconciled measures when ``flag_unreconciled_measures`` is set; the held
    views are those ``min_reconciled_pct`` keeps from being deployed."""
    from .reconciliation_flags import flag_unreconciled

    output = setting("ucmv_output")
    specs = flag_unreconciled(yaml_specs, output, setting("flag_unreconciled_measures"))
    return specs, held_back(specs, output, setting("min_reconciled_pct"))
