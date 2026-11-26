"""How a measure's SQL was reached, recorded per measure as ``source``.

A run's ``resolved_measures_by_table`` says, per measure, whether its SQL came from a rule, from
the LLM, from a rerun pin (``reused``), from the loop's carry-forward, or from a manual override.
Two runs can then be compared without guessing which path produced a measure.
"""

from __future__ import annotations

# The override comments that tell a pin and a carry-forward apart from a manual override.
REUSED_PIN_COMMENT = (
    "reused from the previous run (reconciled at or above the lock threshold)"
)
CARRIED_FORWARD_COMMENT = "carried forward (reconciled in prior cycle)"


def translation_source(
    category: str | None, sql_expr: str | None, skip_reason: str | None
) -> str:
    """``llm``, ``reused``, ``carried_forward``, ``manual``, ``rule``, or ``none`` (no SQL).

    An override is an ``manual_override`` category whose ``skip_reason`` carries the comment it
    was given; the LLM and rule results are labelled by their category."""
    if category == "llm_translated":
        return "llm"
    if category == "manual_override":
        if skip_reason == REUSED_PIN_COMMENT:
            return "reused"
        if skip_reason == CARRIED_FORWARD_COMMENT:
            return "carried_forward"
        return "manual"
    return "rule" if sql_expr else "none"
