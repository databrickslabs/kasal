"""Execution caps, resolved from the ChatMode answer mode.

Before this module the caps lived in three unrelated places — the engine's own
``Agent`` field defaults, a ``DEFAULT_AGENT_MAX_EXECUTION_TIME`` env var applied
in the kernel, and a hardcoded round cap in the LLM transport — and generated
crews set none of them, so chat, research and deep all ran on identical bare
defaults. A mode that is meant to think harder and longer than another has to
say so somewhere.

One cap is new rather than merely relocated. ``run_wall_clock`` bounds the WHOLE
run: ``max_execution_time`` is per agent call and a fresh deadline is computed
on every call, so a six-task crew's real ceiling was 6 × the per-call cap — and
the guardrail retry loop multiplied it again, since each retry re-enters
``agent.execute_task``. With retries switched on for deep mode that compounds to
hours, which is why the budget work ships WITH the guardrail work, not after it.

A run-level TOKEN budget belongs here too and is deliberately absent: nothing
enforces one yet, and this codebase already carries ``max_rpm`` — declared on
Agent and Crew, plumbed through the config builder, enforced nowhere. A knob
that looks like a cap and is not one is worse than no knob. It lands with its
enforcement or not at all. (When it does: a token ceiling, not a dollar one —
tokens are counted exactly and for free by the LLM layer, dollars need
per-model pricing that exists nowhere in this codebase.)

The numbers are a calibrated starting point, not a measurement. Every field is
env-overridable (``KASAL_BUDGET_<MODE>_<FIELD>``, e.g.
``KASAL_BUDGET_DEEP_RUN_WALL_CLOCK=7200``) so ops can retune without a deploy.
"""

import logging
import os
from dataclasses import dataclass, fields
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BudgetProfile:
    """Resolved caps for one execution."""

    #: Tool-calling rounds allowed within a single agent call.
    max_iter: int
    #: Wall-clock seconds for a single agent call.
    max_execution_time: int
    #: Wall-clock seconds for the entire run, retries included.
    run_wall_clock: int
    #: Guardrail rejections a task may be retried through.
    guardrail_max_retries: int


#: Only ``deep`` is APPLIED today (see generation/crew/answer_mode.GATED_MODES).
#: The chat and research rows are the floor this function falls back to and the
#: shape a future widening would take — they are not currently stamped onto any
#: run, so chat and research keep the bare engine defaults they have always had.
_PROFILES = {
    "chat": BudgetProfile(
        max_iter=8,
        max_execution_time=120,
        run_wall_clock=180,
        guardrail_max_retries=0,
    ),
    "research": BudgetProfile(
        max_iter=15,
        max_execution_time=300,
        run_wall_clock=900,
        guardrail_max_retries=1,
    ),
    "deep": BudgetProfile(
        max_iter=30,
        max_execution_time=600,
        run_wall_clock=3600,
        guardrail_max_retries=3,
    ),
}

DEFAULT_MODE = "chat"


def _env_override(mode: str, field_name: str, current: int) -> int:
    raw = os.environ.get(f"KASAL_BUDGET_{mode.upper()}_{field_name.upper()}")
    if raw is None:
        return current
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "ignoring KASAL_BUDGET_%s_%s=%r: not an integer",
            mode.upper(),
            field_name.upper(),
            raw,
        )
        return current
    if value <= 0:
        # 0 would mean "no rounds at all" rather than "unlimited", which is a
        # footgun disguised as a kill switch.
        logger.warning(
            "ignoring KASAL_BUDGET_%s_%s=%d: must be positive",
            mode.upper(),
            field_name.upper(),
            value,
        )
        return current
    return value


def resolve_budget_profile(mode: Optional[str]) -> BudgetProfile:
    """Caps for a ChatMode answer mode (``chat`` / ``research`` / ``deep``).

    An unknown mode resolves to ``chat`` — the tightest profile — so a typo or a
    future mode name cannot accidentally buy an hour of runtime.
    """
    key = (mode or DEFAULT_MODE).strip().lower()
    base = _PROFILES.get(key)
    if base is None:
        logger.info("unknown answer mode %r; using the %s budget", mode, DEFAULT_MODE)
        key, base = DEFAULT_MODE, _PROFILES[DEFAULT_MODE]

    overridden = {
        f.name: _env_override(key, f.name, getattr(base, f.name))
        for f in fields(BudgetProfile)
    }
    return BudgetProfile(**overridden)
