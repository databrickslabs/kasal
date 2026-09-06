"""Resolve a run's effort against the actual served model, on both harnesses."""

import logging
from typing import Any

from src.core.llm.effort import EFFORT_PROFILES, resolve_effort
from src.core.llm.model_capabilities import ReasoningStyle, model_capability
from src.core.llm.output_cap import output_cap

logger = logging.getLogger(__name__)


def apply_execution_effort(
    llm: Any, spec: dict, label: str = "", catalog_output_cap=None
) -> None:
    raw = spec.get("execution_effort")
    if raw is None or llm is None or isinstance(llm, str):
        return
    # Retries belong to the execution allowance, not the SDK hidden retry loop.
    llm.max_retries = 0
    resolved = resolve_effort(raw)
    requested = resolved["tier"]
    model = getattr(llm, "model", None)
    capability = model_capability(model)
    # The catalog output setting remains an upper bound; increasing effort
    # must never send a value larger than the configured endpoint can serve.
    configured_cap = None if spec.get("execution_effort_override") else output_cap(llm)
    cap = resolved["max_output_tokens"]
    for ceiling in (configured_cap, catalog_output_cap):
        if isinstance(ceiling, int):
            cap = min(cap, ceiling)
    field = (
        "max_completion_tokens"
        if getattr(llm, "max_completion_tokens", None)
        else "max_tokens"
    )
    setattr(llm, field, cap)
    effective = None
    if capability and capability.efforts:
        # A mixed crew or a model swap may need a lower native tier. Never send
        # an unsupported enum; log the effective value rather than hiding it.
        order = list(EFFORT_PROFILES)
        candidates = [
            e
            for e in capability.efforts
            if e in order and order.index(e) <= order.index(requested)
        ]
        effective = (
            requested
            if requested in capability.efforts
            else (candidates[-1] if candidates else capability.efforts[0])
        )
        if capability.style == ReasoningStyle.ADAPTIVE_EFFORT:
            llm.thinking_effort = effective
            llm.reasoning_effort = None
        else:
            llm.reasoning_effort = effective
    elif capability and capability.style == ReasoningStyle.TOKEN_BUDGET:
        reserve = max(1024, min(4096, cap // 4))
        budget = min(resolved["thinking_tokens"], cap - reserve)
        if budget >= (capability.budget_min or 1):
            llm.thinking_budget_tokens = budget
            effective = f"{budget} thinking tokens"
        else:
            llm.thinking_budget_tokens = None
    logger.info(
        "[effort] agent %s: requested=%s model=%s native=%s output=%s rounds=%s turn=%ss run=%ss",
        label,
        requested,
        model,
        effective or "unavailable (runtime limits only)",
        cap,
        resolved["max_iter"],
        resolved["max_execution_time"],
        resolved["run_max_seconds"],
    )
