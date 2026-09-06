"""Provider-independent run allowances. Native reasoning is resolved separately.

These are runtime ceilings, not token spend promises. In particular a tool round
may contain several tool calls, and an output allowance applies to one request.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EffortTier = Literal["none", "minimal", "low", "medium", "high", "xhigh", "max"]


class EffortSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tier: EffortTier = "medium"
    max_iter: int | None = Field(None, ge=1, le=1000)
    max_execution_time: int | None = Field(None, ge=1, le=14400)
    run_max_seconds: int | None = Field(None, ge=1, le=14400)
    max_output_tokens: int | None = Field(None, ge=256, le=262144)


# Changing these changes the API's picker descriptions as well as execution.
EFFORT_PROFILES = {
    "none": dict(
        max_iter=5,
        max_execution_time=90,
        run_max_seconds=120,
        max_output_tokens=4096,
        thinking_tokens=0,
        retries=0,
    ),
    "minimal": dict(
        max_iter=5,
        max_execution_time=90,
        run_max_seconds=120,
        max_output_tokens=4096,
        thinking_tokens=1024,
        retries=0,
    ),
    "low": dict(
        max_iter=8,
        max_execution_time=120,
        run_max_seconds=180,
        max_output_tokens=8192,
        thinking_tokens=1024,
        retries=0,
    ),
    "medium": dict(
        max_iter=15,
        max_execution_time=300,
        run_max_seconds=600,
        max_output_tokens=16384,
        thinking_tokens=4096,
        retries=1,
    ),
    "high": dict(
        max_iter=30,
        max_execution_time=900,
        run_max_seconds=1200,
        max_output_tokens=32768,
        thinking_tokens=8192,
        retries=1,
    ),
    "xhigh": dict(
        max_iter=50,
        max_execution_time=1200,
        run_max_seconds=2400,
        max_output_tokens=65536,
        thinking_tokens=16384,
        retries=2,
    ),
    "max": dict(
        max_iter=80,
        max_execution_time=1800,
        run_max_seconds=3600,
        max_output_tokens=131072,
        thinking_tokens=32768,
        retries=2,
    ),
}


def resolve_effort(settings: EffortSettings | dict) -> dict[str, Any]:
    selected = EffortSettings.model_validate(settings)
    resolved = {"tier": selected.tier, **EFFORT_PROFILES[selected.tier]}
    resolved.update(selected.model_dump(exclude_none=True))
    resolved["max_execution_time"] = min(
        resolved["max_execution_time"], resolved["run_max_seconds"]
    )
    return resolved


def apply_effort_to_config(config: Any) -> Any:
    """Snapshot an explicit run override without changing saved task policies."""
    raw = config.inputs.get("execution_effort") or (
        config.inputs.get("reasoning_config") or {}
    ).get("execution_effort")
    if raw is None:
        config.agents_yaml = {
            key: agent_effort_defaults(spec) for key, spec in config.agents_yaml.items()
        }
        return config
    settings = EffortSettings.model_validate(raw)
    resolved = resolve_effort(settings)
    config.inputs = {
        **config.inputs,
        "execution_effort": settings.model_dump(exclude_none=True),
        "resolved_effort": resolved,
    }
    config.agents_yaml = {
        key: {
            **spec,
            "execution_effort": settings.model_dump(exclude_none=True),
            "execution_effort_override": True,
            "max_iter": resolved["max_iter"],
            "max_execution_time": resolved["max_execution_time"],
        }
        for key, spec in config.agents_yaml.items()
    }
    return config


def agent_effort_defaults(spec: dict) -> dict:
    """A saved profile supplies defaults; explicit agent fields still win.

    Called by API validation and the shared kernel, including catalog/flow
    builds that do not pass through CrewConfig. Never mutates its input.
    """
    raw = spec.get("execution_effort")
    if raw is None:
        return spec
    settings = EffortSettings.model_validate(raw)
    resolved = resolve_effort(settings)
    result = {**spec, "execution_effort": settings.model_dump(exclude_none=True)}
    for key in ("max_iter", "max_execution_time"):
        if result.get(key) is None:
            result[key] = resolved[key]
    return result


def apply_manager_limits(agent, settings):
    """Managers may be created lazily, after the crew stamped worker limits."""
    if agent is None or not settings:
        return
    from src.core.llm.transport.request_deadline import current_deadline

    resolved = resolve_effort(settings)
    for key in ("max_iter", "max_execution_time"):
        object.__setattr__(agent, key, resolved[key])
    object.__setattr__(agent, "max_retry_limit", resolved["retries"])
    deadline = current_deadline()
    if deadline is not None:
        object.__setattr__(agent, "run_deadline", deadline)
        object.__setattr__(agent, "_kasal_run_deadline", deadline)
