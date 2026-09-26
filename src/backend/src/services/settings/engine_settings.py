"""System settings from Configuration → Engines, readable from synchronous code.

These used to be environment variables a Databricks App never sets
(JEV_API_BASE, KASAL_AGENT_MAX_EXECUTION_TIME, KASAL_BUDGET_<MODE>_<FIELD>, and
the server-wide memory/knowledge knobs in :data:`SETTINGS`).
They are ``engine_config`` rows (engine ``kasal``), edited by a system admin.

Some readers are synchronous and run deep in the crew build path — including
inside the crew/flow SUBPROCESS — so they cannot await the database. This
module keeps a small in-process SNAPSHOT of the rows instead:

- the server loads it at startup (``main.py`` lifespan);
- a crew/flow subprocess loads it once when it starts;
- :class:`EngineConfigService` updates it whenever a row is written, so a
  change applies to this server at once and to every run started after it.

An empty snapshot (before loading, or in unit tests) means "use the default",
which is exactly the old behaviour with no environment variable set.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Union

logger = logging.getLogger(__name__)

ENGINE_NAME = "kasal"
ENGINE_TYPE = "system"

JEV_API_BASE = "jev_api_base"
AGENT_MAX_EXECUTION_TIME = "agent_max_execution_time"

#: Built-in defaults (unchanged from the former env defaults).
DEFAULT_AGENT_MAX_EXECUTION_TIME = 900


Scalar = Union[bool, int, float]


@dataclass(frozen=True)
class Setting:
    """One scalar setting under Configuration → Engines → Advanced."""

    default: Scalar
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    #: What the setting replaced, for the docs and the log line.
    replaced: str = ""


MEMORY_SWEEP_ENABLED = "memory_sweep_enabled"
MEMORY_SWEEP_INTERVAL_HOURS = "memory_sweep_interval_hours"
MEMORY_SWEEP_BATCH = "memory_sweep_batch"
MEMORY_MAINTENANCE_INTERVAL = "memory_maintenance_interval"
KNOWLEDGE_MIN_SCORE = "knowledge_min_score"
KNOWLEDGE_MAX_SEARCHES = "knowledge_max_searches"
KNOWLEDGE_TTL_DAYS = "knowledge_ttl_days"

#: Server-wide scalar settings: they act across every workspace (a background
#: sweep, a deployment-wide retention), so they are not per-teamspace tuning.
SETTINGS: Dict[str, Setting] = {
    MEMORY_SWEEP_ENABLED: Setting(True, replaced="KASAL_MEMORY_SWEEP"),
    MEMORY_SWEEP_INTERVAL_HOURS: Setting(
        6.0, 0.25, 24 * 30, "KASAL_MEMORY_SWEEP_INTERVAL_HOURS"
    ),
    MEMORY_SWEEP_BATCH: Setting(5, 1, 100, "KASAL_MEMORY_SWEEP_BATCH"),
    MEMORY_MAINTENANCE_INTERVAL: Setting(
        900, 0, 24 * 3600, "KASAL_MEMORY_MAINTENANCE_INTERVAL"
    ),
    KNOWLEDGE_MIN_SCORE: Setting(0.35, 0.0, 1.0, "KNOWLEDGE_MIN_SCORE"),
    KNOWLEDGE_MAX_SEARCHES: Setting(8, 0, 100, "KNOWLEDGE_MAX_SEARCHES"),
    KNOWLEDGE_TTL_DAYS: Setting(7, 0, 3650, "KNOWLEDGE_TTL_DAYS"),
}


def budget_key(mode: str, field: str) -> str:
    """The row key for one run-budget override, e.g. ``budget_deep_run_wall_clock``."""
    return f"budget_{mode.lower()}_{field.lower()}"


_snapshot: Dict[str, str] = {}


def value(key: str) -> Optional[str]:
    """The stored value, or None when the setting is not set."""
    raw = _snapshot.get(key)
    return raw if raw not in (None, "") else None


def get_int(key: str, default: int, *, minimum: int = 1) -> int:
    """An integer setting, falling back to ``default`` when unset or invalid."""
    raw = value(key)
    if raw is None:
        return default
    try:
        number = int(raw)
    except (TypeError, ValueError):
        logger.warning("Ignoring engine setting %s=%r: not an integer", key, raw)
        return default
    if number < minimum:
        logger.warning("Ignoring engine setting %s=%d: below %d", key, number, minimum)
        return default
    return number


def parse(key: str, raw: Optional[str]) -> Optional[Scalar]:
    """``raw`` as the type of ``SETTINGS[key]``, or None when unusable."""
    spec = SETTINGS[key]
    if raw is None or raw == "":
        return None
    if isinstance(spec.default, bool):
        lowered = raw.strip().lower()
        if lowered in ("true", "1", "on"):
            return True
        if lowered in ("false", "0", "off"):
            return False
        return None
    try:
        number: Scalar = int(raw) if isinstance(spec.default, int) else float(raw)
    except ValueError:
        return None
    if spec.minimum is not None and number < spec.minimum:
        return None
    if spec.maximum is not None and number > spec.maximum:
        return None
    return number


def setting(key: str) -> Any:
    """A :data:`SETTINGS` value: the configured one when usable, else its default."""
    raw = value(key)
    parsed = parse(key, raw)
    if parsed is None:
        if raw is not None:
            logger.warning("Ignoring engine setting %s=%r: out of range", key, raw)
        return SETTINGS[key].default
    return parsed


def agent_max_execution_time() -> int:
    """Wall-clock seconds for one agent call when the agent sets none (0 = off)."""
    return get_int(
        AGENT_MAX_EXECUTION_TIME, DEFAULT_AGENT_MAX_EXECUTION_TIME, minimum=0
    )


def replace(rows: Iterable[Any]) -> None:
    """Replace the snapshot with these ``engine_config`` rows (engine ``kasal``)."""
    _snapshot.clear()
    for row in rows:
        apply_row(row)


def apply_row(row: Any) -> None:
    """Apply one written row (ignored unless it is a ``kasal`` setting)."""
    if getattr(row, "engine_name", None) != ENGINE_NAME:
        return
    key = getattr(row, "config_key", None)
    if not key:
        return
    if getattr(row, "enabled", True) is False:
        _snapshot.pop(key, None)
    else:
        _snapshot[key] = str(getattr(row, "config_value", "") or "")


def forget(key: Optional[str] = None) -> None:
    """Drop one setting (or all) from the snapshot, e.g. after a delete."""
    if key is None:
        _snapshot.clear()
    else:
        _snapshot.pop(key, None)


async def load() -> None:
    """(Re)load the snapshot from the database. Never raises: defaults apply."""
    try:
        from src.db.session import routed_scoped_session
        from src.services.settings.engine import EngineConfigService

        async with routed_scoped_session() as session:
            rows = await EngineConfigService(session).find_all()
        replace(rows)
    except Exception as exc:  # noqa: BLE001 — settings must never block a run
        logger.warning("Could not load engine settings; using defaults: %s", exc)
