"""System settings from Configuration → Engines, readable from synchronous code.

These used to be environment variables a Databricks App never sets
(JEV_API_BASE, KASAL_AGENT_MAX_EXECUTION_TIME, KASAL_BUDGET_<MODE>_<FIELD>).
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
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)

ENGINE_NAME = "kasal"
ENGINE_TYPE = "system"

JEV_API_BASE = "jev_api_base"
AGENT_MAX_EXECUTION_TIME = "agent_max_execution_time"

#: Built-in defaults (unchanged from the former env defaults).
DEFAULT_AGENT_MAX_EXECUTION_TIME = 900


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
