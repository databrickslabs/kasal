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
  (both through ``engine_settings_loader.load``, which reads the database —
  kept out of this module so it stays stdlib-only and ships in exported apps,
  where the snapshot is empty and every setting is its default);
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


Scalar = Union[bool, int, float, str]


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
    # Output design (A2UI)
    "a2ui_enabled": Setting(True, replaced="A2UI_ENABLED"),
    "a2ui_early": Setting(True, replaced="A2UI_EARLY"),
    "a2ui_streaming": Setting(True, replaced="A2UI_STREAMING"),
    "a2ui_stream_interval_ms": Setting(120, 0, 5000, "A2UI_STREAM_INTERVAL_MS"),
    "a2ui_compose_retries": Setting(2, 1, 10, "A2UI_COMPOSE_RETRIES"),
    "a2ui_compose_timeout": Setting(240.0, 10, 1800, "A2UI_COMPOSE_TIMEOUT"),
    # Chat and live output
    "chat_token_streaming": Setting(True, replaced="CHAT_TOKEN_STREAMING"),
    "crew_token_streaming": Setting(True, replaced="CREW_TOKEN_STREAMING"),
    "chat_compaction": Setting(True, replaced="CHAT_COMPACTION"),
    "chat_compaction_keep_rows": Setting(24, 1, 1000, "CHAT_COMPACTION_KEEP_ROWS"),
    "chat_compaction_trigger_chars": Setting(
        8000, 500, 500_000, "CHAT_COMPACTION_TRIGGER_CHARS"
    ),
    "chat_summary_max_chars": Setting(2000, 200, 50_000, "CHAT_SUMMARY_MAX_CHARS"),
    "chat_history_recent_limit": Setting(120, 1, 5000, "CHAT_HISTORY_RECENT_LIMIT"),
    "chat_history_user_char_cap": Setting(
        500, 50, 100_000, "CHAT_HISTORY_USER_CHAR_CAP"
    ),
    "chat_history_assistant_char_cap": Setting(
        240, 0, 100_000, "CHAT_HISTORY_ASSISTANT_CHAR_CAP"
    ),
    "chat_history_last_answer_char_cap": Setting(
        12_000, 0, 500_000, "CHAT_HISTORY_LAST_ANSWER_CHAR_CAP"
    ),
    "chat_history_max_assistant_turns": Setting(
        8, 0, 500, "CHAT_HISTORY_MAX_ASSISTANT_TURNS"
    ),
    "chat_history_max_chars": Setting(6000, 100, 1_000_000, "CHAT_HISTORY_MAX_CHARS"),
    "chat_memory_settle_seconds": Setting(20.0, 0, 600, "CHAT_MEMORY_SETTLE_SECONDS"),
    # Event triggers
    "event_triggers_interval": Setting(5, 1, 3600, "KASAL_EVENT_TRIGGERS_INTERVAL"),
    "event_triggers_batch": Setting(5, 1, 500, "KASAL_EVENT_TRIGGERS_BATCH"),
    "event_triggers_max_hops": Setting(5, 1, 100, "KASAL_EVENT_TRIGGERS_MAX_HOPS"),
    # Workflow recipes (Prompts)
    "workflow_recipe_mine_batch": Setting(100, 1, 10_000, "WORKFLOW_RECIPE_MINE_BATCH"),
    "workflow_recipe_min_similarity": Setting(
        0.75, 0.0, 1.0, "WORKFLOW_RECIPE_MIN_SIMILARITY"
    ),
    "workflow_recipe_exemplars": Setting(True, replaced="WORKFLOW_RECIPE_EXEMPLARS"),
    "workflow_recipe_holdout": Setting(0.0, 0.0, 1.0, "WORKFLOW_RECIPE_HOLDOUT"),
    # Tools
    "dax_llm_batch_size": Setting(12, 1, 200, "DAX_LLM_BATCH_SIZE"),
    "scrape_max_chars": Setting(30_000, 1000, 2_000_000, "SCRAPE_WEBSITE_MAX_CHARS"),
    "scrape_max_fetch_bytes": Setting(
        2_000_000, 10_000, 100_000_000, "SCRAPE_WEBSITE_MAX_FETCH_BYTES"
    ),
    "embedding_batch_size": Setting(32, 1, 2048, "EMBEDDING_BATCH_SIZE"),
    "embedding_timeout_seconds": Setting(
        60.0, 1, 3600, "EMBEDDING_TIMEOUT_SECONDS / EMBEDDING_HTTP_TIMEOUT_SECONDS"
    ),
    # Models
    "fallback_model": Setting("", replaced="KASAL_FALLBACK_MODEL"),
    "embedding_request_timeout_seconds": Setting(
        30.0, 1, 3600, "EMBEDDING_HTTP_TIMEOUT_SECONDS (single text)"
    ),
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
    if isinstance(spec.default, str):
        return raw.strip() or None
    if isinstance(spec.default, bool):
        lowered = raw.strip().lower()
        if lowered in ("true", "1", "on"):
            return True
        if lowered in ("false", "0", "off"):
            return False
        return None
    try:
        number: Union[int, float] = (
            int(raw) if isinstance(spec.default, int) else float(raw)
        )
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
