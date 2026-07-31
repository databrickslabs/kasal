"""Letting memories leave the store.

Consolidation removes exact duplicates and supersession retires contradicted
facts, but nothing ever LEAVES: a retired record sits in the table forever, and
an episodic record from a run six months ago is still there competing for the
recall candidate pool. Retention-forever is a recognised failure mode, not a
neutral default.

**Opt-in, and deliberately so.** ``KASAL_MEMORY_FORGETTING`` defaults to off.
This pass deletes rows from a tenant's memory; that is not a decision to make on
someone's behalf via a default, and the value of turning it on scales with store
size, which is exactly what an operator can see and this code cannot.

Two rules, both conservative:

1. **Superseded records past a retention window.** These are already excluded
   from recall (see ``supersession``), so removing them cannot change a single
   recall result — they exist only to answer "what did we believe on date X".
   Dropping them after a window is an ordinary data-retention decision.
2. **Old, low-importance EPISODIC records.** Never semantic or procedural: a
   current fact does not become less true by being old, which is the whole
   point of the kind split.

**What is deliberately NOT implemented: access-based forgetting.** The obvious
rule is "past a TTL *and* never re-accessed", and ``MemoryRecord.last_accessed``
looks like the signal for it. It is not. Nothing refreshes it on recall — both
backends write it at SAVE time only, and neither scoring expression reads it, so
it is inert and always equals the write time. A TTL built on it would silently be
a pure age rule, and would delete the most-used memory in a workspace for the
crime of being old. Making it real means bumping it for every recalled record,
which puts a write on the read path; until that exists, ``importance`` (already
populated by the save-time classifier — routine chatter ~0.3, durable
facts/decisions ~0.8) is the honest proxy for "not worth keeping".
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from src.services.memory.engine import KIND_EPISODIC

logger = logging.getLogger(__name__)

# Bounded like every other maintenance pass — a huge scope must never turn
# teardown into real work.
_SCAN_LIMIT = 500

_DEFAULT_SUPERSEDED_RETENTION_DAYS = 90.0
_DEFAULT_EPISODIC_TTL_DAYS = 180.0
# Records at or above this are kept regardless of age. The classifier puts
# durable facts and decisions well above it and routine chatter well below.
_DEFAULT_IMPORTANCE_FLOOR = 0.4


def forgetting_enabled() -> bool:
    return os.environ.get("KASAL_MEMORY_FORGETTING", "false").lower() == "true"


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _age_days(record: Any) -> float | None:
    """Age of ``record`` in days, or ``None`` when it has no usable timestamp."""
    created = getattr(record, "created_at", None)
    if not isinstance(created, datetime):
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - created).total_seconds() / 86400.0


def _retired_age_days(record: Any) -> float | None:
    """Days since ``record`` was retired, or ``None`` if it is still current."""
    retired = getattr(record, "valid_to", None)
    if not isinstance(retired, datetime):
        return None
    if retired.tzinfo is None:
        retired = retired.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - retired).total_seconds() / 86400.0


def _should_forget(
    record: Any,
    superseded_retention_days: float,
    episodic_ttl_days: float,
    importance_floor: float,
) -> str | None:
    """The reason to drop ``record``, or ``None`` to keep it."""
    retired_for = _retired_age_days(record)
    if retired_for is not None:
        return "superseded" if retired_for >= superseded_retention_days else None

    # Everything below applies to episodic records ONLY. A current fact is not
    # eligible for age-based removal at any age.
    if getattr(record, "kind", KIND_EPISODIC) != KIND_EPISODIC:
        return None
    if float(getattr(record, "importance", 1.0) or 0.0) >= importance_floor:
        return None
    age = _age_days(record)
    if age is None or age < episodic_ttl_days:
        return None
    return "stale_episodic"


def forget_expired_memories(memory: Any, scope: str | None = None) -> dict[str, int]:
    """Delete records past their retention rule. ``{"scanned", "forgotten"}``.

    Off unless ``KASAL_MEMORY_FORGETTING=true``. Best-effort throughout: a
    failure logs and aborts the pass rather than raising into a run.
    """
    stats = {"scanned": 0, "forgotten": 0}
    if memory in (None, True, False) or not forgetting_enabled():
        return stats

    superseded_retention_days = _float_env(
        "KASAL_MEMORY_SUPERSEDED_RETENTION_DAYS", _DEFAULT_SUPERSEDED_RETENTION_DAYS
    )
    episodic_ttl_days = _float_env(
        "KASAL_MEMORY_EPISODIC_TTL_DAYS", _DEFAULT_EPISODIC_TTL_DAYS
    )
    importance_floor = _float_env(
        "KASAL_MEMORY_IMPORTANCE_FLOOR", _DEFAULT_IMPORTANCE_FLOOR
    )

    try:
        records = memory.list_records(scope=scope, limit=_SCAN_LIMIT)
    except Exception as exc:  # noqa: BLE001 — maintenance must never break a run
        logger.debug("Forgetting listing failed: %s", exc)
        return stats
    stats["scanned"] = len(records)

    storage = getattr(memory, "storage", None)
    if not callable(getattr(storage, "delete", None)):
        return stats

    reasons: dict[str, int] = {}
    for record in records:
        reason = _should_forget(
            record, superseded_retention_days, episodic_ttl_days, importance_floor
        )
        if reason is None:
            continue
        try:
            if storage.delete(str(record.id)):
                stats["forgotten"] += 1
                reasons[reason] = reasons.get(reason, 0) + 1
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not forget record %s: %s", record.id, exc)

    if stats["forgotten"]:
        logger.info(
            "Memory forgetting: removed %d record(s) %s (scanned %d, scope=%s)",
            stats["forgotten"],
            reasons,
            stats["scanned"],
            scope or "root",
        )
    return stats


# Kept for callers that want the window without running the pass (metrics, a
# future scheduled sweep that reports what it WOULD remove).
def retention_settings() -> dict[str, float]:
    return {
        "superseded_retention_days": _float_env(
            "KASAL_MEMORY_SUPERSEDED_RETENTION_DAYS",
            _DEFAULT_SUPERSEDED_RETENTION_DAYS,
        ),
        "episodic_ttl_days": _float_env(
            "KASAL_MEMORY_EPISODIC_TTL_DAYS", _DEFAULT_EPISODIC_TTL_DAYS
        ),
        "importance_floor": _float_env(
            "KASAL_MEMORY_IMPORTANCE_FLOOR", _DEFAULT_IMPORTANCE_FLOOR
        ),
    }
