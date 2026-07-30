"""The checkpoint record: one shape for every execution type.

This is the STORAGE contract — what sits inside the ``checkpoint_data`` JSON
column — not the HTTP one. The Pydantic models the API answers with live in
``src/schemas/execution_history.py``; the two are deliberately separate because
they have different lifecycles: a stored payload must stay readable by the code
that wrote it (hence ``version`` and migrate-on-read), while a response model is
free to change whenever the API does.

A checkpoint answers one question — *what completed, and what did it produce* —
and that question has the same shape for a crew (ordered tasks) and a flow
(ordered crews). Only the definition of a "unit" differs, so only that is left
to the path adapters.

``ExecutionHistory.checkpoint_data`` is a SHARED bag: HITL writes
``edited_config`` and ``ucmv_yaml_edits`` into the same column. The record
therefore lives under ONE key (:data:`CHECKPOINT_KEY`) and every writer merges
rather than replacing the column.

Versioning exists because checkpoint payloads outlive the code that wrote them.
Anything stored before this module existed is treated as ``version 0`` and
migrated on read by :func:`normalize`; nothing is rewritten in place, so an old
row stays readable by old code until it is next written.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Where the record lives inside ExecutionHistory.checkpoint_data.
CHECKPOINT_KEY = "checkpoint"

# The pre-unification crew-only key. Read, never written.
LEGACY_CREW_KEY = "crew_task_checkpoint"

CHECKPOINT_VERSION = 1

# Outputs beyond this size are truncated in the checkpoint (a resume with a
# truncated context beats redoing the unit, but fidelity is flagged rather than
# hidden — `truncated: true` is what the UI surfaces).
MAX_OUTPUT_CHARS = 500_000

KIND_CREW = "crew"
KIND_FLOW = "flow"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_unit(
    key: Any,
    name: Optional[str],
    output_raw: Any,
    *,
    output_json: Optional[Dict[str, Any]] = None,
    agent: Optional[str] = None,
    summary: Optional[str] = None,
    identity: Optional[str] = None,
    completed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build one completed-unit entry, bounding the output.

    Args:
        key: Unit identity within the execution — task index for a crew,
            crew sequence for a flow. Stringified for JSON key stability.
        name: Human-readable unit name (task name / crew name).
        output_raw: The unit's raw output. Coerced to str and truncated.
        output_json: Structured output, when the unit produced one.
        agent: Agent that produced the output, when known.
        summary: Short summary, when the runtime produced one.
        identity: Content-addressed identity (e.g. a crew task's ``key``) used
            to detect that inputs changed since the checkpoint was written.
        completed_at: Override the completion timestamp (tests, backfill).

    Returns:
        A unit dict. ``truncated`` is present only when it is True, so the
        absence of the flag never reads as a claim of full fidelity.
    """
    raw = "" if output_raw is None else str(output_raw)
    truncated = len(raw) > MAX_OUTPUT_CHARS

    if not isinstance(output_json, dict):
        output_json = None

    unit: Dict[str, Any] = {
        "key": str(key),
        "name": name,
        "agent": agent,
        "summary": summary,
        "identity": identity,
        "output_raw": raw[:MAX_OUTPUT_CHARS],
        "output_json": output_json,
        "completed_at": completed_at or _now(),
    }
    if truncated:
        unit["truncated"] = True
    return unit


def empty_record(
    kind: str,
    unit_count: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build an empty record for ``kind``."""
    return {
        "version": CHECKPOINT_VERSION,
        "kind": kind,
        "unit_count": unit_count,
        "units": {},
        "meta": dict(meta or {}),
        "updated_at": _now(),
    }


def merge_unit(
    record: Optional[Dict[str, Any]],
    kind: str,
    unit: Dict[str, Any],
    unit_count: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Merge one unit into a record, returning a NEW dict.

    Keyed by ``unit["key"]``, so a retried write or a resumed run overwrites
    rather than appends — recording the same unit twice is a no-op, which is
    what makes the recorder safe to run on a path that retries.

    A brand-new dict is returned deliberately: mutating the object already on
    the model would not register as a change on a JSON column.
    """
    base = normalize_record(record) or empty_record(kind, unit_count, meta)

    units = dict(base.get("units") or {})
    units[str(unit["key"])] = unit

    merged_meta = dict(base.get("meta") or {})
    merged_meta.update(meta or {})

    return {
        "version": CHECKPOINT_VERSION,
        "kind": kind or base.get("kind"),
        "unit_count": unit_count if unit_count is not None else base.get("unit_count"),
        "units": units,
        "meta": merged_meta,
        "updated_at": _now(),
    }


def normalize(checkpoint_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Read a v1 record out of a raw ``checkpoint_data`` column value.

    Falls back to the pre-unification ``crew_task_checkpoint`` key and migrates
    it on read. Returns None when the column holds no checkpoint at all (it may
    still hold HITL keys — those are not our business).
    """
    if not isinstance(checkpoint_data, dict):
        return None

    current = checkpoint_data.get(CHECKPOINT_KEY)
    if isinstance(current, dict):
        return normalize_record(current)

    legacy = checkpoint_data.get(LEGACY_CREW_KEY)
    if isinstance(legacy, dict):
        return _migrate_legacy_crew(legacy)

    return None


def normalize_record(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Bring a record up to the current version.

    A record with no ``version`` predates versioning and is treated as 0. The
    only v0 shape that ever reached a database is the crew task checkpoint, so
    that is the only migration; an unrecognised shape returns None rather than
    guessing, because a wrong resume is worse than no resume.
    """
    if not isinstance(record, dict):
        return None

    version = record.get("version")

    if isinstance(record.get("units"), dict) and version == CHECKPOINT_VERSION:
        # Already current — fill in optional keys so callers need no getattr dance.
        normalized = dict(record)
        normalized.setdefault("kind", KIND_CREW)
        normalized.setdefault("unit_count", None)
        normalized.setdefault("meta", {})
        return normalized

    if isinstance(record.get("completed"), dict):
        # The v0 crew shape, whose inner "version: 1" meant something else.
        return _migrate_legacy_crew(record)

    logger.warning(
        "Unrecognised checkpoint record (version=%r, keys=%r) — ignoring",
        version,
        sorted(record.keys()),
    )
    return None


def _migrate_legacy_crew(legacy: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Migrate ``crew_task_checkpoint`` (v0) to the unified record.

    v0 shape::

        {"version": 1, "task_count": 3, "process": "sequential",
         "completed": {"0": {"index": 0, "task_key": ..., "output_raw": ...}}}
    """
    completed = legacy.get("completed")
    if not isinstance(completed, dict):
        return None

    units: Dict[str, Any] = {}
    for key, entry in completed.items():
        if not isinstance(entry, dict):
            continue
        unit = dict(entry)
        unit["key"] = str(entry.get("index", key))
        # v0 called the content-addressed identity "task_key".
        unit.setdefault("identity", entry.get("task_key"))
        units[unit["key"]] = unit

    return {
        "version": CHECKPOINT_VERSION,
        "kind": KIND_CREW,
        "unit_count": legacy.get("task_count"),
        "units": units,
        "meta": {"process": legacy.get("process")},
        "updated_at": legacy.get("updated_at"),
        # Callers that report provenance to the user (the UI does) can say so.
        "migrated_from_version": 0,
    }


def ordered_units(record: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Units in execution order.

    Keys are numeric strings, so they sort numerically; a non-numeric key falls
    back to lexical order rather than raising, because one malformed unit must
    not make an otherwise-good checkpoint unreadable.
    """
    if not record:
        return []
    units = record.get("units")
    if not isinstance(units, dict) or not units:
        return []

    def sort_key(item):
        key = item[0]
        try:
            return (0, int(key), "")
        except (TypeError, ValueError):
            return (1, 0, str(key))

    return [unit for _, unit in sorted(units.items(), key=sort_key)]


def unit_preview(unit: Dict[str, Any], limit: int = 200) -> str:
    """First ``limit`` chars of a unit's output, for list views."""
    raw = unit.get("output_raw") or ""
    return str(raw)[:limit]


def is_truncated(record: Optional[Dict[str, Any]]) -> bool:
    """True when any unit's output was bounded — a run-level diagnostic."""
    return any(unit.get("truncated") for unit in ordered_units(record))
