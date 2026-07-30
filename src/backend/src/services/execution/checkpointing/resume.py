"""Turning a checkpoint record into something a path can resume from.

The record is storage-shaped and path-neutral. What each path needs to actually
restart is not:

- a **crew** wants the runtime's ``from_checkpoint`` payload — entries keyed
  ``index``/``task_key``, which is the runtime's own vocabulary and is
  translated here rather than by renaming fields inside
  ``services/execution/runtime/`` (that code is deliberately app-agnostic, and
  a rename there would be a change to the engine contract for a storage
  reason);
- a **flow** wants ``{crew_name: output}``, which is what its skipped-crew stub
  methods read.

``from_unit`` means *the unit to resume AT* — everything before it is restored,
it and everything after re-runs. That matches the flow path's existing
``resume_from_crew_sequence`` ("the sequence of the crew TO RUN, not the last
completed") so the two paths cannot drift into off-by-one opposites.
"""

import logging
from typing import Any, Dict, List, Optional

from src.services.execution.checkpointing.record import (
    KIND_CREW,
    KIND_FLOW,
    ordered_units,
)

logger = logging.getLogger(__name__)


def _unit_index(unit: Dict[str, Any]) -> Optional[int]:
    """Numeric position of a unit, or None when it has no usable one."""
    for candidate in (unit.get("index"), unit.get("key")):
        try:
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def select_prefix(
    record: Optional[Dict[str, Any]], from_unit: Optional[Any] = None
) -> List[Dict[str, Any]]:
    """Units to restore, in order.

    With no ``from_unit`` the whole recorded prefix is restored. With one, only
    units ordered strictly before it are — which is how a user rewinds further
    back than the crash point to redo work they did not like.

    An unknown ``from_unit`` restores nothing rather than everything: silently
    treating "resume at a unit that isn't there" as "resume at the end" would
    skip work the user asked to redo.
    """
    units = ordered_units(record)
    if from_unit is None or from_unit == "":
        return units

    try:
        boundary = int(from_unit)
    except (TypeError, ValueError):
        logger.warning("Non-numeric from_unit %r — restoring nothing", from_unit)
        return []

    prefix = []
    for unit in units:
        index = _unit_index(unit)
        if index is None or index >= boundary:
            break
        prefix.append(unit)
    return prefix


def build_crew_payload(
    record: Optional[Dict[str, Any]], from_unit: Optional[Any] = None
) -> Optional[Dict[str, Any]]:
    """Build the runtime's ``from_checkpoint`` payload for a crew.

    Returns None when there is nothing to resume from, which the caller reads
    as "run from scratch" — the same outcome the runtime reaches on any
    validation failure, so a missing checkpoint is never an error.
    """
    if not record or record.get("kind") not in (KIND_CREW, None):
        return None

    units = select_prefix(record, from_unit)
    if not units:
        return None

    entries = []
    for unit in units:
        index = _unit_index(unit)
        if index is None:
            # The runtime rejects the whole payload on a malformed entry, so
            # bailing here gives the same result with a clearer log line.
            logger.warning(
                "Checkpoint unit %r has no usable index — running from scratch",
                unit.get("key"),
            )
            return None
        entries.append(
            {
                # The runtime's vocabulary, not the record's.
                "index": index,
                "task_key": unit.get("identity") or unit.get("task_key"),
                "name": unit.get("name"),
                "agent": unit.get("agent"),
                "summary": unit.get("summary"),
                "output_raw": unit.get("output_raw") or "",
                "output_json": unit.get("output_json"),
            }
        )

    meta = record.get("meta") or {}
    return {
        "version": record.get("version"),
        "task_count": record.get("unit_count"),
        "process": meta.get("process"),
        "completed": entries,
    }


def build_flow_outputs(
    record: Optional[Dict[str, Any]], from_unit: Optional[Any] = None
) -> Dict[str, Any]:
    """Build ``{crew_name: output}`` for a flow resume.

    Reads only what the recorder wrote. The trace reconstruction this
    replaced has been deleted: telemetry is retention-pruned, so a crew whose
    trace row had aged out looked like it never ran.
    """
    if not record or record.get("kind") not in (KIND_FLOW, None):
        return {}

    outputs: Dict[str, Any] = {}
    for unit in select_prefix(record, from_unit):
        name = unit.get("name")
        if not name:
            continue
        outputs[name] = unit.get("output_raw") or ""
    return outputs


def next_unit_index(record: Optional[Dict[str, Any]]) -> int:
    """The first unit that did NOT complete — where an unqualified resume starts.

    Derived from the contiguous prefix rather than the unit count, because a
    gap means the units after it cannot be trusted as context for a re-run.
    """
    expected = 0
    for unit in ordered_units(record):
        index = _unit_index(unit)
        if index != expected:
            break
        expected += 1
    return expected
