"""Shared checkpointing for every execution type.

    record.py     the storage contract — one shape, versioned, migrate-on-read
    store.py      read/write it, inside a subprocess or a request
    recorder.py   the event-driven, idempotent, bounded, fail-open base
    lifecycle.py  active | resumed | expired
    resume.py     record → the payload a given path restarts from

What is common is the CONTRACT, not the implementation: the units of work
genuinely differ (a crew has an ordered task list, a flow a method graph). What
a checkpoint CONTAINS and how it is APPLIED stay with the path, in
``agent_builder/checkpoint_adapter.py`` and
``flow_builder/checkpoint_adapter.py``.

The chat path (``execution_type="agent"``) has NO checkpointing and should not
get any: it is a single in-process ``Agent.kickoff_async``, sub-second, with
nothing to resume. Stated here so the next person unifying something does not
add a third adapter for it.
"""

from src.services.execution.checkpointing.lifecycle import (
    RESUMABLE_EXECUTION_STATUSES,
    CheckpointStatus,
    is_resumable_execution,
    is_resumable_status,
    resumable_blocker,
)
from src.services.execution.checkpointing.record import (
    CHECKPOINT_KEY,
    CHECKPOINT_VERSION,
    KIND_CREW,
    KIND_FLOW,
    LEGACY_CREW_KEY,
    MAX_OUTPUT_CHARS,
    build_unit,
    is_truncated,
    normalize,
    ordered_units,
    unit_preview,
)
from src.services.execution.checkpointing.recorder import CheckpointRecorder
from src.services.execution.checkpointing.resume import (
    build_crew_payload,
    build_flow_outputs,
    next_unit_index,
    select_prefix,
)

__all__ = [
    "CHECKPOINT_KEY",
    "CHECKPOINT_VERSION",
    "CheckpointRecorder",
    "CheckpointStatus",
    "KIND_CREW",
    "KIND_FLOW",
    "LEGACY_CREW_KEY",
    "MAX_OUTPUT_CHARS",
    "RESUMABLE_EXECUTION_STATUSES",
    "build_crew_payload",
    "build_flow_outputs",
    "build_unit",
    "is_resumable_execution",
    "is_resumable_status",
    "is_truncated",
    "next_unit_index",
    "normalize",
    "ordered_units",
    "resumable_blocker",
    "select_prefix",
    "unit_preview",
]
