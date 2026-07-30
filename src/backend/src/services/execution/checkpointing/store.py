"""Reading and writing the checkpoint record.

This module owns the read-merge-write that keeps the record inside a shared
JSON column: the repository is deliberately shape-blind (it may not import a
service), so knowing which key holds the record, how to merge a unit into it,
and which sibling keys must survive lives here.

Two calling conventions, because there are two callers:

- ``record_unit`` / ``clear`` acquire their own session. They run inside the
  crew and flow SUBPROCESSES, which have no request session, and they are
  fail-open — a checkpoint write must never fail the run it is protecting.
- ``read_record`` / ``write_record`` take a session. They serve the API, where
  the request session owns the transaction.
"""

import logging
from typing import Any, Dict, Optional

from src.services.execution.checkpointing.record import (
    CHECKPOINT_KEY,
    LEGACY_CREW_KEY,
    merge_unit,
    normalize,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Subprocess side: owns its session, never raises
# --------------------------------------------------------------------------


async def record_unit(
    job_id: str,
    kind: str,
    unit: Dict[str, Any],
    unit_count: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """Merge one completed unit into the execution's checkpoint.

    Idempotent: the unit is keyed, so re-recording it overwrites rather than
    appends. Marks the checkpoint 'active' — a checkpoint the list endpoint
    cannot see is a checkpoint nobody can resume from.

    Returns False on any failure rather than raising.
    """
    from src.repositories.execution_history_repository import (
        ExecutionHistoryRepository,
    )
    from src.services.execution.checkpointing.lifecycle import CheckpointStatus
    from src.utils.asyncio_utils import execute_db_operation_smart

    async def _op(session):
        repo = ExecutionHistoryRepository(session)
        existing = await repo.get_checkpoint_data(job_id)

        checkpoint_data = dict(existing or {})
        checkpoint_data[CHECKPOINT_KEY] = merge_unit(
            normalize(checkpoint_data),
            kind=kind,
            unit=unit,
            unit_count=unit_count,
            meta=meta,
        )

        ok = await repo.set_checkpoint_data(
            job_id,
            checkpoint_data,
            checkpoint_status=CheckpointStatus.ACTIVE,
        )
        if ok:
            await session.commit()
        return ok

    try:
        return bool(await execute_db_operation_smart(_op))
    except Exception as e:  # noqa: BLE001 — checkpointing must never break a run
        logger.warning(
            f"[CHECKPOINT] Failed to record {kind} unit "
            f"{unit.get('key')} for {job_id} (non-fatal): {e}"
        )
        return False


async def clear(job_id: str) -> bool:
    """Drop the checkpoint after a successful run.

    Clears the lifecycle status with it: a run that finished has nothing to
    resume, and an 'active' status would keep listing it as resumable. Sibling
    keys in the column (HITL ``edited_config``, ``ucmv_yaml_edits``) survive.
    """
    from src.repositories.execution_history_repository import (
        ExecutionHistoryRepository,
    )
    from src.utils.asyncio_utils import execute_db_operation_smart

    async def _op(session):
        repo = ExecutionHistoryRepository(session)
        existing = await repo.get_checkpoint_data(job_id)

        checkpoint_data = dict(existing or {})
        # Both pops must run: `a or b` would skip the legacy key whenever the
        # current one was present, leaving a stale v0 payload behind to be
        # migrated back into existence on the next read.
        dropped_current = checkpoint_data.pop(CHECKPOINT_KEY, None)
        dropped_legacy = checkpoint_data.pop(LEGACY_CREW_KEY, None)
        had_record = dropped_current is not None or dropped_legacy is not None

        ok = await repo.set_checkpoint_data(
            job_id,
            checkpoint_data or None,
            checkpoint_status=None,
        )
        if ok:
            await session.commit()
            if had_record:
                logger.info(f"[CHECKPOINT] Cleared checkpoint for {job_id}")
        return ok

    try:
        return bool(await execute_db_operation_smart(_op))
    except Exception as e:  # noqa: BLE001
        logger.warning(
            f"[CHECKPOINT] Failed to clear checkpoint for {job_id} (non-fatal): {e}"
        )
        return False


# --------------------------------------------------------------------------
# Request side: caller owns the session
# --------------------------------------------------------------------------


async def read_record(
    session,
    job_id: str,
    group_ids: Optional[list] = None,
) -> Optional[Dict[str, Any]]:
    """Read the normalized checkpoint record for an execution.

    Migrates a pre-unification payload on read; returns None when the execution
    has no checkpoint (or is outside the caller's groups).
    """
    from src.repositories.execution_history_repository import (
        ExecutionHistoryRepository,
    )

    repo = ExecutionHistoryRepository(session)
    checkpoint_data = await repo.get_checkpoint_data(job_id, group_ids=group_ids)
    return normalize(checkpoint_data)


async def write_record(
    session,
    job_id: str,
    record: Optional[Dict[str, Any]],
    checkpoint_status: Any = None,
) -> bool:
    """Write a whole record, preserving the column's other keys.

    Used when seeding a resumed execution's checkpoint from its source, where
    the units are copied wholesale rather than recorded one at a time.
    """
    from src.repositories.execution_history_repository import (
        ExecutionHistoryRepository,
    )

    repo = ExecutionHistoryRepository(session)
    existing = await repo.get_checkpoint_data(job_id)

    checkpoint_data = dict(existing or {})
    if record is None:
        checkpoint_data.pop(CHECKPOINT_KEY, None)
        checkpoint_data.pop(LEGACY_CREW_KEY, None)
    else:
        checkpoint_data[CHECKPOINT_KEY] = record

    return await repo.set_checkpoint_data(
        job_id,
        checkpoint_data or None,
        checkpoint_status=checkpoint_status,
    )
