"""The checkpoint lifecycle — one vocabulary for every execution type.

    active ──resume──> resumed
      │
      └──expire────> expired

``active`` is set by the recorder on the first unit written and cleared when a
run completes successfully. ``resumed`` marks a checkpoint that has been used
as the source of another execution; ``expired`` marks one a user dismissed.
Both are terminal — neither is listed as resumable again — which is why they
are distinguished at all: "somebody already resumed this" and "somebody threw
this away" are different answers to "why can't I see it?".

Crews had no lifecycle before unification: the flow path drove these three
values through ``ExecutionHistory.checkpoint_status`` and the crew path left the
column NULL. Nothing here is flow-specific, so both paths now use it.

The status lives in a COLUMN rather than inside the JSON record, deliberately:
the list endpoint filters on it, and filtering on a JSON key is a table scan.
"""

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class CheckpointStatus:
    """Values of ``ExecutionHistory.checkpoint_status``."""

    ACTIVE = "active"
    RESUMED = "resumed"
    EXPIRED = "expired"

    ALL = (ACTIVE, RESUMED, EXPIRED)

    #: Only an active checkpoint may be resumed from.
    RESUMABLE = (ACTIVE,)


#: Statuses that mean a run is still IN FLIGHT. These are the only ones that
#: block a resume, because an in-flight execution is still accumulating
#: checkpoint units and resuming it would race the process writing them.
#:
#: Everything terminal is resumable — including COMPLETED. Resuming a run that
#: SUCCEEDED is not a contradiction: it is how you re-run a flow from the middle
#: after changing a downstream crew, keeping the upstream results you were happy
#: with. Restricting this to failed runs treated checkpoints as crash recovery
#: only, which is true of crews and was never true of flows here.
IN_FLIGHT_EXECUTION_STATUSES = frozenset(
    {"RUNNING", "PENDING", "QUEUED", "IN_PROGRESS", "PREPARING"}
)

#: Kept as the positive form for callers that want to name it.
RESUMABLE_EXECUTION_STATUSES = frozenset(
    {"FAILED", "STOPPED", "CANCELLED", "COMPLETED"}
)


def is_resumable_status(checkpoint_status: Optional[str]) -> bool:
    """True when the checkpoint's own lifecycle allows a resume."""
    return (checkpoint_status or "").lower() in CheckpointStatus.RESUMABLE


def is_resumable_execution(execution_status: Optional[str]) -> bool:
    """True when the execution has finished — however it finished.

    Defined as "not in flight" rather than as a list of terminal states, so an
    unfamiliar terminal status blocks nothing. The hazard being guarded against
    is racing a process that is still writing units, not the run's verdict.
    """
    return (execution_status or "").upper() not in IN_FLIGHT_EXECUTION_STATUSES


def resumable_blocker(
    execution_status: Optional[str], checkpoint_status: Optional[str]
) -> Optional[str]:
    """Why this checkpoint cannot be resumed, or None if it can.

    Returns a message fit to show a user: the UI disables a resume control and
    needs to say why, and an endpoint rejecting the call should give the same
    reason rather than a bare 409.
    """
    if not is_resumable_execution(execution_status):
        return (
            f"Execution is still {execution_status} — wait for it to finish "
            f"before resuming, or it will race the run still writing to it"
        )
    if not is_resumable_status(checkpoint_status):
        if (checkpoint_status or "").lower() == CheckpointStatus.RESUMED:
            return "This checkpoint has already been resumed"
        if (checkpoint_status or "").lower() == CheckpointStatus.EXPIRED:
            return "This checkpoint was expired"
        return "This execution has no active checkpoint"
    return None


async def set_status(
    session,
    job_id: str,
    status: Optional[str],
    group_ids: Optional[List[str]] = None,
) -> bool:
    """Move a checkpoint to ``status`` (None clears it)."""
    from src.repositories.execution_history_repository import (
        ExecutionHistoryRepository,
    )

    if status is not None and status not in CheckpointStatus.ALL:
        raise ValueError(f"Unknown checkpoint status: {status}")

    repo = ExecutionHistoryRepository(session)
    ok = await repo.set_checkpoint_status(job_id, status, group_ids=group_ids)
    if ok:
        logger.info(f"[CHECKPOINT] {job_id} → {status or 'cleared'}")
    return ok


async def expire(session, job_id: str, group_ids: Optional[List[str]] = None) -> bool:
    """Dismiss a checkpoint so it stops appearing as resumable.

    The units are left in place: expiring is a listing decision, and keeping
    the outputs means an operator can still inspect what the failed run
    produced.
    """
    return await set_status(session, job_id, CheckpointStatus.EXPIRED, group_ids)


async def mark_resumed(
    session, job_id: str, group_ids: Optional[List[str]] = None
) -> bool:
    """Mark a checkpoint as having been resumed from."""
    return await set_status(session, job_id, CheckpointStatus.RESUMED, group_ids)
