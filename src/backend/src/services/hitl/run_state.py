"""Putting a run back to RUNNING once the in-run gate it waited on is decided.

The worker blocked on a tool-call or task-review gate restores RUNNING itself.
That used to be unconditional, on the reasoning that a run cannot settle while
its worker is blocked. It can be settled from outside: a restart reaps the gate
and cancels the run, the sweep fails it, or a user stops it. An orphaned worker
that outlived its server then reads the closed gate as a decision and must not
turn that CANCELLED or FAILED run back into a live one.
"""

import logging

from src.models.execution_status import ExecutionStatus

logger = logging.getLogger(__name__)

#: Statuses a gate decision must not overwrite: the run is over, or stopping.
_SETTLED = frozenset(
    {
        ExecutionStatus.COMPLETED.value,
        ExecutionStatus.FAILED.value,
        ExecutionStatus.CANCELLED.value,
        ExecutionStatus.STOPPED.value,
        ExecutionStatus.REJECTED.value,
        ExecutionStatus.STOPPING.value,
    }
)


async def restore_running(execution_id: str, message: str) -> bool:
    """RUNNING again, unless the run was settled meanwhile.

    Returns False only when the write was attempted and failed.
    """
    from src.services.execution.status import ExecutionStatusService

    run = await ExecutionStatusService.get_status(execution_id)
    status = str(getattr(run, "status", "") or "").upper()
    if status in _SETTLED:
        logger.info(
            "Run %s is already %s; its gate decision leaves it so",
            execution_id,
            status,
        )
        return True
    return await ExecutionStatusService.update_status(
        job_id=execution_id,
        status=ExecutionStatus.RUNNING.value,
        message=message,
    )
