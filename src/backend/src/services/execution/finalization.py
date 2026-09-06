"""Persist an execution's outcome independently of whether its worker returned.

Pending outcomes belong to the parent process, not to a live worker. The periodic
cleanup sweep retries them before considering legacy trace-based recovery. This
queue survives transient database failures, but not a restart of this process.
"""

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionOutcome:
    status: str | None
    message: str | None = None
    result: Any = None
    persisted: bool = False


_pending: dict[str, ExecutionOutcome] = {}
_TERMINAL = frozenset({"COMPLETED", "FAILED", "CANCELLED", "STOPPED", "REJECTED"})


def pending_execution_ids() -> frozenset[str]:
    """Runs with known outcomes must never be guessed successful from traces."""
    return frozenset(_pending)


async def persist_execution_outcome(
    execution_id: str,
    outcome: ExecutionOutcome,
    *,
    max_attempts: int = 3,
    retry_delay: float = 0.5,
) -> ExecutionOutcome:
    """Retry only the status write, retaining the actual outcome on failure."""
    from src.services.execution.status import ExecutionStatusService

    if outcome.status is None or outcome.persisted:
        return outcome
    terminal = outcome.status.upper() in _TERMINAL
    if terminal:
        _pending[execution_id] = outcome
    for attempt in range(max_attempts):
        try:
            if await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=outcome.status,
                message=outcome.message or "",
                result=outcome.result,
                **({"preserve_terminal": True} if terminal else {}),
            ):
                if _pending.get(execution_id) is outcome:
                    del _pending[execution_id]
                return replace(outcome, persisted=True)
        except Exception:
            logger.exception("Status persistence failed for execution %s", execution_id)
        if attempt + 1 < max_attempts:
            await asyncio.sleep(retry_delay * 2**attempt)
    logger.error(
        "Execution %s status %s is not persisted (terminal recovery queued: %s)",
        execution_id,
        outcome.status,
        terminal,
    )
    return outcome


async def retry_pending_outcomes() -> int:
    """Called by the existing cleanup sweep; no workload is launched again."""
    recovered = 0
    for execution_id, outcome in list(_pending.items()):
        result = await persist_execution_outcome(execution_id, outcome, max_attempts=1)
        recovered += int(result.persisted)
    return recovered
