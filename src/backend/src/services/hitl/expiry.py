"""Approval gates nobody decided: expired ones, and ones a restart left open.

Two shapes of gate share the approvals table, and they end differently.

* A FLOW gate suspends its run. The flow stops at the gate and a decision
  relaunches it, so when nobody decides in time the run is failed, but only
  while it is still paused there: a run that was stopped or finished keeps
  the status it reached.
* A TOOL-CALL or TASK-REVIEW gate blocks a worker thread inside a run that is
  still going. The worker polls its own row and carries the run on at its own
  deadline, so the run is left to it. Only when the worker is gone (a server
  restart, or a crashed process) does nothing clear the run's
  WAITING_FOR_APPROVAL, and external callers would see a question nobody can
  answer. Those runs are ended here.

A gate is closed through ``HITLApprovalRepository.update_status``, which writes
only while the row is pending, so it is closed once whoever else is deciding.
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.execution_status import ExecutionStatus
from src.models.hitl_approval import (
    HITLApproval,
    HITLApprovalStatus,
    HITLTimeoutAction,
)
from src.repositories.hitl_repository import HITLApprovalRepository

logger = logging.getLogger(__name__)

#: Gates whose blocked worker polls its own row and carries the run on once the
#: gate is decided or its deadline passes. They never suspend the run.
IN_RUN_GATE_KINDS = ("tool_call", "task_review")

#: How long past ``expires_at`` an in-run gate is left to its worker. The
#: worker's own deadline falls a moment after ``expires_at``; at it the worker
#: applies its policy and restores RUNNING, and a row closed before then would
#: read to it as a rejection. A run still waiting after this has no worker.
IN_RUN_GATE_GRACE = timedelta(minutes=2)

_ABANDONED = "Abandoned: the server restarted while this approval was open"


def is_in_run_gate(approval: HITLApproval) -> bool:
    return (approval.gate_config or {}).get("kind") in IN_RUN_GATE_KINDS


def _as_utc(moment: datetime) -> datetime:
    # SQLite hands timestamps back naive; they were written in UTC.
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


class GateExpiry:
    """Closes gates nobody decided, and ends the runs they would strand."""

    def __init__(self, session: AsyncSession, approval_repo: HITLApprovalRepository):
        self.session = session
        self.approval_repo = approval_repo

    async def process_expired(self) -> List[int]:
        """Close every gate whose time is up. Returns the ids closed."""
        now = datetime.now(timezone.utc)
        closed: List[int] = []
        for approval in await self.approval_repo.get_expired_pending():
            try:
                if await self._expire(approval, now):
                    closed.append(approval.id)
            except Exception:
                logger.exception("Error processing expired approval %s", approval.id)
        return closed

    async def reap_abandoned(self, started_before: datetime) -> List[int]:
        """Close the in-run gates a previous server process left open.

        Their worker lived in that process (a chat turn) or in a run subprocess
        it started, and neither survives a restart. Startup cleanup cancels
        RUNNING runs for the same reason but keeps WAITING_FOR_APPROVAL ones,
        which is right for a flow gate (its decision relaunches the flow) and
        wrong for these. A run is cancelled only while it is still waiting and
        every gate pending on it is an in-run one. Returns the ids closed.
        """
        by_run: Dict[str, List[HITLApproval]] = defaultdict(list)
        for approval in await self.approval_repo.get_pending_created_before(
            started_before
        ):
            by_run[approval.execution_id].append(approval)

        closed: List[int] = []
        for gates in by_run.values():
            if not all(is_in_run_gate(gate) for gate in gates):
                continue
            for gate in gates:
                if await self.approval_repo.update_status(
                    approval_id=gate.id,
                    status=HITLApprovalStatus.TIMEOUT,
                    responded_by="system",
                    rejection_reason=_ABANDONED,
                ):
                    closed.append(gate.id)
            if await self._run_waiting(gates[0]):
                await self._set_run_status(
                    gates[0],
                    ExecutionStatus.CANCELLED.value,
                    "Run cancelled - service was restarted while it waited for "
                    "approval",
                )
        if closed:
            logger.info("Closed %d approvals abandoned by a restart", len(closed))
        return closed

    async def _expire(self, approval: HITLApproval, now: datetime) -> bool:
        in_run = is_in_run_gate(approval)
        if (
            in_run
            and approval.expires_at is not None
            and now < _as_utc(approval.expires_at) + IN_RUN_GATE_GRACE
        ):
            return False  # still its worker's to decide
        auto = approval.timeout_action == HITLTimeoutAction.AUTO_REJECT
        if not await self.approval_repo.update_status(
            approval_id=approval.id,
            status=HITLApprovalStatus.TIMEOUT,
            responded_by="system",
            rejection_reason=(
                "Approval timed out (auto-rejected)" if auto else "Approval timed out"
            ),
        ):
            return False  # decided between the sweep's read and its write

        # A run that moved on (finished, stopped, or carried on by its worker)
        # keeps the status it reached.
        waiting = await self._run_waiting(approval)
        if waiting and not in_run:
            reason = (
                "HITL gate timed out and was auto-rejected"
                if auto
                else "HITL gate timed out"
            )
            await self._set_run_status(
                approval,
                ExecutionStatus.REJECTED.value,
                f"HITL gate rejected: {reason}",
            )
        elif waiting and not await self._other_gate_pending(approval):
            await self._set_run_status(
                approval,
                ExecutionStatus.FAILED.value,
                "Approval timed out and nothing resumed the run: the process "
                "running it is gone",
            )
        logger.info(
            "Processed expired HITL approval %s with action %s",
            approval.id,
            approval.timeout_action,
        )
        return True

    async def _run_waiting(self, approval: HITLApproval) -> bool:
        """Whether the gate's run is still WAITING_FOR_APPROVAL.

        Read, then written, rather than written with ``preserve_terminal``:
        that guard also protects WAITING_FOR_APPROVAL, so it would turn every
        flow-gate timeout into a no-op.
        """
        from src.services.execution.status import ExecutionStatusService

        run = await ExecutionStatusService.get_status(
            approval.execution_id, session=self.session
        )
        status = str(getattr(run, "status", "") or "").upper()
        return status == ExecutionStatus.WAITING_FOR_APPROVAL.value

    async def _other_gate_pending(self, approval: HITLApproval) -> bool:
        others = await self.approval_repo.get_all_for_execution(
            approval.execution_id, approval.group_id
        )
        return any(
            other.id != approval.id and other.status == HITLApprovalStatus.PENDING
            for other in others
        )

    async def _set_run_status(
        self, approval: HITLApproval, status: str, message: str
    ) -> None:
        from src.services.execution.status import ExecutionStatusService

        await ExecutionStatusService.update_status(
            job_id=approval.execution_id,
            status=status,
            message=message,
            session=self.session,
        )
        logger.info(
            "Run %s set to %s after gate %s: %s",
            approval.execution_id,
            status,
            approval.gate_node_id,
            message,
        )
