"""Task-boundary human approval (HITL) — implemented as a guardrail.

A task with ``human_input: true`` gets this attached as its LAST guardrail:
after the task's output passes any content guardrails, the run pauses, an
``HITLApproval`` row (``gate_config.kind == "task_review"``) is created with
the output attached for review, the UI is notified (SSE ``hitl_request`` /
event-pipe frame — same channel as tool gates), and the worker thread polls
for the decision:

- approve → the guardrail passes and the crew advances to the next task;
- reject  → the reviewer's reason becomes guardrail feedback, so the ENGINE'S
  EXISTING retry loop re-runs the task with that feedback and the new output
  comes back for review again (up to the task's ``max_retries``);
- timeout → per policy: ``timeout_action: "approve"`` proceeds, the default
  rejects, which fails the task rather than looping forever.

Reuses the tool-gate notification transport (in-process SSE vs subprocess
pipe) from ``kernel.tool_approval``.
"""

import logging
import time
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 2.0
_DEFAULT_TIMEOUT_SECONDS = 3600  # reviewing output mid-run can take a while
_OUTPUT_PREVIEW_CHARS = 500


class HumanReviewGuardrail:
    """Engine-guardrail-contract callable: ``(task_output) -> (bool, Any)``."""

    def __init__(
        self,
        task_name: str,
        execution_id: str,
        group_id: str,
        timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
        timeout_action: str = "reject",
    ) -> None:
        self.task_name = task_name
        self.execution_id = execution_id
        self.group_id = group_id
        self.timeout_seconds = int(timeout_seconds)
        self.timeout_action = str(timeout_action).lower()

    # ------------------------------ async I/O ------------------------------

    async def _create(self, raw_output: str) -> Optional[str]:
        from src.db.session import request_scoped_session
        from src.services.hitl_service import HITLService

        gate_config = {
            "kind": "task_review",
            "task_name": self.task_name,
            "timeout_seconds": self.timeout_seconds,
            "timeout_action": self.timeout_action,
            "message": (
                f"Task '{self.task_name}' finished — review the output "
                "before the run continues."
            ),
        }
        async with request_scoped_session() as session:
            service = HITLService(session)
            approval = await service.create_approval_request(
                execution_id=self.execution_id,
                flow_id="",
                gate_node_id=f"task:{self.task_name}",
                crew_sequence=0,
                gate_config=gate_config,
                group_id=self.group_id,
                previous_crew_name=self.task_name,
                previous_crew_output=raw_output,
            )
            await session.commit()
            return str(approval.id) if approval is not None else None

    async def _decision(self, approval_id: str) -> Tuple[Optional[str], Optional[str]]:
        from src.db.session import request_scoped_session
        from src.repositories.hitl_repository import HITLApprovalRepository

        async with request_scoped_session() as session:
            approval = await HITLApprovalRepository(session).get_by_id(approval_id)
            if approval is None:
                return None, None
            status = approval.status
            status = status.value if hasattr(status, "value") else str(status)
            reason = approval.rejection_reason or approval.approval_comment
            return status, reason

    async def _restore_running(self) -> None:
        # create_approval_request flips the execution to WAITING_FOR_APPROVAL;
        # once decided, the run is live again. Safe unconditionally: the run
        # cannot have reached a terminal state while blocked here.
        from src.models.execution_status import ExecutionStatus
        from src.services.execution_status_service import ExecutionStatusService

        await ExecutionStatusService.update_status(
            job_id=self.execution_id,
            status=ExecutionStatus.RUNNING.value,
            message=f"Review of task '{self.task_name}' decided — run continuing",
        )

    # ------------------------------ guardrail ------------------------------

    def __call__(self, task_output: Any) -> Tuple[bool, Any]:
        from src.engines.kasal.kernel.tool_approval import _notify
        from src.engines.kasal.tools.async_bridge import run_async_with_context

        raw = getattr(task_output, "raw", None)
        raw = raw if isinstance(raw, str) else str(task_output)

        try:
            approval_id = run_async_with_context(self._create(raw), timeout=30)
        except Exception as create_err:  # noqa: BLE001
            # Fail closed: a review-required task must not silently advance.
            logger.error(f"[task_review] could not create approval: {create_err}")
            raise RuntimeError(
                f"Task '{self.task_name}' requires human review but the "
                f"approval request could not be created ({create_err})."
            ) from create_err

        logger.info(
            f"[task_review] execution {self.execution_id}: task "
            f"'{self.task_name}' waiting for approval {approval_id}"
        )
        _notify(self.execution_id, {
            "job_id": self.execution_id,
            "approval_id": approval_id,
            "kind": "task_review",
            "task_name": self.task_name,
            "output_preview": raw[:_OUTPUT_PREVIEW_CHARS],
            "message": f"Task '{self.task_name}' finished — review the output.",
        })

        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            try:
                status, reason = run_async_with_context(
                    self._decision(approval_id), timeout=15
                )
            except Exception as poll_err:  # noqa: BLE001
                logger.warning(f"[task_review] poll failed (retrying): {poll_err}")
                status, reason = None, None
            if status == "approved":
                self._best_effort_restore()
                return True, task_output
            if status in ("rejected", "timeout"):
                self._best_effort_restore()
                feedback = (
                    f"The human reviewer rejected this output"
                    f"{f': {reason}' if reason else ''}. "
                    "Address the feedback and produce an improved result."
                )
                return False, feedback
            time.sleep(_POLL_INTERVAL_SECONDS)

        if self.timeout_action == "approve":
            logger.warning(
                f"[task_review] {approval_id} timed out — policy allows proceed"
            )
            self._best_effort_restore()
            return True, task_output
        raise RuntimeError(
            f"Human review of task '{self.task_name}' timed out after "
            f"{self.timeout_seconds}s — the run cannot continue unreviewed."
        )

    def _best_effort_restore(self) -> None:
        from src.engines.kasal.tools.async_bridge import run_async_with_context

        try:
            run_async_with_context(self._restore_running(), timeout=15)
        except Exception as restore_err:  # noqa: BLE001
            logger.debug(f"[task_review] status restore skipped: {restore_err}")
