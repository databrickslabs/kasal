"""How a finished flow run ended, read one way by every reader.

``FlowRunnerService.run_flow`` does not raise when a flow fails. A crew error,
an LLM or tool error and the per-crew timeout all come back as a RETURN value,
``{"status": "failed", "error": ...}`` (``FlowExecutionStatus`` is lowercase).

The flow subprocess reads that value twice: to announce the terminal status
ahead of teardown (:func:`early_announcement`), and to build the dict it hands
the parent. The announcement used to check only for a HITL pause, so a failed
flow was announced and stored as COMPLETED, with its error envelope as the
answer, and fired the "completed" triggers and webhooks. The parent's FAILED
write then lost to ``preserve_terminal``, because COMPLETED was already stored.

Both reads go through :func:`is_failed`, so they cannot disagree again.
"""

from typing import NotRequired, Optional, TypedDict

from src.models.execution_status import ExecutionStatus
from src.services.flow_builder.completion_payload import (
    answer_from_result,
    is_paused_for_approval,
)

#: Stored when a failed result carries neither an error nor a message.
DEFAULT_FAILURE_MESSAGE = "Flow execution failed"


class Announcement(TypedDict):
    """Keyword arguments for ``ExecutionStatusService.update_status``."""

    status: str
    message: str
    result: NotRequired[object]


def is_failed(result: object) -> bool:
    """True when ``result`` is a failure, in any shape the flow path returns.

    - ``success`` is False: what a flow body returns;
    - an ``error`` is present: what ``run_flow`` and the subprocess return;
    - ``status`` is FAILED in either case: ``FlowExecutionStatus.FAILED`` is
      ``"failed"`` and ``ExecutionStatus.FAILED`` is ``"FAILED"``.

    A HITL pause is not a failure (it returns ``success=True``), and neither is
    a result that is not a dict.
    """
    if not isinstance(result, dict):
        return False
    if result.get("success") is False or result.get("error"):
        return True
    status = result.get("status")
    return isinstance(status, str) and status.upper() == "FAILED"


def failure_message(result: object) -> str:
    """The error to store and announce for a failed ``result``."""
    if isinstance(result, dict):
        message = result.get("error") or result.get("message")
        if message:
            return str(message)
    return DEFAULT_FAILURE_MESSAGE


def early_announcement(result: object) -> Optional[Announcement]:
    """The status write announcing how a flow ended, or None if it has not.

    The subprocess makes this write ahead of teardown, so the UI hears the
    outcome without waiting for the process to exit. Its status is the shared
    UPPERCASE ``ExecutionStatus``, never ``FlowExecutionStatus``: mixing the two
    once left a finished run at a lowercase "running" that nothing read as
    terminal.

    - A flow that stopped at a HITL gate has NOT finished, so None. It returns
      ``success=True`` and ``status="COMPLETED"`` beside its pause markers;
      announcing on the status alone marked a waiting run as done and showed
      the approval bookkeeping to the reader as their answer.
    - A failure is FAILED with its error as the message and no result, so the
      error envelope is never stored or shown as the answer.
    - Anything else is COMPLETED WITH its answer. An announcement carrying only
      the status leaves the answer to a later write that may never happen, and
      the UI finalises on the first one either way (see ``completion_payload``).
    """
    if is_paused_for_approval(result):
        return None
    if is_failed(result):
        return {
            "status": ExecutionStatus.FAILED.value,
            "message": failure_message(result),
        }
    return {
        "status": ExecutionStatus.COMPLETED.value,
        "message": "Flow execution completed",
        "result": answer_from_result(result),
    }
