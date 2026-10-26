"""The canonical EXTERNAL task-state vocabulary, and the only translation of it.

Kasal is exposed over two protocols. If each adapter mapped ``ExecutionStatus``
itself, the MCP and A2A surfaces would eventually disagree about whether a run is
``running`` or ``working`` — a bug report nobody can reproduce, because each
surface looks correct on its own.

So the mapping is here, once, and the adapters are forbidden from re-deriving it.

**The vocabulary is A2A's**, deliberately, for both protocols:

* it is the only one of the candidates that models the whole lifecycle,
  including a run paused for a human and a run blocked on authentication;
* it is a published standard, so external callers are not learning a Kasal
  invention;
* ``ExecutionStatus`` already maps onto it nine-of-ten;
* MCP defines no task lifecycle at all, so an MCP tool returning
  ``{"state": "input_required"}`` is not bending MCP — it is filling a gap MCP
  leaves to the application.

That last point is the concrete sense in which the MCP surface "integrates A2A":
it inherits human-in-the-loop pauses because the modelling was done once, here.
"""

from enum import Enum
from typing import Optional

from src.models.execution_status import ExecutionStatus


class ExternalTaskState(str, Enum):
    """A2A's task states, used as Kasal's canonical external vocabulary."""

    SUBMITTED = "submitted"
    WORKING = "working"
    INPUT_REQUIRED = "input_required"
    AUTH_REQUIRED = "auth_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    REJECTED = "rejected"


#: The whole translation. Several Kasal statuses collapse onto one external
#: state on purpose: an external caller has no use for the distinction between
#: PENDING and PREPARING, or between STOPPING, STOPPED and CANCELLED — it needs
#: to know whether to keep polling.
_STATUS_TO_EXTERNAL = {
    ExecutionStatus.PENDING: ExternalTaskState.SUBMITTED,
    ExecutionStatus.PREPARING: ExternalTaskState.SUBMITTED,
    ExecutionStatus.RUNNING: ExternalTaskState.WORKING,
    ExecutionStatus.WAITING_FOR_APPROVAL: ExternalTaskState.INPUT_REQUIRED,
    ExecutionStatus.COMPLETED: ExternalTaskState.COMPLETED,
    ExecutionStatus.FAILED: ExternalTaskState.FAILED,
    ExecutionStatus.STOPPING: ExternalTaskState.CANCELED,
    ExecutionStatus.STOPPED: ExternalTaskState.CANCELED,
    ExecutionStatus.CANCELLED: ExternalTaskState.CANCELED,
    ExecutionStatus.REJECTED: ExternalTaskState.REJECTED,
}

#: States after which polling is pointless — the run will not change again.
TERMINAL_STATES = frozenset(
    {
        ExternalTaskState.COMPLETED,
        ExternalTaskState.FAILED,
        ExternalTaskState.CANCELED,
        ExternalTaskState.REJECTED,
    }
)


def to_external_state(status: Optional[str]) -> ExternalTaskState:
    """Map a Kasal execution status onto the canonical external state.

    Accepts the raw string as persisted in ``execution_history.status``, since
    that is what callers actually hold.

    An unrecognised status maps to ``WORKING``, not ``FAILED``: a status this
    layer has not been taught about means Kasal grew a state, and reporting a
    live run as failed would make an external client give up on a run that is
    still going. ``WORKING`` keeps it polling until the run reaches a state that
    IS known — every terminal status is in the table above.

    ``AUTH_REQUIRED`` is never produced here. It has no ``ExecutionStatus``
    counterpart because it is not a property of a run; it is the answer to an
    invocation that could not start. The adapters raise it directly.
    """
    if status is None:
        return ExternalTaskState.WORKING

    try:
        parsed = ExecutionStatus(str(status).upper())
    except ValueError:
        return ExternalTaskState.WORKING

    return _STATUS_TO_EXTERNAL.get(parsed, ExternalTaskState.WORKING)


def is_terminal(state: ExternalTaskState) -> bool:
    """True when a caller can stop polling."""
    return state in TERMINAL_STATES
