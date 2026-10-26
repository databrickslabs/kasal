"""``input_required`` — a run that has paused for a human, over either protocol.

This is the piece Kasal has and almost nothing an external agent can call does:
a run that stops mid-task, asks a question, and continues once answered. A2A
models it (``TASK_STATE_INPUT_REQUIRED``); MCP does not model it at all.

Because the round-trip lives here rather than in an adapter, the MCP surface
gets it for the cost of one tool definition — a Claude Code or Cursor user can
answer an approval gate on a Kasal crew. That is the concrete payoff of doing
the two protocols together instead of in sequence.

``services/hitl/`` is unchanged and does the actual work. This is a translation
layer: pending approval → the prompt an external caller sees, and the caller's
reply → approve/reject.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    GoneError,
)
from src.schemas.hitl import (
    HITLActionResponse,
    HITLApprovalResponse,
    HITLApprovalStatusEnum,
)
from src.services.external.identity import ExternalCaller

logger = logging.getLogger(__name__)

#: Replies that mean "no". Everything else is taken as approval WITH the text as
#: a comment — an agent answering a gate will phrase things freely, and reading
#: an ambiguous answer as rejection is the safer direction to be wrong in only
#: if rejection is cheap. It is not: rejection usually fails the run. So the
#: rule is narrow and explicit, and anything unrecognised approves-with-comment
#: rather than guessing.
_REJECTION_WORDS = frozenset(
    {
        "no",
        "reject",
        "rejected",
        "deny",
        "denied",
        "decline",
        "declined",
    }
)


@dataclass(frozen=True)
class PendingInteraction:
    """What the run is waiting to be told."""

    approval_id: int
    prompt: str
    context: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "approval_id": self.approval_id,
            "prompt": self.prompt,
        }
        if self.context:
            payload["context"] = self.context
        return payload


async def _waiting_approvals(
    caller: ExternalCaller, run_id: str, session: AsyncSession
) -> Tuple[Optional[str], List[HITLApprovalResponse]]:
    """The run's undecided approvals, and the teamspace holding them.

    Expired ones included, until the sweep closes them: an answer to one gets
    "expired" from the HITL service rather than "not waiting".

    Every teamspace the caller is scoped to is asked, not only the first: a run
    lives in its publication's teamspace, and an unpinned caller's context lists
    all of theirs, the run's not necessarily first. Asking only ``group_ids[0]``
    missed the run whenever it was published anywhere else. A teamspace the
    caller is not in is never asked.
    """
    from src.services.hitl.service import HITLService

    service = HITLService(session)
    try:
        for group_id in caller.group_ids:
            status = await service.get_execution_hitl_status(
                execution_id=run_id, group_id=group_id
            )
            # The history, not `pending_approval`: that names only one gate,
            # and respond() lets a caller pick among several by approval_id.
            pending = [
                approval
                for approval in status.approval_history
                if approval.status == HITLApprovalStatusEnum.PENDING
            ]
            if pending:
                return group_id, pending
    except Exception as exc:  # noqa: BLE001
        # A run with no HITL configuration is not an error condition, and the
        # status call is an enrichment — never let it break a status poll.
        logger.debug("[external] hitl status skipped for %s: %s", run_id, exc)
    return None, []


async def pending_for_run(
    caller: ExternalCaller, run_id: str, session: Any = None
) -> List[PendingInteraction]:
    """What a paused run is waiting for, if anything.

    An empty list means the run is not waiting — which is the common case, so
    this must be cheap and must not raise when there is no HITL involvement at
    all.
    """
    _, pending = await _waiting_approvals(caller, run_id, session)
    return [
        PendingInteraction(
            approval_id=approval.id,
            prompt=_gate_prompt(approval) or "This run is waiting for approval.",
            # The status read leaves the reviewed output out (it can be ~1 MB
            # and is polled), so this is normally empty.
            context=approval.previous_crew_output,
        )
        for approval in pending
        if not approval.is_expired
    ]


def _gate_prompt(approval: Any) -> Optional[str]:
    """The question configured on the gate, if the author wrote one."""
    config = getattr(approval, "gate_config", None)
    if isinstance(config, dict):
        return config.get("prompt") or config.get("message")
    return None


def _client_error(exc: Exception, approval_id: int) -> Exception:
    """A refused decision as the error both protocols answer with.

    The HITL errors are not HTTP errors, so the MCP and A2A routers answered
    each with a 500. These are the codes the HITL router gives the same
    refusals; anything else is re-raised as it is.
    """
    from src.services.hitl.service import (
        HITLApprovalAlreadyProcessedError,
        HITLApprovalExpiredError,
        HITLApprovalValidationError,
        HITLPermissionDeniedError,
    )

    if isinstance(exc, HITLApprovalAlreadyProcessedError):
        return ConflictError(f"Approval {approval_id} has already been decided.")
    if isinstance(exc, HITLApprovalExpiredError):
        return GoneError(f"Approval {approval_id} has expired.")
    if isinstance(exc, HITLPermissionDeniedError):
        return ForbiddenError(f"You may not decide approval {approval_id}.")
    if isinstance(exc, HITLApprovalValidationError):
        return BadRequestError(str(exc))
    return exc


async def respond(
    caller: ExternalCaller,
    run_id: str,
    response: str,
    approval_id: Optional[int] = None,
    session: Any = None,
) -> Optional[HITLActionResponse]:
    """Answer a paused run: the decision's outcome, or None if not waiting.

    The outcome says whether a flow was relaunched (``execution_resumed``); a
    tool-call or task-review gate's own worker carries its run on instead.

    Resumes on the caller's own token when they presented one (OBO), because
    the resumed work continues under that identity. Without one the Databricks
    auth chain applies, as it does for an approval given through the UI.

    ``approval_id`` is optional: with one gate pending, which one is meant is
    unambiguous, and forcing the caller to fetch an id first turns a one-call
    answer into three.
    """
    token = caller.obo_token()
    group_id, pending = await _waiting_approvals(caller, run_id, session)
    if not group_id or not pending:
        return None

    if approval_id is None:
        # The live gates, or else the one that just expired, which the HITL
        # service then refuses as expired.
        candidates = [a for a in pending if not a.is_expired] or pending
        if len(candidates) > 1:
            raise ValueError(
                f"Run {run_id} has {len(candidates)} pending approvals; "
                "specify approval_id."
            )
        approval_id = candidates[0].id
    elif approval_id not in {approval.id for approval in pending}:
        # An id from another run, or one already decided: this run is not
        # waiting for it.
        return None

    from src.services.hitl.service import HITLService, HITLServiceError

    service = HITLService(session)
    is_rejection = response.strip().lower() in _REJECTION_WORDS

    try:
        if is_rejection:
            outcome = await service.reject(
                approval_id=approval_id,
                rejected_by=caller.identifier,
                group_id=group_id,
                reason=response,
            )
        else:
            outcome = await service.approve(
                approval_id=approval_id,
                approved_by=caller.identifier,
                group_id=group_id,
                comment=response,
                user_token=token,
            )
    except HITLServiceError as exc:
        raise _client_error(exc, approval_id) from exc

    logger.info(
        "[external] %s %s approval %s on run %s",
        caller.origin,
        "rejected" if is_rejection else "approved",
        approval_id,
        run_id,
    )
    return outcome
