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
from typing import Any, Dict, List, Optional

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


async def pending_for_run(
    caller: ExternalCaller, run_id: str, session: Any = None
) -> List[PendingInteraction]:
    """What a paused run is waiting for, if anything.

    An empty list means the run is not waiting — which is the common case, so
    this must be cheap and must not raise when there is no HITL involvement at
    all.
    """
    group_id = caller.group_context.primary_group_id
    if not group_id:
        return []

    from src.services.hitl.service import HITLService

    try:
        status = await HITLService(session).get_execution_hitl_status(
            execution_id=run_id, group_id=group_id
        )
    except Exception as exc:  # noqa: BLE001
        # A run with no HITL configuration is not an error condition, and the
        # status call is an enrichment — never let it break a status poll.
        logger.debug("[external] hitl status skipped for %s: %s", run_id, exc)
        return []

    pending = getattr(status, "pending_approvals", None) or []
    out: List[PendingInteraction] = []
    for approval in pending:
        out.append(
            PendingInteraction(
                approval_id=getattr(approval, "id", 0),
                prompt=(
                    getattr(approval, "prompt", None)
                    or _gate_prompt(approval)
                    or "This run is waiting for approval."
                ),
                context=getattr(approval, "previous_crew_output", None),
            )
        )
    return out


def _gate_prompt(approval: Any) -> Optional[str]:
    """The question configured on the gate, if the author wrote one."""
    config = getattr(approval, "gate_config", None)
    if isinstance(config, dict):
        return config.get("prompt") or config.get("message")
    return None


async def respond(
    caller: ExternalCaller,
    run_id: str,
    response: str,
    approval_id: Optional[int] = None,
    session: Any = None,
) -> bool:
    """Answer a paused run. True if an approval was actioned.

    Runs on the caller's own token (OBO), because resuming the run continues
    work under that identity — resuming as anyone else would let an external
    caller's approval execute with different access than the caller has.

    ``approval_id`` is optional: with one gate pending, which one is meant is
    unambiguous, and forcing the caller to fetch an id first turns a one-call
    answer into three.
    """
    token = caller.require_obo_token()
    group_id = caller.group_context.primary_group_id
    if not group_id:
        return False

    pending = await pending_for_run(caller, run_id, session=session)
    if not pending:
        return False

    if approval_id is None:
        if len(pending) > 1:
            raise ValueError(
                f"Run {run_id} has {len(pending)} pending approvals; "
                "specify approval_id."
            )
        approval_id = pending[0].approval_id

    from src.services.hitl.service import HITLService

    service = HITLService(session)
    is_rejection = response.strip().lower() in _REJECTION_WORDS

    if is_rejection:
        await service.reject(
            approval_id=approval_id,
            rejected_by=caller.identifier,
            group_id=group_id,
            comment=response,
            user_token=token,
        )
    else:
        await service.approve(
            approval_id=approval_id,
            approved_by=caller.identifier,
            group_id=group_id,
            comment=response,
            user_token=token,
        )

    logger.info(
        "[external] %s %s approval %s on run %s",
        caller.origin,
        "rejected" if is_rejection else "approved",
        approval_id,
        run_id,
    )
    return True
