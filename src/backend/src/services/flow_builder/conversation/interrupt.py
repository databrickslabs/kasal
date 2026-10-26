"""Delivering a human's decision back into the flow that asked for it.

A HITL gate already stops a flow and records what a person decided —
``HITLApproval`` carries the status, the comment, who responded and, on a
rejection, what to do next. What was missing is the return path: the resumed
run restored its state and continued as though nothing had been asked, so a
flow could pause for an answer and then had no way to READ it. Anything the
approver typed reached the database and stopped there.

This is the half LangGraph expresses as ``interrupt()`` returning whatever
``Command(resume=...)`` supplied. Kasal has the pause; this gives the value.

The decision lands in a normal state channel, not a side table, and that is the
point: once it is state, a router condition reads it the same way it reads
anything else —

    state.get("approval", {}).get("status") == "approved"

— so an approval gate and a conversational turn stop being two mechanisms. Both
are "restore the thread, apply an input, continue".
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

#: The channel a decision is written to. A flow reads it by declaring a channel
#: of this name; one well-known name means a gate's condition looks the same in
#: every flow.
APPROVAL_CHANNEL = "approval"

#: Where the decision travels on the resume run's config.
APPROVAL_CONFIG_KEY = "approval_decision"


def approval_payload(approval: Any) -> Dict[str, Any]:
    """The part of an approval record a flow should be able to branch on.

    Deliberately not the whole row. Ids, webhook bookkeeping and timestamps of
    the gate's own lifecycle are Kasal's business; what the FLOW needs is what
    was decided, by whom, and anything they wrote — the things a condition or a
    downstream task could reasonably use.
    """
    responded_at = getattr(approval, "responded_at", None)
    return {
        "status": getattr(approval, "status", None),
        "comment": getattr(approval, "approval_comment", None),
        "rejection_reason": getattr(approval, "rejection_reason", None),
        "rejection_action": getattr(approval, "rejection_action", None),
        "responded_by": getattr(approval, "responded_by", None),
        "responded_at": responded_at.isoformat() if responded_at else None,
        "gate_node_id": getattr(approval, "gate_node_id", None),
    }


def declares_channel(state_config: Any, channel: str = APPROVAL_CHANNEL) -> bool:
    """Whether a flow's declared state has room for this channel.

    Checked rather than assumed, because a TYPED state raises on an input it has
    no channel for — which is the behaviour that makes a misspelled input
    visible, and it must not be turned into "approving a gate crashes the
    resume". A flow that never declared the channel simply does not receive the
    decision, exactly as before.
    """
    if not isinstance(state_config, dict):
        return False
    model = state_config.get("model")
    if not isinstance(model, dict):
        return False
    properties = model.get("properties")
    return isinstance(properties, dict) and channel in properties


def interrupt_inputs(
    state_config: Any, decision: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """The state writes that carry a decision into the resumed run.

    Empty when there is no decision, or when the flow did not declare the
    channel to receive one.
    """
    if not decision:
        return {}
    if not declares_channel(state_config):
        logger.info(
            "[flow-interrupt] a decision was available but this flow declares no "
            "%r channel; add one to branch on it",
            APPROVAL_CHANNEL,
        )
        return {}
    return {APPROVAL_CHANNEL: decision}
