"""The key that makes turn N+1 a continuation of turn N.

A flow's checkpoints are stored under its ``state.id``, and that id is minted
fresh (``uuid4``) on every construction — so two runs of the same flow in the
same conversation are two unrelated lineages, and nothing carries forward. A
thread is just a STABLE id used across turns; the restore path already exists.

Why the chat session id is not used directly
============================================

It is the obvious candidate — ``session_id`` is already on ``ExecutionRequest``
(it scopes memory recall) and is stable across the messages of a conversation.
But one session routes to MANY capabilities: that is exactly what "Use
existing" does. ``flow_states`` is keyed by a single ``flow_uuid``, so using
the session id as that key would put two different flows in one lineage, and
turn 2 of one flow would restore the state of the other.

LangGraph hits the same wall and answers it with a ``checkpoint_ns`` beside
``thread_id``. Here the namespace is the flow, folded into a deterministic id.

Derived, not stored
===================

``uuid5`` of ``group:session:flow`` is the same value every time, so turn N+1
addresses turn N's lineage without a mapping table to keep in sync — the
pattern crew memory ids already use (a deterministic hash including
``group_id``). A row keyed BY this value is still worth adding later for what
derivation cannot express — forking to a NEW lineage, a concurrency guard,
turn counts — and because the row would be keyed by the derived value, adding
it is not a migration of identity.
"""

import logging
import uuid
from typing import Optional

logger = logging.getLogger(__name__)

#: Fixed namespace for flow threads. Never change it: every existing thread's
#: id is derived through it, and a new namespace orphans every conversation.
FLOW_THREAD_NAMESPACE = uuid.UUID("6f1b5c8e-4f3a-5d2b-9c7e-0a1b2c3d4e5f")


def thread_state_uuid(
    session_id: Optional[str],
    flow_id: Optional[str],
    group_id: Optional[str] = None,
) -> Optional[str]:
    """The checkpoint lineage for one conversation with one flow.

    Args:
        session_id: The conversation. A chat session id, or any caller-supplied
            correlation id that is stable across the turns of one exchange.
        flow_id: Which flow definition. Namespaces the lineage, so a session
            that talks to two flows keeps two histories.
        group_id: Tenant scope, for the same reason every other key here has
            one — two groups must not derive the same id from the same session
            id, however unlikely that collision is.

    Returns:
        A stable UUID string, or None when there is no session to thread on.
        None is the signal for "run this as a one-shot", which is what every
        flow does today.
    """
    if not session_id or not flow_id:
        return None
    seed = f"{group_id or ''}:{session_id}:{flow_id}"
    return str(uuid.uuid5(FLOW_THREAD_NAMESPACE, seed))
