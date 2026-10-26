"""Everything that makes a flow able to hold a conversation.

One package rather than eight modules scattered across ``flow_builder/`` and
``flow_builder/modules/``, because they are one feature and they are only
comprehensible together: a channel is defined by its reducer, a turn is defined
by the channels it writes, a thread is what makes two turns one conversation,
and reuse is what stops the second turn paying for the first one's work again.

The pieces, in the order a turn uses them:

* :mod:`channels` — how a write to a channel merges (``replace``, ``append``,
  ``merge``, ``add``). Without a reducer a state can only overwrite, and a
  conversation cannot accumulate.
* :mod:`state_model` — a declared schema compiled into a real state class that
  still answers to dict access, because every condition ever written for a flow
  uses ``state.get("x")``.
* :mod:`turn` — the turn contract: ``ConversationState``, the writes that open a
  turn, and the bookkeeping that closes one.
* :mod:`thread` — the derived key that makes turn N+1 address turn N's
  checkpoint lineage.
* :mod:`reuse` — returning a crew's stored answer instead of running it again.
* :mod:`interrupt` — a human's approval decision, delivered back as state.
* :mod:`lifecycle` — folding a long history into a summary, and noticing two
  turns racing on one thread.
* :mod:`history` — reading a thread's past, and forking from the middle of it.

Import from the submodules, not from here. This ``__init__`` deliberately
re-exports only the handful of names other packages need, so the surface this
feature presents to the rest of the codebase stays small enough to keep honest.
"""

from src.services.flow_builder.conversation.channels import (
    REDUCERS,
    apply_reducer,
    normalize_reducer,
)
from src.services.flow_builder.conversation.interrupt import (
    APPROVAL_CHANNEL,
    APPROVAL_CONFIG_KEY,
    approval_payload,
    interrupt_inputs,
)
from src.services.flow_builder.conversation.state_model import (
    DictLikeState,
    build_state_model,
)
from src.services.flow_builder.conversation.thread import thread_state_uuid
from src.services.flow_builder.conversation.turn import (
    ConversationState,
    append_assistant_message,
    close_turn,
    is_conversational,
    turn_inputs,
)

__all__ = [
    # channels
    "REDUCERS",
    "apply_reducer",
    "normalize_reducer",
    # state
    "DictLikeState",
    "build_state_model",
    # turn
    "ConversationState",
    "append_assistant_message",
    "close_turn",
    "is_conversational",
    "turn_inputs",
    # thread
    "thread_state_uuid",
    # interrupt
    "APPROVAL_CHANNEL",
    "APPROVAL_CONFIG_KEY",
    "approval_payload",
    "interrupt_inputs",
]
