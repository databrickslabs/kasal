"""The state a flow carries when it is holding a conversation.

A one-shot flow answers a request. A conversational flow answers a TURN — it
needs to know what was said before, what was just said, and what the caller
appears to want. Those are the same four things in every system that does this
(LangGraph's ``messages`` channel with ``add_messages``, CrewAI's ``ChatState``
with ``messages``/``last_user_message``/``last_intent``/``session_ready``), so
the field names here follow CrewAI's — a flow author reading either set of docs
should not have to translate.

What makes it work is not the fields but the reducer on ``messages``. Kasal
runs every flow in a fresh subprocess, so turn 2 is a new process with a new
``Flow`` object; continuity comes entirely from restoring the checkpoint and
MERGING the new turn into it. Without ``append`` on that channel, the restore
loads the history and the turn's write immediately replaces it — the flow would
remember exactly one message, which is indistinguishable from remembering
nothing.
"""

import logging
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import Field

from src.services.flow_builder.conversation.channels import APPEND
from src.services.flow_builder.conversation.state_model import DictLikeState

logger = logging.getLogger(__name__)

#: Cap on how many messages a thread carries forward. A conversation that grows
#: without bound makes every later turn slower and more expensive, and the whole
#: history is on the checkpoint anyway — this trims what the FLOW sees, not what
#: was recorded. Summarisation (the pattern `chat_sessions.context_summary`
#: already uses) is the eventual answer; a cap is the honest interim one.
MAX_THREAD_MESSAGES = 100

USER = "user"
ASSISTANT = "assistant"


class ConversationState(DictLikeState):
    """Flow state plus the turn contract.

    A flow's own declared channels are added on top of this by
    ``build_state_model(schema, base=ConversationState)``.
    """

    __reducers__: ClassVar[Dict[str, str]] = {"messages": APPEND}

    #: The thread — a checkpoint lineage, stable across every turn.
    id: str = ""
    #: The conversation so far, as ``{"role", "content"}`` pairs. Appends.
    messages: List[Dict[str, Any]] = Field(default_factory=list)
    #: What the caller said THIS turn. A convenience: the same content is the
    #: last entry of `messages`, and a condition reading it should not have to
    #: index into a list.
    last_user_message: str = ""
    #: What the CHAT router classified this turn as, when it classified one.
    last_intent: str = ""
    #: Which OUTCOME of this flow the turn selected. Separate from
    #: ``last_intent`` on purpose: one is the chat deciding which capability to
    #: run, the other is this flow deciding what to produce. Collapsing them
    #: would repeat the router/selection confusion one level down.
    last_outcome: str = ""
    #: One-time bootstrap marker, so a flow can do setup on turn 1 only.
    session_ready: bool = False


def message(role: str, content: Any) -> Dict[str, Any]:
    """One conversation entry, in the shape the channel holds."""
    return {"role": role, "content": "" if content is None else str(content)}


def turn_inputs(
    user_message: Optional[str], intent: Optional[str] = None
) -> Dict[str, Any]:
    """The state writes that OPEN a turn.

    Returned as inputs rather than applied directly because that is the path a
    flow already has: ``kickoff_async(inputs=...)`` restores the thread, then
    merges these through their reducers. So the user line appends to the
    restored history in the one place merging happens, instead of a second
    code path that has to remember to.

    Empty when there is no user line — a flow can be threaded without being
    conversational, and a blank turn must not append an empty message.
    """
    if not user_message:
        return {}
    writes: Dict[str, Any] = {
        "messages": [message(USER, user_message)],
        "last_user_message": str(user_message),
    }
    if intent:
        writes["last_intent"] = str(intent)
    return writes


def append_assistant_message(state: Any, content: Any) -> None:
    """Record what the flow answered, so the next turn can see it.

    Deliberately explicit rather than automatic. A flow's last method output is
    not always the reply — a router returns a route name, a state operation
    returns nothing — and a runtime that guessed would fill the history with
    control-flow noise.
    """
    if content is None or not hasattr(state, "merge"):
        return
    state.merge({"messages": [message(ASSISTANT, content)]})


def _reply_text(result: Any) -> str:
    """The answer a turn produced, as text.

    Mirrors what the A2UI runner does with a crew result: unwrap the raw text,
    then a structured model, then fall back to ``str``. A pydantic result
    stringified by ``str()`` gives its repr, which is not something to put in a
    conversation the next turn will read back.
    """
    if result is None:
        return ""
    if isinstance(result, str):
        return result
    raw = getattr(result, "raw", None)
    if isinstance(raw, str) and raw.strip():
        return raw
    dump = getattr(result, "model_dump_json", None)
    if callable(dump):
        try:
            return dump()
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"[flow-thread] could not serialize turn result: {exc}")
    return str(result)


async def close_turn_async(
    state: Any, result: Any, model: Optional[str] = None
) -> None:
    """Finish a turn, folding the older history rather than dropping it.

    ``close_turn`` truncates: past the cap the oldest messages are simply gone,
    so a long conversation forgets what it was asked to do first. Folding keeps
    the facts and still bounds the size, using the summarizer chat already has.

    Falls back to truncation when the flow declares no ``summary`` channel or
    the summarizer is unavailable — a bounded history is the requirement; how it
    is bounded is the improvement.
    """
    close_turn(state, result)
    try:
        from src.services.flow_builder.conversation.lifecycle import (
            fold_thread_history,
        )

        await fold_thread_history(state, model)
    except Exception as exc:  # noqa: BLE001 — never fail a turn for bookkeeping
        logger.debug(f"[flow-thread] could not fold history: {exc}")


def close_turn(state: Any, result: Any, limit: int = MAX_THREAD_MESSAGES) -> None:
    """Finish a turn: make sure it left an answer, then bound the history.

    The answer is recorded only when the flow did not record one itself. That
    rule is what keeps ``append_assistant_message`` explicit while still
    guaranteeing a conversation never has two user lines in a row — a flow whose
    last method is a router would otherwise contribute the route NAME as its
    reply, and a flow that appended a proper answer would get it duplicated.
    """
    messages = getattr(state, "messages", None)
    if isinstance(messages, list) and (
        not messages or messages[-1].get("role") == USER
    ):
        append_assistant_message(state, _reply_text(result))
    trim_messages(state, limit)


def trim_messages(state: Any, limit: int = MAX_THREAD_MESSAGES) -> None:
    """Keep the newest ``limit`` messages on the state.

    Applied after a turn completes, so the checkpoint written for the NEXT turn
    is already bounded. The turn that just ran keeps the full history it
    reasoned over.
    """
    messages = getattr(state, "messages", None)
    if not isinstance(messages, list) or len(messages) <= limit:
        return
    dropped = len(messages) - limit
    state.messages = messages[-limit:]
    logger.info(
        "[flow-thread] trimmed %d message(s) from thread %s (cap %d)",
        dropped,
        getattr(state, "id", "?"),
        limit,
    )


def is_conversational(state_config: Any) -> bool:
    """Whether a flow's state config asks for the turn contract."""
    if not isinstance(state_config, dict):
        return False
    return bool(state_config.get("conversational"))
