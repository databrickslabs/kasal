"""The recent conversation, as the capability router needs to see it.

The router used to decide from ONE sentence and a list of descriptions. That is
enough for "kick off the Q3 risk review for DACH" and wrong for everything a
conversation actually does:

* "what is this Aviation sector" — a question ABOUT the answer on screen. Judged
  alone it looks like a news request, matches a news capability, and bills a full
  crew run to be told about aviation.
* "now do the same for Germany" — meaningless alone, unambiguous with the
  previous turn in view.
* "turn this into a deck" — wants a DIFFERENT capability, chosen from what
  "this" refers to.

Note what those three need: not a follow-up/new-request classifier, which would
force the second and third into the same box as the first. They need the router
to SEE the conversation, so its one existing decision — which capability, if any
— is made with the information a person would have.

Deliberately not the light agent's ``_chat_history_block``. That builds a
weighted transcript for ANSWERING (keep every user fact, truncate assistant
prose hard, fold old turns into a compaction summary), and it is a private method
bound to a run config and a logger. The router is doing something else: it needs
a SHORT window, and it needs assistant turns to be individually addressable — a
run that refers to an earlier answer has to say WHICH, the same way an extracted
value has to quote its span.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

#: Turns handed to the router. Small on purpose: this is context for ONE
#: decision, not the material for an answer, and every turn is prompt cost on
#: the routing call.
RECENT_TURN_LIMIT = 8

#: Per-turn character cap in the rendered context. An assistant turn is often a
#: whole deck; the router needs to know WHAT it was about, not to read it.
TURN_CHAR_CAP = 400

#: Placeholder rows the chat writes while a run is in flight. They carry no
#: meaning and would crowd out real turns.
_PLACEHOLDERS = {"thinking...", "[ui-card]", ""}

#: Chat furniture: rows that are announcements ABOUT a run rather than anything
#: said in the conversation. A routed run posts "Running **Gather News**", and
#: each task posts its agent's name in bold. Both are assistant rows, both would
#: take an [answer N] slot, and neither is something a follow-up can refer to —
#: so a window of eight turns could be five labels and one real answer.
_RUN_ANNOUNCEMENT = re.compile(r"^Running \*\*[^*]+\*\*\.?$")
_BOLD_LABEL_ONLY = re.compile(r"^\*\*[^*]+\*\*\.?$")


def _is_chat_furniture(content: str) -> bool:
    return bool(_RUN_ANNOUNCEMENT.match(content) or _BOLD_LABEL_ONLY.match(content))


@dataclass
class Turn:
    """One prior message, addressable by the router."""

    #: 1-based position in the rendered window. This is what the router quotes
    #: when it says a request refers to an earlier answer.
    index: int
    role: str  # "user" | "assistant"
    #: Truncated for the prompt.
    preview: str
    #: The turn in full, for binding into a run. Never sent to the router.
    content: str


async def recent_turns(
    session: Any,
    session_id: Optional[str],
    group_ids: List[str],
    limit: int = RECENT_TURN_LIMIT,
    exclude_message: Optional[str] = None,
) -> List[Turn]:
    """The last few real turns of this chat session, oldest→newest.

    Best-effort and read-only: any failure returns ``[]`` and the router simply
    decides as it did before. Routing must never break because history is
    unavailable.

    ``exclude_message`` is the message being routed right now, dropped if it is
    already persisted so the router cannot treat the question as its own
    antecedent.

    It is matched by CONTENT, not by position, and that is the whole point.
    Routing happens mid-turn: ``chat_history`` is written when the turn ends, so
    the current user row usually does not exist yet. The light agent's
    ``_conversation_preamble`` can drop "everything from the last user row
    onward" because it runs INSIDE the run, after that row is written. Copying
    that rule here dropped the PREVIOUS exchange instead — the answer on screen
    that a follow-up is asking about — so the router saw a conversation with its
    most recent turn missing and read the follow-up as a fresh request.
    """
    if not session_id or not group_ids:
        return []

    try:
        from src.repositories.chat_history_repository import ChatHistoryRepository

        rows = await ChatHistoryRepository(session).get_recent_by_session_and_group(
            session_id, group_ids, limit=limit * 3
        )
    except Exception as exc:  # noqa: BLE001 — context is an enhancement, not a gate
        logger.debug("[capability_router] conversation context unavailable: %s", exc)
        return []

    current = " ".join((exclude_message or "").split()).lower()
    prior = list(rows)
    # Only when it is genuinely the trailing row: an identical question asked
    # earlier in the session is real history and must stay.
    while prior and current:
        last = prior[-1]
        if getattr(last, "message_type", "") != "user":
            break
        if " ".join((getattr(last, "content", "") or "").split()).lower() != current:
            break
        prior.pop()

    kept: List[Turn] = []
    for row in prior:
        role = getattr(row, "message_type", "")
        if role not in ("user", "assistant"):
            continue
        content = (getattr(row, "content", "") or "").strip()
        if content.lower() in _PLACEHOLDERS or content.startswith("[ui-card]"):
            continue
        if role == "assistant" and _is_chat_furniture(content):
            continue
        kept.append(Turn(index=0, role=role, preview="", content=content))

    kept = kept[-limit:]
    # Numbered AFTER the window is chosen, so an index the router quotes always
    # matches what it was shown.
    return [
        Turn(
            index=position,
            role=turn.role,
            preview=_cap(turn.content),
            content=turn.content,
        )
        for position, turn in enumerate(kept, start=1)
    ]


def _cap(text: str) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= TURN_CHAR_CAP:
        return collapsed
    return collapsed[:TURN_CHAR_CAP] + "…"


def render_turns(turns: List[Turn]) -> str:
    """The conversation as the router reads it, or '' when there is none.

    Assistant turns carry their number so the router can point at one. User
    turns do not: a request never "refers to" an earlier question, only to an
    earlier answer.
    """
    if not turns:
        return ""
    lines = []
    for turn in turns:
        if turn.role == "assistant":
            lines.append(f"[answer {turn.index}] Assistant: {turn.preview}")
        else:
            lines.append(f"User: {turn.preview}")
    return "\n".join(lines)


def turn_by_index(turns: List[Turn], index: Any) -> Optional[Turn]:
    """The assistant turn the router pointed at, or None.

    Only assistant turns resolve. A router that names a user turn has not found
    something to work from — it has found the question again.
    """
    try:
        wanted = int(index)
    except (TypeError, ValueError):
        return None
    for turn in turns:
        if turn.index == wanted and turn.role == "assistant":
            return turn
    return None
