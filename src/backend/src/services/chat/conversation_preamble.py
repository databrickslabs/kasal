"""The light agent's transcript of THIS chat session.

Each light-agent turn is an isolated kickoff with no built-in history; this
module builds the "conversation so far" block that is prepended to the kickoff
prompt so the agent can recall what was said (the user's name, an earlier
instruction, the answer on screen). :func:`build_conversation_preamble` owns the
scoring — which turns survive a long, bloated session and how far each is cut —
and the rationale lives on its docstring. ``context_compaction`` produces the
summary block it folds in for turns older than the fold marker.

Read-only and best-effort: any failure yields ``""`` and the turn still answers.
"""

import logging
import os
from typing import Any, Callable, Optional

from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


async def build_conversation_preamble(
    config: Any,
    group_context: Optional[GroupContext],
    group_id: str,
    log: Callable[[str], None],
) -> str:
    """Recent turns of THIS chat session as a short transcript, weighted so
    the user's own statements survive a long/bloated conversation.

    Each light-agent turn is an isolated ``kickoff_async`` with no built-in
    conversation history, so without this the assistant cannot recall what was
    said earlier (e.g. the user's name). Read-only and best-effort: returns
    ``""`` on any issue. The current turn (the just-written user row + its
    ``Thinking...`` / ``[ui-card]`` placeholder rows) is excluded.

    Scoring (why this beats a flat "last N turns" window): a long chat is
    dominated by large ASSISTANT outputs (decks, reports) that would otherwise
    push the short, high-signal USER facts out of the window. So USER turns are
    prioritized — every user turn is kept (capped per-turn), and only the most
    recent assistant turns are kept and hard-truncated. Under the overall
    character budget the OLDEST assistant turns are dropped first; user turns
    are never dropped. The header also tells the model to treat the user's
    statements as authoritative.

    One exception to the truncation: the MOST RECENT answer. It is what the
    next turn refers to — "as a table", "shorter", the same question asked
    again — and a 240-char stub of it left the agent unable to restate a report
    it could not see, so it went and researched it again. That answer is kept
    whole under its own cap (``CHAT_HISTORY_LAST_ANSWER_CHAR_CAP``, 12k chars,
    about 3k tokens) and sits outside the budget. Older answers stay stubs: a
    whole transcript of decks and reports would overflow the model window and
    slow every turn, and the compaction summary already carries what they said.
    """
    session_id = getattr(config, "session_id", None)
    if not session_id:
        return ""
    group_ids = list(getattr(group_context, "group_ids", None) or [])
    if not group_ids and group_id and group_id != "default":
        group_ids = [group_id]
    if not group_ids:
        return ""

    # Tunables (env-overridable). Defaults favor keeping user facts.
    recent_limit = int(os.getenv("CHAT_HISTORY_RECENT_LIMIT", "120"))
    user_cap = int(os.getenv("CHAT_HISTORY_USER_CHAR_CAP", "500"))
    assistant_cap = int(os.getenv("CHAT_HISTORY_ASSISTANT_CHAR_CAP", "240"))
    last_answer_cap = int(os.getenv("CHAT_HISTORY_LAST_ANSWER_CHAR_CAP", "12000"))
    max_assistant_turns = int(os.getenv("CHAT_HISTORY_MAX_ASSISTANT_TURNS", "8"))
    max_chars = int(os.getenv("CHAT_HISTORY_MAX_CHARS", "6000"))

    context_summary = None
    summary_upto = None
    try:
        from src.db.session import routed_scoped_session
        from src.repositories.chat_history_repository import (
            ChatHistoryRepository,
        )
        from src.repositories.chat_session_repository import (
            ChatSessionRepository,
        )

        async with routed_scoped_session() as db_session:
            # MOST RECENT window (not the oldest page) — a session longer than
            # one page must still recall what was just said.
            messages = await ChatHistoryRepository(
                db_session
            ).get_recent_by_session_and_group(session_id, group_ids, limit=recent_limit)
            # Running compaction summary: turns at or before summary_upto are
            # represented by the summary block, not injected verbatim.
            #
            # Its own try: this is an ENHANCEMENT of the transcript, not a
            # precondition for it. Sharing the outer handler meant any failure
            # here — a missing session row, a schema drift, a transient error —
            # threw away the history that had already been fetched and returned
            # an empty preamble, so the agent silently lost all cross-turn
            # recall (it would not know the user's name) with only a debug line
            # to say why.
            try:
                session_record = await ChatSessionRepository(
                    db_session
                ).get_by_id_and_group(session_id, group_ids)
                if session_record is not None:
                    context_summary = getattr(session_record, "context_summary", None)
                    summary_upto = getattr(session_record, "context_summary_upto", None)
            except Exception as summary_err:  # noqa: BLE001
                logger.debug(
                    f"[light_agent] compaction summary unavailable, using the "
                    f"raw transcript: {summary_err}"
                )
    except Exception as hist_err:  # noqa: BLE001
        logger.debug(f"[light_agent] chat history fetch skipped: {hist_err}")
        return ""

    if summary_upto is not None:
        messages = [
            m
            for m in messages
            if getattr(m, "timestamp", None) is None or m.timestamp > summary_upto
        ]

    # Drop the current turn: everything from the LAST 'user' row onward is
    # this run (that user message + its placeholder assistant rows).
    last_user = -1
    for i, m in enumerate(messages):
        if getattr(m, "message_type", "") == "user":
            last_user = i
    prior = messages[:last_user] if last_user >= 0 else list(messages)

    placeholders = {"thinking...", "[ui-card]", ""}
    # Real turns, in order: user/assistant rows minus placeholders and cards.
    real = []
    for m in prior:
        mtype = getattr(m, "message_type", "")
        if mtype not in ("user", "assistant"):
            continue
        content = (getattr(m, "content", "") or "").strip()
        if content.lower() in placeholders or content.startswith("[ui-card]"):
            continue
        real.append((mtype, content))
    if not real:
        return ""
    # The MOST RECENT answer is pinned: whole (under its own cap), never
    # dropped by the budget — it is what the next turn refers to.
    last_answer_at = max(
        (i for i, (mtype, _) in enumerate(real) if mtype == "assistant"), default=-1
    )

    # Build (role, "User: ..."/"Assistant: ..." line, pinned), chronological.
    entries: list = []
    for i, (mtype, content) in enumerate(real):
        pinned = i == last_answer_at
        if mtype == "user":
            cap = user_cap
        else:
            cap = last_answer_cap if pinned else assistant_cap
        if len(content) > cap:
            content = content[:cap] + "…"
        label = "User" if mtype == "user" else "Assistant"
        entries.append((mtype, f"{label}: {content}", pinned))

    # Keep ALL user turns; keep only the most recent N assistant turns.
    assistant_positions = [
        i for i, (role, _, _) in enumerate(entries) if role == "assistant"
    ]
    keep_assistant = set(assistant_positions[-max_assistant_turns:])
    selected = [
        (role, line, pinned)
        for i, (role, line, pinned) in enumerate(entries)
        if role == "user" or i in keep_assistant
    ]

    # Enforce the character budget by dropping the OLDEST assistant turns
    # first; user turns are never dropped (they carry the facts to recall).
    # The pinned last answer sits outside the budget and is never dropped.
    def _total(items) -> int:
        return sum(len(line) + 1 for _, line, pinned in items if not pinned)

    def _first(items, want_assistant: bool) -> Optional[int]:
        for i, (role, _, pinned) in enumerate(items):
            if not pinned and (role == "assistant" or not want_assistant):
                return i
        return None

    while selected and _total(selected) > max_chars:
        drop_at = _first(selected, want_assistant=True)
        if drop_at is None:
            # Only user turns remain. This used to be an unbounded growth
            # path (user turns were never dropped, so hundred-question
            # sessions eventually overflowed the model window and the run
            # died). Now the OLDEST user turns go too — their facts live on
            # in the compaction summary — under a hard 1.5x budget ceiling.
            if _total(selected) <= int(max_chars * 1.5):
                break
            drop_at = _first(selected, want_assistant=False)
            if drop_at is None:
                break
        selected.pop(drop_at)

    lines = [line for _, line, _ in selected]
    user_count = sum(1 for role, _, _ in selected if role == "user")
    log(
        f"Recalling {len(lines)} prior message(s) from this chat session "
        f"({user_count} from you)"
        + (" + earlier-conversation summary" if context_summary else "")
    )
    from src.services.chat.context_compaction import (
        SUMMARY_HEADER,
    )

    summary_block = (
        f"{SUMMARY_HEADER}\n{context_summary.strip()}\n\n" if context_summary else ""
    )
    return (
        summary_block
        + "Conversation so far in THIS chat session (most recent last). The "
        "User's statements below are authoritative facts about the user and "
        "their request — rely on them directly when answering (e.g. the user's "
        "name, preferences, and earlier instructions):\n" + "\n".join(lines)
    )
