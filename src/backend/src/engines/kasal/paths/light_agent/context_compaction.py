"""Chat context compaction — long sessions must never die of window overflow.

Two cooperating pieces:

1. ``maintain_session_summary`` (post-answer, fire-and-forget): when the
   un-summarized history grows past a threshold, fold the turns OLDER than the
   verbatim window into a running per-session summary (one cheap LLM call,
   incremental — it updates the previous summary with only the newly-dropped
   turns) persisted on ``chat_sessions.context_summary`` with a fold-marker
   timestamp. Never blocks or fails a run.

2. ``_conversation_preamble`` (in light_agent_service) consumes it: injects
   the summary block + only the turns NEWER than the fold marker verbatim,
   under a hard character budget that can now drop even old user turns —
   their facts live on in the summary, so the preamble finally has a true
   upper bound.

Kill-switch: ``CHAT_COMPACTION=false`` disables summarization (the hard
budget in the preamble still applies — that alone prevents the overflow
drops, at the cost of forgetting instead of summarizing).
"""

import logging
import os
from typing import Any, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

SUMMARY_HEADER = (
    "Summary of the EARLIER part of this chat session (older turns compacted; "
    "treat these as established facts):"
)

_SUMMARIZE_INSTRUCTIONS = (
    "You maintain a running memory summary of a chat session. Update the "
    "existing summary with the new conversation turns. Preserve, verbatim "
    "where short: the user's name and personal facts, decisions made, "
    "preferences, constraints, open questions, and anything the user asked "
    "to remember. Drop pleasantries and superseded details. Write compact "
    "prose or bullets, max {max_chars} characters. Return ONLY the updated "
    "summary text."
)


def compaction_enabled() -> bool:
    return os.getenv("CHAT_COMPACTION", "true").strip().lower() not in (
        "0", "false", "no",
    )


def keep_recent_rows() -> int:
    """How many newest chat_history rows stay OUT of the summary (verbatim)."""
    return int(os.getenv("CHAT_COMPACTION_KEEP_ROWS", "24"))


def trigger_chars() -> int:
    """Fold when the to-be-folded turns exceed this many characters."""
    return int(os.getenv("CHAT_COMPACTION_TRIGGER_CHARS", "8000"))


def summary_max_chars() -> int:
    return int(os.getenv("CHAT_SUMMARY_MAX_CHARS", "2000"))


def transcript_of(rows: Sequence[Any], per_turn_cap: int = 700) -> str:
    """Plain-text transcript of chat_history rows (user/assistant only)."""
    lines: List[str] = []
    for row in rows:
        mtype = getattr(row, "message_type", "")
        if mtype not in ("user", "assistant"):
            continue
        content = (getattr(row, "content", "") or "").strip()
        if not content or content.lower() == "thinking..." or content.startswith("[ui-card]"):
            continue
        if len(content) > per_turn_cap:
            content = content[:per_turn_cap] + "…"
        lines.append(f"{'User' if mtype == 'user' else 'Assistant'}: {content}")
    return "\n".join(lines)


def split_for_compaction(
    rows: Sequence[Any],
    summary_upto,
    keep_rows: int,
) -> Tuple[List[Any], List[Any]]:
    """(to_fold, verbatim): rows newer than the fold marker, split so the
    newest ``keep_rows`` stay verbatim and the older remainder is foldable."""
    unsummarized = [
        row for row in rows
        if summary_upto is None or (getattr(row, "timestamp", None) and row.timestamp > summary_upto)
    ]
    if len(unsummarized) <= keep_rows:
        return [], unsummarized
    return unsummarized[:-keep_rows], unsummarized[-keep_rows:]


def build_summary_prompt(existing_summary: Optional[str], fold_transcript: str) -> list:
    system = _SUMMARIZE_INSTRUCTIONS.format(max_chars=summary_max_chars())
    user = (
        (f"Existing summary:\n{existing_summary}\n\n" if existing_summary else "")
        + f"New turns to fold in:\n{fold_transcript}"
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


async def maintain_session_summary(
    session_id: str,
    group_ids: List[str],
    model_name: Optional[str],
) -> bool:
    """Fold old turns into the session summary when due. Returns True if a
    new summary was persisted. Best-effort by design — any failure is logged
    and swallowed (the hard preamble budget is the safety net)."""
    if not compaction_enabled() or not session_id or not group_ids:
        return False
    try:
        from src.core.llm_manager import LLMManager
        from src.db.session import request_scoped_session
        from src.repositories.chat_history_repository import ChatHistoryRepository
        from src.repositories.chat_session_repository import ChatSessionRepository

        async with request_scoped_session() as db_session:
            session_record = await ChatSessionRepository(
                db_session
            ).get_by_id_and_group(session_id, group_ids)
            if session_record is None:
                return False
            rows = await ChatHistoryRepository(
                db_session
            ).get_recent_by_session_and_group(
                session_id, group_ids,
                limit=int(os.getenv("CHAT_HISTORY_RECENT_LIMIT", "120")),
            )
            existing_summary = getattr(session_record, "context_summary", None)
            summary_upto = getattr(session_record, "context_summary_upto", None)

        to_fold, _verbatim = split_for_compaction(rows, summary_upto, keep_recent_rows())
        fold_transcript = transcript_of(to_fold)
        if len(fold_transcript) < trigger_chars():
            return False

        model = os.getenv("CHAT_COMPACTION_MODEL") or model_name
        if not model:
            return False
        new_summary = await LLMManager.completion(
            messages=build_summary_prompt(existing_summary, fold_transcript),
            model=model,
            temperature=0.2,
            max_tokens=800,
        )
        new_summary = (new_summary or "").strip()
        if not new_summary:
            return False
        if len(new_summary) > summary_max_chars():
            new_summary = new_summary[: summary_max_chars()] + "…"

        new_upto = getattr(to_fold[-1], "timestamp", None)
        if new_upto is None:
            return False
        async with request_scoped_session() as db_session:
            await ChatSessionRepository(db_session).set_context_summary(
                session_id, group_ids, new_summary, new_upto
            )
            await db_session.commit()
        logger.info(
            f"[compaction] session {session_id}: folded {len(to_fold)} rows "
            f"into summary ({len(new_summary)} chars)"
        )
        # Same event the tool-loop trim emits, so BOTH kinds of compaction show
        # up as one row type in the trace instead of only the chat log knowing.
        try:
            from kasal_engine.events.bus import crewai_event_bus
            from kasal_engine.events.types import ContextCompactionEvent

            crewai_event_bus.emit(
                None,
                ContextCompactionEvent(
                    model=model,
                    strategy="chat_history_summary",
                    tokens_before=len(fold_transcript) // 4,
                    tokens_after=len(new_summary) // 4,
                    messages_compacted=len(to_fold),
                    reason=(
                        f"folded {len(to_fold)} earlier chat turn(s) "
                        f"({len(fold_transcript)} chars) into a running summary "
                        f"({len(new_summary)} chars)"
                    ),
                ),
            )
        except Exception:  # noqa: BLE001 — observability must never fail a fold
            pass
        return True
    except Exception as compact_err:  # noqa: BLE001
        logger.warning(f"[compaction] session {session_id} skipped: {compact_err}")
        return False
